use async_trait::async_trait;
use futures_util::StreamExt;
use object_store::{
    CopyMode, CopyOptions, Error, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
    buffered::BufWriter, gcp::GoogleCloudStorageBuilder, path::Path as ObjectPath,
};
use std::{io, path::Path, sync::Arc};
use tokio::io::{AsyncWriteExt, BufReader};
use uuid::Uuid;

use super::{Blob, BlobStore, PutOutcome};
use crate::{config::Config, model::Digest};

/// A Google Cloud Storage-backed content-addressed blob store.
pub struct GcsStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl GcsStore {
    pub async fn new(config: &Config) -> anyhow::Result<Self> {
        let bucket = config
            .gcs_bucket
            .clone()
            .ok_or_else(|| anyhow::anyhow!("--gcs-bucket is required with GCS storage"))?;
        // Without a service account key, the client falls back to application
        // default credentials and then the metadata server (GKE Workload Identity).
        let builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket);
        Ok(Self::from_store(
            Arc::new(builder.build()?),
            &config.gcs_prefix,
        ))
    }

    fn from_store(store: Arc<dyn ObjectStore>, prefix: &str) -> Self {
        Self {
            store,
            prefix: prefix.trim_matches('/').to_owned(),
        }
    }

    fn key(&self, digest: &Digest) -> ObjectPath {
        ObjectPath::from(format!("{}/blobs/{}", self.prefix, digest.key()))
    }

    fn upload_key(&self) -> ObjectPath {
        ObjectPath::from(format!("{}/uploads/{}", self.prefix, Uuid::new_v4()))
    }
}

#[async_trait]
impl BlobStore for GcsStore {
    async fn get(&self, digest: &Digest) -> anyhow::Result<Option<Blob>> {
        let result = match self.store.get(&self.key(digest)).await {
            Ok(result) => result,
            Err(Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let size = result.meta.size;
        let stream = result
            .into_stream()
            .map(|result| result.map_err(io::Error::other))
            .boxed();
        Ok(Some(Blob { size, stream }))
    }

    async fn put(&self, digest: &Digest, source: &Path) -> anyhow::Result<PutOutcome> {
        let destination_key = self.key(digest);
        if digest.size == 0 {
            let result = self
                .store
                .put_opts(
                    &destination_key,
                    Vec::new().into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await;
            return match result {
                Ok(_) => Ok(PutOutcome::Created),
                Err(Error::AlreadyExists { .. } | Error::Precondition { .. }) => {
                    Ok(PutOutcome::AlreadyExists)
                }
                Err(error) => Err(error.into()),
            };
        }

        // A multipart upload cannot be conditional, so stage it under a unique
        // key and publish it with a create-only copy.
        let upload_key = self.upload_key();
        let file = tokio::fs::File::open(source).await?;
        let mut reader = BufReader::new(file);
        let mut writer = BufWriter::new(Arc::clone(&self.store), upload_key.clone());

        if let Err(error) = tokio::io::copy(&mut reader, &mut writer).await {
            let _ = writer.abort().await;
            return Err(error.into());
        }
        if let Err(error) = writer.shutdown().await {
            let _ = self.store.delete(&upload_key).await;
            return Err(error.into());
        }

        // GCS copies within a bucket complete before the response, so the
        // staged object can go at once.
        let result = self
            .store
            .copy_opts(
                &upload_key,
                &destination_key,
                CopyOptions {
                    mode: CopyMode::Create,
                    ..Default::default()
                },
            )
            .await;
        let _ = self.store.delete(&upload_key).await;
        match result {
            Ok(_) => Ok(PutOutcome::Created),
            Err(Error::AlreadyExists { .. } | Error::Precondition { .. }) => {
                Ok(PutOutcome::AlreadyExists)
            }
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Algorithm;
    use futures_util::TryStreamExt;
    use object_store::memory::InMemory;
    use sha2::{Digest as _, Sha256};

    #[tokio::test]
    async fn streams_blobs_and_does_not_replace_existing_content() {
        exercise(
            GcsStore::from_store(Arc::new(InMemory::new()), "/v1/"),
            true,
        )
        .await;
    }

    /// Set `MBX_CACHE_TEST_FAKE_GCS` to a fake-gcs-server URL with a `cache` bucket.
    /// The emulator ignores `x-goog-if-generation-match`, so create-only
    /// publication is checked against the in-memory store only.
    #[tokio::test]
    async fn round_trips_with_fake_gcs_when_configured() {
        let Some(url) = std::env::var_os("MBX_CACHE_TEST_FAKE_GCS") else {
            return;
        };
        let key = tempfile::NamedTempFile::new().unwrap();
        let service_account = serde_json::json!({
            "gcs_base_url": url.to_string_lossy(),
            "disable_oauth": true,
            "client_email": "",
            "private_key": "",
            "private_key_id": "",
        });
        std::fs::write(key.path(), service_account.to_string()).unwrap();
        let backend = GoogleCloudStorageBuilder::new()
            .with_service_account_path(key.path().to_string_lossy())
            .with_bucket_name("cache")
            .build()
            .unwrap();
        let store =
            GcsStore::from_store(Arc::new(backend), &format!("tests/{}/v1", Uuid::new_v4()));
        exercise(store, false).await;
    }

    async fn exercise(store: GcsStore, enforces_preconditions: bool) {
        let source = tempfile::NamedTempFile::new().unwrap();
        tokio::fs::write(source.path(), b"gcs blob!!")
            .await
            .unwrap();
        let digest = Digest {
            algorithm: Algorithm::Sha256.into(),
            hash: hex::encode(Sha256::digest(b"gcs blob!!")),
            size: 10,
        };

        assert!(store.get(&digest).await.unwrap().is_none());
        assert_eq!(
            store.put(&digest, source.path()).await.unwrap(),
            PutOutcome::Created
        );
        let blob = store.get(&digest).await.unwrap().unwrap();
        assert_eq!(blob.size, 10);
        assert_eq!(
            blob.stream.try_collect::<Vec<_>>().await.unwrap().concat(),
            b"gcs blob!!"[..]
        );

        if enforces_preconditions {
            tokio::fs::write(source.path(), b"different").await.unwrap();
            assert_eq!(
                store.put(&digest, source.path()).await.unwrap(),
                PutOutcome::AlreadyExists
            );
            assert_eq!(store.get(&digest).await.unwrap().unwrap().size, 10);
        }

        tokio::fs::write(source.path(), b"").await.unwrap();
        let empty_digest = Digest {
            algorithm: Algorithm::Sha256.into(),
            hash: hex::encode(Sha256::digest(b"")),
            size: 0,
        };
        assert_eq!(
            store.put(&empty_digest, source.path()).await.unwrap(),
            PutOutcome::Created
        );
        assert_eq!(store.get(&empty_digest).await.unwrap().unwrap().size, 0);
        if enforces_preconditions {
            assert_eq!(
                store.put(&empty_digest, source.path()).await.unwrap(),
                PutOutcome::AlreadyExists
            );
        }
    }
}
