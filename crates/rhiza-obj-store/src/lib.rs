use std::{fmt, fs, path::PathBuf, sync::Arc};

use bytes::Bytes;
use futures::{lock::Mutex, TryStreamExt};
use object_store::{
    aws::{AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut},
    azure::MicrosoftAzureBuilder,
    gcp::GoogleCloudStorageBuilder,
    local::LocalFileSystem,
    path::Path as ObjPath,
    ObjectStore, ObjectStoreExt, PutMode, RetryConfig,
};
use serde::{Deserialize, Serialize};

pub use object_store::UpdateVersion;

pub type Result<T> = std::result::Result<T, Error>;

fn gcs_control_retry_config() -> RetryConfig {
    RetryConfig {
        // The archive owns the bounded retry budget for its shared GC-control
        // CAS. Retrying that one operation again in the SDK could hide a
        // request for the SDK's default three-minute window.
        max_retries: 0,
        ..RetryConfig::default()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    AlreadyExists {
        key: String,
    },
    Precondition {
        key: String,
    },
    NotFound {
        key: String,
    },
    MissingVersion {
        key: String,
    },
    ReadLimitExceeded {
        key: String,
        limit: u64,
        actual: u64,
    },
    ContentLengthMismatch {
        key: String,
        expected: u64,
        actual: u64,
    },
    Configuration(String),
    Transport {
        key: String,
        message: String,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists { key } => write!(f, "object already exists: {key}"),
            Self::Precondition { key } => write!(f, "object precondition failed: {key}"),
            Self::NotFound { key } => write!(f, "object not found: {key}"),
            Self::MissingVersion { key } => {
                write!(f, "object has no safe version identity: {key}")
            }
            Self::ReadLimitExceeded { key, limit, actual } => write!(
                f,
                "object read exceeds limit for {key}: limit {limit} bytes, got at least {actual} bytes"
            ),
            Self::ContentLengthMismatch {
                key,
                expected,
                actual,
            } => write!(
                f,
                "object content length mismatch for {key}: metadata declared {expected} bytes, stream yielded {actual} bytes"
            ),
            Self::Configuration(message) => {
                write!(f, "object store configuration failed: {message}")
            }
            Self::Transport { key, message } => {
                write!(f, "object store operation failed for {key}: {message}")
            }
        }
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Eq, PartialEq)]
pub enum ObjStoreConfig {
    Local {
        root: PathBuf,
    },
    S3 {
        endpoint: Option<String>,
        bucket: String,
        access_key: Option<String>,
        secret_key: Option<String>,
        region: String,
        allow_http: bool,
    },
    Gcs {
        bucket: String,
        service_account_path: Option<String>,
        service_account_key: Option<String>,
    },
    AzureBlob {
        account: String,
        container: String,
        access_key: Option<String>,
    },
}

impl fmt::Debug for ObjStoreConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local { root } => formatter.debug_struct("Local").field("root", root).finish(),
            Self::S3 {
                endpoint,
                bucket,
                region,
                allow_http,
                ..
            } => formatter
                .debug_struct("S3")
                .field("endpoint", endpoint)
                .field("bucket", bucket)
                .field("access_key", &"[redacted]")
                .field("secret_key", &"[redacted]")
                .field("region", region)
                .field("allow_http", allow_http)
                .finish(),
            Self::Gcs {
                bucket,
                service_account_path,
                ..
            } => formatter
                .debug_struct("Gcs")
                .field("bucket", bucket)
                .field("service_account_path", service_account_path)
                .field("service_account_key", &"[redacted]")
                .finish(),
            Self::AzureBlob {
                account, container, ..
            } => formatter
                .debug_struct("AzureBlob")
                .field("account", account)
                .field("container", container)
                .field("access_key", &"[redacted]")
                .finish(),
        }
    }
}

#[derive(Clone)]
pub struct ObjStore {
    inner: Arc<dyn ObjectStore>,
    conditional_once_inner: Option<Arc<dyn ObjectStore>>,
    local_update_lock: Option<Arc<Mutex<()>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionedObject {
    bytes: Vec<u8>,
    version: UpdateVersion,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectVersion {
    e_tag: Option<String>,
    version: Option<String>,
}

impl ObjectVersion {
    pub const fn e_tag(&self) -> Option<&String> {
        self.e_tag.as_ref()
    }

    pub const fn version(&self) -> Option<&String> {
        self.version.as_ref()
    }
}

impl From<UpdateVersion> for ObjectVersion {
    fn from(value: UpdateVersion) -> Self {
        Self {
            e_tag: value.e_tag,
            version: value.version,
        }
    }
}

impl From<ObjectVersion> for UpdateVersion {
    fn from(value: ObjectVersion) -> Self {
        Self {
            e_tag: value.e_tag,
            version: value.version,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectMetadata {
    key: String,
    size_bytes: u64,
    last_modified_ms: u64,
    version: ObjectVersion,
}

impl ObjectMetadata {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub const fn last_modified_ms(&self) -> u64 {
        self.last_modified_ms
    }

    pub const fn version(&self) -> &ObjectVersion {
        &self.version
    }
}

impl VersionedObject {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub const fn version(&self) -> &UpdateVersion {
        &self.version
    }
}

fn build_gcs_store(
    bucket: &str,
    service_account_path: Option<&str>,
    service_account_key: Option<&str>,
    retry: Option<RetryConfig>,
) -> Result<object_store::gcp::GoogleCloudStorage> {
    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);
    if let Some(retry) = retry {
        builder = builder.with_retry(retry);
    }
    if let Some(path) = service_account_path {
        builder = builder.with_service_account_path(path);
    }
    if let Some(key) = service_account_key {
        builder = builder.with_service_account_key(key);
    }
    builder
        .build()
        .map_err(|err| Error::Configuration(err.to_string()))
}

impl ObjStore {
    pub fn new(config: ObjStoreConfig) -> Result<Self> {
        let (inner, conditional_once_inner, local_update_lock): (
            Arc<dyn ObjectStore>,
            Option<Arc<dyn ObjectStore>>,
            _,
        ) = match config {
            ObjStoreConfig::Local { root } => {
                fs::create_dir_all(&root).map_err(|err| Error::Configuration(err.to_string()))?;
                let store = LocalFileSystem::new_with_prefix(root)
                    .map_err(|err| Error::Configuration(err.to_string()))?;
                (Arc::new(store), None, Some(Arc::new(Mutex::new(()))))
            }
            ObjStoreConfig::S3 {
                endpoint,
                bucket,
                access_key,
                secret_key,
                region,
                allow_http,
            } => {
                validate_optional("S3 endpoint", endpoint.as_deref())?;
                validate_required("S3 bucket", &bucket)?;
                validate_optional("S3 access key", access_key.as_deref())?;
                validate_optional("S3 secret key", secret_key.as_deref())?;
                validate_required("S3 region", &region)?;
                let mut builder = match (access_key, secret_key) {
                    (Some(access_key), Some(secret_key)) => AmazonS3Builder::new()
                        .with_access_key_id(access_key)
                        .with_secret_access_key(secret_key),
                    (None, None) => AmazonS3Builder::from_env(),
                    _ => {
                        return Err(Error::Configuration(
                            "S3 access key and secret key must be provided together".to_string(),
                        ));
                    }
                };
                if let Some(endpoint) = endpoint {
                    builder = builder
                        .with_config(AmazonS3ConfigKey::S3Endpoint, endpoint)
                        .with_virtual_hosted_style_request(false);
                }
                let store = builder
                    .with_bucket_name(bucket)
                    .with_region(region)
                    .with_allow_http(allow_http)
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    .build()
                    .map_err(|err| Error::Configuration(err.to_string()))?;
                (Arc::new(store), None, None)
            }
            ObjStoreConfig::Gcs {
                bucket,
                service_account_path,
                service_account_key,
            } => {
                validate_required("GCS bucket", &bucket)?;
                validate_optional("GCS service account path", service_account_path.as_deref())?;
                validate_optional("GCS service account key", service_account_key.as_deref())?;
                if service_account_path.is_some() && service_account_key.is_some() {
                    return Err(Error::Configuration(
                        "GCS service account path and key are mutually exclusive".to_string(),
                    ));
                }

                let store = build_gcs_store(
                    &bucket,
                    service_account_path.as_deref(),
                    service_account_key.as_deref(),
                    None,
                )?;
                let conditional_once_store = build_gcs_store(
                    &bucket,
                    service_account_path.as_deref(),
                    service_account_key.as_deref(),
                    Some(gcs_control_retry_config()),
                )?;
                (
                    Arc::new(store),
                    Some(Arc::new(conditional_once_store)),
                    None,
                )
            }
            ObjStoreConfig::AzureBlob {
                account,
                container,
                access_key,
            } => {
                validate_required("Azure storage account", &account)?;
                validate_required("Azure Blob container", &container)?;
                validate_optional("Azure storage access key", access_key.as_deref())?;

                let mut builder = MicrosoftAzureBuilder::new()
                    .with_account(account)
                    .with_container_name(container);
                if let Some(key) = access_key {
                    builder = builder.with_access_key(key);
                }
                let store = builder
                    .build()
                    .map_err(|err| Error::Configuration(err.to_string()))?;
                (Arc::new(store), None, None)
            }
        };

        Ok(Self {
            inner,
            conditional_once_inner,
            local_update_lock,
        })
    }

    pub fn supports_strong_cross_process_cas(&self) -> bool {
        self.local_update_lock.is_none()
    }

    pub async fn put(&self, key: &str, bytes: impl AsRef<[u8]>) -> Result<()> {
        self.put_with_mode(key, bytes.as_ref(), PutMode::Overwrite)
            .await?;
        Ok(())
    }

    pub async fn create(&self, key: &str, bytes: impl AsRef<[u8]>) -> Result<UpdateVersion> {
        let bytes = bytes.as_ref();
        match self.put_with_mode(key, bytes, PutMode::Create).await {
            Ok(version) => Ok(version),
            Err(error @ Error::AlreadyExists { .. }) => match self.get_with_version(key).await {
                Ok(existing) if existing.bytes() == bytes => Ok(existing.version().clone()),
                Ok(_) | Err(Error::NotFound { .. }) => Err(error),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        }
    }

    /// Performs exactly one provider request for a conditional create when a
    /// backend-specific one-shot client is configured. The caller owns any
    /// retry and idempotency policy.
    pub async fn create_conditional_once(
        &self,
        key: &str,
        bytes: impl AsRef<[u8]>,
    ) -> Result<UpdateVersion> {
        let store = self.conditional_once_inner.as_ref().unwrap_or(&self.inner);
        self.put_with_store_mode(store, key, bytes.as_ref(), PutMode::Create)
            .await
    }

    pub async fn update(
        &self,
        key: &str,
        bytes: impl AsRef<[u8]>,
        version: UpdateVersion,
    ) -> Result<UpdateVersion> {
        self.put_with_mode(key, bytes.as_ref(), PutMode::Update(version))
            .await
    }

    /// Performs exactly one provider request for a conditional update when a
    /// backend-specific one-shot client is configured. Normal object I/O keeps
    /// the provider SDK's retry policy. This is intended for callers that own
    /// a separate, bounded CAS retry loop.
    pub async fn update_conditional_once(
        &self,
        key: &str,
        bytes: impl AsRef<[u8]>,
        version: UpdateVersion,
    ) -> Result<UpdateVersion> {
        let store = self.conditional_once_inner.as_ref().unwrap_or(&self.inner);
        self.put_with_store_mode(store, key, bytes.as_ref(), PutMode::Update(version))
            .await
    }

    pub async fn get(&self, key: &str) -> Result<Vec<u8>> {
        Ok(self.get_with_version(key).await?.bytes)
    }

    pub async fn get_with_version(&self, key: &str) -> Result<VersionedObject> {
        self.get_with_version_bounded(key, u64::MAX).await
    }

    /// Reads one object without allowing provider metadata or streamed bytes
    /// to exceed the caller's mandatory cap.
    pub async fn get_bounded(&self, key: &str, max_bytes: u64) -> Result<Vec<u8>> {
        Ok(self.get_with_version_bounded(key, max_bytes).await?.bytes)
    }

    /// Reads one versioned object without allowing provider metadata or
    /// streamed bytes to exceed the caller's mandatory cap.
    pub async fn get_with_version_bounded(
        &self,
        key: &str,
        max_bytes: u64,
    ) -> Result<VersionedObject> {
        let result = self
            .inner
            .get(&ObjPath::from(key))
            .await
            .map_err(|err| map_store_error(key, err))?;
        let declared_size = result.meta.size;
        if declared_size > max_bytes {
            return Err(Error::ReadLimitExceeded {
                key: key.to_string(),
                limit: max_bytes,
                actual: declared_size,
            });
        }
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let bytes = collect_bounded_stream(key, result.into_stream(), max_bytes).await?;
        let actual_size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        validate_content_length(key, declared_size, actual_size)?;
        Ok(VersionedObject { bytes, version })
    }

    pub async fn get_versioned(&self, key: &str) -> Result<VersionedObject> {
        self.get_with_version(key).await
    }

    pub async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .list_metadata(prefix)
            .await?
            .into_iter()
            .map(|meta| meta.key)
            .collect())
    }

    pub async fn list_metadata(&self, prefix: &str) -> Result<Vec<ObjectMetadata>> {
        let objects = self
            .inner
            .list(Some(&ObjPath::from(prefix)))
            .try_collect::<Vec<_>>()
            .await
            .map_err(|err| map_store_error(prefix, err))?;
        let mut metadata = objects
            .into_iter()
            .map(|meta| ObjectMetadata {
                key: meta.location.to_string(),
                size_bytes: meta.size,
                last_modified_ms: u64::try_from(meta.last_modified.timestamp_millis()).unwrap_or(0),
                version: ObjectVersion {
                    e_tag: meta.e_tag,
                    version: meta.version,
                },
            })
            .collect::<Vec<_>>();
        if let Some(object) = metadata
            .iter()
            .find(|object| object.version.e_tag.is_none() && object.version.version.is_none())
        {
            return Err(Error::MissingVersion {
                key: object.key.clone(),
            });
        }
        metadata.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(metadata)
    }

    /// Deletes only the listed immutable object version. Callers must fence writers because
    /// object_store does not expose provider-native conditional DELETE across all backends.
    pub async fn delete_exact(&self, key: &str, expected: &ObjectVersion) -> Result<bool> {
        let _guard = match &self.local_update_lock {
            Some(lock) => Some(lock.lock().await),
            None => None,
        };
        let path = ObjPath::from(key);
        let actual = match self.inner.head(&path).await {
            Ok(meta) => ObjectVersion {
                e_tag: meta.e_tag,
                version: meta.version,
            },
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(error) => return Err(map_store_error(key, error)),
        };
        if &actual != expected {
            return Err(Error::Precondition {
                key: key.to_string(),
            });
        }
        match self.inner.delete(&path).await {
            Ok(()) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(map_store_error(key, error)),
        }
    }

    async fn put_with_mode(&self, key: &str, bytes: &[u8], mode: PutMode) -> Result<UpdateVersion> {
        self.put_with_store_mode(&self.inner, key, bytes, mode)
            .await
    }

    async fn put_with_store_mode(
        &self,
        store: &Arc<dyn ObjectStore>,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<UpdateVersion> {
        let path = ObjPath::from(key);
        let payload: object_store::PutPayload = Bytes::copy_from_slice(bytes).into();

        if let (PutMode::Update(expected), Some(lock)) = (&mode, &self.local_update_lock) {
            let _guard = lock.lock().await;
            let native = store
                .put_opts(&path, payload.clone(), mode.clone().into())
                .await;
            let result = match native {
                Err(object_store::Error::NotImplemented { .. }) => {
                    let meta = match store.head(&path).await {
                        Ok(meta) => meta,
                        Err(object_store::Error::NotFound { .. }) => {
                            return Err(Error::Precondition {
                                key: key.to_string(),
                            });
                        }
                        Err(err) => return Err(map_store_error(key, err)),
                    };
                    let actual = UpdateVersion {
                        e_tag: meta.e_tag,
                        version: meta.version,
                    };
                    if &actual != expected {
                        return Err(Error::Precondition {
                            key: key.to_string(),
                        });
                    }
                    store
                        .put_opts(&path, payload, PutMode::Overwrite.into())
                        .await
                }
                result => result,
            };
            return result
                .map(UpdateVersion::from)
                .map_err(|err| map_store_error(key, err));
        }

        store
            .put_opts(&path, payload, mode.into())
            .await
            .map(UpdateVersion::from)
            .map_err(|err| map_store_error(key, err))
    }
}

async fn collect_bounded_stream(
    key: &str,
    mut stream: futures::stream::BoxStream<'static, object_store::Result<Bytes>>,
    max_bytes: u64,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut actual = 0_u64;
    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|error| map_store_error(key, error))?
    {
        let chunk_len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        actual = actual
            .checked_add(chunk_len)
            .ok_or_else(|| Error::ReadLimitExceeded {
                key: key.to_string(),
                limit: max_bytes,
                actual: u64::MAX,
            })?;
        if actual > max_bytes {
            return Err(Error::ReadLimitExceeded {
                key: key.to_string(),
                limit: max_bytes,
                actual,
            });
        }
        bytes
            .try_reserve(chunk.len())
            .map_err(|error| Error::Transport {
                key: key.to_string(),
                message: format!("object read allocation failed: {error}"),
            })?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn validate_content_length(key: &str, expected: u64, actual: u64) -> Result<()> {
    if actual != expected {
        return Err(Error::ContentLengthMismatch {
            key: key.to_string(),
            expected,
            actual,
        });
    }
    Ok(())
}

fn validate_required(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::Configuration(format!("{name} must not be empty")));
    }
    Ok(())
}

fn validate_optional(name: &str, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        validate_required(name, value)?;
    }
    Ok(())
}

fn map_store_error(key: &str, error: object_store::Error) -> Error {
    match error {
        object_store::Error::AlreadyExists { .. } => Error::AlreadyExists {
            key: key.to_string(),
        },
        object_store::Error::Precondition { .. } | object_store::Error::NotModified { .. } => {
            Error::Precondition {
                key: key.to_string(),
            }
        }
        object_store::Error::NotFound { .. } => Error::NotFound {
            key: key.to_string(),
        },
        error => Error::Transport {
            key: key.to_string(),
            message: error.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[test]
    fn only_gcs_control_cas_disables_sdk_retries() {
        let normal = RetryConfig::default();
        let control = gcs_control_retry_config();
        assert!(normal.max_retries > 0);
        assert_eq!(control.max_retries, 0);
        assert_eq!(control.retry_timeout, normal.retry_timeout);
    }

    #[tokio::test]
    async fn bounded_get_accepts_exact_boundary_and_rejects_metadata_before_read() {
        let directory = tempfile::tempdir().unwrap();
        let store = ObjStore::new(ObjStoreConfig::Local {
            root: directory.path().to_path_buf(),
        })
        .unwrap();
        store.put("bounded", b"four").await.unwrap();

        assert_eq!(store.get_bounded("bounded", 4).await.unwrap(), b"four");
        assert_eq!(
            store.get_bounded("bounded", 3).await.unwrap_err(),
            Error::ReadLimitExceeded {
                key: "bounded".into(),
                limit: 3,
                actual: 4,
            }
        );
    }

    #[tokio::test]
    async fn bounded_stream_rejects_a_dishonest_chunk_without_accumulating_it() {
        let stream = futures::stream::iter([
            Ok(Bytes::from_static(b"four")),
            Ok(Bytes::from_static(b"dishonest")),
        ])
        .boxed();

        assert_eq!(
            collect_bounded_stream("dishonest", stream, 4)
                .await
                .unwrap_err(),
            Error::ReadLimitExceeded {
                key: "dishonest".into(),
                limit: 4,
                actual: 13,
            }
        );
    }

    #[test]
    fn content_length_mismatch_is_typed_with_path_and_exact_sizes() {
        assert_eq!(
            validate_content_length("dishonest", 5, 4).unwrap_err(),
            Error::ContentLengthMismatch {
                key: "dishonest".into(),
                expected: 5,
                actual: 4,
            }
        );
        validate_content_length("boundary", 4, 4).unwrap();
    }
}
