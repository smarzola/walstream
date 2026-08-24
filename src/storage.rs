use std::sync::Arc;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use object_store::{
    Error as StoreError, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion,
    aws::{AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut},
    path::Path,
};
use uuid::Uuid;

use crate::config::S3Settings;

/// Build the configured S3-compatible store.
pub fn build_s3_store(settings: &S3Settings) -> Result<Arc<dyn ObjectStore>> {
    settings.validate()?;

    let mut builder = AmazonS3Builder::from_env()
        .with_bucket_name(&settings.bucket)
        .with_region(&settings.region)
        .with_virtual_hosted_style_request(false)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_allow_http(settings.allow_http);

    if let Some(endpoint) = &settings.endpoint {
        // The S3-specific slot takes precedence over both AWS_ENDPOINT_URL
        // and AWS_ENDPOINT_URL_S3 loaded by `from_env`. An explicit Walstream
        // CLI/environment value must never be silently redirected by ambient
        // SDK configuration.
        builder = builder.with_config(AmazonS3ConfigKey::S3Endpoint, endpoint);
    }

    Ok(Arc::new(builder.build()?))
}

/// Prove that a store provides the conditional writes required for manifests.
///
/// The probe uses a unique object under the configured cluster prefix and
/// removes it before returning. A stale update must fail with a precondition
/// error; mere connectivity is not sufficient.
pub async fn verify_store_contract(store: &dyn ObjectStore, prefix: &str) -> Result<()> {
    let path = Path::from(format!("{prefix}/_verify/{}.probe", Uuid::new_v4()));
    let result = async {
        let created = store
            .put_opts(
                &path,
                Bytes::from_static(b"created").into(),
                PutMode::Create.into(),
            )
            .await
            .context("conditional create probe failed")?;
        let stale = UpdateVersion::from(created);

        match store
            .put_opts(
                &path,
                Bytes::from_static(b"duplicate").into(),
                PutMode::Create.into(),
            )
            .await
        {
            Err(StoreError::AlreadyExists { .. } | StoreError::Precondition { .. }) => {}
            Err(error) => return Err(error).context("duplicate create probe failed unexpectedly"),
            Ok(_) => bail!("object store accepted a duplicate conditional create"),
        }

        let current = store.get(&path).await.context("probe read failed")?;
        let current = UpdateVersion {
            e_tag: current.meta.e_tag,
            version: current.meta.version,
        };
        store
            .put_opts(
                &path,
                Bytes::from_static(b"updated").into(),
                PutMode::Update(current).into(),
            )
            .await
            .context("conditional update probe failed")?;

        match store
            .put_opts(
                &path,
                Bytes::from_static(b"stale").into(),
                PutMode::Update(stale).into(),
            )
            .await
        {
            Err(StoreError::Precondition { .. }) => Ok(()),
            Err(error) => Err(error).context("stale update probe failed unexpectedly"),
            Ok(_) => bail!("object store accepted a stale conditional update"),
        }
    }
    .await;

    let cleanup = store.delete(&path).await;
    match result {
        Err(error) => Err(error),
        Ok(()) => cleanup.context("conditional-write probe cleanup failed"),
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn verifies_create_and_stale_update_preconditions() {
        let store = InMemory::new();
        verify_store_contract(&store, "walstream/clusters/test")
            .await
            .unwrap();
        assert!(store.list(None).next().await.is_none());
    }
}
