use clap::Args;
use thiserror::Error;
use url::Url;

/// S3-compatible object-store settings shared by all Walstream commands.
#[derive(Clone, Debug, Args)]
pub struct S3Settings {
    /// Bucket containing all Walstream data.
    #[arg(long, env = "WALSTREAM_S3_BUCKET")]
    pub bucket: String,

    /// AWS region used to sign requests.
    #[arg(long, env = "WALSTREAM_S3_REGION", default_value = "us-east-1")]
    pub region: String,

    /// Optional S3-compatible endpoint.
    #[arg(long, env = "WALSTREAM_S3_ENDPOINT")]
    pub endpoint: Option<String>,

    /// Permit a plaintext HTTP endpoint. Intended only for local development.
    #[arg(long, env = "WALSTREAM_S3_ALLOW_HTTP", default_value_t = false)]
    pub allow_http: bool,

    /// Prefix inside the bucket. Must contain safe path components only.
    #[arg(long, env = "WALSTREAM_S3_PREFIX", default_value = "walstream")]
    pub prefix: String,

    /// Stable cluster identifier within the prefix.
    #[arg(long, env = "WALSTREAM_CLUSTER_ID", default_value = "default")]
    pub cluster_id: String,
}

impl S3Settings {
    /// Validate settings before any network request is attempted.
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_path_component("bucket", &self.bucket)?;
        validate_path_component("cluster id", &self.cluster_id)?;
        validate_prefix(&self.prefix)?;

        if self.region.trim().is_empty() {
            return Err(ConfigError::EmptyRegion);
        }

        if let Some(endpoint) = &self.endpoint {
            let endpoint =
                Url::parse(endpoint).map_err(|source| ConfigError::InvalidEndpoint { source })?;
            match endpoint.scheme() {
                "https" => {}
                "http" if self.allow_http => {}
                "http" => return Err(ConfigError::HttpRequiresOptIn),
                scheme => {
                    return Err(ConfigError::UnsupportedEndpointScheme {
                        scheme: scheme.to_owned(),
                    });
                }
            }
        }

        Ok(())
    }

    /// Object key prefix for this logical cluster.
    pub fn cluster_prefix(&self) -> String {
        format!(
            "{}/clusters/{}",
            self.prefix.trim_matches('/'),
            self.cluster_id
        )
    }
}

/// Configuration validation failure.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{field} must be a non-empty safe path component")]
    InvalidPathComponent { field: &'static str },
    #[error("object-store prefix must contain only non-empty safe path components")]
    InvalidPrefix,
    #[error("S3 region must not be empty")]
    EmptyRegion,
    #[error("invalid S3 endpoint URL")]
    InvalidEndpoint { source: url::ParseError },
    #[error("plaintext HTTP S3 endpoints require --allow-http")]
    HttpRequiresOptIn,
    #[error("unsupported S3 endpoint scheme {scheme:?}; use https or explicitly allowed http")]
    UnsupportedEndpointScheme { scheme: String },
}

fn validate_path_component(field: &'static str, value: &str) -> Result<(), ConfigError> {
    let valid = !value.is_empty()
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    valid
        .then_some(())
        .ok_or(ConfigError::InvalidPathComponent { field })
}

fn validate_prefix(prefix: &str) -> Result<(), ConfigError> {
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty()
        || trimmed
            .split('/')
            .any(|component| validate_path_component("prefix", component).is_err())
    {
        return Err(ConfigError::InvalidPrefix);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> S3Settings {
        S3Settings {
            bucket: "walstream-test".into(),
            region: "us-east-1".into(),
            endpoint: None,
            allow_http: false,
            prefix: "walstream".into(),
            cluster_id: "test".into(),
        }
    }

    #[test]
    fn rejects_plaintext_endpoint_without_opt_in() {
        let mut value = settings();
        value.endpoint = Some("http://127.0.0.1:9000".into());
        assert!(matches!(
            value.validate(),
            Err(ConfigError::HttpRequiresOptIn)
        ));
        value.allow_http = true;
        value.validate().unwrap();
    }

    #[test]
    fn rejects_unsafe_cluster_and_prefix_paths() {
        let mut value = settings();
        value.cluster_id = "../other".into();
        assert!(matches!(
            value.validate(),
            Err(ConfigError::InvalidPathComponent {
                field: "cluster id"
            })
        ));

        value.cluster_id = "test".into();
        value.prefix = "walstream/../other".into();
        assert!(matches!(value.validate(), Err(ConfigError::InvalidPrefix)));
    }
}
