use crate::aws::list_profiles;
use crate::context::KiekContext;
use crate::exception::KiekException;
use crate::feedback::Feedback;
use crate::highlight::Highlighting;
use crate::CoreResult;
use aws_credential_types::provider::error::CredentialsError::ProviderError;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_msk_iam_sasl_signer::generate_auth_token_from_credentials_provider;
use aws_types::region::Region;
use log::{error, info};
use rdkafka::client::OAuthToken;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::ConsumerContext;
use rdkafka::ClientContext;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::time::timeout;

const AUTH_TOKEN_TIMEOUT: Duration = Duration::from_secs(5);

///
/// This client & consumer context generates OAuth tokens for the Kafka cluster based on an AWS
/// credentials provider.
///
#[derive(Clone)]
pub(crate) struct IamContext {
    credentials_provider: SharedCredentialsProvider,
    profile: String,
    region: Region,
    feedback: Feedback,
    rt: Handle,
    last_fail: Arc<Mutex<Option<String>>>,
}

impl IamContext {
    pub(crate) fn new(
        credentials_provider: SharedCredentialsProvider,
        profile: String,
        region: Region,
        feedback: &Feedback,
    ) -> Self {
        let feedback = feedback.clone();
        Self {
            credentials_provider,
            profile,
            region,
            feedback,
            rt: Handle::current(),
            last_fail: Arc::new(Mutex::new(None)),
        }
    }
}

impl ConsumerContext for IamContext {}

impl ClientContext for IamContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.capturing_log(level, fac, log_message);
    }

    ///
    /// Use generate_auth_token_from_credentials_provider from the aws_msk_iam_sasl_signer crate to
    /// generate an OAuth token from the AWS credentials provider.
    ///
    fn generate_oauth_token(&self, _oauthbearer_config: Option<&str>) -> CoreResult<OAuthToken> {
        let region = self.region.clone();
        let credentials_provider = self.credentials_provider.clone();
        let handle = self.rt.clone();
        self.feedback
            .info("Authorizing", "using AWS IAM auth token");
        let (token, expiration_time_ms) = {
            let handle = thread::spawn(move || {
                handle.block_on(async {
                    timeout(
                        AUTH_TOKEN_TIMEOUT,
                        generate_auth_token_from_credentials_provider(region, credentials_provider),
                    )
                    .await
                    .map(|r| match r {
                        Ok(token) => {
                            info!("Generated OAuth token: {} {}", token.0, token.1);
                            Ok(token)
                        }
                        Err(err) => {
                            error!("Failed to generate OAuth token: {}", err);
                            Err(err)
                        }
                    })
                })
            });
            handle.join().unwrap()??
        };
        Ok(OAuthToken {
            token,
            principal_name: "".to_string(),
            lifetime_ms: expiration_time_ms,
        })
    }
}

impl KiekContext for IamContext {
    ///
    /// Verifies that the AWS credentials provider works fine, so that it can generate an OAuth token
    /// later.
    /// If credentials are not provided, tries to find out why and gives a hint to the user.
    ///
    async fn verify(&self, highlighting: &Highlighting) -> crate::Result<()> {
        match self.credentials_provider.provide_credentials().await {
            Err(ProviderError(e)) if format!("{e:?}").contains("Session token not found or invalid") => {
                Err(KiekException::new(format!("Session token not found or invalid. Run {bold}aws sso login --profile {profile}{bold:#} to refresh your session.", bold = highlighting.bold, profile = self.profile)))
            }
            Err(ProviderError(e)) if format!("{e:?}").contains("is not authorized to perform: sts:AssumeRole") => {
                Err(KiekException::new(format!("Assuming the passed role failed. Check if profile {profile} has the rights.", profile = self.profile)))
            }
            Err(_) if !list_profiles().await.unwrap_or(vec![self.profile.clone()]).contains(&self.profile) => {
                Err(KiekException::new(format!("AWS profile {profile} does not exist.", profile = self.profile)))
            }
            Err(e) => {
                error!("Failed to provide AWS credentials: {e:?}");
                Err(KiekException::from(e))
            }
            Ok(_) => {
                info!("AWS credentials provided successfully");
                Ok(())
            }
        }
    }

    fn last_fail(&self) -> Arc<Mutex<Option<String>>> {
        self.last_fail.clone()
    }
}
