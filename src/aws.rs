use aws_runtime::env_config;
use aws_types;
use aws_config::BehaviorVersion;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::sts::AssumeRoleProvider;
use aws_sdk_glue::config::SharedCredentialsProvider;
use aws_types::region::Region;
use log::info;
use crate::aws;

const DEFAULT_PROFILE: &str = "default";
const DEFAULT_REGION: &str = "eu-central-1";

/// Get the AWS profile name
pub fn profile() -> String {
    std::env::var("AWS_PROFILE").unwrap_or(DEFAULT_PROFILE.to_string())
}

/// List all AWS profiles names
pub async fn list_profiles() -> crate::Result<Vec<String>> {
    let fs = aws_types::os_shim_internal::Fs::real();
    let env = aws_types::os_shim_internal::Env::real();
    let profile_files = env_config::file::EnvConfigFiles::default();
    let profiles_set = aws_config::profile::load(&fs, &env, &profile_files, None).await?;
    Ok(profiles_set.profiles().map(|profile| profile.to_string()).collect())
}

/// Get the AWS region identifier
pub fn region() -> String {
    std::env::var("AWS_DEFAULT_REGION").unwrap_or(DEFAULT_REGION.to_string())
}

///
/// Creates a basic credentials provider with the provided profile and region or defaults if no
/// role arn is provided. If a role arn is provided, the credentials provider will assume the role.
///
/// Returns the region as well which is sometimes needed for AWS SDK clients.
///
pub async fn create_credentials_provider(profile: Option<String>, region: Option<String>, role_arn: Option<String>) -> (SharedCredentialsProvider, String, Region) {
    let profile = profile.unwrap_or(aws::profile());
    let region = region.unwrap_or(aws::region());
    let region = Region::new(region);

    info!("Using AWS profile {profile} in region {region}.");

    match role_arn {
        Some(role_arn) => {
            let aws_config = aws_config::defaults(BehaviorVersion::latest())
                .profile_name(profile.clone())
                .region(region.clone())
                .load()
                .await;

            info!("Preparing STS credentials provider assuming role {role_arn}.");

            let assume_role_provider = AssumeRoleProvider::builder(role_arn.clone())
                .configure(&aws_config)
                .session_name("kieker")
                .build()
                .await;

            info!("Using STS credentials provider assuming role {role_arn}.");

            (SharedCredentialsProvider::new(assume_role_provider), profile, region)
        }
        None => {
            info!("Using default AWS credentials provider for {profile} in {region}.");
            let default_provider = DefaultCredentialsChain::builder()
                .profile_name(&profile)
                .region(region.clone())
                .build()
                .await;
            (SharedCredentialsProvider::new(default_provider), profile, region)
        }
    }
}
