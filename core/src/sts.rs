use aws_config::BehaviorVersion;
use aws_config::sts::AssumeRoleProvider;
use aws_sdk_dynamodb::Client as DynamoClient;

/// Builds a DynamoDB client, assuming an STS role if an ARN is provided.
/// Replaces the legacy boto3.Session and boto3.client("sts") logic.
pub async fn build_ddb_client(
    role_arn: Option<String>,
    region: Option<String>,
    session_name: &str,
    external_id: Option<String>,
    _duration_seconds: Option<u64>,
) -> Result<DynamoClient, Box<dyn std::error::Error>> {
    
    // Loads the base environment configuration
    let mut config_loader = aws_config::defaults(BehaviorVersion::latest());
    
    if let Some(ref reg) = region {
        config_loader = config_loader.region(aws_config::Region::new(reg.clone()));
    }
    let base_config = config_loader.load().await;

    // Returns the standard client if no Role ARN is provided
    let role_arn = match role_arn {
        Some(arn) => arn,
        None => return Ok(DynamoClient::new(&base_config)),
    };

    // Configures the STS AssumeRole provider
    let mut provider_builder = AssumeRoleProvider::builder(role_arn)
        .session_name(session_name)
        .configure(&base_config); 

    if let Some(ext_id) = external_id {
        provider_builder = provider_builder.external_id(ext_id);
    }
    
    // Note: The Rust SDK assumes the default session duration and refreshes 
    // the token automatically in the background. The manual duration is ignored.
    let provider = provider_builder.build().await;

    // Injects the temporary credentials into a new configuration
    let assumed_config = aws_config::defaults(BehaviorVersion::latest())
        .credentials_provider(provider)
        .region(base_config.region().cloned())
        .load()
        .await;

    Ok(DynamoClient::new(&assumed_config))
}
