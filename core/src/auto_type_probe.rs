use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use std::collections::HashMap;

/// Represents the result of an automatic type inference probe.
#[derive(Debug)]
pub struct AutoTypeResult {
    pub ddb_type: Option<String>,
    pub samples_seen: usize,
    pub type_counts: HashMap<String, usize>,
}

/// Probes a DynamoDB table to infer the data type of a specific attribute.
/// It performs a limited scan using `attribute_exists` to find a representative sample.
pub async fn probe_attribute_type(
    client: &Client,
    table_name: &str,
    attribute_name: &str,
    index_name: Option<String>,
    sample_size: i32,
    consistent_read: bool,
) -> Result<AutoTypeResult, Box<dyn std::error::Error>> {
    // Enforces a minimum sample size of 20 if an invalid number is provided
    let actual_sample_size = if sample_size <= 0 { 20 } else { sample_size };

    let mut request = client
        .scan()
        .table_name(table_name)
        .select(aws_sdk_dynamodb::types::Select::SpecificAttributes)
        .projection_expression("#a")
        .expression_attribute_names("#a", attribute_name)
        .filter_expression("attribute_exists(#a)")
        .limit(actual_sample_size)
        .consistent_read(consistent_read);

    if let Some(idx) = index_name {
        request = request.index_name(idx);
    }

    let response = request.send().await?;
    let items = response.items.unwrap_or_default();
    
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut seen = 0;

    for item in items {
        if let Some(attr_val) = item.get(attribute_name) {
            // Maps the AWS SDK AttributeValue enum to its string representation
            let type_str = match attr_val {
                AttributeValue::S(_) => "S",
                AttributeValue::N(_) => "N",
                AttributeValue::Bool(_) => "BOOL",
                AttributeValue::B(_) => "B",
                AttributeValue::Ss(_) => "SS",
                AttributeValue::Ns(_) => "NS",
                AttributeValue::Bs(_) => "BS",
                AttributeValue::M(_) => "M",
                AttributeValue::L(_) => "L",
                AttributeValue::Null(_) => "NULL",
                _ => "UNKNOWN",
            };
            
            // Increments the counter for the detected type
            *counts.entry(type_str.to_string()).or_insert(0) += 1;
            seen += 1;
        }
    }

    // Finds the most common type among the retrieved samples
    let most_common = counts
        .iter()
        .max_by_key(|entry| entry.1)
        .map(|(k, _)| k.clone());

    Ok(AutoTypeResult {
        ddb_type: most_common,
        samples_seen: seen,
        type_counts: counts,
    })
}

/// Builds an AttributeValue based on the inferred DynamoDB type.
/// Safely converts raw string inputs into their respective AWS types.
pub fn build_attribute_value(ddb_type: &str, raw_value: &str) -> Result<AttributeValue, String> {
    match ddb_type {
        "S" => Ok(AttributeValue::S(raw_value.to_string())),
        "N" => Ok(AttributeValue::N(raw_value.to_string())),
        "BOOL" => {
            let v = raw_value.trim().to_lowercase();
            let b = matches!(v.as_str(), "1" | "true" | "t" | "yes" | "y");
            Ok(AttributeValue::Bool(b))
        }
        "SS" => {
            let items: Vec<String> = raw_value
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            Ok(AttributeValue::Ss(items))
        }
        "NS" => {
            let items: Vec<String> = raw_value
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            Ok(AttributeValue::Ns(items))
        }
        _ => Err(format!("Unsupported DynamoDB type for automatic filters: {}", ddb_type)),
    }
}
