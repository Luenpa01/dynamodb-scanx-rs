use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{self, Write};

// Imports your custom core library
use dynamodb_scanx_core::paginator::ParallelScanPaginator;
use dynamodb_scanx_core::sts::build_ddb_client;
use aws_sdk_dynamodb::types::AttributeValue;

/// Blazing fast, concurrent DynamoDB parallel scanner & SRE toolkit.
#[derive(Parser, Debug)]
#[command(name = "dpscan")]
#[command(author = "Luis Parada <pradapop78@gmail.com>")]
#[command(version = "0.1.0")]
#[command(about = "Fast parallel scan for DynamoDB with STS AssumeRole and tuned connection pooling.", long_about = None)]
pub struct Args {
    #[arg(long, required = true, help_heading = "DynamoDB Options")]
    pub table_name: String,

    #[arg(long, default_value_t = 32, help_heading = "DynamoDB Options")]
    pub total_segments: i32,

    /// AWS Region. Defaults to us-east-1 (N. Virginia).
    #[arg(long, default_value = "us-east-1", help_heading = "AWS Credentials")]
    pub region: String,

    #[arg(long, help_heading = "AWS Credentials")]
    pub role_arn: Option<String>,

    #[arg(long, help_heading = "AWS Credentials")]
    pub external_id: Option<String>,

    #[arg(long, default_value = "dpscan-session", help_heading = "AWS Credentials")]
    pub role_session_name: String,

    #[arg(short, long, help_heading = "Performance")]
    pub workers: Option<usize>,

    #[arg(long, default_value_t = 10, help_heading = "Performance")]
    pub retries_max_attempts: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // 1. Initialize the STS Client
    let client = build_ddb_client(
        args.role_arn,
        Some(args.region),
        &args.role_session_name,
        args.external_id,
        None,
    ).await?;

    // 2. Setup the Progress Bar
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {msg} {pos} items scanned")?
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );
    pb.set_message("Scanning DynamoDB...");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    // 3. Launch the Asynchronous Paginator
    let paginator = ParallelScanPaginator::new(client, args.workers, args.retries_max_attempts);
    let mut rx = paginator.paginate(args.table_name.clone(), args.total_segments).await;

    let mut total_items = 0;
    let stdout = io::stdout();
    let mut handle = stdout.lock();

    // 4. Consume the MPSC Channel and stream to JSONL
    while let Some(result) = rx.recv().await {
        match result {
            Ok(output) => {
                if let Some(items) = output.items {
                    for item in items {
                        total_items += 1;
                        pb.set_position(total_items);

                        // Converts DynamoDB AttributeValues to standard JSON
                        let json_val = parse_item(&item);
                        
                        // Writes to standard output instantly (streaming)
                        // We use pb.suspend to avoid corrupting the progress bar in the terminal
                        pb.suspend(|| {
                            writeln!(handle, "{}", json_val.to_string()).unwrap();
                        });
                    }
                }
            }
            Err(e) => {
                pb.suspend(|| {
                    eprintln!("Error during scan: {}", e);
                });
            }
        }
    }

    pb.finish_with_message("✅ Scan complete!");
    Ok(())
}

/// Converts a DynamoDB item (HashMap of AttributeValues) into a serde_json::Value.
fn parse_item(item: &HashMap<String, AttributeValue>) -> Value {
    let mut map = serde_json::Map::new();
    for (k, v) in item {
        map.insert(k.clone(), parse_attr(v));
    }
    Value::Object(map)
}

/// Recursively parses DynamoDB AttributeValues into standard JSON primitives.
fn parse_attr(attr: &AttributeValue) -> Value {
    match attr {
        AttributeValue::S(s) => Value::String(s.clone()),
        AttributeValue::N(n) => {
            if let Ok(i) = n.parse::<i64>() { json!(i) }
            else if let Ok(f) = n.parse::<f64>() { json!(f) }
            else { Value::String(n.clone()) }
        },
        AttributeValue::Bool(b) => Value::Bool(*b),
        AttributeValue::M(m) => parse_item(m),
        AttributeValue::L(l) => Value::Array(l.iter().map(parse_attr).collect()),
        AttributeValue::Ss(ss) => Value::Array(ss.iter().map(|s| Value::String(s.clone())).collect()),
        AttributeValue::Ns(ns) => Value::Array(ns.iter().map(|n| {
            if let Ok(i) = n.parse::<i64>() { json!(i) }
            else if let Ok(f) = n.parse::<f64>() { json!(f) }
            else { Value::String(n.clone()) }
        }).collect()),
        AttributeValue::Null(_) => Value::Null,
        _ => Value::Null, // Safely ignores binary types for standard JSON output
    }
}
