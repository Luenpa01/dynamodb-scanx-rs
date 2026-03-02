use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};

use dynamodb_scanx_core::paginator::ParallelScanPaginator;
use dynamodb_scanx_core::sts::build_ddb_client;
use dynamodb_scanx_core::auto_type_probe::{probe_attribute_type, build_attribute_value};
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

    #[arg(short, long, help_heading = "Output")]
    pub output: Option<String>,

    #[arg(short = 'f', long = "filter-field", help_heading = "Filters")]
    pub filter_fields: Vec<String>,

    #[arg(short = 'v', long = "filter-value", help_heading = "Filters")]
    pub filter_values: Vec<String>,

    #[arg(long, default_value = "AND", help_heading = "Filters")]
    pub filter_logic: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let client = build_ddb_client(
        args.role_arn,
        Some(args.region),
        &args.role_session_name,
        args.external_id,
        None,
    ).await?;

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {msg} {pos} items scanned")?
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    let mut filter_expr = None;
    let mut eav = None;
    let mut ean = None;

    if !args.filter_fields.is_empty() {
        if args.filter_fields.len() != args.filter_values.len() {
            eprintln!("❌ Error: The number of fields (-f) must match the number of values (-v).");
            std::process::exit(1);
        }

        let mut fe_parts = Vec::new();
        let mut values_map = HashMap::new();
        let mut names_map = HashMap::new();

        for (i, (field, val)) in args.filter_fields.iter().zip(args.filter_values.iter()).enumerate() {
            pb.set_message(format!("🔍 Probing type for '{}'...", field));
            
            let probe_res = probe_attribute_type(
                &client,
                &args.table_name,
                field,
                None,
                20, 
                false,
            ).await?;

            let ddb_type = probe_res.ddb_type.unwrap_or_else(|| "S".to_string());
            pb.suspend(|| {
                println!("💡 Inferred type for '{}': {} (based on {} samples)", field, ddb_type, probe_res.samples_seen);
            });

            let attr_val = build_attribute_value(&ddb_type, val)?;
            let name_key = format!("#f{}", i);
            let val_key = format!(":v{}", i);

            fe_parts.push(format!("{} = {}", name_key, val_key));
            names_map.insert(name_key, field.clone());
            values_map.insert(val_key, attr_val);
        }

        filter_expr = Some(fe_parts.join(&format!(" {} ", args.filter_logic)));
        eav = Some(values_map);
        ean = Some(names_map);
        
        pb.suspend(|| {
            println!("⚙️  Filter Expression generated: {}", filter_expr.as_ref().unwrap());
        });
    }

    pb.set_message("Scanning DynamoDB...");
    let paginator = ParallelScanPaginator::new(client, args.workers, args.retries_max_attempts);
    
    let mut rx = paginator.paginate(
        args.table_name.clone(), 
        args.total_segments,
        filter_expr,
        eav,
        ean
    ).await;

    // ==========================================
    // OUTPUT ROUTING LOGIC (JSONL vs CSV)
    // ==========================================
    let is_csv = args.output.as_ref().map(|p| p.ends_with(".csv")).unwrap_or(false);
    
    // Initializes the CSV writer if the extension is .csv
    let mut csv_writer = if is_csv {
        Some(csv::Writer::from_path(args.output.as_ref().unwrap())?)
    } else {
        None
    };
    
    // Initializes the JSONL / Stdout writer otherwise
    let mut json_writer: Option<Box<dyn Write>> = if !is_csv {
        match &args.output {
            Some(path) => {
                let file = File::create(path)?;
                Some(Box::new(BufWriter::with_capacity(8 * 1024 * 1024, file)))
            }
            None => Some(Box::new(io::stdout())),
        }
    } else {
        None
    };

    let mut total_items = 0;
    let mut csv_headers: Option<Vec<String>> = None;

    while let Some(result) = rx.recv().await {
        match result {
            Ok(output) => {
                if let Some(items) = output.items {
                    for item in items {
                        total_items += 1;
                        pb.set_position(total_items);

                        let json_val = parse_item(&item);
                        
                        if is_csv {
                            // Extracts CSV headers from the very first item dynamically
                            if csv_headers.is_none() {
                                if let Value::Object(map) = &json_val {
                                    let mut headers: Vec<String> = map.keys().cloned().collect();
                                    headers.sort(); // Sorts alphabetically for consistent column ordering
                                    csv_writer.as_mut().unwrap().write_record(&headers)?;
                                    csv_headers = Some(headers);
                                }
                            }
                            
                            // Flattens the JSON and writes the CSV row
                            if let Some(headers) = &csv_headers {
                                if let Value::Object(map) = &json_val {
                                    let mut record = Vec::new();
                                    for h in headers {
                                        let val_str = match map.get(h) {
                                            Some(Value::String(s)) => s.clone(),
                                            Some(Value::Null) => "".to_string(),
                                            Some(other) => other.to_string(), // Safely stringifies nested objects/arrays
                                            None => "".to_string(),
                                        };
                                        record.push(val_str);
                                    }
                                    csv_writer.as_mut().unwrap().write_record(&record)?;
                                }
                            }
                        } else {
                            // Standard JSONL fast path
                            let output_line = json_val.to_string();
                            if args.output.is_some() {
                                writeln!(json_writer.as_mut().unwrap(), "{}", output_line)?;
                            } else {
                                pb.suspend(|| {
                                    writeln!(json_writer.as_mut().unwrap(), "{}", output_line).unwrap();
                                });
                            }
                        }
                    }
                }
            }
            Err(e) => {
                pb.suspend(|| {
                    eprintln!("Error during scan: {:?}", e);
                });
            }
        }
    }

    // Ensures all buffers are flushed safely to disk
    if let Some(mut w) = csv_writer {
        w.flush()?;
    }
    if let Some(mut w) = json_writer {
        w.flush()?;
    }

    pb.finish_with_message(format!("✅ Scan complete! {} items exported.", total_items));
    Ok(())
}

fn parse_item(item: &HashMap<String, AttributeValue>) -> Value {
    let mut map = serde_json::Map::new();
    for (k, v) in item {
        map.insert(k.clone(), parse_attr(v));
    }
    Value::Object(map)
}

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
        AttributeValue::Ss(ss) => Value::Array(ss.iter().map(|s: &String| Value::String(s.clone())).collect()),
        AttributeValue::Ns(ns) => Value::Array(ns.iter().map(|n: &String| {
            if let Ok(i) = n.parse::<i64>() { json!(i) }
            else if let Ok(f) = n.parse::<f64>() { json!(f) }
            else { Value::String(n.clone()) }
        }).collect()),
        AttributeValue::Null(_) => Value::Null,
        _ => Value::Null,
    }
}
