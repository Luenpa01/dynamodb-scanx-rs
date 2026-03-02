use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};

#[derive(Debug)]
pub struct ScanOutput {
    pub items: Option<Vec<HashMap<String, AttributeValue>>>,
}

pub struct ParallelScanPaginator {
    client: Client,
    workers: usize,
    max_retries: u32,
    semaphore: Arc<Semaphore>,
}

impl ParallelScanPaginator {
    pub fn new(client: Client, workers: Option<usize>, max_retries: u32) -> Self {
        let actual_workers = workers.unwrap_or(256);
        Self {
            client,
            workers: actual_workers,
            max_retries,
            // Initializes the semaphore to limit concurrent connections to AWS
            semaphore: Arc::new(Semaphore::new(actual_workers)),
        }
    }

    pub async fn paginate(
        &self,
        table_name: String,
        total_segments: i32,
        filter_expression: Option<String>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
        expression_attribute_names: Option<HashMap<String, String>>,
    ) -> mpsc::Receiver<Result<ScanOutput, String>> {
        let (tx, rx) = mpsc::channel(100);

        for segment in 0..total_segments {
            let tx = tx.clone();
            let client = self.client.clone();
            let table_name = table_name.clone();
            let permit = self.semaphore.clone().acquire_owned().await.unwrap();
            let max_retries = self.max_retries;
            
            // Clones the AWS filter logic for each async worker
            let fe = filter_expression.clone();
            let eav = expression_attribute_values.clone();
            let ean = expression_attribute_names.clone();

            tokio::spawn(async move {
                let _permit = permit;
                // Explicit type annotation required by Rust compiler for AWS SDK v1+
                let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

                loop {
                    let mut retries = 0;
                    let mut backoff = Duration::from_millis(50);

                    loop {
                        let mut request = client
                            .scan()
                            .table_name(&table_name)
                            .segment(segment)
                            .total_segments(total_segments);

                        if let Some(key) = &exclusive_start_key {
                            for (k, v) in key {
                                request = request.exclusive_start_key(k, v.clone());
                            }
                        }

                        // INJECTS DYNAMODB FILTERS HERE
                        if let Some(ref expr) = fe {
                            request = request.filter_expression(expr);
                        }
                        if let Some(ref values) = eav {
                            for (k, v) in values {
                                request = request.expression_attribute_values(k, v.clone());
                            }
                        }
                        if let Some(ref names) = ean {
                            for (k, v) in names {
                                request = request.expression_attribute_names(k, v);
                            }
                        }

                        match request.send().await {
                            Ok(output) => {
                                exclusive_start_key = output.last_evaluated_key;
                                
                                let scan_output = ScanOutput { items: output.items };

                                if tx.send(Ok(scan_output)).await.is_err() {
                                    return;
                                }
                                break;
                            }
                            Err(e) => {
                                if retries >= max_retries {
                                    let err_msg = format!("Segment {} failed after {} retries: {:?}", segment, retries, e);
                                    let _ = tx.send(Err(err_msg)).await;
                                    return;
                                }
                                retries += 1;
                                tokio::time::sleep(backoff).await;
                                backoff *= 2;
                            }
                        }
                    }

                    if exclusive_start_key.is_none() {
                        break;
                    }
                }
            });
        }
        rx
    }
}
