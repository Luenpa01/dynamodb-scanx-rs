use aws_sdk_dynamodb::operation::scan::ScanOutput;
use aws_sdk_dynamodb::types::AttributeValue; // Imports the DynamoDB AttributeValue type
use aws_sdk_dynamodb::Client;
use std::collections::HashMap; // Imports the HashMap for the key structure
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::sleep;

/// Manages parallel scanning operations for a DynamoDB table.
/// Utilizes asynchronous tasks and an MPSC channel to stream results safely.
pub struct ParallelScanPaginator {
    client: Client,
    workers: usize,
    max_retries: u32,
}

impl ParallelScanPaginator {
    /// Initializes a new instance of the paginator.
    /// Defaults to 256 workers if not explicitly provided.
    pub fn new(client: Client, workers: Option<usize>, max_retries: u32) -> Self {
        Self {
            client,
            workers: workers.unwrap_or(256),
            max_retries,
        }
    }

    /// Starts the parallel scan and returns a receiver channel to stream pages.
    /// Limits concurrent requests using a semaphore based on the worker count.
    pub async fn paginate(
        &self,
        table_name: String,
        total_segments: i32,
    ) -> mpsc::Receiver<Result<ScanOutput, String>> {
        // Calculates the actual number of workers to prevent over-allocation
        let actual_workers = self.workers.min(total_segments.max(1) as usize);
        
        // Creates a channel to stream the pages back to the caller
        let (tx, rx) = mpsc::channel(actual_workers * 2);
        
        // Semaphore controls the maximum number of concurrent active tasks
        let semaphore = Arc::new(Semaphore::new(actual_workers));
        let client = self.client.clone(); 

        for segment in 0..total_segments {
            let tx_clone = tx.clone();
            let client_clone = client.clone();
            let sem_clone = semaphore.clone();
            let table_clone = table_name.clone();
            let retries = self.max_retries;

            // Spawns an asynchronous, non-blocking task for each segment
            tokio::spawn(async move {
                // Waits for an available permit before sending the network request
                let _permit = match sem_clone.acquire().await {
                    Ok(p) => p,
                    Err(_) => return, // Stops execution if the semaphore is closed
                };
                
                let mut current_retries = 0;
                
                // Explicitly defines the type for the exclusive start key
                let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

                loop {
                    let mut request = client_clone
                        .scan()
                        .table_name(&table_clone)
                        .segment(segment)
                        .total_segments(total_segments);

                    if let Some(key) = &exclusive_start_key {
                        request = request.set_exclusive_start_key(Some(key.clone()));
                    }

                    match request.send().await {
                        Ok(output) => {
                            current_retries = 0;
                            exclusive_start_key = output.last_evaluated_key.clone();
                            let has_more = exclusive_start_key.is_some();
                            
                            // Sends the retrieved page through the channel
                            if tx_clone.send(Ok(output)).await.is_err() {
                                break; // Breaks the loop if the receiver is dropped
                            }

                            if !has_more {
                                break; // Completes the segment processing
                            }
                        }
                        Err(e) => {
                            current_retries += 1;
                            if current_retries > retries {
                                let err_msg = format!("Segment {} failed after {} retries: {}", segment, retries, e);
                                let _ = tx_clone.send(Err(err_msg)).await;
                                break;
                            }
                            
                            // Implements a simple exponential backoff strategy
                            let backoff = 2u64.pow(current_retries) * 100;
                            sleep(Duration::from_millis(backoff)).await;
                        }
                    }
                }
            });
        }

        // Returns the receiver so the caller can iterate asynchronously
        rx
    }
}
