# DynamoDB ScanX (Rust Edition)

A high-performance, concurrent DynamoDB parallel scanner and SRE toolkit built in Rust. 

DynamoDB ScanX is designed to export massive datasets from AWS DynamoDB at maximum speed with a constant O(1) memory footprint. It dynamically routes output to standard JSONL or flattens nested NoSQL structures into perfectly formatted CSV files.

## Core Features

* Asynchronous Concurrency: Utilizes Tokio and highly tuned connection pooling (Semaphores) to saturate network I/O without hitting DynamoDB throttling limits.
* O(1) Memory Streaming: Employs MPSC channels and 8MB file buffers. It streams data directly to your solid-state drive, meaning you can download terabytes of data using megabytes of RAM.
* Intelligent Type Inference: Features an auto-probe engine that samples the DynamoDB table to automatically infer raw data types (S, N, BOOL, etc.), allowing you to write natural filter queries in the terminal.
* Dynamic Output Routing: Automatically formats and flattens nested DynamoDB JSON into structured CSV files or standard JSONL based on your output extension.
* Native STS Integration: Seamlessly assumes AWS IAM roles using standard AWS credentials and session names.

## Prerequisites

You need the Rust toolchain installed on your system to build this project. If you do not have it, install it via rustup:

```bash
curl --proto '=https' --tlsv1.2 -sSf [https://sh.rustup.rs](https://sh.rustup.rs) | sh
You must also have valid AWS credentials configured on your machine (via ~/.aws/credentials, environment variables, or AWS SSO).

Installation
Clone the repository and build the CLI binary with release optimizations.

Bash
git clone [https://github.com/your-username/dynamodb-scanx-rs.git](https://github.com/your-username/dynamodb-scanx-rs.git)
cd dynamodb-scanx-rs
cargo build --release
The compiled binary will be available at target/release/dpscan. You can move it to your system's binary path for global access:

Bash
sudo mv target/release/dpscan /usr/local/bin/
Usage Examples
Basic JSONL Export
Scan a table using 32 concurrent segments and 16 async workers, saving the output to a JSON Lines file.

Bash
dpscan --table-name users_table --total-segments 32 --workers 16 --output users_export.jsonl
Flatten to CSV
Provide a .csv extension, and the engine will automatically infer headers from the first record and flatten the NoSQL dataset.

Bash
dpscan --table-name transactions --output transactions.csv
Server-Side Filtering (Auto Type Inference)
Use multiple filters at once. The engine will probe the table to discover the data types of status and countryCode before executing the scan.

Bash
dpscan --table-name banks \
  -f status -v ACTIVE \
  -f countryCode -v COL \
  --filter-logic AND \
  --output active_colombian_banks.csv
Cross-Account Access (Assume Role)
Connect to a table in a different AWS account by providing an IAM Role ARN.

Bash
dpscan --table-name production_data \
  --role-arn arn:aws:iam::123456789012:role/DataExportRole \
  --region us-west-2 \
  --output prod_data.jsonl
License
This project is licensed under the MIT License - see the LICENSE file for details.
