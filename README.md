# dynamodb-scanx-rs

A high-performance, concurrent DynamoDB parallel scanner and SRE toolkit built in Rust.

Designed to export massive datasets from AWS DynamoDB at maximum speed with a constant **O(1) memory footprint**. It dynamically routes output to standard JSONL or flattens nested NoSQL structures into CSV files.

## Features

- **Parallel scan**: Splits the table into N segments and scans them concurrently using Tokio.
- **O(1) memory streaming**: Data is streamed through MPSC channels and 8MB file buffers directly to disk — you can export terabytes using only megabytes of RAM.
- **Auto type inference**: Probes the table to automatically infer DynamoDB attribute types (`S`, `N`, `BOOL`, etc.), so you can write filter queries without specifying types manually.
- **Dynamic output routing**: Saves as JSONL or flattens nested DynamoDB JSON into CSV based on the output file extension.
- **STS role assumption**: Seamlessly assumes IAM roles for cross-account access using standard AWS credentials.

## Prerequisites

- Rust toolchain ([install via rustup](https://rustup.rs))
- Valid AWS credentials configured on your machine (`~/.aws/credentials`, environment variables, or AWS SSO)

## Installation

Clone the repository and build the release binary:

```bash
git clone https://github.com/Luenpa01/dynamodb-scanx-rs.git
cd dynamodb-scanx-rs
cargo build --release
```

The binary will be at `target/release/dpscan`. Optionally move it to your PATH:

```bash
sudo mv target/release/dpscan /usr/local/bin/
```

## Usage

```
dpscan [OPTIONS] --table-name <TABLE_NAME>
```

### Options

| Flag | Default | Description |
|---|---|---|
| `--table-name` | *(required)* | DynamoDB table to scan |
| `--total-segments` | `32` | Number of parallel scan segments |
| `--region` | `us-east-1` | AWS region |
| `--role-arn` | — | IAM Role ARN to assume |
| `--external-id` | — | External ID for cross-account role assumption |
| `--role-session-name` | `dpscan-session` | Session name for the assumed role |
| `-w, --workers` | `256` | Max concurrent DynamoDB connections |
| `--retries-max-attempts` | `10` | Max retry attempts per segment on throttling |
| `-o, --output` | stdout | Output file path (`.jsonl` or `.csv`) |
| `-f, --filter-field` | — | Field name to filter on (repeatable) |
| `-v, --filter-value` | — | Value to match (repeatable, paired with `-f`) |
| `--filter-logic` | `AND` | Logical operator to combine filters (`AND` / `OR`) |

## Examples

**Basic JSONL export**

```bash
dpscan --table-name users_table --total-segments 32 -w 16 -o users_export.jsonl
```

**Flatten to CSV**

Use a `.csv` extension and the engine automatically infers headers from the first record and flattens the dataset:

```bash
dpscan --table-name transactions -o transactions.csv
```

**Server-side filtering with auto type inference**

The engine probes the table to discover the data types of `status` and `countryCode` before executing the scan:

```bash
dpscan --table-name banks \
  -f status -v ACTIVE \
  -f countryCode -v COL \
  --filter-logic AND \
  -o active_colombian_banks.csv
```

**Filtering with OR logic**

```bash
dpscan --table-name orders \
  -f status -v PENDING \
  -f status -v FAILED \
  --filter-logic OR \
  -o pending_or_failed.jsonl
```

**Filtering on a String Set attribute (`SS`)**

For set-type attributes, pass comma-separated values:

```bash
dpscan --table-name products \
  -f tags -v "electronics,sale" \
  -o tagged_products.jsonl
```

**Cross-account access via IAM role**

```bash
dpscan --table-name production_data \
  --role-arn arn:aws:iam::123456789012:role/DataExportRole \
  --external-id my-external-id \
  --region us-west-2 \
  -o prod_data.jsonl
```

**Print to stdout (pipe-friendly)**

```bash
dpscan --table-name my_table | jq .
```

## Performance Tuning

- **`--total-segments`**: Each segment is scanned in a separate concurrent task. Higher values increase parallelism. For large tables, values between `64` and `256` are common. AWS charges per request, so very high values may increase cost without proportional speed gains.
- **`-w, --workers`**: Controls how many DynamoDB connections can be in-flight simultaneously. The default (`256`) is sufficient for most workloads. Reduce it if you hit `ProvisionedThroughputExceededException` errors frequently.
- **Retries**: The scanner uses exponential backoff starting at 50ms. Increase `--retries-max-attempts` on tables with aggressive throttling.

## CSV Limitations

When exporting to CSV, keep the following in mind:

- **Headers are inferred from the first item.** If your items have inconsistent schemas (sparse attributes), some columns may be missing for records that don't include those fields.
- **Nested structures are stringified.** Attributes of type `M` (Map) or `L` (List) are serialized as a JSON string in a single cell rather than being flattened into multiple columns.
- **Filter support is limited to scalar and set types.** Filtering on `B` (Binary), `M` (Map), and `L` (List) attributes is not supported. Supported filter types are: `S`, `N`, `BOOL`, `SS`, and `NS`.

## License

MIT — see [LICENSE](LICENSE) for details.
