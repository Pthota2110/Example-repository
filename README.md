# Financial Data Platform — AWS Cloud Data Engineering

A production-grade cloud data engineering platform for financial data processing, built on AWS. Demonstrates end-to-end capabilities across ETL pipelines, real-time fraud detection, financial reporting, and data quality governance.

## Architecture

```
                         ┌─────────────────────────────────────────────┐
                         │                  AWS Cloud                   │
  Raw Financial Data     │                                             │
  (CSV / API / Kafka) ───┼──► S3 (Raw)                                │
                         │        │                                    │
                         │        ▼                                    │
                         │   AWS Glue ETL ──────────────────────────► S3 (Processed)
                         │        │                                    │      │
                         │        ▼                                    │      ▼
                         │   Glue Data Catalog                         │  Redshift DW
                         │                                             │      │
  Transaction Stream ────┼──► Kinesis Data Streams                    │      ▼
                         │        │                                    │  QuickSight
                         │        ▼                                    │  Dashboards
                         │   Lambda (Fraud Rules)                      │
                         │        │                                    │
                         │        ├──► DynamoDB (Fraud Alerts)         │
                         │        └──► SNS (Notifications)             │
                         │                                             │
                         │   Great Expectations + CloudWatch Logs      │
                         │   (Data Quality & Audit Trail)              │
                         └─────────────────────────────────────────────┘
```

## Features

| Feature | AWS Services | Description |
|---|---|---|
| **ETL Pipeline** | Glue, S3, Redshift | Ingest, transform, and load financial transactions |
| **Fraud Detection** | Kinesis, Lambda, DynamoDB, SNS | Real-time streaming fraud detection with rule engine |
| **Financial Reporting** | Redshift, QuickSight | P&L, balance sheet, and transaction summary dashboards |
| **Data Quality** | Great Expectations, CloudWatch | Schema validation, data lineage, audit logging |

## Project Structure

```
financial-data-platform/
├── etl/                        # ETL pipeline (AWS Glue)
│   ├── glue_jobs/
│   │   ├── ingest_transactions.py
│   │   ├── transform_transactions.py
│   │   └── load_to_redshift.py
│   └── schemas/
│       └── transactions_schema.json
├── fraud_detection/            # Real-time fraud detection (Lambda + Kinesis)
│   ├── lambda_handler.py
│   ├── fraud_rules.py
│   ├── kinesis_consumer.py
│   └── alert_manager.py
├── reporting/                  # Financial reporting
│   ├── sql/
│   │   ├── daily_pnl.sql
│   │   ├── balance_sheet.sql
│   │   ├── transaction_summary.sql
│   │   └── views/create_views.sql
│   └── quicksight/
│       └── dashboard_config.json
├── data_quality/               # Data quality & governance
│   ├── validators.py
│   ├── audit_logger.py
│   ├── data_lineage.py
│   └── great_expectations/
│       └── expectations.json
├── terraform/                  # Infrastructure as Code
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── data/                       # Sample data generators
│   └── generate_sample_data.py
├── tests/                      # Unit & integration tests
│   ├── test_etl.py
│   ├── test_fraud_detection.py
│   └── test_data_quality.py
├── .github/workflows/          # CI/CD
│   └── deploy.yml
└── requirements.txt
```

## Quick Start

### Prerequisites
- Python 3.11+
- AWS CLI configured (`aws configure`)
- Terraform 1.6+

### 1. Clone & Install
```bash
git clone https://github.com/Pthota2110/Example-repository.git
cd Example-repository
pip install -r requirements.txt
```

### 2. Deploy Infrastructure
```bash
cd terraform
terraform init
terraform plan -var="environment=dev"
terraform apply -var="environment=dev"
```

### 3. Generate Sample Data
```bash
python data/generate_sample_data.py --records 100000 --output s3://your-bucket/raw/
```

### 4. Run ETL Pipeline
```bash
aws glue start-job-run --job-name financial-ingest-job
aws glue start-job-run --job-name financial-transform-job
aws glue start-job-run --job-name financial-load-job
```

### 5. Run Data Quality Checks
```bash
python data_quality/validators.py --config data_quality/great_expectations/expectations.json
```

### 6. Run Tests
```bash
pytest tests/ -v --cov=.
```

## Key Design Decisions

- **Medallion Architecture**: Raw → Bronze → Silver → Gold layers in S3
- **Idempotent ETL**: Glue jobs use job bookmarks to avoid reprocessing
- **Fraud Rules**: Configurable rule engine — no model redeployment needed to update thresholds
- **Partitioning**: Redshift tables partitioned by `transaction_date` for query performance
- **PII Handling**: Account numbers masked using AWS KMS before writing to S3

## Data Model

```sql
-- Core fact table
transactions (
    transaction_id    VARCHAR(36) PRIMARY KEY,
    account_id        VARCHAR(20),
    transaction_date  DATE,
    amount            DECIMAL(18,2),
    currency          CHAR(3),
    transaction_type  VARCHAR(20),   -- DEBIT | CREDIT | TRANSFER
    merchant_id       VARCHAR(20),
    category          VARCHAR(50),
    status            VARCHAR(20),   -- PENDING | SETTLED | FLAGGED | REVERSED
    created_at        TIMESTAMP
)
```

## CI/CD Pipeline

GitHub Actions runs on every push to `main`:
1. Lint (flake8, black)
2. Unit tests (pytest)
3. Data quality validation on sample data
4. Terraform plan (infrastructure diff)
5. Deploy to AWS dev environment

## License

MIT
