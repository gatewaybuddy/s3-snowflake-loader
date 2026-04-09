# S3 → Snowflake Automated Data Loader

Event-driven data loading pipeline. Drop files in S3, they land in Snowflake automatically.

## How It Works

1. **Drop a file** in S3 under a configured prefix
2. **S3 event** triggers a database-specific Lambda
3. **Lambda** parses the path, detects the file format, and runs `COPY INTO`
4. **Everything is logged** to a centralized admin database

```
s3://bucket/<prefix>/CUSTOMERS.T/customers_2026.csv
              │          │    │
              │          │    └── Mode: T=Truncate, A=Append, M=Merge
              │          └── Parent folder = target table name
              └── Configurable prefix maps to a specific database + Lambda
```

## Features

- **Convention over configuration** — folder structure determines routing
- **Auto-detect file format** — CSV, JSON, Parquet, pipe-delimited, tab-delimited
- **Auto-create tables** — new tables created from file schema automatically
- **Database isolation** — separate Lambda, credentials, and IAM per database
- **Centralized logging** — unified load history across all databases
- **Control table overrides** — per-table format, mode, pre/post SQL hooks
- **Portable** — generates SQL + CloudFormation for your team to deploy

## Quick Start

```bash
# 1. Configure your environment
cp config.example.yaml config.yaml
# Edit config.yaml with your AWS + Snowflake details

# 2. Generate deployment artifacts
python3 generate.py

# 3. Review and deploy
#    → Give snowflake-setup.sql to your Snowflake admin
#    → Deploy infrastructure.yaml via CloudFormation
```

## What Gets Generated

| File | Who Runs It | What It Does |
|------|-------------|--------------|
| `output/snowflake-setup.sql` | Snowflake Admin | Creates admin DB, tables, roles, users, stages |
| `output/infrastructure.yaml` | AWS Admin | Deploys Lambdas, IAM roles, S3 events, Secrets Manager |
| `output/secrets-bootstrap.sh` | AWS Admin | Seeds Secrets Manager with Snowflake credentials |
| `output/teardown.sql` | Snowflake Admin | Clean removal of all created objects |

## Project Structure

```
s3-snowflake-loader/
├── config.example.yaml          # Template — copy to config.yaml
├── generate.py                  # Generates all deployment artifacts
├── lambda/
│   └── loader/
│       ├── handler.py           # Lambda entry point
│       ├── path_parser.py       # S3 key → table name + mode
│       ├── format_detector.py   # Auto-detect file format
│       ├── snowflake_client.py  # Connection + COPY INTO
│       ├── table_creator.py     # Auto-create tables from schema
│       ├── history_logger.py    # Write to LOAD_HISTORY
│       └── requirements.txt     # snowflake-connector-python
├── templates/
│   ├── snowflake-setup.sql.j2   # Snowflake object templates
│   ├── infrastructure.yaml.j2  # CloudFormation template
│   ├── secrets-bootstrap.sh.j2 # Secrets Manager seeding
│   └── teardown.sql.j2         # Clean removal
├── monitoring/
│   ├── dashboard-queries.sql    # Ready-to-run monitoring queries
│   └── alerts.sql               # Optional SNS alerting
├── tests/
│   ├── test_path_parser.py
│   ├── test_format_detector.py
│   ├── test_handler.py
│   └── sample-data/
│       ├── customers.csv
│       ├── orders.json
│       ├── metrics.parquet
│       └── legacy-pipe.txt
└── docs/
    ├── architecture.md          # Full technical spec
    ├── runbook.md               # Troubleshooting
    └── adding-databases.md      # How to add new databases
```

## Requirements

- Python 3.12+
- AWS CLI configured (for deployment)
- Snowflake account with ACCOUNTADMIN or equivalent
- Jinja2 (`pip install jinja2 pyyaml`)

## Configuration

See `config.example.yaml` for all options. Key sections:

```yaml
aws:
  region: us-east-1
  s3_bucket: my-data-lake

snowflake:
  account: xy12345.us-east-1
  warehouse: LOADING_WH
  admin_database: ADMIN_DB

databases:
  - name: ANALYTICS_DB
    s3_prefix: analytics/    # Configurable per-deployment
    schema: STAGING
  - name: REPORTING_DB
    s3_prefix: reporting/    # Can be any prefix you choose
    schema: STAGING
```

**Important**: The `s3_prefix` determines which S3 path routes to which database and can be anything (e.g., `data/`, `snowflake/`, `warehouse/`, `my-org/`). Each deployment can use different prefixes based on your S3 organization scheme.

## Roadmap

- [x] Core loader (CSV, JSON, Parquet auto-detection)
- [x] CloudFormation + Snowflake SQL generation
- [x] Control table overrides
- [x] Centralized load history + error logging
- [ ] Web GUI for configuration management
- [ ] Merge/upsert mode (.M folders)
- [ ] Schema evolution (auto-add new columns)
- [ ] Slack/Teams notifications
- [ ] Multi-file atomic loads

## License

MIT
