# S3-to-Snowflake Loader — Setup Guide

Complete guide for deploying the event-driven ETL pipeline from S3 to Snowflake.

---

## Quick Start (5 Minutes)

**For users of an already-deployed pipeline** — skip the 500-line setup guide below and get data flowing in minutes.

### Prerequisites
- Infrastructure is already deployed (AWS Lambda, Snowflake objects, S3 bucket)
- You just want to use the pipeline, not deploy it from scratch

### Steps

1. **Get S3 write access**: Ask Rado or your admin for AWS credentials to the target bucket
2. **Upload a CSV file** following the naming convention:
   ```bash
   aws s3 cp myfile.csv s3://etl-loader-test-pickybat/analytics/MY_TABLE.T/myfile.csv
   ```
3. **Wait ~10 seconds** for the Lambda function to process it
4. **Check your data** at https://data.pickybat.com/ to see it loaded into Snowflake
5. **That's it!** Your CSV is now in `ANALYTICS_DB.STAGING.MY_TABLE`

### Folder Convention (Quick Reference)
- `.T` = **Truncate** (delete all rows, then load) — default if no suffix
- `.A` = **Append** (add rows to existing table)  
- `.M` = **Merge** (upsert based on configured merge keys)

Example paths:
- `analytics/SALES.T/daily-revenue.csv` → truncate and reload SALES table
- `analytics/EVENTS.A/user-clicks.json` → append to EVENTS table
- `analytics/INVENTORY/products.parquet` → truncate INVENTORY (no suffix = .T)

### For Production Setup
If you need to deploy your own instance, configure advanced options, or troubleshoot issues, see the full guide sections below.

---

## Table of Contents

0. [Quick Start (5 Minutes)](#quick-start-5-minutes)
1. [Prerequisites](#1-prerequisites)
2. [Snowflake Setup](#2-snowflake-setup)
3. [AWS Setup](#3-aws-setup)
4. [RSA Key Generation & Rotation](#4-rsa-key-generation--rotation)
5. [Folder Structure Convention](#5-folder-structure-convention)
6. [Troubleshooting](#6-troubleshooting)
7. [Duplicate Detection & Idempotency](#7-duplicate-detection--idempotency)
8. [Network Policies (Production Security)](#8-network-policies-production-security)
9. [Verification Checklist](#9-verification-checklist)

---

## 1. Prerequisites

> **Estimated setup time**: 
> - Fresh deployment: ~30 minutes
> - Adding a new database to existing setup: ~15 minutes  
> - Troubleshooting common issues: +10-20 minutes
>
> **Estimated running cost**: 
> - **AWS Lambda**: Free tier covers 1M requests/month + 400K GB-seconds
> - **AWS S3**: Storage ~$0.023/GB/month, requests ~$0.0004/1K requests  
> - **Snowflake**: X-Small warehouse = $2-4/credit, 1 credit/hour, auto-suspends after 60s idle
> - **Example**: 100 CSV files/day × 1MB each = ~$15-25/month total

### Accounts & Access

| Requirement | Details |
|---|---|
| **AWS Account** | See [AWS IAM Permissions](#aws-iam-permissions) below |
| **Snowflake Account** | With `ACCOUNTADMIN` role (for initial setup only) |
| **Snowflake PAT** | Programmatic Access Token for setup scripts (see [Section 1.1](#11-create-a-snowflake-pat)) |
| **AWS CLI** | v2.x installed and configured (`aws configure`) |
| **Python** | 3.12+ with `pip` |
| **Git** | To clone this repository |
| **pass** | (Optional) GNU pass for local credential management. Install with: `sudo apt install pass` (Ubuntu) or `brew install pass` (macOS) |

### AWS IAM Permissions

The IAM user/role running the deployment needs these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:CreateFunction", "lambda:UpdateFunctionCode",
        "lambda:UpdateFunctionConfiguration", "lambda:AddPermission",
        "lambda:GetFunction", "lambda:DeleteFunction",
        "iam:CreateRole", "iam:PutRolePolicy", "iam:PassRole",
        "iam:AttachRolePolicy", "iam:GetRole",
        "s3:CreateBucket", "s3:PutBucketNotification",
        "s3:GetBucketLocation", "s3:PutObject", "s3:GetObject",
        "secretsmanager:CreateSecret", "secretsmanager:PutSecretValue",
        "secretsmanager:GetSecretValue",
        "sns:CreateTopic", "sns:Subscribe", "sns:Publish",
        "cloudformation:CreateStack", "cloudformation:UpdateStack",
        "cloudformation:DescribeStacks", "cloudformation:DeleteStack",
        "cloudformation:DescribeStackEvents",
        "logs:CreateLogGroup", "logs:DescribeLogGroups"
      ],
      "Resource": "*"
    }
  ]
}
```

> **Tip**: For production, scope `Resource` to your specific ARNs instead of `"*"`.

### Tools

```bash
# Verify AWS CLI
aws --version        # aws-cli/2.x.x
aws sts get-caller-identity  # confirms credentials

# Verify Python
python3 --version    # 3.12+
```

### Clone & Configure

```bash
git clone https://github.com/gatewaybuddy/s3-snowflake-loader.git
cd s3-snowflake-loader

# Set up Python environment
python3 -m venv .venv
source .venv/bin/activate
pip install snowflake-connector-python cryptography boto3 pyyaml jinja2

# Create your config from the example
cp config.example.yaml config.yaml
# Edit config.yaml with your Snowflake account, AWS region, S3 bucket, etc.
```

> **Important**: `config.yaml` is gitignored — it contains your credentials and account-specific settings. Never commit it.

### 1.1 Create a Snowflake PAT

Setup scripts authenticate with a **Programmatic Access Token (PAT)** — never hardcode passwords.

1. Log into Snowflake → click your name (bottom-left) → **My Profile**
2. Scroll to **Programmatic Access Tokens** → **Generate New Token**
3. Set:
   - **Name**: `etl-setup` (or anything descriptive)
   - **Expires**: 24 hours (enough for initial setup)
   - **Role**: `ACCOUNTADMIN`
   - **Network policy**: Click **"Bypass requirement for network policy"** if prompted
4. Copy the token immediately (it won't be shown again)

Store it securely:

```bash
# Option 1: Using GNU pass (recommended if you have it set up)
echo '<your-pat-token>' | pass insert -m -f snowflake/setup-pat

# Option 2: Export for this session only (if you don't use pass)
export SNOWFLAKE_TOKEN='<your-pat-token>'

# Option 3: Save to a temporary file (remember to delete it after setup)
echo '<your-pat-token>' > /tmp/snowflake_token.txt
export SNOWFLAKE_TOKEN="$(cat /tmp/snowflake_token.txt)"
```

The setup scripts accept credentials via environment variables:

```bash
export SNOWFLAKE_ACCOUNT='YOUR_ACCOUNT-ID'    # example: YOUR-ACCOUNT-ID (example only)
export SNOWFLAKE_USER='YOUR_USERNAME'
export SNOWFLAKE_TOKEN='<pat-from-step-4>'
```

> **Why PATs over passwords?**
> - Time-limited (you choose the expiry)
> - Scoped to a specific role
> - No risk of hardcoding — if you forget to set the env var, the script fails explicitly
> - Visible in Snowflake's audit log as a distinct auth method
> - Can be revoked instantly without changing your password

> **For production**: Set up a [network policy](#network-policies) instead of bypassing. The service user (`SVC_LOADER_*`) uses RSA key-pair auth and doesn't need PATs or passwords at all.

---

### S3 Bucket

Create or identify the S3 bucket for data landing:

```bash
aws s3 mb s3://your-bucket-name --region us-east-1
```

The loader uses **prefix-based routing**: files dropped under `analytics/` route to `ANALYTICS_DB`, files under `reporting/` route to `REPORTING_DB`, etc.

---

## 2. Snowflake Setup

All SQL below is executed as `ACCOUNTADMIN`. Statements are idempotent (`IF NOT EXISTS`).

### 2.1 Admin Database & Pipeline Schema

The admin database stores centralized load history, error logs, and per-table configuration. All Lambda loaders (across all databases) write to this single location.

```sql
-- Central metadata database
CREATE DATABASE IF NOT EXISTS ADMIN_DB;
CREATE SCHEMA IF NOT EXISTS ADMIN_DB._PIPELINE;
```

### 2.2 Load History Table

Tracks every COPY INTO operation — success, failure, and partial loads.

```sql
CREATE TABLE IF NOT EXISTS ADMIN_DB._PIPELINE.LOAD_HISTORY (
    LOAD_ID             VARCHAR(36)     DEFAULT UUID_STRING(),
    S3_BUCKET           VARCHAR(256)    NOT NULL,
    S3_KEY              VARCHAR(1024)   NOT NULL,
    S3_SIZE_BYTES       NUMBER,
    S3_ETAG             VARCHAR(256),
    TARGET_DATABASE     VARCHAR(128)    NOT NULL,
    TARGET_SCHEMA       VARCHAR(128)    NOT NULL,
    TARGET_TABLE        VARCHAR(128)    NOT NULL,
    LOAD_MODE           VARCHAR(10)     NOT NULL,       -- TRUNCATE | APPEND | MERGE
    TABLE_CREATED       BOOLEAN         DEFAULT FALSE,
    FILE_FORMAT_USED    VARCHAR(4000),
    ROWS_LOADED         NUMBER,
    ROWS_PARSED         NUMBER,
    ERRORS_SEEN         NUMBER          DEFAULT 0,
    STATUS              VARCHAR(20)     NOT NULL,       -- LOADING | SUCCESS | PARTIAL | FAILED
    ERROR_MESSAGE       VARCHAR(4000),
    COPY_INTO_QUERY_ID  VARCHAR(256),
    LAMBDA_REQUEST_ID   VARCHAR(256),
    LAMBDA_FUNCTION     VARCHAR(256),
    STARTED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT        TIMESTAMP_NTZ,
    DURATION_SECONDS    NUMBER,
    PRIMARY KEY (LOAD_ID)
);
```

**Key columns:**
- `LOAD_ID` — Auto-generated UUID, primary key
- `STATUS` — Lifecycle: `LOADING` → `SUCCESS` / `PARTIAL` / `FAILED`
- `LOAD_MODE` — Determined by folder suffix (`.T`, `.A`, `.M`)
- `DURATION_SECONDS` — Computed by Lambda after COPY INTO completes

### 2.3 Load Errors Table

Stores individual rejected records from COPY INTO operations.

```sql
CREATE TABLE IF NOT EXISTS ADMIN_DB._PIPELINE.LOAD_ERRORS (
    ERROR_ID            VARCHAR(36)     DEFAULT UUID_STRING(),
    LOAD_ID             VARCHAR(36)     NOT NULL,
    TARGET_DATABASE     VARCHAR(128)    NOT NULL,
    S3_KEY              VARCHAR(1024)   NOT NULL,
    ERROR_LINE          NUMBER,
    ERROR_COLUMN        NUMBER,
    ERROR_MESSAGE       VARCHAR(4000),
    REJECTED_RECORD     VARCHAR(16777216),
    CREATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);
```

### 2.4 Control Table

Optional per-table overrides. Allows customizing load behavior without code changes.

```sql
CREATE TABLE IF NOT EXISTS ADMIN_DB._PIPELINE.CONTROL_TABLE (
    TARGET_DATABASE     VARCHAR(128)    NOT NULL,
    TARGET_SCHEMA       VARCHAR(128)    NOT NULL,
    TARGET_TABLE        VARCHAR(128)    NOT NULL,
    LOAD_MODE           VARCHAR(10)     DEFAULT NULL,
    FILE_FORMAT_NAME    VARCHAR(256)    DEFAULT NULL,
    FILE_FORMAT_OPTIONS VARCHAR(4000)   DEFAULT NULL,
    COPY_OPTIONS        VARCHAR(4000)   DEFAULT NULL,
    PRE_LOAD_SQL        VARCHAR(4000)   DEFAULT NULL,
    POST_LOAD_SQL       VARCHAR(4000)   DEFAULT NULL,
    MERGE_KEYS          VARCHAR(1000)   DEFAULT NULL,
    AUTO_CREATE_TABLE   BOOLEAN         DEFAULT TRUE,
    CREATE_TABLE_DDL    VARCHAR(16777216) DEFAULT NULL,
    ENABLED             BOOLEAN         DEFAULT TRUE,
    NOTES               VARCHAR(2000)   DEFAULT NULL,
    CREATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE)
);
```

**Example: Override load mode for a specific table:**
```sql
INSERT INTO ADMIN_DB._PIPELINE.CONTROL_TABLE
    (TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, LOAD_MODE, NOTES)
VALUES
    ('ANALYTICS_DB', 'STAGING', 'DAILY_REVENUE', 'APPEND', 'Always append, never truncate');
```

### 2.5 Target Database & Schema

```sql
-- Your data landing database
CREATE DATABASE IF NOT EXISTS ANALYTICS_DB;
CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.STAGING;
```

### 2.6 Loading Warehouse

A dedicated X-Small warehouse for COPY INTO operations. Separate from interactive query warehouses to avoid contention.

```sql
CREATE WAREHOUSE IF NOT EXISTS LOADING_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60       -- Suspend after 60s idle (saves credits)
    AUTO_RESUME = TRUE      -- Auto-start on first query
    INITIALLY_SUSPENDED = TRUE;
```

**Cost note:** X-Small = 1 credit/hour. With AUTO_SUSPEND = 60, you pay only for active loading time rounded up to 60-second increments.

### 2.7 Service Role & Grants

```sql
-- Create a role with only the privileges needed for loading
CREATE ROLE IF NOT EXISTS DATA_LOADER_ANALYTICS;

-- Warehouse access
GRANT USAGE ON WAREHOUSE LOADING_WH TO ROLE DATA_LOADER_ANALYTICS;

-- Target database: read, write, create tables
GRANT USAGE ON DATABASE ANALYTICS_DB TO ROLE DATA_LOADER_ANALYTICS;
GRANT USAGE ON SCHEMA ANALYTICS_DB.STAGING TO ROLE DATA_LOADER_ANALYTICS;
GRANT CREATE TABLE ON SCHEMA ANALYTICS_DB.STAGING TO ROLE DATA_LOADER_ANALYTICS;
GRANT SELECT, INSERT, TRUNCATE ON ALL TABLES IN SCHEMA ANALYTICS_DB.STAGING TO ROLE DATA_LOADER_ANALYTICS;
GRANT SELECT, INSERT, TRUNCATE ON FUTURE TABLES IN SCHEMA ANALYTICS_DB.STAGING TO ROLE DATA_LOADER_ANALYTICS;

-- Admin database: logging access
GRANT USAGE ON DATABASE ADMIN_DB TO ROLE DATA_LOADER_ANALYTICS;
GRANT USAGE ON SCHEMA ADMIN_DB._PIPELINE TO ROLE DATA_LOADER_ANALYTICS;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA ADMIN_DB._PIPELINE TO ROLE DATA_LOADER_ANALYTICS;
```

### 2.8 Service User (RSA Key Auth)

Generate the RSA key pair first (see [Section 4](#4-rsa-key-generation--rotation)), then create the user:

```sql
CREATE USER IF NOT EXISTS SVC_LOADER_ANALYTICS
    RSA_PUBLIC_KEY = '<public-key-body-no-header-footer>'
    DEFAULT_ROLE = DATA_LOADER_ANALYTICS
    DEFAULT_WAREHOUSE = LOADING_WH
    MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE DATA_LOADER_ANALYTICS TO USER SVC_LOADER_ANALYTICS;
```

**Verify the key fingerprint:**
```sql
DESC USER SVC_LOADER_ANALYTICS;
-- Look for RSA_PUBLIC_KEY_FP — should start with SHA256:
```

### 2.9 Storage Integration

Connects Snowflake to your S3 bucket. This is a two-phase process because of cross-account trust.

> **⚠️ Chicken-and-egg note**: This is intentionally a two-phase process. You create the storage integration first (Phase A), get Snowflake's IAM details from `DESC INTEGRATION` (Phase B), then use those details to create the AWS IAM role in Section 3.1 (Phase C). The cross-reference between sections is by design — you need Snowflake's generated values before you can configure the AWS side.

**Phase A — Create the integration (Snowflake side):**

```sql
CREATE STORAGE INTEGRATION IF NOT EXISTS S3_INT_ANALYTICS
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR-AWS-ACCOUNT-ID:role/etl-analytics-db-snowflake-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://your-bucket/analytics/');
```

**Phase B — Get Snowflake's IAM details:**

```sql
DESC INTEGRATION S3_INT_ANALYTICS;
```

Record these values (you'll need them for the AWS IAM role):

| Property | Example Value |
|---|---|
| `STORAGE_AWS_IAM_USER_ARN` | `arn:aws:iam::088253294712:user/wi3k1000-s` |
| `STORAGE_AWS_EXTERNAL_ID` | `YOUR-EXTERNAL-ID` |

**Phase C — Create the AWS IAM role** (see [Section 3.1](#31-iam-roles)).

### 2.10 External Stage

```sql
GRANT CREATE STAGE ON SCHEMA ANALYTICS_DB.STAGING TO ROLE DATA_LOADER_ANALYTICS;

CREATE OR REPLACE STAGE ANALYTICS_DB.STAGING.S3_LOAD_STAGE
    URL = 's3://your-bucket/analytics/'
    STORAGE_INTEGRATION = S3_INT_ANALYTICS;

GRANT USAGE ON STAGE ANALYTICS_DB.STAGING.S3_LOAD_STAGE TO ROLE DATA_LOADER_ANALYTICS;
```

**Verify the stage can see your S3 files:**
```sql
LIST @ANALYTICS_DB.STAGING.S3_LOAD_STAGE;
```

---

## 3. AWS Setup

### 3.1 IAM Roles

Two IAM roles are needed per database:

#### Lambda Execution Role

Allows the Lambda function to read from S3 and fetch credentials from Secrets Manager.

```json
{
  "RoleName": "etl-analytics-db-lambda-role",
  "AssumeRolePolicyDocument": {
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "lambda.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  },
  "Policies": [{
    "PolicyName": "etl-analytics-db-policy",
    "PolicyDocument": {
      "Statement": [
        {
          "Effect": "Allow",
          "Action": ["s3:GetObject", "s3:ListBucket"],
          "Resource": [
            "arn:aws:s3:::your-bucket/analytics/*",
            "arn:aws:s3:::your-bucket"
          ]
        },
        {
          "Effect": "Allow",
          "Action": "secretsmanager:GetSecretValue",
          "Resource": "arn:aws:secretsmanager:us-east-1:*:secret:etl/analytics_db/*"
        }
      ]
    }
  }],
  "ManagedPolicyArns": [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  ]
}
```

#### Snowflake Cross-Account Role

Allows Snowflake to read from S3 via the storage integration. Use the values from `DESC INTEGRATION`:

```json
{
  "RoleName": "etl-analytics-db-snowflake-role",
  "AssumeRolePolicyDocument": {
    "Statement": [{
      "Effect": "Allow",
      "Principal": {
        "AWS": "<STORAGE_AWS_IAM_USER_ARN from DESC INTEGRATION>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "YOUR-EXTERNAL-ID"
        }
      }
    }]
  },
  "Policies": [{
    "PolicyName": "snowflake-s3-analytics",
    "PolicyDocument": {
      "Statement": [{
        "Effect": "Allow",
        "Action": [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ],
        "Resource": [
          "arn:aws:s3:::your-bucket/analytics/*",
          "arn:aws:s3:::your-bucket"
        ]
      }]
    }
  }]
}
```

### 3.2 Lambda Deployment

```bash
# Package the Lambda code
cd lambda/loader
pip install -r requirements.txt -t package/
cd package && zip -r ../../../output/loader.zip . && cd ..
zip -g ../../output/loader.zip *.py

# Upload to S3
aws s3 cp output/loader.zip s3://your-code-bucket/etl-analytics-db-loader.zip
```

### 3.3 CloudFormation Deployment

Deploy the generated CloudFormation template:

```bash
aws cloudformation deploy \
  --template-file output/infrastructure.yaml \
  --stack-name s3-snowflake-loader \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    S3BucketName=your-bucket \
    CodeBucketName=your-code-bucket \
    SnowflakeIamArnANALYTICSDB='arn:aws:iam::088253294712:user/wi3k1000-s' \
    SnowflakeExternalIdANALYTICSDB='YOUR-EXTERNAL-ID'
```

### 3.4 Secrets Manager

Seed credentials after CloudFormation creates the secret shells:

```bash
# Run the generated bootstrap script
chmod +x output/secrets-bootstrap.sh
./output/secrets-bootstrap.sh
```

Or manually:

```bash
aws secretsmanager update-secret \
  --secret-id etl/analytics_db/config \
  --secret-string "$(cat <<EOF
{
  "snowflake_account": "YOUR-ACCOUNT",
  "snowflake_user": "SVC_LOADER_ANALYTICS",
  "snowflake_private_key": "$(cat output/keys/svc_loader_analytics_private.pem)",
  "snowflake_role": "DATA_LOADER_ANALYTICS",
  "snowflake_warehouse": "LOADING_WH",
  "snowflake_database": "ANALYTICS_DB",
  "snowflake_schema": "STAGING",
  "snowflake_stage": "S3_LOAD_STAGE",
  "admin_database": "ADMIN_DB",
  "admin_schema": "_PIPELINE"
}
EOF
)"
```

### 3.5 S3 Event Notifications

Configure the S3 bucket to trigger Lambda on file uploads:

```bash
aws s3api put-bucket-notification-configuration \
  --bucket your-bucket \
  --notification-configuration file://output/s3-notifications.json
```

---

## 4. RSA Key Generation & Rotation

### Initial Generation

```bash
# Using the generator
python3 generate.py
# Keys are saved to output/keys/svc_loader_analytics_private.pem (mode 0600)
#                    output/keys/svc_loader_analytics_public.pem

# Or manually with OpenSSL
openssl genrsa -out svc_loader_analytics_private.pem 2048
openssl rsa -in svc_loader_analytics_private.pem -pubout -out svc_loader_analytics_public.pem
chmod 600 svc_loader_analytics_private.pem
```

### Extracting the Public Key Body for Snowflake

Snowflake wants the public key body **without** `-----BEGIN/END PUBLIC KEY-----` headers:

```bash
grep -v "^-----" svc_loader_analytics_public.pem | tr -d '\n'
```

### Key Rotation Procedure

Snowflake supports two RSA keys per user, enabling zero-downtime rotation:

```sql
-- Step 1: Set the NEW key as RSA_PUBLIC_KEY_2
ALTER USER SVC_LOADER_ANALYTICS SET RSA_PUBLIC_KEY_2 = '<new-public-key-body>';

-- Step 2: Update Secrets Manager with the new private key
-- (use aws secretsmanager update-secret or the bootstrap script)

-- Step 3: Verify the new key works by running a test load

-- Step 4: Promote the new key to primary
ALTER USER SVC_LOADER_ANALYTICS SET RSA_PUBLIC_KEY = '<new-public-key-body>';

-- Step 5: Remove the old key from slot 2
ALTER USER SVC_LOADER_ANALYTICS UNSET RSA_PUBLIC_KEY_2;
```

### Rotation Schedule

Recommended: Rotate every 90 days. Set a calendar reminder or automate with a scheduled Lambda.

### Emergency Key Revocation

```sql
-- Immediately disable the user (stops all loads)
ALTER USER SVC_LOADER_ANALYTICS SET DISABLED = TRUE;

-- Generate a new key pair, then re-enable
ALTER USER SVC_LOADER_ANALYTICS SET RSA_PUBLIC_KEY = '<new-key>';
ALTER USER SVC_LOADER_ANALYTICS SET DISABLED = FALSE;
```

---

## 5. Folder Structure Convention

The loader uses **convention over configuration**: the S3 path determines the target table and load behavior.

### Path Format

```
s3://<bucket>/<prefix>/<TABLE_NAME>.<MODE>/filename.ext
```

### Components

| Component | Description | Example |
|---|---|---|
| `<bucket>` | S3 bucket | `etl-loader-test-pickybat` |
| `<prefix>` | Routes to a database (configured in `config.yaml`) | `analytics/` |
| `<TABLE_NAME>` | Becomes the Snowflake table name (uppercased) | `DAILY_REVENUE` |
| `<MODE>` | Load mode suffix | `.T`, `.A`, `.M` |
| `filename.ext` | Any filename; extension determines format | `2024-01-15.csv` |

### Load Modes

| Suffix | Mode | Behavior |
|---|---|---|
| `.T` (or none) | TRUNCATE | Delete all rows, then load. **Default.** |
| `.A` | APPEND | Add rows to existing data |
| `.M` | MERGE | Upsert based on merge keys (from control table) |

### Supported File Formats

| Extension | Format | Auto-detected |
|---|---|---|
| `.csv` | CSV | Delimiter, quoting, header |
| `.tsv` | TSV (tab-delimited) | Yes |
| `.json` | JSON | Array vs. object |
| `.jsonl`, `.ndjson` | JSON Lines | Yes |
| `.parquet` | Parquet | Magic bytes `PAR1` |
| `.avro` | Avro | Magic bytes `Obj\x01` |
| `.orc` | ORC | Magic bytes `ORC` |

### Compression

Append the compression extension: `.gz`, `.bz2`, `.zst`

Example: `ORDERS.A/2024-Q1.csv.gz` — append gzipped CSV to ORDERS table.

### Full Examples

```
s3://etl-loader-test-pickybat/
└── analytics/
    ├── DAILY_REVENUE.T/           → TRUNCATE into ANALYTICS_DB.STAGING.DAILY_REVENUE
    │   └── 2024-01-15.csv
    ├── DAILY_REVENUE.A/           → APPEND into ANALYTICS_DB.STAGING.DAILY_REVENUE
    │   └── 2024-01-16.csv
    ├── USER_EVENTS/               → TRUNCATE (default) into ANALYTICS_DB.STAGING.USER_EVENTS
    │   └── events.json
    ├── PRODUCTS/
    │   └── catalog.parquet
    └── LARGE_DATASET.A/
        ├── part-001.csv.gz        → Append compressed CSV (multiple files)
        ├── part-002.csv.gz
        └── part-003.csv.gz
```

### Nested Prefixes

You can organize files into subfolders — only the **last folder** before the filename is parsed:

```
analytics/team-a/SALES_DATA.T/quarterly/2024-Q1.csv
                 ↑ table name + mode    ↑ ignored (just a filename path)
```

---

## 6. Troubleshooting

### "Storage integration not found" or stage errors

```sql
-- Verify the integration exists
SHOW INTEGRATIONS;
SHOW STAGES IN SCHEMA ANALYTICS_DB.STAGING;

-- Re-describe to get the IAM ARN and External ID
DESC INTEGRATION S3_INT_ANALYTICS;
```

Ensure the AWS IAM role trust policy uses the exact `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` values.

### "Access denied" when listing stage

The Snowflake IAM user must be able to assume the AWS role. Check:
1. The trust policy on `etl-analytics-db-snowflake-role` references the correct principal
2. The external ID matches exactly
3. The S3 bucket policy doesn't explicitly deny cross-account access

```bash
# Test AWS-side permissions
aws sts assume-role \
  --role-arn arn:aws:iam::YOUR-AWS-ACCOUNT-ID:role/etl-analytics-db-snowflake-role \
  --role-session-name test \
  --external-id 'YOUR-EXTERNAL-ID'
```

### RSA key authentication failures

```sql
-- Check the user's key fingerprint
DESC USER SVC_LOADER_ANALYTICS;
-- Look for RSA_PUBLIC_KEY_FP
```

Compare with the local key:
```bash
openssl rsa -pubin -in output/keys/svc_loader_analytics_public.pem -outform DER | \
  openssl dgst -sha256 -binary | openssl enc -base64
```

The fingerprint should match (after the `SHA256:` prefix).

### Lambda not triggering

1. Check S3 event notification configuration:
```bash
aws s3api get-bucket-notification-configuration --bucket your-bucket
```

2. Check Lambda permissions:
```bash
aws lambda get-policy --function-name etl-analytics-db-loader
```

3. Check CloudWatch Logs:
```bash
aws logs filter-log-events \
  --log-group-name /aws/lambda/etl-analytics-db-loader \
  --start-time $(date -d '1 hour ago' +%s000)
```

### COPY INTO loads 0 rows

Common causes:
- **Wrong delimiter detected:** Check `FILE_FORMAT_USED` in LOAD_HISTORY. Override via CONTROL_TABLE if needed.
- **Header row counted as data:** Ensure `SKIP_HEADER = 1` for CSV files with headers.
- **File already loaded:** Snowflake tracks loaded files by name. Use `FORCE = TRUE` in COPY_OPTIONS to reload, or rename the file.
- **Empty file:** Files with 0 bytes are silently skipped.

```sql
-- Check recent loads
SELECT * FROM ADMIN_DB._PIPELINE.LOAD_HISTORY
ORDER BY STARTED_AT DESC
LIMIT 10;

-- Check errors for a specific load
SELECT * FROM ADMIN_DB._PIPELINE.LOAD_ERRORS
WHERE LOAD_ID = '<load-id>'
ORDER BY ERROR_LINE;
```

### Warehouse not starting

```sql
-- Check warehouse status
SHOW WAREHOUSES LIKE 'LOADING_WH';

-- Manually resume
ALTER WAREHOUSE LOADING_WH RESUME;
```

If `AUTO_RESUME = TRUE` and it still doesn't start, check that the role has `USAGE` on the warehouse.

### Secret not found in Secrets Manager

```bash
# List secrets
aws secretsmanager list-secrets --filter Key=name,Values=etl/

# Verify the secret exists and is readable
aws secretsmanager get-secret-value --secret-id etl/analytics_db/config
```

Ensure the Lambda's IAM role has `secretsmanager:GetSecretValue` permission on the specific secret ARN.

---

## 7. Duplicate Detection & Idempotency

The loader detects duplicate file uploads by checking LOAD_HISTORY for matching S3 ETag + file path before each load. Behavior varies by load mode:

### How It Works

Before COPY INTO, the handler queries:

```sql
SELECT LOAD_ID FROM ADMIN_DB._PIPELINE.LOAD_HISTORY
WHERE S3_ETAG = '<etag>' AND S3_KEY = '<s3_key>'
AND STATUS IN ('SUCCESS', 'PARTIAL')
ORDER BY STARTED_AT DESC LIMIT 1;
```

If a match is found, the `DUPLICATE_OF` column in LOAD_HISTORY is set to the original load's LOAD_ID.

### Behavior by Load Mode

| Mode | Duplicate Behavior | Rationale |
|---|---|---|
| **TRUNCATE** (`.T`) | Proceed normally | TRUNCATE is inherently idempotent — the table is wiped and reloaded, so loading the same file twice produces the same result. |
| **APPEND** (`.A`) | Proceed with WARNING | APPEND will create duplicate rows if the same file is loaded twice. The loader proceeds but logs a `WARNING` and marks `DUPLICATE_OF` in LOAD_HISTORY. Operators should review duplicates. |
| **MERGE** (`.M`) | Proceed normally | MERGE is naturally idempotent — upsert logic means the same data applied twice produces the same final state. |

### LOAD_HISTORY Schema Addition

The `DUPLICATE_OF` column is added to LOAD_HISTORY:

```sql
ALTER TABLE ADMIN_DB._PIPELINE.LOAD_HISTORY
ADD COLUMN IF NOT EXISTS DUPLICATE_OF VARCHAR(36);

COMMENT ON COLUMN ADMIN_DB._PIPELINE.LOAD_HISTORY.DUPLICATE_OF IS
    'LOAD_ID of the original load if this file (same ETag + path) was previously loaded. NULL if first load.';
```

### Querying for Duplicates

```sql
-- Find all duplicate loads
SELECT h1.LOAD_ID, h1.S3_KEY, h1.LOAD_MODE, h1.STARTED_AT,
       h1.DUPLICATE_OF, h2.STARTED_AT AS ORIGINAL_LOADED_AT
FROM ADMIN_DB._PIPELINE.LOAD_HISTORY h1
JOIN ADMIN_DB._PIPELINE.LOAD_HISTORY h2
  ON h1.DUPLICATE_OF = h2.LOAD_ID
ORDER BY h1.STARTED_AT DESC;

-- Count duplicates by table
SELECT TARGET_TABLE, COUNT(*) AS DUPLICATE_COUNT
FROM ADMIN_DB._PIPELINE.LOAD_HISTORY
WHERE DUPLICATE_OF IS NOT NULL
GROUP BY TARGET_TABLE
ORDER BY DUPLICATE_COUNT DESC;
```

### Why Not Block Duplicates?

The loader **warns** but does not **block** duplicate loads because:

1. **S3 events can be delivered more than once** — S3 event notifications are "at least once," so the same file can trigger multiple Lambda invocations
2. **ETags change on re-upload** — if a user re-uploads the same filename with different content, the ETag will differ and it won't be detected as a duplicate
3. **TRUNCATE and MERGE are safe** — only APPEND produces actual duplicate data, and the warning + DUPLICATE_OF column gives operators visibility
4. **Blocking would require distributed locking** — preventing concurrent duplicate loads would need DynamoDB or similar, adding complexity for marginal benefit

---

## 8. Network Policies (Production Security)

Network policies restrict which IP addresses can authenticate to Snowflake. Use them alongside PATs and RSA key auth for defense-in-depth.

### Per-User Policy (Recommended)

Apply policies to individual users rather than account-wide. This lets you lock down human admin access without affecting the service user (which uses RSA key auth from Lambda's rotating IPs).

```sql
-- Create the policy with your known IPs
CREATE OR REPLACE NETWORK POLICY admin_access_policy
  ALLOWED_IP_LIST = ('YOUR.HOME.IP', 'YOUR.EC2.IP')
  COMMENT = 'Admin access from known locations';

-- Apply to human user only
ALTER USER YOUR_USERNAME SET NETWORK_POLICY = admin_access_policy;
```

> **Find your IPs**: `curl -s ifconfig.me` from each location.

### Why Per-User > Account-Level

| Approach | Pros | Cons |
|---|---|---|
| **Per-user** | Service user unaffected, granular control | Must set per user |
| **Account-level** | One policy for everything | Blocks Lambda (rotating IPs), forces VPC+NAT ($32/mo) |

### Service User (No Network Policy Needed)

The `SVC_LOADER_*` user authenticates via RSA key pairs — no password, no PAT. Lambda runs on AWS's public IP pool which rotates constantly. Since RSA key auth is already strong (2048-bit minimum), adding a network policy to the service user would require putting Lambda in a VPC with a NAT Gateway (~$32/month) for minimal security gain.

### Removing a Policy

```sql
-- Remove from user
ALTER USER YOUR_USERNAME UNSET NETWORK_POLICY;

-- Drop the policy
DROP NETWORK POLICY admin_access_policy;
```

### Updating Allowed IPs

```sql
-- Add or change IPs (replaces the full list)
ALTER NETWORK POLICY admin_access_policy
  SET ALLOWED_IP_LIST = ('NEW.IP.1', 'NEW.IP.2', 'OLD.IP.STILL.VALID');
```

> **Home IP changed?** If you get locked out, log into Snowflake from a different network (mobile hotspot) or contact Snowflake support. This is why per-user policies are safer than account-level — you can always create a new admin user to recover.

---

## 9. Verification Checklist

After completing setup, verify each component is working:

```bash
# 1. AWS - Lambda exists and is configured
aws lambda get-function --function-name etl-YOUR_DB-loader --query 'Configuration.[FunctionName,Runtime,State]'

# 2. AWS - S3 event notification is set
aws s3api get-bucket-notification-configuration --bucket YOUR-BUCKET

# 3. AWS - Secrets Manager has credentials
aws secretsmanager get-secret-value --secret-id etl/YOUR_DB/config --query 'Name'

# 4. Snowflake - All objects exist (run with PAT or password)
python3 scripts/verify_setup.py

# 5. End-to-end test - upload a CSV and check Snowflake
echo "id,name,value" > /tmp/test.csv
echo "1,test,100" >> /tmp/test.csv
aws s3 cp /tmp/test.csv s3://YOUR-BUCKET/YOUR-PREFIX/TEST_TABLE.T/test.csv

# Wait 10 seconds for Lambda to fire
sleep 10

# Check Lambda logs
aws logs tail /aws/lambda/etl-YOUR_DB-loader --since 1m

# Check Snowflake (via PAT)
export SNOWFLAKE_TOKEN='your-pat'
python3 -c "
import snowflake.connector, os
conn = snowflake.connector.connect(
    account=os.environ.get('SNOWFLAKE_ACCOUNT','YOUR-ACCOUNT'),
    user=os.environ.get('SNOWFLAKE_USER','YOUR-USER'),
    token=os.environ['SNOWFLAKE_TOKEN'],
    authenticator='programmatic_access_token',
    warehouse='COMPUTE_WH', role='ACCOUNTADMIN'
)
cur = conn.cursor()
cur.execute('SELECT * FROM YOUR_DB.STAGING.TEST_TABLE')
print(cur.fetchall())
"
```

### Expected Results

| Check | Expected |
|---|---|
| Lambda function | State = `Active`, Runtime = `python3.12` |
| S3 notifications | `LambdaFunctionArn` pointing to your loader |
| Secrets Manager | Secret exists with Snowflake credentials |
| `verify_setup.py` | All checks PASS |
| End-to-end CSV | Row appears in `TEST_TABLE`, `LOAD_HISTORY` has a SUCCESS entry |
