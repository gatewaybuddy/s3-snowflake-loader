# File Validation Guide

The pipeline validates every file **before** loading into Snowflake. Bad files are rejected early with clear error messages, saving warehouse compute and preventing junk data from entering your tables.

---

## Table of Contents

1. [Supported File Types](#1-supported-file-types)
2. [Validation Checks](#2-validation-checks)
3. [PGP Encryption](#3-pgp-encryption)
4. [Size Limits](#4-size-limits)
5. [Error Messages Reference](#5-error-messages-reference)
6. [Configuration](#6-configuration)

---

## 1. Supported File Types

### Text Formats

| Format | Extensions | Validation |
|---|---|---|
| CSV | `.csv`, `.tsv`, `.txt` | Delimiter detection, column consistency, header detection |
| JSON | `.json` | Parse validation (valid JSON object or array) |
| JSON Lines | `.jsonl`, `.ndjson` | Per-line parse validation |

### Binary Formats

| Format | Extensions | Validation |
|---|---|---|
| Parquet | `.parquet` | Magic bytes check (`PAR1`) |
| Avro | `.avro` | Magic bytes check (`Obj\x01`) |
| ORC | `.orc` | Magic bytes check (`ORC`) |

### Compression (Passthrough)

| Compression | Extensions | Behavior |
|---|---|---|
| GZIP | `.gz` | Snowflake decompresses during COPY INTO |
| BZ2 | `.bz2` | Snowflake decompresses during COPY INTO |
| ZSTD | `.zst` | Snowflake decompresses during COPY INTO |

Compressed files are **not** decompressed for validation — Snowflake handles decompression natively during `COPY INTO`. The validator only notes the compression type in metadata.

### Encrypted Formats

| Format | Extensions | Behavior |
|---|---|---|
| PGP | `.pgp`, `.gpg`, `.asc` | Auto-detected, decrypted using key from Secrets Manager |

---

## 2. Validation Checks

Validations run in this order. The first failure stops processing.

### 2.1 Empty File Detection

**What:** Files with 0 bytes are rejected immediately.

**Why:** Empty files waste Lambda invocation time and produce confusing 0-row COPY INTO results.

**Error:** `EMPTY_FILE` — "File is empty (0 bytes). No data to load."

### 2.2 Size Limit Enforcement

**What:** Files exceeding the configured maximum size are rejected.

**Default limit:** 5 GB (configurable via `MAX_FILE_SIZE_BYTES` environment variable).

**Why:** Prevents runaway loads that could exhaust Lambda's 15-minute timeout or `/tmp` storage (10 GB). Lambda can handle large files via S3 streaming, but extremely large files may be better processed via Snowpipe or direct COPY INTO from Snowflake.

**Error:** `FILE_TOO_LARGE` — "File size (X GB) exceeds maximum allowed size (Y GB)."

### 2.3 PGP Detection

**What:** Files encrypted with PGP/GPG are detected by magic bytes (binary format) or ASCII armor headers (`-----BEGIN PGP MESSAGE-----`).

**Behavior:** If PGP is detected, the file is routed to the PGP decryption handler. The decrypted cleartext is then re-validated and loaded normally.

**Not an error** — PGP-encrypted files are valid, they just require an extra decryption step.

### 2.4 Compression Passthrough

**What:** GZIP, BZ2, and ZSTD compression are detected via magic bytes.

**Behavior:** The compression type is noted in metadata, and the file is passed through to Snowflake without decompression. Snowflake's `COPY INTO` decompresses automatically using the `COMPRESSION = AUTO` option.

**Not an error** — compressed files are expected and handled natively.

### 2.5 Format Sniffing

**What:** The file content is inspected to verify it matches the expected format (from the file extension).

| Format | Validation Method |
|---|---|
| CSV | Checks for consistent column counts, valid delimiter detection, non-empty content |
| JSON | Attempts to parse as JSON object, array, or JSON Lines |
| Parquet | Checks for `PAR1` magic bytes at file start |
| Avro | Checks for `Obj\x01` magic bytes |
| ORC | Checks for `ORC` magic bytes |

**Errors:**
- `INVALID_JSON` — File has `.json` extension but content is not valid JSON
- `FORMAT_MISMATCH` — File extension says one format but content is another (e.g., `.parquet` file that's actually CSV)
- `INVALID_FORMAT` — File doesn't have expected magic bytes

### 2.6 Poison File Detection

**What:** Files that are unrecognized binary (not Parquet, Avro, ORC, compressed, or PGP) are rejected.

**Detection method:** High ratio (>30%) of non-printable characters in the first 4 KB of content.

**Why:** Prevents loading random binary data (executables, images, PDFs, etc.) into Snowflake tables where it would appear as garbled text.

**Error:** `POISON_FILE` — "File appears to be binary data disguised as a text file."

---

## 3. PGP Encryption

### How It Works

1. Upload a PGP-encrypted file to S3 (via SFTP or direct upload)
2. The validator detects PGP encryption automatically
3. The Lambda loads the PGP private key from AWS Secrets Manager
4. The file is decrypted to Lambda's `/tmp` directory
5. The cleartext file is validated and loaded into Snowflake
6. The cleartext file is cleaned up from `/tmp`

### Key Format

The PGP private key is stored in AWS Secrets Manager as a JSON secret:

```json
{
    "pgp_private_key": "-----BEGIN PGP PRIVATE KEY BLOCK-----\n...\n-----END PGP PRIVATE KEY BLOCK-----",
    "pgp_passphrase": "optional-passphrase-for-the-key"
}
```

### Setup

```bash
# 1. Generate a PGP key pair (if you don't have one)
gpg --batch --gen-key <<EOF
Key-Type: RSA
Key-Length: 4096
Name-Real: ETL Pipeline
Name-Email: etl@your-company.com
Expire-Date: 0
%no-protection
%commit
EOF

# 2. Export the private key
gpg --armor --export-secret-keys etl@your-company.com > etl-pgp-private.asc

# 3. Export the public key (share this with file uploaders)
gpg --armor --export etl@your-company.com > etl-pgp-public.asc

# 4. Store the private key in Secrets Manager
aws secretsmanager create-secret \
    --name etl/pgp/key \
    --secret-string "$(jq -n \
        --arg key "$(cat etl-pgp-private.asc)" \
        --arg pass "" \
        '{pgp_private_key: $key, pgp_passphrase: $pass}')"

# 5. Set the Lambda environment variable
# Add PGP_SECRET_NAME=etl/pgp/key to the Lambda's environment
```

### Sharing the Public Key

Give file uploaders the **public key** so they can encrypt files before uploading:

```bash
# Uploader encrypts a file
gpg --recipient etl@your-company.com --output data.csv.pgp --encrypt data.csv

# Upload the encrypted file
aws s3 cp data.csv.pgp s3://bucket/analytics/CUSTOMERS.T/data.csv.pgp
# Or via SFTP:
sftp> put data.csv.pgp CUSTOMERS.T/data.csv.pgp
```

### Supported PGP Formats

| Format | Detected By |
|---|---|
| ASCII-armored (`.asc`, `.pgp`) | `-----BEGIN PGP MESSAGE-----` header |
| Binary PGP (`.gpg`) | PGP packet tag bytes |

### Lambda Requirements

- Add `python-gnupg` to `lambda/loader/requirements.txt`
- Lambda needs at least 512 MB memory (GPG operations are memory-intensive)
- Lambda `/tmp` must have enough space for the decrypted file (max 10 GB ephemeral storage)
- Lambda IAM role needs `secretsmanager:GetSecretValue` on the PGP secret

---

## 4. Size Limits

### Default: 5 GB

The default maximum file size is 5 GB. This is well within Lambda's streaming capabilities.

### Adjusting the Limit

Set the `MAX_FILE_SIZE_BYTES` environment variable on the Lambda function:

```bash
# Increase to 10 GB
aws lambda update-function-configuration \
    --function-name etl-analytics-db-loader \
    --environment "Variables={SECRET_NAME=etl/analytics_db/config,S3_PREFIX=analytics/,MAX_FILE_SIZE_BYTES=10737418240}"
```

### Practical Limits

| Component | Limit |
|---|---|
| Lambda timeout | 15 minutes |
| Lambda memory | Up to 10 GB |
| Lambda `/tmp` | 10 GB (for PGP decryption) |
| S3 object size | 5 TB |
| Snowflake COPY INTO | No inherent limit (uses S3 streaming) |

For files larger than 5 GB, consider:
- Splitting the file into smaller chunks before upload
- Using Snowpipe for continuous ingestion
- Running `COPY INTO` directly from Snowflake (bypassing Lambda)

---

## 5. Error Messages Reference

| Error Code | Message | Resolution |
|---|---|---|
| `EMPTY_FILE` | File is empty (0 bytes) | Upload a file with actual data |
| `UNREADABLE` | Could not read file content | Check file permissions and S3 access |
| `FILE_TOO_LARGE` | File size exceeds maximum | Split file or increase `MAX_FILE_SIZE_BYTES` |
| `INVALID_JSON` | File is not valid JSON | Fix JSON syntax errors; validate with `jq . file.json` |
| `FORMAT_MISMATCH` | Extension doesn't match content | Rename file with correct extension |
| `INVALID_FORMAT` | Missing magic bytes for binary format | File may be corrupted; re-export from source |
| `POISON_FILE` | Binary data disguised as text | Upload a valid text file (CSV/JSON) or use correct binary format extension |
| `EMPTY_CONTENT` | File has no data lines | Add data rows below the header |

All validation errors are recorded in `LOAD_HISTORY` with `STATUS = FAILED` and the error message in `ERROR_MESSAGE`.

---

## 6. Configuration

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MAX_FILE_SIZE_BYTES` | `5368709120` (5 GB) | Maximum file size in bytes |
| `PGP_SECRET_NAME` | _(none)_ | Secrets Manager secret with PGP private key |

### Control Table Overrides

The control table (`ADMIN_DB._PIPELINE.CONTROL_TABLE`) can override validation behavior per table:

```sql
-- Disable a specific table (files are skipped, not validated)
UPDATE ADMIN_DB._PIPELINE.CONTROL_TABLE
SET ENABLED = FALSE
WHERE TARGET_TABLE = 'RAW_DUMP';
```

Validation cannot be disabled per-table (it always runs), but the control table's `ENABLED` flag is checked before validation to skip files for disabled tables.
