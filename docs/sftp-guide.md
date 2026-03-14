# SFTP Gateway Guide

Upload files to the S3-to-Snowflake pipeline via SFTP using AWS Transfer Family.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Connecting via SFTP](#2-connecting-via-sftp)
3. [Folder Structure & Naming](#3-folder-structure--naming)
4. [User Provisioning](#4-user-provisioning)
5. [Security Considerations](#5-security-considerations)
6. [Troubleshooting](#6-troubleshooting)

---

## 1. Overview

The SFTP gateway provides a standard file transfer interface for uploading data into the ETL pipeline. Files uploaded via SFTP land in S3, which triggers the Lambda loader to process them into Snowflake.

```
SFTP Client → AWS Transfer Family → S3 Bucket → Lambda → Snowflake
```

**Key facts:**
- Protocol: SFTP (SSH File Transfer Protocol) over port 22
- Authentication: SSH key pairs only (no passwords)
- Each user is scoped to a specific S3 prefix (e.g., `analytics/`)
- Files are immediately available for processing once the upload completes
- Managed service: no servers to patch or maintain

---

## 2. Connecting via SFTP

### Prerequisites

You need three items from your administrator:
1. **SFTP endpoint** — hostname like `s-1234567890abcdef0.server.transfer.us-east-1.amazonaws.com`
2. **Username** — e.g., `analytics-upload`
3. **Private SSH key** — a `.pem` or ED25519 key file

### Command-Line (sftp)

```bash
# Connect interactively
sftp -i /path/to/private_key analytics-upload@s-1234567890abcdef0.server.transfer.us-east-1.amazonaws.com

# Once connected:
sftp> ls                          # List folders
sftp> mkdir CUSTOMERS.T           # Create table folder (TRUNCATE mode)
sftp> cd CUSTOMERS.T
sftp> put customers.csv           # Upload file
sftp> bye

# One-liner upload:
sftp -i /path/to/private_key analytics-upload@endpoint <<< 'put customers.csv CUSTOMERS.T/customers.csv'

# Upload a directory of files:
sftp -i /path/to/private_key analytics-upload@endpoint <<EOF
mkdir ORDERS.A
put -r ./daily_orders/* ORDERS.A/
EOF
```

### WinSCP (Windows)

1. **Open WinSCP** → New Site
2. **File protocol:** SFTP
3. **Host name:** `s-1234567890abcdef0.server.transfer.us-east-1.amazonaws.com`
4. **Port:** 22
5. **User name:** `analytics-upload`
6. Click **Advanced** → **SSH** → **Authentication**
7. **Private key file:** Browse to your `.ppk` file
   - If you have a `.pem` or OpenSSH key, WinSCP offers to convert it to `.ppk` format
8. Click **Login**

### FileZilla

1. **Edit** → **Settings** → **SFTP** → **Add key file** → select your private key
   - FileZilla will prompt to convert non-`.ppk` keys
2. **File** → **Site Manager** → **New Site**
3. **Protocol:** SFTP
4. **Host:** `s-1234567890abcdef0.server.transfer.us-east-1.amazonaws.com`
5. **Port:** 22
6. **Logon Type:** Key file
7. **User:** `analytics-upload`
8. **Key file:** select your private key
9. Click **Connect**

### Programmatic (Python with paramiko)

```python
import paramiko

transport = paramiko.Transport(("s-xxx.server.transfer.us-east-1.amazonaws.com", 22))
pkey = paramiko.Ed25519Key.from_private_key_file("/path/to/private_key")
transport.connect(username="analytics-upload", pkey=pkey)

sftp = paramiko.SFTPClient.from_transport(transport)
sftp.put("local_file.csv", "/CUSTOMERS.T/local_file.csv")
sftp.close()
transport.close()
```

### Programmatic (Bash with scp/rsync)

```bash
# SCP (single file)
scp -i /path/to/key local_file.csv analytics-upload@endpoint:/CUSTOMERS.T/

# Note: rsync is NOT supported by AWS Transfer Family.
# Use sftp batch mode instead for multi-file transfers.
```

---

## 3. Folder Structure & Naming

The SFTP server mirrors the S3 folder convention. Your home directory maps to your S3 prefix (e.g., `analytics/`). Within it, create table folders:

### Folder Format

```
/<TABLE_NAME>.<MODE>/filename.ext
```

### Load Modes

| Folder Suffix | Mode | Behavior |
|---|---|---|
| `.T` (or no suffix) | TRUNCATE | Delete all rows, then load fresh. **Default.** |
| `.A` | APPEND | Add rows to existing data |
| `.M` | MERGE | Upsert based on merge keys (configured in control table) |

### File Formats

| Extension | Format | Notes |
|---|---|---|
| `.csv` | CSV | Auto-detects delimiter, quoting, header |
| `.tsv` | TSV | Tab-delimited (treated as CSV) |
| `.json` | JSON | JSON objects or arrays |
| `.jsonl` / `.ndjson` | JSON Lines | One JSON object per line |
| `.parquet` | Parquet | Best for typed/large datasets |
| `.avro` | Avro | Schema-embedded format |
| `.orc` | ORC | Columnar format |

### Compression

Append `.gz`, `.bz2`, or `.zst` to the filename. Snowflake decompresses automatically.

Example: `ORDERS.A/2024-Q1.csv.gz` — append gzipped CSV to ORDERS table.

### Examples

```
/                                   ← Your home directory (maps to s3://bucket/analytics/)
├── CUSTOMERS.T/                    ← Truncate+reload CUSTOMERS table
│   └── customers_2024.csv
├── ORDERS.A/                       ← Append to ORDERS table
│   ├── orders_jan.csv.gz
│   ├── orders_feb.csv.gz
│   └── orders_mar.csv.gz
├── PRODUCTS/                       ← No suffix = TRUNCATE (default)
│   └── catalog.parquet
└── USER_EVENTS.A/                  ← Append to USER_EVENTS
    └── events.jsonl
```

### Rules

1. **Table name** = folder name (uppercased), minus the mode suffix
2. **One table per folder** — don't mix table data in one folder
3. **Filenames don't matter** for routing — only the folder name determines the target table
4. **Multiple files** in a folder are each loaded independently (each triggers one Lambda)
5. **Nested subfolders** within a table folder are fine — only the parent folder matters

---

## 4. User Provisioning

### Automated (Recommended)

Use the provisioning script to create a new SFTP user with all required AWS resources:

```bash
python scripts/create_sftp_user.py \
    --server-id s-1234567890abcdef0 \
    --username analytics-upload \
    --s3-prefix analytics/ \
    --bucket etl-loader-test-pickybat
```

This script:
1. Generates an ED25519 SSH key pair (saved to `output/sftp-keys/`)
2. Creates an IAM role scoped to the user's S3 prefix
3. Registers the user with the Transfer Family server
4. Imports the SSH public key
5. Prints connection instructions

**Options:**

| Flag | Description |
|---|---|
| `--server-id` | Transfer Family server ID (required) |
| `--username` | SFTP username (required) |
| `--s3-prefix` | S3 prefix the user can access, e.g., `analytics/` (required) |
| `--bucket` | S3 bucket name (required) |
| `--key-dir` | Output directory for keys (default: `output/sftp-keys/`) |
| `--region` | AWS region (default: from AWS config) |
| `--dry-run` | Generate keys only, skip AWS resource creation |

### Manual Provisioning

If you need to create a user without the script:

```bash
# 1. Generate SSH key pair
ssh-keygen -t ed25519 -f sftp_user_key -N "" -C "sftp-myuser@etl-loader"

# 2. Create IAM role (see templates/sftp-server.yaml.j2 for policy template)
aws iam create-role \
    --role-name etl-sftp-myuser-role \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"transfer.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

# Attach S3 policy (scoped to prefix)
aws iam put-role-policy \
    --role-name etl-sftp-myuser-role \
    --policy-name sftp-s3-myuser \
    --policy-document file://user-s3-policy.json

# 3. Wait for IAM propagation
sleep 10

# 4. Create Transfer Family user
aws transfer create-user \
    --server-id s-1234567890abcdef0 \
    --user-name myuser \
    --role arn:aws:iam::152924643524:role/etl-sftp-myuser-role \
    --home-directory-type LOGICAL \
    --home-directory-mappings '[{"Entry":"/","Target":"/etl-loader-test-pickybat/analytics/"}]'

# 5. Import SSH public key
aws transfer import-ssh-public-key \
    --server-id s-1234567890abcdef0 \
    --user-name myuser \
    --ssh-public-key-body "$(cat sftp_user_key.pub)"
```

### Adding Keys to Existing Users

A user can have multiple SSH keys (e.g., for different machines):

```bash
aws transfer import-ssh-public-key \
    --server-id s-1234567890abcdef0 \
    --user-name analytics-upload \
    --ssh-public-key-body "ssh-ed25519 AAAA... second-key"
```

### Revoking a User

```bash
# List the user's SSH keys
aws transfer list-users --server-id s-1234567890abcdef0

# Delete specific SSH key
aws transfer delete-ssh-public-key \
    --server-id s-1234567890abcdef0 \
    --user-name analytics-upload \
    --ssh-public-key-id key-xxxxxxxxxxxx

# Or delete the user entirely
aws transfer delete-user \
    --server-id s-1234567890abcdef0 \
    --user-name analytics-upload
```

---

## 5. Security Considerations

### Authentication

- **SSH keys only** — password authentication is disabled at the server level
- **ED25519 keys** are generated by default (faster, more secure than RSA for SSH)
- Private keys should be treated as credentials — never share via email or Slack
- Recommend distributing keys via a secure channel (encrypted email, password manager, or physical USB)

### Authorization (Least Privilege)

- Each SFTP user has an IAM role that **only** permits access to their assigned S3 prefix
- Users **cannot** access other prefixes, other buckets, or any AWS services beyond S3
- The `ListBucket` permission is restricted via IAM condition to the user's prefix
- Logical home directory mapping means users see `/` but are actually scoped to `s3://bucket/prefix/`

### Network

- **Public endpoint** is accessible from the internet on port 22
- For production workloads, consider:
  - **VPC endpoint** to restrict access to your network
  - **Security groups** to allowlist specific IP ranges
  - **AWS Network Firewall** or **NACLs** for additional filtering
- The Transfer Family server uses the `TransferSecurityPolicy-2024-01` policy, which enforces:
  - TLS 1.2+ for in-transit encryption
  - Strong cipher suites only (no CBC, no MD5)

### Monitoring & Audit

- All SFTP connections are logged to **CloudWatch Logs** (`/aws/transfer/s-*`)
- Log entries include: username, source IP, actions (login, upload, download, delete)
- S3 access can be additionally logged via **S3 Access Logs** or **CloudTrail Data Events**
- The Lambda handler records all file loads in **LOAD_HISTORY** (Snowflake)

### Key Rotation

- Rotate SFTP keys periodically (recommended: every 90 days)
- To rotate without downtime:
  1. Generate a new key pair
  2. Import the new public key (`aws transfer import-ssh-public-key`)
  3. Distribute the new private key to the user
  4. Confirm the new key works
  5. Delete the old public key (`aws transfer delete-ssh-public-key`)

### Data in Transit

- SFTP encrypts all data in transit via SSH
- S3 storage uses server-side encryption (SSE-S3 or SSE-KMS depending on bucket config)
- No unencrypted protocols are exposed (no FTP, no FTPS)

---

## 6. Troubleshooting

### "Permission denied (publickey)"

- Verify you're using the correct private key: `sftp -i /correct/path/to/key user@endpoint`
- Verify the public key was imported: `aws transfer list-users --server-id s-xxx`
- Check that the key format matches (ED25519 or RSA — both are supported)

### "Connection refused" or timeout

- Verify the server is ONLINE: `aws transfer describe-server --server-id s-xxx`
- For VPC endpoints: check security groups allow inbound port 22 from your IP
- For public endpoints: check your local firewall/corporate proxy allows outbound port 22

### "No such file or directory" when uploading

- Create the table folder first: `mkdir CUSTOMERS.T` then `cd CUSTOMERS.T` then `put file.csv`
- Folder names are case-sensitive in SFTP but the loader uppercases them for Snowflake

### Files uploaded but not loading into Snowflake

- Verify S3 event notifications are configured: `aws s3api get-bucket-notification-configuration --bucket your-bucket`
- Check CloudWatch Logs for Lambda errors: `/aws/lambda/etl-*-loader`
- Verify the file has a supported extension (`.csv`, `.json`, `.parquet`, etc.)
- Check LOAD_HISTORY in Snowflake for the file's status

### WinSCP: "Cannot convert key"

- WinSCP requires keys in `.ppk` format
- Open PuTTYgen → **Conversions** → **Import key** → select your `.pem` key → **Save private key**
- Or let WinSCP auto-convert when you first select the key

### Slow uploads

- AWS Transfer Family throughput depends on the endpoint type:
  - **Public:** shared bandwidth, best for moderate workloads
  - **VPC:** dedicated bandwidth via your VPC's internet gateway or Direct Connect
- For very large files (multi-GB), consider direct S3 upload via `aws s3 cp` with multipart
- SFTP has inherent overhead vs. raw S3 APIs due to the SSH protocol layer
