# Decisions Log

Record of design decisions, reasoning, and outcomes for the S3-to-Snowflake ETL pipeline.

---

## 2026-03-14: Infrastructure Setup

### D1: RSA Key Size — 2048-bit

**Decision:** Use 2048-bit RSA keys for service user authentication.

**Reasoning:** 2048-bit is Snowflake's minimum accepted key size and is considered secure through ~2030. 4096-bit adds no practical benefit for machine-to-machine auth where keys are rotated regularly. Keeps key operations fast in Lambda's constrained environment.

**Alternatives considered:** 4096-bit (slower, no practical security gain for this use case).

---

### D2: Separate LOADING_WH Warehouse

**Decision:** Create a dedicated `LOADING_WH` (X-Small, auto-suspend 60s) rather than reuse `COMPUTE_WH`.

**Reasoning:** Isolation of loading workloads from ad-hoc queries. X-Small is sufficient for COPY INTO which is metadata-intensive, not compute-intensive. 60-second auto-suspend minimizes credit burn during idle periods between file drops. `INITIALLY_SUSPENDED = TRUE` means no credits are consumed until the first load.

**Alternatives considered:** Sharing `COMPUTE_WH` (simpler but no isolation; loading contends with queries).

---

### D3: Admin DB Schema Name — `_PIPELINE`

**Decision:** Use `_PIPELINE` (leading underscore) for the admin schema.

**Reasoning:** The leading underscore visually separates infrastructure schemas from business schemas. It sorts first alphabetically, making it easy to find. The existing generator code already uses this convention.

---

### D4: Storage Integration Role ARN — Pre-declared

**Decision:** Reference an IAM role ARN (`arn:aws:iam::152924643524:role/etl-analytics-db-snowflake-role`) that doesn't exist yet in the storage integration.

**Reasoning:** The storage integration must exist before we can `DESC INTEGRATION` to get the Snowflake-side IAM user ARN and external ID. These values are then used to configure the trust policy on the AWS IAM role. This is the standard chicken-and-egg bootstrap for Snowflake S3 integrations. The IAM role will be created during the CloudFormation deploy phase with the correct trust policy.

**Integration output captured:**
- `STORAGE_AWS_IAM_USER_ARN`: `arn:aws:iam::088253294712:user/wi3k1000-s`
- `STORAGE_AWS_EXTERNAL_ID`: `FIC98367_SFCRole=4_CrOSCRcVKljsfrjg65WVQm5jl78=`

These values are required when creating the AWS IAM role trust policy.

---

### D5: Service User — No Password, RSA-Only

**Decision:** Create `SVC_LOADER_ANALYTICS` with `MUST_CHANGE_PASSWORD = FALSE` and RSA public key. No password is set.

**Reasoning:** Service accounts should authenticate exclusively via RSA key pairs. Password auth is disabled by not setting one. This eliminates password rotation concerns and aligns with Snowflake best practices for programmatic access. The private key is stored in AWS Secrets Manager (at deployment time) and in `output/keys/` locally (gitignored).

---

### D6: LOAD_HISTORY Table — Extended vs. Minimal Schema

**Decision:** The LOAD_HISTORY table includes Lambda-specific fields (LAMBDA_REQUEST_ID, LAMBDA_FUNCTION) alongside the requested fields.

**Reasoning:** The task specified a minimal set of columns (source_file, target_db, target_table, etc.), but the existing handler.py and history_logger.py already write to a richer schema. I kept the full schema from the generator to maintain compatibility with the existing Lambda code. The additional columns (S3_BUCKET, S3_ETAG, TABLE_CREATED, COPY_INTO_QUERY_ID, LAMBDA_REQUEST_ID, LAMBDA_FUNCTION) provide essential operational visibility at no cost.

---

### D7: Stage Naming — S3_LOAD_STAGE (not S3_LOAD_STAGE_ANALYTICS)

**Decision:** The stage is created as `ANALYTICS_DB.STAGING.S3_LOAD_STAGE` rather than having the DB name as a suffix.

**Reasoning:** Each stage lives inside its own database.schema, so the database name in the stage name would be redundant. The existing generator uses `S3_LOAD_STAGE` as the bare name. The stage is scoped by the fully qualified path `ANALYTICS_DB.STAGING.S3_LOAD_STAGE`.

---

### D8: Jinja2 Templates vs. Inline Generation

**Decision:** Create Jinja2 templates in `templates/` alongside the existing inline generator in `generate.py`.

**Reasoning:** The existing `generate.py` builds SQL/YAML/Bash via string concatenation. Jinja2 templates provide: (1) separation of template from logic, (2) easier review of generated artifacts, (3) ability for non-Python users to understand the output format. The generator can be updated later to use these templates; for now both exist.

---

### D9: Grant Strategy — Principle of Least Privilege

**Decision:** Grant only specific privileges needed:
- `USAGE` on warehouse, database, schema
- `CREATE TABLE` on target schema (for auto-create)
- `SELECT, INSERT, TRUNCATE` on current and future tables (for all load modes)
- `SELECT, INSERT, UPDATE` on admin tables (for logging)
- `CREATE STAGE` on target schema (one-time setup)
- `USAGE` on stage (for COPY INTO)

**Reasoning:** The service user/role can only operate within its designated database and the shared admin database. It cannot DROP databases, ALTER warehouses, CREATE users, or access other databases. This limits blast radius if credentials are compromised.

---

### D10: Teardown Template — Revoke Before Drop

**Decision:** The teardown template revokes grants before dropping objects, rather than just using DROP IF EXISTS.

**Reasoning:** Dropping objects without revoking grants can leave orphaned grant records in Snowflake's metadata. While Snowflake handles this gracefully, explicit revocation is cleaner and produces more predictable audit logs. The database/warehouse DROP statements are commented out by default to require explicit opt-in for destructive actions.

---

### D11: Verification Script — Comprehensive, Automated

**Decision:** Created `scripts/verify_setup.py` to validate all 22 infrastructure checks programmatically.

**Reasoning:** Manual verification via Snowflake UI is error-prone and doesn't test RSA authentication end-to-end. The script connects as both ACCOUNTADMIN (to verify object existence) and as the service user (to verify RSA auth and grants work). This catches configuration errors immediately and serves as a regression test for future infrastructure changes.

**Results:** 22/22 checks passed on first run (2026-03-14):
- All databases, schemas, and tables exist
- LOADING_WH is X-Small with correct auto-suspend
- DATA_LOADER_ANALYTICS role has all required grants
- SVC_LOADER_ANALYTICS user authenticates with RSA key (fingerprint: SHA256:o7GAWER9L4JMTINX/Ob1ev5RQ2X6NMeubH0szUdxaeE=)
- Storage integration points to s3://etl-loader-test-pickybat/analytics/
- Service user can read LOAD_HISTORY and create/drop tables in STAGING

---

## 2026-03-14: SFTP Gateway (Task 1)

### D12: AWS Transfer Family — Public Endpoint with VPC Option

**Decision:** Default to a PUBLIC endpoint for the SFTP server, with CloudFormation parameters for optional VPC endpoint.

**Reasoning:** A public endpoint is the simplest to deploy and test. For production workloads that need IP-based access control, the same template supports VPC endpoint deployment by providing `VpcId`, `SubnetIds`, and `SecurityGroupIds` parameters. The security policy `TransferSecurityPolicy-2024-01` enforces TLS 1.2+ and strong ciphers regardless of endpoint type.

**Alternatives considered:** VPC-only (more secure but harder to test, requires VPN or Direct Connect for external partners); API Gateway custom identity provider (more flexible user management but significantly more complex).

---

### D13: SFTP Authentication — SSH Keys Only, No Passwords

**Decision:** Use SERVICE_MANAGED identity provider with SSH key-based auth. Password auth is not configured.

**Reasoning:** SSH keys are the standard for automated file transfer. Passwords are vulnerable to brute-force, require rotation policies, and can't be easily automated. Transfer Family's SERVICE_MANAGED provider stores SSH public keys directly, avoiding the complexity of a custom Lambda authorizer. Each user can have multiple SSH keys for different machines.

---

### D14: SFTP User SSH Key Type — ED25519

**Decision:** Generate ED25519 SSH keys for SFTP users (via `create_sftp_user.py`).

**Reasoning:** ED25519 is faster, shorter (256-bit vs 2048-bit RSA), and more secure against side-channel attacks. AWS Transfer Family supports ED25519 since 2022. The shorter key strings are also easier to handle in scripts and APIs. Note: Snowflake service users still use RSA 2048 (Snowflake requirement) — the ED25519 choice only applies to SFTP SSH keys.

**Alternatives considered:** RSA 2048 (slower, longer keys, legacy); RSA 4096 (even slower, no security benefit over ED25519).

---

### D15: SFTP Home Directory — Logical Mapping to S3 Prefix

**Decision:** Use `HomeDirectoryType: LOGICAL` with a mapping of `/` → `/<bucket>/<prefix>` per user.

**Reasoning:** Logical home directories mean users see a clean `/` root that maps directly to their allowed S3 prefix. This prevents users from navigating to other prefixes or the bucket root. Combined with the IAM role's S3 policy (scoped to the prefix), this provides defense-in-depth: even if a user somehow escapes their home directory, IAM blocks access to other prefixes.

---

### D16: Per-User IAM Roles for SFTP

**Decision:** Each SFTP user gets their own IAM role (`etl-sftp-<username>-role`) scoped to their S3 prefix.

**Reasoning:** Sharing a single IAM role across users would give all users access to all prefixes. Per-user roles enforce least privilege: the `analytics-upload` user can only write to `s3://bucket/analytics/*` and cannot access `reporting/` or any other prefix. The `create_sftp_user.py` script automates role creation so operators don't manually craft policies.

---

## 2026-03-14: File Validation Gate (Task 2)

### D17: Validation Before Snowflake — Fail Fast

**Decision:** Run all file validation checks (empty file, size, format, PGP, poison) BEFORE connecting to Snowflake and executing COPY INTO.

**Reasoning:** Connecting to Snowflake, starting a warehouse, and running COPY INTO on a garbage file wastes credits and produces confusing errors. By validating first, we reject bad files in milliseconds with clear error messages. The only exception is PGP-encrypted files, which need decryption before format validation.

**Tradeoff:** Validation reads the first 64KB of each file from S3 (`GET_OBJECT` with Range header), adding one S3 API call per file. Cost: ~$0.0004 per 1000 files. Worth it for the clarity.

---

### D18: Compressed Files — Passthrough, No Pre-Validation

**Decision:** GZIP/BZ2/ZSTD compressed files are passed through to Snowflake without decompressing for validation.

**Reasoning:** Decompressing in Lambda would require reading the entire file into memory, which could exceed Lambda's memory limits for large files. Snowflake handles decompression natively during COPY INTO and provides its own error messages for corrupt compressed files. The validator only notes the compression type in metadata.

**Alternatives considered:** Streaming decompression for format sniffing (adds complexity, memory risk); requiring senders to upload uncompressed (impractical for large files).

---

### D19: PGP Decryption — python-gnupg with Secrets Manager Key

**Decision:** Use `python-gnupg` for PGP decryption, with the private key stored in AWS Secrets Manager.

**Reasoning:** `python-gnupg` wraps the system `gpg` binary, which handles all OpenPGP standard formats. The private key is stored in Secrets Manager alongside the Snowflake credentials, following the existing credential management pattern. Decrypted cleartext files are written to Lambda's `/tmp` (10GB ephemeral storage) and cleaned up after loading.

**Alternatives considered:**
- `age` (lighter weight, simpler key format, but not PGP-compatible — partners already using PGP would need to change)
- `PGPy` (pure Python, no system dependency, but less mature and slower)
- Pre-decrypt before upload (simplest, but shifts burden to uploaders)

**Note:** `python-gnupg` is optional — if not installed, PGP files produce a clear error telling the operator to install it. This avoids bloating the Lambda package for users who don't need PGP support.

---

### D20: Poison File Detection — Non-Printable Character Ratio

**Decision:** Detect binary garbage by checking the ratio of non-printable characters in the first 4KB. Threshold: >30% non-printable = poison file.

**Reasoning:** Files that aren't recognized binary formats (Parquet, Avro, ORC, compressed) and contain high ratios of non-printable characters are almost certainly not valid CSV, JSON, or text data. Loading them into Snowflake would create garbled VARCHAR columns. The 30% threshold avoids false positives from files with embedded binary fields or UTF-8 multibyte characters.

**Known edge cases:** Files with heavy emoji usage or CJK characters can approach 20% "non-printable" in ASCII terms but stay well below 30%. Latin-1 encoded files with accented characters are handled by the fallback decoder.

---

### D21: File Size Limit — 5GB Default, Configurable

**Decision:** Default maximum file size of 5GB, configurable via `MAX_FILE_SIZE_BYTES` environment variable.

**Reasoning:** Lambda can handle files up to 10GB (its `/tmp` storage limit) and Snowflake COPY INTO can stream from S3 without loading into memory. However, extremely large files risk exceeding Lambda's 15-minute timeout, especially with table auto-creation and format detection overhead. 5GB is a practical default that covers most use cases. Operators can increase to 10GB for specific Lambda functions.

---

### D22: Validation Failures Logged to LOAD_HISTORY

**Decision:** Files that fail validation are still logged to LOAD_HISTORY with `STATUS = FAILED` and the validation error in `ERROR_MESSAGE`.

**Reasoning:** Without logging, validation failures would be invisible to operators monitoring LOAD_HISTORY. By inserting a LOADING record and immediately updating to FAILED, the dashboard shows all attempted loads, including those rejected by validation. The error message includes the error code (e.g., `[POISON_FILE]`, `[EMPTY_FILE]`) for programmatic filtering.

---

## 2026-03-14: Duplicate Detection (Task 3)

### D23: Duplicate Detection — ETag + S3 Key Match

**Decision:** Detect duplicates by querying LOAD_HISTORY for rows with matching `S3_ETAG` AND `S3_KEY` where `STATUS IN ('SUCCESS', 'PARTIAL')`.

**Reasoning:** S3 ETags are content hashes (MD5 for single-part uploads, composite for multipart). Two uploads of the same file to the same path will have the same ETag. Matching on both ETag AND key prevents false positives: the same file content uploaded to different table folders should load independently. Only successful/partial loads count as duplicates — failed loads should be retried.

**Limitations:**
- Multipart upload ETags are not pure content hashes (they include part count), so re-uploading the same file via different methods may produce different ETags
- S3 object versioning can assign different version IDs to the same content

---

### D24: Duplicate Behavior — Warn, Don't Block

**Decision:** Duplicate detection logs warnings and sets `DUPLICATE_OF` but never blocks a load.

**Reasoning:**
1. **S3 events are "at least once"** — duplicate Lambda invocations from the same upload are normal
2. **TRUNCATE is idempotent** — loading the same data into a truncated table produces identical results
3. **MERGE is idempotent** — upsert logic handles duplicate data gracefully
4. **APPEND is the only risky mode** — but blocking would require distributed locking (DynamoDB), adding cost and complexity. The WARNING + DUPLICATE_OF column gives operators enough visibility to clean up duplicates manually if needed

**Alternatives considered:**
- DynamoDB-based locking (prevents concurrent duplicates but adds dependency and cost)
- Snowflake MERGE for all modes (treats all loads as idempotent but slower than COPY INTO)
- Block and fail duplicates (breaks S3's at-least-once delivery guarantee)

---

### D25: DUPLICATE_OF Column — Nullable VARCHAR(36)

**Decision:** Add `DUPLICATE_OF VARCHAR(36)` column to LOAD_HISTORY, referencing the LOAD_ID of the original load.

**Reasoning:** A foreign key to the same table is the simplest way to link duplicate loads to their originals. VARCHAR(36) matches the UUID format of LOAD_ID. NULL means "first load" (not a duplicate). This enables queries like "show me all duplicate loads" and "how many times was this file loaded?" without complex self-joins. The column is added with `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for backwards compatibility.

**Schema change required:**
```sql
ALTER TABLE ADMIN_DB._PIPELINE.LOAD_HISTORY
ADD COLUMN IF NOT EXISTS DUPLICATE_OF VARCHAR(36);
```
