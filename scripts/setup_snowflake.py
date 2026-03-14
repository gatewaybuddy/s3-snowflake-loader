#!/usr/bin/env python3
"""
Execute Snowflake infrastructure setup against the trial account.
Connects as ACCOUNTADMIN with PAT (preferred) or password auth.

Auth via environment variables:
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_TOKEN (PAT) or SNOWFLAKE_PASSWORD
  Or pass password as first CLI argument (legacy).
"""

import sys
import os
import snowflake.connector

# ── Configuration ──────────────────────────────────────────────
ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "YOUR-ACCOUNT-ID")
USER = os.environ.get("SNOWFLAKE_USER", "YOUR_USERNAME")
TOKEN = os.environ.get("SNOWFLAKE_TOKEN", "")
PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", "") or (sys.argv[1] if len(sys.argv) > 1 else "")
WAREHOUSE = "COMPUTE_WH"
ROLE = "ACCOUNTADMIN"

# Read the public key body (no header/footer) — optional, only needed for user creation
pub_key_body = ""
pub_key_path = "output/keys/svc_loader_analytics_public.pem"
if os.path.exists(pub_key_path):
    with open(pub_key_path) as f:
        lines = f.read().strip().splitlines()
        pub_key_body = "".join(l for l in lines if not l.startswith("-----"))
else:
    print(f"⚠️  Public key not found at {pub_key_path} — service user RSA key won't be set")

AWS_ACCOUNT_ID = "152924643524"
S3_BUCKET = "etl-loader-test-pickybat"

# ── SQL Statements ─────────────────────────────────────────────
STATEMENTS = [
    # 1. Admin Database & Pipeline Schema
    ("Create ADMIN_DB", "CREATE DATABASE IF NOT EXISTS ADMIN_DB"),
    ("Create _PIPELINE schema", "CREATE SCHEMA IF NOT EXISTS ADMIN_DB._PIPELINE"),

    # 2. LOAD_HISTORY table
    ("Create LOAD_HISTORY table", """
CREATE TABLE IF NOT EXISTS ADMIN_DB._PIPELINE.LOAD_HISTORY (
    LOAD_ID             VARCHAR(36)     DEFAULT UUID_STRING(),
    S3_BUCKET           VARCHAR(256)    NOT NULL,
    S3_KEY              VARCHAR(1024)   NOT NULL,
    S3_SIZE_BYTES       NUMBER,
    S3_ETAG             VARCHAR(256),
    TARGET_DATABASE     VARCHAR(128)    NOT NULL,
    TARGET_SCHEMA       VARCHAR(128)    NOT NULL,
    TARGET_TABLE        VARCHAR(128)    NOT NULL,
    LOAD_MODE           VARCHAR(10)     NOT NULL,
    TABLE_CREATED       BOOLEAN         DEFAULT FALSE,
    FILE_FORMAT_USED    VARCHAR(4000),
    ROWS_LOADED         NUMBER,
    ROWS_PARSED         NUMBER,
    ERRORS_SEEN         NUMBER          DEFAULT 0,
    STATUS              VARCHAR(20)     NOT NULL,
    ERROR_MESSAGE       VARCHAR(4000),
    COPY_INTO_QUERY_ID  VARCHAR(256),
    LAMBDA_REQUEST_ID   VARCHAR(256),
    LAMBDA_FUNCTION     VARCHAR(256),
    STARTED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT        TIMESTAMP_NTZ,
    DURATION_SECONDS    NUMBER,
    DUPLICATE_OF        VARCHAR(36),
    PRIMARY KEY (LOAD_ID)
)"""),

    # 3. LOAD_ERRORS table
    ("Create LOAD_ERRORS table", """
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
)"""),

    # 4. CONTROL_TABLE
    ("Create CONTROL_TABLE", """
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
)"""),

    # 5. ANALYTICS_DB with STAGING schema
    ("Create ANALYTICS_DB", "CREATE DATABASE IF NOT EXISTS ANALYTICS_DB"),
    ("Create STAGING schema", "CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.STAGING"),

    # 6. LOADING_WH warehouse
    ("Create LOADING_WH warehouse", """
CREATE WAREHOUSE IF NOT EXISTS LOADING_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE"""),

    # 7. Role DATA_LOADER_ANALYTICS
    ("Create DATA_LOADER_ANALYTICS role", "CREATE ROLE IF NOT EXISTS DATA_LOADER_ANALYTICS"),

    # Warehouse grant
    ("Grant warehouse to role", "GRANT USAGE ON WAREHOUSE LOADING_WH TO ROLE DATA_LOADER_ANALYTICS"),

    # Target DB grants
    ("Grant USAGE on ANALYTICS_DB", "GRANT USAGE ON DATABASE ANALYTICS_DB TO ROLE DATA_LOADER_ANALYTICS"),
    ("Grant USAGE on STAGING schema", "GRANT USAGE ON SCHEMA ANALYTICS_DB.STAGING TO ROLE DATA_LOADER_ANALYTICS"),
    ("Grant CREATE TABLE on STAGING", "GRANT CREATE TABLE ON SCHEMA ANALYTICS_DB.STAGING TO ROLE DATA_LOADER_ANALYTICS"),
    ("Grant DML on all tables", "GRANT SELECT, INSERT, TRUNCATE ON ALL TABLES IN SCHEMA ANALYTICS_DB.STAGING TO ROLE DATA_LOADER_ANALYTICS"),
    ("Grant DML on future tables", "GRANT SELECT, INSERT, TRUNCATE ON FUTURE TABLES IN SCHEMA ANALYTICS_DB.STAGING TO ROLE DATA_LOADER_ANALYTICS"),

    # Admin DB grants (for logging)
    ("Grant USAGE on ADMIN_DB to role", "GRANT USAGE ON DATABASE ADMIN_DB TO ROLE DATA_LOADER_ANALYTICS"),
    ("Grant USAGE on _PIPELINE to role", "GRANT USAGE ON SCHEMA ADMIN_DB._PIPELINE TO ROLE DATA_LOADER_ANALYTICS"),
    ("Grant DML on admin tables", "GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA ADMIN_DB._PIPELINE TO ROLE DATA_LOADER_ANALYTICS"),

    # 8. Service user with RSA auth
    ("Create SVC_LOADER_ANALYTICS user", f"""
CREATE USER IF NOT EXISTS SVC_LOADER_ANALYTICS
    RSA_PUBLIC_KEY = '{pub_key_body}'
    DEFAULT_ROLE = DATA_LOADER_ANALYTICS
    DEFAULT_WAREHOUSE = LOADING_WH
    MUST_CHANGE_PASSWORD = FALSE"""),

    ("Grant role to user", "GRANT ROLE DATA_LOADER_ANALYTICS TO USER SVC_LOADER_ANALYTICS"),

    # 9. Storage integration
    ("Create S3_INT_ANALYTICS storage integration", f"""
CREATE STORAGE INTEGRATION IF NOT EXISTS S3_INT_ANALYTICS
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::{AWS_ACCOUNT_ID}:role/etl-analytics-db-snowflake-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://{S3_BUCKET}/analytics/')"""),

    # 10. External stage
    ("Grant CREATE STAGE to role", "GRANT CREATE STAGE ON SCHEMA ANALYTICS_DB.STAGING TO ROLE DATA_LOADER_ANALYTICS"),

    ("Create S3_LOAD_STAGE_ANALYTICS", f"""
CREATE OR REPLACE STAGE ANALYTICS_DB.STAGING.S3_LOAD_STAGE
    URL = 's3://{S3_BUCKET}/analytics/'
    STORAGE_INTEGRATION = S3_INT_ANALYTICS"""),

    ("Grant USAGE on stage", "GRANT USAGE ON STAGE ANALYTICS_DB.STAGING.S3_LOAD_STAGE TO ROLE DATA_LOADER_ANALYTICS"),
]


def main():
    if not TOKEN and not PASSWORD:
        print("Set SNOWFLAKE_TOKEN (PAT, preferred) or SNOWFLAKE_PASSWORD, or pass password as argument.")
        print("  export SNOWFLAKE_TOKEN='<your-pat>'")
        print("  python setup_snowflake.py")
        sys.exit(1)

    auth_method = "PAT" if TOKEN else "password"
    print(f"Connecting to Snowflake: {ACCOUNT} as {USER} ({auth_method}) ...")
    params = dict(account=ACCOUNT, user=USER, role=ROLE, warehouse=WAREHOUSE)
    if TOKEN:
        params["token"] = TOKEN
        params["authenticator"] = "programmatic_access_token"
    else:
        params["password"] = PASSWORD
    conn = snowflake.connector.connect(**params)
    print(f"Connected. Session ID: {conn.session_id}\n")

    results = []
    for label, sql in STATEMENTS:
        print(f"  [{label}] ... ", end="", flush=True)
        cur = conn.cursor()
        try:
            cur.execute(sql.strip())
            result = cur.fetchone()
            status_msg = result[0] if result else "OK"
            print(f"OK  ({status_msg})")
            results.append((label, "OK", status_msg))
        except Exception as e:
            print(f"FAIL  ({e})")
            results.append((label, "FAIL", str(e)))
        finally:
            cur.close()

    # Describe integration to get IAM ARN and External ID
    print("\n  [Describe S3_INT_ANALYTICS] ... ", end="", flush=True)
    cur = conn.cursor()
    try:
        cur.execute("DESC INTEGRATION S3_INT_ANALYTICS")
        rows = cur.fetchall()
        print("OK")
        print("\n  Storage Integration Details:")
        for row in rows:
            prop_name = row[0]
            prop_val = row[2] if len(row) > 2 else row[1]
            if "ARN" in prop_name or "EXTERNAL_ID" in prop_name or "LOCATIONS" in prop_name:
                print(f"    {prop_name} = {prop_val}")
    except Exception as e:
        print(f"FAIL ({e})")
    finally:
        cur.close()

    conn.close()

    # Summary
    print("\n" + "=" * 60)
    ok_count = sum(1 for _, s, _ in results if s == "OK")
    fail_count = sum(1 for _, s, _ in results if s == "FAIL")
    print(f"Results: {ok_count} OK, {fail_count} FAILED out of {len(results)} statements")
    if fail_count:
        print("\nFailed statements:")
        for label, status, msg in results:
            if status == "FAIL":
                print(f"  - {label}: {msg}")
        sys.exit(1)


if __name__ == "__main__":
    main()
