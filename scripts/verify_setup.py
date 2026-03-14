#!/usr/bin/env python3
"""
Verify that all Snowflake infrastructure objects were created correctly.
Tests:
  1. All databases, schemas, tables exist
  2. Warehouse exists and is configured correctly
  3. Role and grants are correct
  4. Service user exists with RSA key
  5. Storage integration is configured
  6. Stage exists and points to S3
  7. Service user can authenticate with RSA key
"""

import sys
import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization

ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "YOUR-ACCOUNT-ID")
USER = os.environ.get("SNOWFLAKE_USER", "YOUR_USERNAME")
TOKEN = os.environ.get("SNOWFLAKE_TOKEN", "")
PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", "") or (sys.argv[1] if len(sys.argv) > 1 else "")
WAREHOUSE = "COMPUTE_WH"
ROLE = "ACCOUNTADMIN"


def check(label, cursor, sql, expected=None):
    """Run a check query and report pass/fail."""
    print(f"  [{label}] ... ", end="", flush=True)
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        if expected is not None:
            if callable(expected):
                ok = expected(rows)
            else:
                ok = len(rows) >= expected
        else:
            ok = True
        status = "PASS" if ok else "FAIL"
        print(f"{status}  (rows={len(rows)})")
        return ok, rows
    except Exception as e:
        print(f"FAIL  ({e})")
        return False, []


def main():
    if not TOKEN and not PASSWORD:
        print("Set SNOWFLAKE_TOKEN (PAT, preferred) or SNOWFLAKE_PASSWORD.")
        print("  export SNOWFLAKE_TOKEN='<your-pat>'")
        sys.exit(1)

    params = dict(account=ACCOUNT, user=USER, role=ROLE, warehouse=WAREHOUSE)
    if TOKEN:
        params["token"] = TOKEN
        params["authenticator"] = "programmatic_access_token"
    else:
        params["password"] = PASSWORD
    conn = snowflake.connector.connect(**params)
    cur = conn.cursor()
    results = []

    print("\n=== Phase 1: Object Existence ===\n")

    # Databases
    ok, _ = check("ADMIN_DB exists",
        cur, "SHOW DATABASES LIKE 'ADMIN_DB'", 1)
    results.append(("ADMIN_DB exists", ok))

    ok, _ = check("ANALYTICS_DB exists",
        cur, "SHOW DATABASES LIKE 'ANALYTICS_DB'", 1)
    results.append(("ANALYTICS_DB exists", ok))

    # Schemas
    ok, _ = check("_PIPELINE schema exists",
        cur, "SHOW SCHEMAS LIKE '_PIPELINE' IN DATABASE ADMIN_DB", 1)
    results.append(("_PIPELINE schema", ok))

    ok, _ = check("STAGING schema exists",
        cur, "SHOW SCHEMAS LIKE 'STAGING' IN DATABASE ANALYTICS_DB", 1)
    results.append(("STAGING schema", ok))

    # Tables
    for table in ["LOAD_HISTORY", "LOAD_ERRORS", "CONTROL_TABLE"]:
        ok, _ = check(f"{table} table exists",
            cur, f"SHOW TABLES LIKE '{table}' IN SCHEMA ADMIN_DB._PIPELINE", 1)
        results.append((f"{table} table", ok))

    # LOAD_HISTORY columns
    ok, rows = check("LOAD_HISTORY has correct columns",
        cur, "DESCRIBE TABLE ADMIN_DB._PIPELINE.LOAD_HISTORY",
        lambda rows: len(rows) >= 22)
    results.append(("LOAD_HISTORY columns", ok))
    if ok:
        col_names = [r[0] for r in rows]
        expected_cols = [
            "LOAD_ID", "S3_BUCKET", "S3_KEY", "S3_SIZE_BYTES", "S3_ETAG",
            "TARGET_DATABASE", "TARGET_SCHEMA", "TARGET_TABLE", "LOAD_MODE",
            "TABLE_CREATED", "FILE_FORMAT_USED", "ROWS_LOADED", "ROWS_PARSED",
            "ERRORS_SEEN", "STATUS", "ERROR_MESSAGE", "COPY_INTO_QUERY_ID",
            "LAMBDA_REQUEST_ID", "LAMBDA_FUNCTION", "STARTED_AT",
            "COMPLETED_AT", "DURATION_SECONDS",
        ]
        missing = [c for c in expected_cols if c not in col_names]
        if missing:
            print(f"    Missing columns: {missing}")

    print("\n=== Phase 2: Warehouse ===\n")

    ok, rows = check("LOADING_WH exists",
        cur, "SHOW WAREHOUSES LIKE 'LOADING_WH'", 1)
    results.append(("LOADING_WH exists", ok))

    if ok and rows:
        # Check warehouse properties
        ok2, rows2 = check("LOADING_WH is XSMALL",
            cur, "SHOW WAREHOUSES LIKE 'LOADING_WH'",
            lambda rows: rows[0][3] == 'X-Small')  # SIZE column
        results.append(("LOADING_WH size", ok2))

    print("\n=== Phase 3: Role & Grants ===\n")

    ok, _ = check("DATA_LOADER_ANALYTICS role exists",
        cur, "SHOW ROLES LIKE 'DATA_LOADER_ANALYTICS'", 1)
    results.append(("Role exists", ok))

    ok, _ = check("Role has warehouse grant",
        cur, "SHOW GRANTS TO ROLE DATA_LOADER_ANALYTICS",
        lambda rows: any('LOADING_WH' in str(r) and 'USAGE' in str(r) for r in rows))
    results.append(("Warehouse grant", ok))

    ok, _ = check("Role has ANALYTICS_DB grant",
        cur, "SHOW GRANTS TO ROLE DATA_LOADER_ANALYTICS",
        lambda rows: any('ANALYTICS_DB' in str(r) and 'USAGE' in str(r) for r in rows))
    results.append(("ANALYTICS_DB grant", ok))

    print("\n=== Phase 4: Service User ===\n")

    ok, _ = check("SVC_LOADER_ANALYTICS user exists",
        cur, "SHOW USERS LIKE 'SVC_LOADER_ANALYTICS'", 1)
    results.append(("User exists", ok))

    ok, rows = check("User has RSA key configured",
        cur, "DESC USER SVC_LOADER_ANALYTICS",
        lambda rows: any('RSA_PUBLIC_KEY_FP' in str(r) and 'SHA256:' in str(r) for r in rows))
    results.append(("RSA key set", ok))
    if ok:
        for r in rows:
            if 'RSA_PUBLIC_KEY_FP' in str(r):
                print(f"    Key fingerprint: {r[1]}")

    ok, _ = check("Role granted to user",
        cur, "SHOW GRANTS TO USER SVC_LOADER_ANALYTICS",
        lambda rows: any('DATA_LOADER_ANALYTICS' in str(r) for r in rows))
    results.append(("Role granted to user", ok))

    print("\n=== Phase 5: Storage Integration ===\n")

    ok, _ = check("S3_INT_ANALYTICS integration exists",
        cur, "SHOW INTEGRATIONS LIKE 'S3_INT_ANALYTICS'", 1)
    results.append(("Integration exists", ok))

    ok, rows = check("Integration configured correctly",
        cur, "DESC INTEGRATION S3_INT_ANALYTICS",
        lambda rows: any('etl-loader-test-pickybat' in str(r) for r in rows))
    results.append(("Integration config", ok))
    if ok:
        for r in rows:
            if 'STORAGE_AWS_IAM_USER_ARN' in str(r[0]):
                print(f"    IAM User ARN: {r[2]}")
            elif 'STORAGE_AWS_EXTERNAL_ID' in str(r[0]):
                print(f"    External ID: {r[2]}")

    print("\n=== Phase 6: External Stage ===\n")

    ok, _ = check("S3_LOAD_STAGE exists",
        cur, "SHOW STAGES LIKE 'S3_LOAD_STAGE' IN SCHEMA ANALYTICS_DB.STAGING", 1)
    results.append(("Stage exists", ok))

    print("\n=== Phase 7: RSA Key Authentication ===\n")

    print("  [Service user RSA auth] ... ", end="", flush=True)
    try:
        with open("output/keys/svc_loader_analytics_private.pem", "rb") as f:
            private_key_pem = f.read()

        private_key = serialization.load_pem_private_key(private_key_pem, password=None)
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        svc_conn = snowflake.connector.connect(
            account=ACCOUNT,
            user="SVC_LOADER_ANALYTICS",
            private_key=private_key_bytes,
            role="DATA_LOADER_ANALYTICS",
            warehouse="LOADING_WH",
            database="ANALYTICS_DB",
            schema="STAGING",
        )
        svc_cur = svc_conn.cursor()

        # Test: can we query?
        svc_cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()")
        row = svc_cur.fetchone()
        print(f"PASS")
        print(f"    user={row[0]} role={row[1]} warehouse={row[2]} database={row[3]}")
        results.append(("RSA auth", True))

        # Test: can we write to admin DB?
        svc_cur.execute("SELECT COUNT(*) FROM ADMIN_DB._PIPELINE.LOAD_HISTORY")
        count = svc_cur.fetchone()[0]
        print(f"  [Can read LOAD_HISTORY] ... PASS  (rows={count})")
        results.append(("Read LOAD_HISTORY", True))

        # Test: can we create a table in STAGING?
        svc_cur.execute("CREATE TABLE IF NOT EXISTS ANALYTICS_DB.STAGING._VERIFY_TEST (ID NUMBER)")
        print(f"  [Can create table in STAGING] ... PASS")
        svc_cur.execute("DROP TABLE IF EXISTS ANALYTICS_DB.STAGING._VERIFY_TEST")
        print(f"  [Dropped test table] ... PASS")
        results.append(("Create table in STAGING", True))

        svc_cur.close()
        svc_conn.close()

    except Exception as e:
        print(f"FAIL  ({e})")
        results.append(("RSA auth", False))

    cur.close()
    conn.close()

    # Summary
    print("\n" + "=" * 60)
    passed = sum(1 for _, ok in results if ok)
    failed = sum(1 for _, ok in results if not ok)
    total = len(results)
    print(f"\nVerification: {passed}/{total} passed, {failed} failed\n")

    if failed:
        print("FAILED checks:")
        for label, ok in results:
            if not ok:
                print(f"  - {label}")
        sys.exit(1)
    else:
        print("All checks passed.")


if __name__ == "__main__":
    main()
