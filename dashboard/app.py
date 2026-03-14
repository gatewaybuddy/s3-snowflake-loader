"""
ETL Pipeline Status Dashboard.

Read-only dashboard that queries ADMIN_DB._PIPELINE.LOAD_HISTORY
to display pipeline health, recent loads, and error details.

This dashboard NEVER writes to Snowflake or AWS.
It is a window, not a control panel.
"""

import os
from datetime import datetime

import snowflake.connector
from flask import Flask, jsonify, render_template

app = Flask(__name__)

# ─── Configuration ──────────────────────────────────────────
# Set via environment variables or .env file
SF_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "")
SF_USER = os.environ.get("SNOWFLAKE_USER", "")
SF_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", "")
SF_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "LOADING_WH")
SF_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "ADMIN_DB")
SF_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "_PIPELINE")
SF_ROLE = os.environ.get("SNOWFLAKE_ROLE", "")


def _get_connection():
    """Create a Snowflake connection using password auth (dashboard only)."""
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        role=SF_ROLE or None,
    )


def _query(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a read-only query and return results as list of dicts."""
    conn = _get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        columns = [desc[0] for desc in cur.description]
        rows = []
        for row in cur.fetchall():
            rows.append(dict(zip(columns, row)))
        return rows
    finally:
        conn.close()


@app.route("/")
def index():
    """Render the main dashboard page."""
    return render_template("index.html")


@app.route("/api/recent-loads")
def recent_loads():
    """Get the 50 most recent loads."""
    rows = _query("""
        SELECT
            LOAD_ID,
            S3_KEY,
            TARGET_DATABASE,
            TARGET_TABLE,
            LOAD_MODE,
            STATUS,
            ROWS_LOADED,
            ROWS_PARSED,
            ERRORS_SEEN,
            DURATION_SECONDS,
            TABLE_CREATED,
            FILE_FORMAT_USED,
            ERROR_MESSAGE,
            STARTED_AT,
            COMPLETED_AT
        FROM LOAD_HISTORY
        ORDER BY STARTED_AT DESC
        LIMIT 50
    """)
    # Serialize datetimes
    for row in rows:
        for key in ("STARTED_AT", "COMPLETED_AT"):
            if isinstance(row.get(key), datetime):
                row[key] = row[key].isoformat()
    return jsonify(rows)


@app.route("/api/health-summary")
def health_summary():
    """Get pipeline health metrics for today."""
    rows = _query("""
        SELECT
            COUNT(*) AS total_loads,
            SUM(CASE WHEN STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS successes,
            SUM(CASE WHEN STATUS = 'PARTIAL' THEN 1 ELSE 0 END) AS partials,
            SUM(CASE WHEN STATUS = 'FAILED' THEN 1 ELSE 0 END) AS failures,
            SUM(CASE WHEN STATUS = 'LOADING' THEN 1 ELSE 0 END) AS in_progress,
            COALESCE(AVG(DURATION_SECONDS), 0) AS avg_duration,
            COALESCE(SUM(ROWS_LOADED), 0) AS total_rows_loaded,
            COALESCE(SUM(ERRORS_SEEN), 0) AS total_errors
        FROM LOAD_HISTORY
        WHERE STARTED_AT >= CURRENT_DATE()
    """)
    summary = rows[0] if rows else {}

    # Calculate success rate
    total = summary.get("TOTAL_LOADS", 0) or 0
    successes = (summary.get("SUCCESSES", 0) or 0) + (summary.get("PARTIALS", 0) or 0)
    summary["SUCCESS_RATE"] = round((successes / total * 100), 1) if total > 0 else 0

    # Convert Decimal types to plain numbers
    for key, val in summary.items():
        if hasattr(val, "is_finite"):  # Decimal
            summary[key] = float(val)
        elif isinstance(val, int):
            summary[key] = val

    return jsonify(summary)


@app.route("/api/load-errors/<load_id>")
def load_errors(load_id: str):
    """Get error details for a specific load."""
    rows = _query("""
        SELECT
            ERROR_LINE,
            ERROR_COLUMN,
            ERROR_MESSAGE,
            LEFT(REJECTED_RECORD, 500) AS REJECTED_RECORD_PREVIEW,
            CREATED_AT
        FROM LOAD_ERRORS
        WHERE LOAD_ID = %s
        ORDER BY ERROR_LINE
        LIMIT 100
    """, (load_id,))
    for row in rows:
        if isinstance(row.get("CREATED_AT"), datetime):
            row["CREATED_AT"] = row["CREATED_AT"].isoformat()
    return jsonify(rows)


@app.route("/api/notifications/<load_id>")
def load_notifications(load_id: str):
    """Get notification history for a specific load."""
    rows = _query("""
        SELECT
            NOTIFICATION_TYPE,
            CHANNEL,
            MESSAGE_SUBJECT,
            DELIVERY_STATUS,
            SENT_AT,
            SNS_MESSAGE_ID
        FROM NOTIFICATIONS
        WHERE LOAD_ID = %s
        ORDER BY SENT_AT DESC
    """, (load_id,))
    for row in rows:
        if isinstance(row.get("SENT_AT"), datetime):
            row["SENT_AT"] = row["SENT_AT"].isoformat()
    return jsonify(rows)


# ─── NOTE: No auth is implemented ────────────────────────────
# For production, add authentication. Options:
#   - Flask-Login with simple password
#   - OAuth2 via AWS Cognito
#   - IP whitelisting via security group
#   - nginx basic auth in front of this app
# ──────────────────────────────────────────────────────────────


if __name__ == "__main__":
    # Load .env file if present (local development)
    try:
        from dotenv import load_dotenv
        load_dotenv()
        # Re-read after dotenv
        SF_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", SF_ACCOUNT)
        SF_USER = os.environ.get("SNOWFLAKE_USER", SF_USER)
        SF_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", SF_PASSWORD)
        SF_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", SF_WAREHOUSE)
        SF_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", SF_DATABASE)
        SF_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", SF_SCHEMA)
        SF_ROLE = os.environ.get("SNOWFLAKE_ROLE", SF_ROLE)
    except ImportError:
        pass

    app.run(host="0.0.0.0", port=5000, debug=True)
