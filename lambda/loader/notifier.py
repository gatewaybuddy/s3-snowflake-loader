"""
SNS notification publisher for pipeline events.

Publishes structured notifications on SUCCESS, FAILURE, and WARNING events.
Messages are dual-format: human-readable for email subscribers and
structured JSON for automation consumers.
"""

import json
import logging
import os
from datetime import datetime
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

sns_client = boto3.client("sns")


def _get_topic_arn() -> Optional[str]:
    """Get SNS topic ARN from environment variable."""
    return os.environ.get("SNS_TOPIC_ARN")


def _format_duration(seconds: Optional[int]) -> str:
    """Format duration in human-readable form."""
    if seconds is None:
        return "N/A"
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    secs = seconds % 60
    return f"{minutes}m {secs}s"


def _build_success_message(
    s3_key: str,
    table: str,
    database: str,
    rows_loaded: int,
    rows_parsed: int,
    duration_seconds: Optional[int],
    load_id: str,
    table_created: bool,
) -> tuple[str, str, dict]:
    """Build SUCCESS notification. Returns (subject, body, structured_data)."""
    subject = f"[SUCCESS] Loaded {table} - {rows_loaded} rows"

    body_lines = [
        f"Pipeline Load Completed Successfully",
        f"{'=' * 45}",
        f"",
        f"Table:        {database}.{table}",
        f"Source:        {s3_key}",
        f"Rows loaded:  {rows_loaded:,}",
        f"Rows parsed:  {rows_parsed:,}",
        f"Duration:     {_format_duration(duration_seconds)}",
        f"Load ID:      {load_id}",
    ]
    if table_created:
        body_lines.append(f"Note:         Table was auto-created during this load")

    structured = {
        "event_type": "SUCCESS",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "load_id": load_id,
        "s3_key": s3_key,
        "target_database": database,
        "target_table": table,
        "rows_loaded": rows_loaded,
        "rows_parsed": rows_parsed,
        "duration_seconds": duration_seconds,
        "table_created": table_created,
    }

    return subject, "\n".join(body_lines), structured


def _build_failure_message(
    s3_key: str,
    table: str,
    database: str,
    error_message: str,
    load_id: str,
) -> tuple[str, str, dict]:
    """Build FAILURE notification. Returns (subject, body, structured_data)."""
    subject = f"[FAILED] Load failed for {table}"

    body_lines = [
        f"Pipeline Load FAILED",
        f"{'=' * 45}",
        f"",
        f"Table:        {database}.{table}",
        f"Source:        {s3_key}",
        f"Error:        {error_message}",
        f"Load ID:      {load_id}",
        f"",
        f"Troubleshooting:",
        f"  1. Check CloudWatch Logs for the full stack trace",
        f"  2. Query ADMIN_DB._PIPELINE.LOAD_HISTORY for load_id = '{load_id}'",
        f"  3. Verify the source file format matches expectations",
        f"  4. Check ADMIN_DB._PIPELINE.CONTROL_TABLE for overrides",
    ]

    if "does not exist" in error_message.lower():
        body_lines.append(f"  5. Table may need to be created manually or auto_create_table enabled")
    if "access" in error_message.lower() or "permission" in error_message.lower():
        body_lines.append(f"  5. Check IAM role and Snowflake grants for the service user")
    if "timeout" in error_message.lower():
        body_lines.append(f"  5. Consider increasing Lambda timeout or splitting the file")

    structured = {
        "event_type": "FAILURE",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "load_id": load_id,
        "s3_key": s3_key,
        "target_database": database,
        "target_table": table,
        "error_message": error_message,
    }

    return subject, "\n".join(body_lines), structured


def _build_warning_message(
    s3_key: str,
    table: str,
    database: str,
    warning_type: str,
    detail: str,
    load_id: str,
    rows_loaded: int = 0,
    errors_seen: int = 0,
) -> tuple[str, str, dict]:
    """Build WARNING notification. Returns (subject, body, structured_data)."""
    subject = f"[WARNING] {warning_type} — {table}"

    body_lines = [
        f"Pipeline Load Warning",
        f"{'=' * 45}",
        f"",
        f"Warning:      {warning_type}",
        f"Table:        {database}.{table}",
        f"Source:        {s3_key}",
        f"Detail:       {detail}",
        f"Load ID:      {load_id}",
    ]
    if rows_loaded:
        body_lines.append(f"Rows loaded:  {rows_loaded:,}")
    if errors_seen:
        body_lines.append(f"Errors seen:  {errors_seen:,}")
        body_lines.extend([
            f"",
            f"Action required:",
            f"  - Review rejected rows in ADMIN_DB._PIPELINE.LOAD_ERRORS",
            f"  - Query: SELECT * FROM ADMIN_DB._PIPELINE.LOAD_ERRORS WHERE LOAD_ID = '{load_id}'",
        ])

    structured = {
        "event_type": "WARNING",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "load_id": load_id,
        "warning_type": warning_type,
        "s3_key": s3_key,
        "target_database": database,
        "target_table": table,
        "detail": detail,
        "rows_loaded": rows_loaded,
        "errors_seen": errors_seen,
    }

    return subject, "\n".join(body_lines), structured


def _publish(subject: str, body: str, structured: dict) -> Optional[str]:
    """
    Publish to SNS with dual-format message.

    Email subscribers get the human-readable body.
    SQS/Lambda/HTTP subscribers get the structured JSON.

    Returns the SNS MessageId on success, None if topic not configured.
    """
    topic_arn = _get_topic_arn()
    if not topic_arn:
        logger.debug("SNS_TOPIC_ARN not set — skipping notification")
        return None

    # SNS subject max is 100 chars
    subject = subject[:100]

    message = json.dumps({
        "default": body,
        "email": body,
        "sqs": json.dumps(structured),
        "lambda": json.dumps(structured),
        "https": json.dumps(structured),
    })

    try:
        response = sns_client.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=message,
            MessageStructure="json",
        )
        message_id = response["MessageId"]
        logger.info(f"Published {structured['event_type']} notification: {message_id}")
        return message_id
    except ClientError as e:
        logger.error(f"Failed to publish SNS notification: {e}")
        return None


def notify_success(
    s3_key: str,
    table: str,
    database: str,
    rows_loaded: int,
    rows_parsed: int,
    duration_seconds: Optional[int],
    load_id: str,
    table_created: bool = False,
) -> Optional[str]:
    """Send SUCCESS notification. Returns SNS MessageId or None."""
    subject, body, structured = _build_success_message(
        s3_key, table, database, rows_loaded, rows_parsed,
        duration_seconds, load_id, table_created,
    )
    return _publish(subject, body, structured)


def notify_failure(
    s3_key: str,
    table: str,
    database: str,
    error_message: str,
    load_id: str,
) -> Optional[str]:
    """Send FAILURE notification. Returns SNS MessageId or None."""
    subject, body, structured = _build_failure_message(
        s3_key, table, database, error_message, load_id,
    )
    return _publish(subject, body, structured)


def notify_warning(
    s3_key: str,
    table: str,
    database: str,
    warning_type: str,
    detail: str,
    load_id: str,
    rows_loaded: int = 0,
    errors_seen: int = 0,
) -> Optional[str]:
    """
    Send WARNING notification. Returns SNS MessageId or None.

    Common warning_type values:
      - "Partial Load": some rows rejected
      - "Duplicate Detected": file already loaded (same S3 ETag)
      - "Schema Drift": columns in file don't match table
      - "Format Mismatch": extension doesn't match detected content
    """
    subject, body, structured = _build_warning_message(
        s3_key, table, database, warning_type, detail,
        load_id, rows_loaded, errors_seen,
    )
    return _publish(subject, body, structured)
