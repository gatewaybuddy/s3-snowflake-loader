"""
Load history and error logging to the centralized admin database.

All Lambdas write to ADMIN_DB._PIPELINE regardless of which target
database they load into. This gives a single pane of glass for
monitoring all pipeline activity.
"""

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class LoadRecord:
    """A single load operation record."""
    load_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    s3_bucket: str = ""
    s3_key: str = ""
    s3_size_bytes: int = 0
    s3_etag: str = ""
    target_database: str = ""
    target_schema: str = ""
    target_table: str = ""
    load_mode: str = ""
    table_created: bool = False
    file_format_used: str = ""
    rows_loaded: int = 0
    rows_parsed: int = 0
    errors_seen: int = 0
    status: str = "LOADING"  # LOADING | SUCCESS | PARTIAL | FAILED
    error_message: Optional[str] = None
    copy_into_query_id: Optional[str] = None
    lambda_request_id: str = ""
    lambda_function: str = ""
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    duplicate_of: Optional[str] = None   # LOAD_ID of original load if this is a duplicate


class HistoryLogger:
    """Writes load records to the admin database."""

    def __init__(self, conn, admin_database: str, admin_schema: str):
        self.conn = conn
        self.admin_db = admin_database
        self.admin_schema = admin_schema
        self.table_prefix = f"{admin_database}.{admin_schema}"

    def insert_loading(self, record: LoadRecord) -> None:
        """Insert initial LOADING record at start of load."""
        sql = (
            f"INSERT INTO {self.table_prefix}.LOAD_HISTORY "
            f"(LOAD_ID, S3_BUCKET, S3_KEY, S3_SIZE_BYTES, S3_ETAG, "
            f"TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, LOAD_MODE, "
            f"LAMBDA_REQUEST_ID, LAMBDA_FUNCTION, STATUS, STARTED_AT) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (
                record.load_id,
                record.s3_bucket,
                record.s3_key,
                record.s3_size_bytes,
                record.s3_etag,
                record.target_database,
                record.target_schema,
                record.target_table,
                record.load_mode,
                record.lambda_request_id,
                record.lambda_function,
                "LOADING",
                record.started_at.isoformat(),
            ))
            logger.info(f"Logged LOADING record: {record.load_id}")
        finally:
            cur.close()

    def update_complete(self, record: LoadRecord) -> None:
        """Update record with final status after load completes."""
        record.completed_at = datetime.utcnow()
        if record.started_at:
            record.duration_seconds = int(
                (record.completed_at - record.started_at).total_seconds()
            )

        sql = (
            f"UPDATE {self.table_prefix}.LOAD_HISTORY SET "
            f"TABLE_CREATED = %s, FILE_FORMAT_USED = %s, "
            f"ROWS_LOADED = %s, ROWS_PARSED = %s, ERRORS_SEEN = %s, "
            f"STATUS = %s, ERROR_MESSAGE = %s, COPY_INTO_QUERY_ID = %s, "
            f"COMPLETED_AT = %s, DURATION_SECONDS = %s, DUPLICATE_OF = %s "
            f"WHERE LOAD_ID = %s"
        )
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (
                record.table_created,
                record.file_format_used,
                record.rows_loaded,
                record.rows_parsed,
                record.errors_seen,
                record.status,
                record.error_message,
                record.copy_into_query_id,
                record.completed_at.isoformat(),
                record.duration_seconds,
                record.duplicate_of,
                record.load_id,
            ))
            logger.info(
                f"Updated load record {record.load_id}: "
                f"status={record.status} rows={record.rows_loaded} "
                f"errors={record.errors_seen} duration={record.duration_seconds}s"
            )
        finally:
            cur.close()

    def check_duplicate(self, s3_etag: str, s3_key: str) -> Optional[str]:
        """
        Check LOAD_HISTORY for a previous successful load with the same
        S3 ETag and file path.

        Returns the LOAD_ID of the original load if a duplicate is found,
        or None if this is a new file.
        """
        sql = (
            f"SELECT LOAD_ID FROM {self.table_prefix}.LOAD_HISTORY "
            f"WHERE S3_ETAG = %s AND S3_KEY = %s "
            f"AND STATUS IN ('SUCCESS', 'PARTIAL') "
            f"ORDER BY STARTED_AT DESC LIMIT 1"
        )
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (s3_etag, s3_key))
            row = cur.fetchone()
            if row:
                logger.info(
                    f"Duplicate detected: ETag={s3_etag} key={s3_key} "
                    f"matches load {row[0]}"
                )
                return row[0]
            return None
        except Exception as e:
            logger.warning(f"Duplicate check failed (non-fatal): {e}")
            return None
        finally:
            cur.close()

    def log_errors(self, load_id: str, target_database: str, s3_key: str,
                   errors: list[dict]) -> None:
        """Write rejected records to LOAD_ERRORS table."""
        if not errors:
            return

        sql = (
            f"INSERT INTO {self.table_prefix}.LOAD_ERRORS "
            f"(ERROR_ID, LOAD_ID, TARGET_DATABASE, S3_KEY, "
            f"ERROR_LINE, ERROR_COLUMN, ERROR_MESSAGE, REJECTED_RECORD) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        )

        cur = self.conn.cursor()
        try:
            for error in errors:
                cur.execute(sql, (
                    str(uuid.uuid4()),
                    load_id,
                    target_database,
                    s3_key,
                    error.get("line"),
                    error.get("column"),
                    error.get("error", "Unknown error"),
                    error.get("record", "")[:16777216],  # Max VARCHAR
                ))
            logger.info(f"Logged {len(errors)} errors for load {load_id}")
        finally:
            cur.close()

    def log_notification(
        self,
        load_id: str,
        notification_type: str,
        channel: str,
        subject: str,
        body: str,
        delivery_status: str,
        sns_message_id: str = None,
        recipient: str = None,
    ) -> None:
        """Write a notification record to the NOTIFICATIONS audit table."""
        sql = (
            f"INSERT INTO {self.table_prefix}.NOTIFICATIONS "
            f"(NOTIFICATION_ID, LOAD_ID, NOTIFICATION_TYPE, CHANNEL, "
            f"RECIPIENT, MESSAGE_SUBJECT, MESSAGE_BODY, DELIVERY_STATUS, SNS_MESSAGE_ID) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        cur = self.conn.cursor()
        try:
            cur.execute(sql, (
                str(uuid.uuid4()),
                load_id,
                notification_type,
                channel,
                recipient,
                subject[:256] if subject else None,
                body,
                delivery_status,
                sns_message_id,
            ))
            logger.info(
                f"Logged notification: type={notification_type} "
                f"status={delivery_status} load={load_id}"
            )
        except Exception as e:
            logger.warning(f"Failed to log notification to NOTIFICATIONS table: {e}")
        finally:
            cur.close()

    def get_copy_errors(self, query_id: str) -> list[dict]:
        """
        Fetch detailed error info from Snowflake's COPY history.
        Call after COPY INTO to get rejected row details.
        """
        sql = (
            f"SELECT ERROR, FILE, LINE, CHARACTER, REJECTED_RECORD "
            f"FROM TABLE(VALIDATE({self.table_prefix}.LOAD_HISTORY, "
            f"JOB_ID => '{query_id}'))"
        )
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
            errors = []
            for row in cur.fetchall():
                errors.append({
                    "error": row[0],
                    "file": row[1],
                    "line": row[2],
                    "column": row[3],
                    "record": row[4],
                })
            return errors
        except Exception as e:
            logger.warning(f"Could not fetch COPY errors: {e}")
            return []
        finally:
            cur.close()
