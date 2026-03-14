"""
Snowflake connection and data loading operations.

Uses snowflake-connector-python (not Snowpark) for lighter Lambda footprint.
Handles: connection, COPY INTO, table creation, control table lookups.
"""

import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Optional

import snowflake.connector
from cryptography.hazmat.primitives import serialization

logger = logging.getLogger(__name__)


@dataclass
class SnowflakeConfig:
    """Connection configuration from Secrets Manager."""
    account: str
    user: str
    private_key_pem: str       # RSA private key (PEM format)
    role: str
    warehouse: str
    database: str
    schema: str
    stage: str
    admin_database: str
    admin_schema: str


@dataclass
class ControlTableOverride:
    """Per-table overrides from the control table."""
    load_mode: Optional[str] = None
    file_format_name: Optional[str] = None
    file_format_options: Optional[str] = None
    copy_options: Optional[str] = None
    pre_load_sql: Optional[str] = None
    post_load_sql: Optional[str] = None
    merge_keys: Optional[str] = None
    auto_create_table: bool = True
    create_table_ddl: Optional[str] = None
    enabled: bool = True


class SnowflakeClient:
    """Manages Snowflake operations for the loader."""

    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self._conn = None

    @contextmanager
    def connect(self):
        """Context manager for Snowflake connection with RSA key auth."""
        try:
            # Parse the PEM private key
            private_key = serialization.load_pem_private_key(
                self.config.private_key_pem.encode("utf-8"),
                password=None,
            )
            private_key_bytes = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

            self._conn = snowflake.connector.connect(
                account=self.config.account,
                user=self.config.user,
                private_key=private_key_bytes,
                role=self.config.role,
                warehouse=self.config.warehouse,
                database=self.config.database,
                schema=self.config.schema,
            )
            logger.info(
                f"Connected to Snowflake: {self.config.account} as {self.config.user} "
                f"role={self.config.role} db={self.config.database}"
            )
            yield self
        finally:
            if self._conn:
                self._conn.close()
                self._conn = None

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the target schema."""
        cur = self._conn.cursor()
        try:
            cur.execute(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (self.config.schema.upper(), table_name.upper()),
            )
            row = cur.fetchone()
            return row[0] > 0 if row else False
        finally:
            cur.close()

    def create_table_from_header(self, table_name: str, columns: list[str]) -> str:
        """
        Create a table with all VARCHAR columns from CSV header.
        Returns the DDL executed.
        """
        # Sanitize column names
        safe_columns = []
        for col in columns:
            safe = col.strip().strip('"').upper()
            safe = ''.join(c if c.isalnum() or c == '_' else '_' for c in safe)
            if not safe or safe[0].isdigit():
                safe = f"COL_{safe}"
            safe_columns.append(safe)

        ddl = (
            f"CREATE TABLE IF NOT EXISTS "
            f"{self.config.database}.{self.config.schema}.{table_name} (\n"
        )
        ddl += ",\n".join(f"    {col} VARCHAR(16777216)" for col in safe_columns)
        ddl += "\n)"

        cur = self._conn.cursor()
        try:
            cur.execute(ddl)
            logger.info(f"Created table {table_name} with {len(safe_columns)} columns")
        finally:
            cur.close()

        return ddl

    def create_table_for_json(self, table_name: str) -> str:
        """
        Create a table with a single VARIANT column for JSON data.
        JSON Lines / JSON arrays get loaded into RAW_DATA VARIANT.
        Returns the DDL executed.
        """
        ddl = (
            f"CREATE TABLE IF NOT EXISTS "
            f"{self.config.database}.{self.config.schema}.{table_name} (\n"
            f"    RAW_DATA VARIANT\n"
            f")"
        )
        cur = self._conn.cursor()
        try:
            cur.execute(ddl)
            logger.info(f"Created JSON table {table_name} with VARIANT column")
        finally:
            cur.close()
        return ddl

    def create_table_from_ddl(self, ddl: str) -> None:
        """Execute custom DDL from the control table."""
        cur = self._conn.cursor()
        try:
            cur.execute(ddl)
            logger.info("Created table from custom DDL")
        finally:
            cur.close()

    def create_table_from_infer_schema(
        self, table_name: str, s3_key: str, file_format: str
    ) -> str:
        """
        Create a table using Snowflake's INFER_SCHEMA for typed formats.
        Returns the DDL executed.
        """
        stage_path = f"@{self.config.database}.{self.config.schema}.{self.config.stage}/{s3_key}"

        cur = self._conn.cursor()
        try:
            # Infer schema
            cur.execute(
                f"SELECT * FROM TABLE(INFER_SCHEMA("
                f"LOCATION => '{stage_path}', "
                f"FILE_FORMAT => '{file_format}'"
                f"))"
            )
            columns = cur.fetchall()

            if not columns:
                raise ValueError(f"INFER_SCHEMA returned no columns for {s3_key}")

            # Build DDL from inferred schema
            col_defs = []
            for row in columns:
                col_name = row[0]  # COLUMN_NAME
                col_type = row[1]  # TYPE
                col_defs.append(f"    {col_name} {col_type}")

            ddl = (
                f"CREATE TABLE IF NOT EXISTS "
                f"{self.config.database}.{self.config.schema}.{table_name} (\n"
            )
            ddl += ",\n".join(col_defs)
            ddl += "\n)"
            cur.execute(ddl)
            logger.info(f"Created table {table_name} from INFER_SCHEMA ({len(columns)} cols)")
            return ddl

        finally:
            cur.close()

    def truncate_table(self, table_name: str) -> None:
        """Truncate target table for TRUNCATE mode."""
        cur = self._conn.cursor()
        try:
            cur.execute(
                f"TRUNCATE TABLE {self.config.database}.{self.config.schema}.{table_name}"
            )
            logger.info(f"Truncated table {table_name}")
        finally:
            cur.close()

    def copy_into(
        self,
        table_name: str,
        s3_relative_path: str,
        file_format: str = "CSV",
        dialect: Optional[dict] = None,
        format_name: Optional[str] = None,
        format_options: Optional[str] = None,
        copy_options: Optional[str] = None,
        compression: Optional[str] = None,
    ) -> dict:
        """
        Execute COPY INTO and return results.
        
        Returns:
            {
                "rows_loaded": int,
                "rows_parsed": int,
                "errors_seen": int,
                "query_id": str,
                "status": "SUCCESS" | "PARTIAL" | "FAILED",
                "errors": [{"file": ..., "line": ..., "error": ...}]
            }
        """
        stage = f"@{self.config.database}.{self.config.schema}.{self.config.stage}"
        target = f"{self.config.database}.{self.config.schema}.{table_name}"

        # Build format clause
        if format_name:
            format_clause = f"FILE_FORMAT = {format_name}"
        elif format_options:
            format_clause = f"FILE_FORMAT = ({format_options})"
        else:
            format_clause = self._build_format_clause(file_format, dialect, compression)

        # Build COPY INTO
        if file_format == "JSON" and not format_name and not format_options:
            # JSON → VARIANT column: use SELECT to wrap each row as a variant
            sql = (
                f"COPY INTO {target}\n"
                f"FROM (SELECT $1 FROM {stage}/{s3_relative_path})\n"
                f"{format_clause}\n"
                f"ON_ERROR = 'CONTINUE'\n"
                f"PURGE = FALSE"
            )
        else:
            sql = (
                f"COPY INTO {target}\n"
                f"FROM {stage}/{s3_relative_path}\n"
                f"{format_clause}\n"
                f"ON_ERROR = 'CONTINUE'\n"
                f"PURGE = FALSE"
            )

        if copy_options:
            sql += f"\n{copy_options}"

        logger.info(f"Executing COPY INTO: {target} from {s3_relative_path}")
        logger.debug(f"SQL: {sql}")

        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            query_id = cur.sfqid

            # Parse results
            results = cur.fetchall()
            rows_loaded = 0
            rows_parsed = 0
            errors_seen = 0
            error_details = []

            for row in results:
                # COPY INTO result columns:
                # file, status, rows_parsed, rows_loaded, error_limit, errors_seen, ...
                if len(row) >= 6:
                    rows_parsed += row[2] or 0
                    rows_loaded += row[3] or 0
                    errors_seen += row[5] or 0

                    if row[5] and row[5] > 0:
                        error_details.append({
                            "file": row[0],
                            "status": row[1],
                            "errors": row[5],
                        })

            status = "SUCCESS"
            if errors_seen > 0 and rows_loaded > 0:
                status = "PARTIAL"
            elif errors_seen > 0 and rows_loaded == 0:
                status = "FAILED"

            return {
                "rows_loaded": rows_loaded,
                "rows_parsed": rows_parsed,
                "errors_seen": errors_seen,
                "query_id": query_id,
                "status": status,
                "errors": error_details,
            }

        finally:
            cur.close()

    def get_control_override(
        self, table_name: str
    ) -> Optional[ControlTableOverride]:
        """
        Look up per-table overrides from the control table.
        Returns None if no override exists or control table doesn't exist.
        """
        cur = self._conn.cursor()
        try:
            cur.execute(
                f"SELECT LOAD_MODE, FILE_FORMAT_NAME, FILE_FORMAT_OPTIONS, "
                f"COPY_OPTIONS, PRE_LOAD_SQL, POST_LOAD_SQL, MERGE_KEYS, "
                f"AUTO_CREATE_TABLE, CREATE_TABLE_DDL, ENABLED "
                f"FROM {self.config.admin_database}.{self.config.admin_schema}.CONTROL_TABLE "
                f"WHERE TARGET_DATABASE = %s AND TARGET_SCHEMA = %s AND TARGET_TABLE = %s",
                (self.config.database.upper(), self.config.schema.upper(), table_name.upper()),
            )
            row = cur.fetchone()
            if not row:
                return None

            return ControlTableOverride(
                load_mode=row[0],
                file_format_name=row[1],
                file_format_options=row[2],
                copy_options=row[3],
                pre_load_sql=row[4],
                post_load_sql=row[5],
                merge_keys=row[6],
                auto_create_table=row[7] if row[7] is not None else True,
                create_table_ddl=row[8],
                enabled=row[9] if row[9] is not None else True,
            )

        except snowflake.connector.errors.ProgrammingError as e:
            # Control table might not exist yet — that's fine
            if "does not exist" in str(e).lower():
                logger.warning("Control table not found — using defaults")
                return None
            raise
        finally:
            cur.close()

    def execute_sql(self, sql: str) -> None:
        """Execute arbitrary SQL (for pre/post load hooks)."""
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            logger.info(f"Executed SQL hook: {sql[:100]}...")
        finally:
            cur.close()

    def _build_format_clause(
        self,
        file_format: str,
        dialect: Optional[dict],
        compression: Optional[str],
    ) -> str:
        """Build FILE_FORMAT clause from detected format and dialect."""
        parts = [f"TYPE = {file_format}"]

        if file_format == "CSV":
            d = dialect or {}
            delim = d.get("field_delimiter", ",")
            if delim == "\\t":
                parts.append("FIELD_DELIMITER = '\\t'")
            elif delim != ",":
                parts.append(f"FIELD_DELIMITER = '{delim}'")

            skip = d.get("skip_header", 1)
            parts.append(f"SKIP_HEADER = {skip}")

            enclosed = d.get("field_optionally_enclosed_by")
            if enclosed:
                parts.append(f"FIELD_OPTIONALLY_ENCLOSED_BY = '\"'")

            parts.append("NULL_IF = ('NULL', '', 'null', '\\\\N')")

        elif file_format == "JSON":
            parts.append("STRIP_OUTER_ARRAY = TRUE")

        if compression:
            parts.append(f"COMPRESSION = {compression}")

        return f"FILE_FORMAT = ({' '.join(parts)})"
