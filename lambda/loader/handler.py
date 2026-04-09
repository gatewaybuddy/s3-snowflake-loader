"""
Lambda handler for S3 → Snowflake data loading.

Triggered by S3 ObjectCreated events. Each Lambda instance handles
one database. The configurable S3 prefix filter ensures only relevant files arrive.

Flow:
  1. Parse S3 event → extract bucket, key, size
  2. Parse path → table name, load mode, file format
  3. Load config from Secrets Manager
  4. Validate file (empty, size, format, PGP, poison detection)
  5. Decrypt PGP-encrypted files if detected
  6. Check for duplicate loads (ETag + path in LOAD_HISTORY)
  7. Check control table for overrides
  8. Auto-detect format from file content if needed
  9. Create table if it doesn't exist
  10. COPY INTO Snowflake
  11. Log everything to admin database
"""

import json
import logging
import os
import traceback
from datetime import datetime

import boto3

from config_loader import load_config
from format_detector import detect_format_from_content
from history_logger import HistoryLogger, LoadRecord
from notifier import notify_failure, notify_success, notify_warning
from path_parser import parse_s3_key
from pgp_handler import PGPDecryptionError, cleanup_decrypted_file, decrypt_file
from snowflake_client import SnowflakeClient
from validator import validate_file

# Configure logging
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger = logging.getLogger()
logger.setLevel(getattr(logging, log_level))

s3_client = boto3.client("s3")


def lambda_handler(event, context):
    """
    Main entry point. Processes one or more S3 ObjectCreated events.
    
    Each event record is processed independently — a batch of files
    won't fail atomically (one failure doesn't block others).
    """
    results = []
    s3_prefix = os.environ.get("S3_PREFIX", "")

    for record in event.get("Records", []):
        try:
            result = _process_record(record, s3_prefix, context)
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to process record: {e}\n{traceback.format_exc()}")
            results.append({
                "status": "FAILED",
                "error": str(e),
                "s3_key": record.get("s3", {}).get("object", {}).get("key", "unknown"),
            })

    # Summary
    succeeded = sum(1 for r in results if r.get("status") == "SUCCESS")
    partial = sum(1 for r in results if r.get("status") == "PARTIAL")
    failed = sum(1 for r in results if r.get("status") == "FAILED")

    logger.info(
        f"Batch complete: {succeeded} succeeded, {partial} partial, {failed} failed "
        f"out of {len(results)} files"
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": len(results),
            "succeeded": succeeded,
            "partial": partial,
            "failed": failed,
            "results": results,
        }),
    }


def _process_record(record: dict, s3_prefix: str, context) -> dict:
    """Process a single S3 event record."""
    # 1. Extract S3 info
    s3_info = record["s3"]
    bucket = s3_info["bucket"]["name"]
    key = s3_info["object"]["key"]
    size = s3_info["object"].get("size", 0)
    etag = s3_info["object"].get("eTag", "")

    logger.info(f"Processing: s3://{bucket}/{key} ({size} bytes)")

    # Skip folder markers and zero-byte files
    if key.endswith("/") or size == 0:
        logger.info(f"Skipping folder marker or empty file: {key}")
        return {"status": "SKIPPED", "s3_key": key, "reason": "folder marker or empty"}

    # 2. Parse path
    parsed = parse_s3_key(key, s3_prefix)
    logger.info(
        f"Parsed: table={parsed.table_name} mode={parsed.load_mode} "
        f"format={parsed.file_format} compression={parsed.compression}"
    )

    # 3. Load config
    config = load_config()

    # 4. Initialize load record
    load_record = LoadRecord(
        s3_bucket=bucket,
        s3_key=key,
        s3_size_bytes=size,
        s3_etag=etag,
        target_database=config.database,
        target_schema=config.schema,
        target_table=parsed.table_name,
        load_mode=parsed.load_mode,
        lambda_request_id=context.aws_request_id if context else "",
        lambda_function=context.function_name if context else "",
    )

    # 5. Pre-load validation gate
    head = _read_s3_head(bucket, key, bytes_count=65536)
    validation = validate_file(
        head_bytes=head,
        file_size=size,
        file_extension=parsed.file_extension,
        expected_format=parsed.file_format,
    )

    if not validation.valid:
        logger.error(
            f"Validation failed for {key}: [{validation.error_code}] {validation.error}"
        )
        # Still log to Snowflake so the failure is visible in LOAD_HISTORY
        client = SnowflakeClient(config)
        with client.connect():
            history = HistoryLogger(client._conn, config.admin_database, config.admin_schema)
            load_record.status = "FAILED"
            load_record.error_message = f"[{validation.error_code}] {validation.error}"
            history.insert_loading(load_record)
            history.update_complete(load_record)
        return {
            "status": "FAILED",
            "s3_key": key,
            "error_code": validation.error_code,
            "error": validation.error,
            "load_id": load_record.load_id,
        }

    for warning in validation.warnings:
        logger.warning(f"Validation warning for {key}: {warning}")

    # 5b. Check if file should be processed by Fargate
    fargate_threshold_bytes = int(os.environ.get("FARGATE_SIZE_THRESHOLD", "5368709120"))  # 5GB default
    
    should_use_fargate = False
    fargate_reason = None
    
    # Check file size threshold
    if size >= fargate_threshold_bytes:
        should_use_fargate = True
        fargate_reason = f"file size ({size / (1024**3):.1f}GB) exceeds threshold ({fargate_threshold_bytes / (1024**3):.1f}GB)"
    
    # Check if PGP encrypted AND large (configurable threshold for PGP files)
    elif validation.is_pgp_encrypted:
        pgp_threshold_bytes = int(os.environ.get("PGP_FARGATE_THRESHOLD", "1073741824"))  # 1GB default for PGP files
        if size >= pgp_threshold_bytes:
            should_use_fargate = True
            fargate_reason = f"PGP-encrypted file ({size / (1024**2):.1f}MB) exceeds PGP threshold ({pgp_threshold_bytes / (1024**2):.1f}MB)"
    
    # Special handling for chunks directory (ignore individual chunks and markers)
    if "/_chunks/" in key:
        if key.endswith("_COMPLETE"):
            logger.info(f"Fargate completion marker detected: {key}")
            return _handle_fargate_completion(bucket, key, parsed, config, load_record)
        elif key.endswith("_FAILED"):
            logger.info(f"Fargate failure marker detected: {key}")
            return _handle_fargate_failure(bucket, key, parsed, config, load_record)
        elif key.endswith(".json") and "_manifest.json" in key:
            logger.info(f"Skipping Fargate manifest file: {key}")
            return {"status": "SKIPPED", "s3_key": key, "reason": "fargate manifest file"}
        else:
            logger.info(f"Skipping individual Fargate chunk: {key}")
            return {"status": "SKIPPED", "s3_key": key, "reason": "individual fargate chunk"}
    
    # Trigger Fargate if needed
    if should_use_fargate:
        logger.info(f"Triggering Fargate processing: {fargate_reason}")
        try:
            fargate_result = _trigger_fargate_task(bucket, key, config, parsed, load_record)
            return fargate_result
        except Exception as e:
            logger.error(f"Failed to trigger Fargate task: {e}")
            # Fall through to Lambda processing as fallback
            logger.warning("Attempting Lambda processing as fallback...")

    # 5c. PGP decryption (if detected)
    decrypted_path = None
    if validation.is_pgp_encrypted:
        try:
            logger.info(f"PGP-encrypted file detected: {key} — decrypting")
            s3_body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
            decrypted_path = decrypt_file(s3_body, parsed.file_name)
            logger.info(f"PGP decryption complete: {decrypted_path}")
            # Re-read head from decrypted file for format detection
            with open(decrypted_path, "rb") as f:
                head = f.read(65536)
            # Re-validate the decrypted content
            decrypted_size = os.path.getsize(decrypted_path)
            validation = validate_file(
                head_bytes=head,
                file_size=decrypted_size,
                file_extension=parsed.file_extension,
                expected_format=parsed.file_format,
            )
            if not validation.valid:
                cleanup_decrypted_file(decrypted_path)
                raise ValueError(
                    f"Decrypted file failed validation: [{validation.error_code}] "
                    f"{validation.error}"
                )
        except PGPDecryptionError as e:
            if decrypted_path:
                cleanup_decrypted_file(decrypted_path)
            raise ValueError(f"PGP decryption failed: {e}") from e

    # 6. Connect to Snowflake and process
    client = SnowflakeClient(config)

    with client.connect():
        history = HistoryLogger(client._conn, config.admin_database, config.admin_schema)

        # Log initial LOADING status
        history.insert_loading(load_record)

        try:
            # 6a. Check control table
            override = client.get_control_override(parsed.table_name)

            if override and not override.enabled:
                load_record.status = "SKIPPED"
                load_record.error_message = "Disabled in control table"
                history.update_complete(load_record)
                return {"status": "SKIPPED", "s3_key": key, "reason": "disabled"}

            # Apply overrides
            load_mode = (override.load_mode if override and override.load_mode
                         else parsed.load_mode)
            load_record.load_mode = load_mode

            # 6b. Duplicate detection — check LOAD_HISTORY for matching ETag + path
            duplicate_of = history.check_duplicate(etag, key)
            if duplicate_of:
                if load_mode == "TRUNCATE":
                    # TRUNCATE mode is idempotent — proceed anyway
                    logger.info(
                        f"Duplicate detected (ETag+path match load {duplicate_of}) "
                        f"but TRUNCATE mode is idempotent — proceeding"
                    )
                elif load_mode == "APPEND":
                    # APPEND mode — warn and proceed but mark as duplicate
                    logger.warning(
                        f"DUPLICATE DETECTED: ETag {etag} + path {key} already loaded "
                        f"in load {duplicate_of}. APPEND mode — proceeding but marking "
                        f"as DUPLICATE in history."
                    )
                    load_record.duplicate_of = duplicate_of
                    try:
                        notify_warning(
                            s3_key=key,
                            table=parsed.table_name,
                            database=config.database,
                            warning_type="Duplicate File",
                            detail=(
                                f"File with same ETag and path was previously loaded "
                                f"(load_id={duplicate_of}). Proceeding in APPEND mode."
                            ),
                            load_id=load_record.load_id,
                        )
                    except Exception:
                        pass  # Notification failure shouldn't block loading
                elif load_mode == "MERGE":
                    # MERGE mode is naturally idempotent — proceed
                    logger.info(
                        f"Duplicate detected (ETag+path match load {duplicate_of}) "
                        f"but MERGE mode is idempotent — proceeding"
                    )

            # 7. Auto-detect format if needed
            dialect = {}
            file_format = parsed.file_format

            if file_format == "CSV":
                # Use head bytes from validation (already read)
                if head:
                    detection = detect_format_from_content(head, parsed.file_extension)
                    if detection["type"] != "CSV":
                        # Extension said CSV but content says otherwise
                        file_format = detection["type"]
                        logger.info(f"Format override: extension={parsed.file_format} → content={file_format}")
                        try:
                            notify_warning(
                                s3_key=key,
                                table=parsed.table_name,
                                database=config.database,
                                warning_type="Format Mismatch",
                                detail=f"Extension indicates {parsed.file_format} but content detected as {file_format}",
                                load_id=load_record.load_id,
                            )
                        except Exception as notif_err:
                            logger.error(f"Failed to send format warning: {notif_err}")
                    dialect = detection.get("dialect", {})

            load_record.file_format_used = file_format
            if dialect:
                load_record.file_format_used += f" ({json.dumps(dialect)})"

            # 8. Check/create table
            auto_create = override.auto_create_table if override else True

            if not client.table_exists(parsed.table_name):
                if not auto_create:
                    raise ValueError(
                        f"Table {parsed.table_name} does not exist and "
                        f"auto_create_table is disabled in control table"
                    )

                logger.info(f"Table {parsed.table_name} doesn't exist — creating")

                if override and override.create_table_ddl:
                    client.create_table_from_ddl(override.create_table_ddl)
                elif file_format in ("PARQUET", "AVRO", "ORC"):
                    client.create_table_from_infer_schema(
                        parsed.table_name, parsed.relative_path, file_format
                    )
                elif file_format == "JSON":
                    # JSON files get a single VARIANT column
                    client.create_table_for_json(parsed.table_name)
                else:
                    # CSV: read header to get column names
                    head_text = _read_s3_head_text(bucket, key)
                    if head_text:
                        header_line = head_text.split("\n")[0]
                        delim = dialect.get("field_delimiter", ",")
                        if delim == "\\t":
                            delim = "\t"
                        columns = [c.strip().strip('"') for c in header_line.split(delim)]
                        client.create_table_from_header(parsed.table_name, columns)
                    else:
                        raise ValueError(
                            f"Cannot read file header to create table {parsed.table_name}"
                        )

                load_record.table_created = True

            # 9. Pre-load SQL
            if override and override.pre_load_sql:
                logger.info(f"Running pre-load SQL for {parsed.table_name}")
                client.execute_sql(override.pre_load_sql)

            # 10. Truncate if needed
            if load_mode == "TRUNCATE":
                client.truncate_table(parsed.table_name)

            # 11. COPY INTO
            copy_result = client.copy_into(
                table_name=parsed.table_name,
                s3_relative_path=parsed.relative_path,
                file_format=file_format,
                dialect=dialect,
                format_name=override.file_format_name if override else None,
                format_options=override.file_format_options if override else None,
                copy_options=override.copy_options if override else None,
                compression=parsed.compression,
            )

            load_record.rows_loaded = copy_result["rows_loaded"]
            load_record.rows_parsed = copy_result["rows_parsed"]
            load_record.errors_seen = copy_result["errors_seen"]
            load_record.copy_into_query_id = copy_result["query_id"]
            load_record.status = copy_result["status"]

            # 12. Log errors if any
            if copy_result["errors_seen"] > 0:
                error_details = history.get_copy_errors(copy_result["query_id"])
                history.log_errors(
                    load_record.load_id,
                    config.database,
                    key,
                    error_details,
                )

            # 13. Post-load SQL
            if override and override.post_load_sql:
                logger.info(f"Running post-load SQL for {parsed.table_name}")
                client.execute_sql(override.post_load_sql)

            # 14. Update history (includes DUPLICATE_OF if set)
            history.update_complete(load_record)

            logger.info(
                f"Load complete: {parsed.table_name} — "
                f"{load_record.rows_loaded} rows loaded, "
                f"{load_record.errors_seen} errors, "
                f"status={load_record.status}"
                + (f" (duplicate of {load_record.duplicate_of})" if load_record.duplicate_of else "")
            )

            # 15. Send notifications and log to audit trail
            try:
                if load_record.status == "PARTIAL":
                    msg_id = notify_warning(
                        s3_key=key,
                        table=parsed.table_name,
                        database=config.database,
                        warning_type="Partial Load",
                        detail=f"{load_record.errors_seen} of {load_record.rows_parsed} rows rejected",
                        load_id=load_record.load_id,
                        rows_loaded=load_record.rows_loaded,
                        errors_seen=load_record.errors_seen,
                    )
                    history.log_notification(
                        load_id=load_record.load_id,
                        notification_type="WARNING",
                        channel="SNS",
                        subject=f"[WARNING] Partial Load — {parsed.table_name}",
                        body=f"{load_record.errors_seen} of {load_record.rows_parsed} rows rejected",
                        delivery_status="SENT" if msg_id else "SKIPPED",
                        sns_message_id=msg_id,
                    )
                else:
                    msg_id = notify_success(
                        s3_key=key,
                        table=parsed.table_name,
                        database=config.database,
                        rows_loaded=load_record.rows_loaded,
                        rows_parsed=load_record.rows_parsed,
                        duration_seconds=load_record.duration_seconds,
                        load_id=load_record.load_id,
                        table_created=load_record.table_created,
                    )
                    history.log_notification(
                        load_id=load_record.load_id,
                        notification_type="SUCCESS",
                        channel="SNS",
                        subject=f"[SUCCESS] Loaded {parsed.table_name} - {load_record.rows_loaded} rows",
                        body=f"Loaded {load_record.rows_loaded} rows in {load_record.duration_seconds}s",
                        delivery_status="SENT" if msg_id else "SKIPPED",
                        sns_message_id=msg_id,
                    )
            except Exception as notif_err:
                logger.error(f"Failed to send notification: {notif_err}")

            return {
                "status": load_record.status,
                "s3_key": key,
                "table": parsed.table_name,
                "rows_loaded": load_record.rows_loaded,
                "rows_parsed": load_record.rows_parsed,
                "errors": load_record.errors_seen,
                "load_id": load_record.load_id,
                "table_created": load_record.table_created,
                "duplicate_of": load_record.duplicate_of,
            }

        except Exception as e:
            # Update history with failure
            load_record.status = "FAILED"
            load_record.error_message = str(e)[:4000]
            try:
                history.update_complete(load_record)
            except Exception as log_err:
                logger.error(f"Failed to log error to history: {log_err}")

            # Send failure notification and log to audit trail
            try:
                msg_id = notify_failure(
                    s3_key=key,
                    table=parsed.table_name,
                    database=config.database,
                    error_message=str(e)[:4000],
                    load_id=load_record.load_id,
                )
                history.log_notification(
                    load_id=load_record.load_id,
                    notification_type="FAILURE",
                    channel="SNS",
                    subject=f"[FAILED] Load failed for {parsed.table_name}",
                    body=str(e)[:4000],
                    delivery_status="SENT" if msg_id else "SKIPPED",
                    sns_message_id=msg_id,
                )
            except Exception as notif_err:
                logger.error(f"Failed to send failure notification: {notif_err}")

            raise

        finally:
            # Clean up any decrypted PGP files
            if decrypted_path:
                cleanup_decrypted_file(decrypted_path)


def _read_s3_head(bucket: str, key: str, bytes_count: int = 65536) -> bytes:
    """Read the first N bytes of an S3 object."""
    try:
        response = s3_client.get_object(
            Bucket=bucket,
            Key=key,
            Range=f"bytes=0-{bytes_count - 1}",
        )
        return response["Body"].read()
    except Exception as e:
        logger.warning(f"Could not read head of s3://{bucket}/{key}: {e}")
        return b""


def _read_s3_head_text(bucket: str, key: str, bytes_count: int = 8192) -> str:
    """Read the first N bytes of an S3 object as text."""
    head = _read_s3_head(bucket, key, bytes_count)
    if not head:
        return ""
    try:
        return head.decode("utf-8-sig")
    except UnicodeDecodeError:
        return head.decode("latin-1", errors="replace")


def _trigger_fargate_task(bucket: str, key: str, config, parsed, load_record) -> dict:
    """
    Trigger Fargate task to process large/PGP-encrypted files.
    
    Returns immediately after task starts - does not wait for completion.
    """
    ecs_client = boto3.client('ecs')
    
    # Get Fargate configuration from environment
    fargate_cluster = os.environ.get("FARGATE_CLUSTER_ARN")
    fargate_task_definition = os.environ.get("FARGATE_TASK_DEFINITION_ARN")
    fargate_subnet_ids = os.environ.get("FARGATE_SUBNET_IDS", "").split(",")
    fargate_security_group_ids = os.environ.get("FARGATE_SECURITY_GROUP_IDS", "").split(",")
    
    if not all([fargate_cluster, fargate_task_definition, fargate_subnet_ids]):
        raise ValueError(
            "Fargate configuration missing. Required: FARGATE_CLUSTER_ARN, "
            "FARGATE_TASK_DEFINITION_ARN, FARGATE_SUBNET_IDS"
        )
    
    # Prepare task environment
    task_env = {
        "S3_BUCKET": bucket,
        "S3_KEY": key,
        "CHUNK_SIZE_MB": os.environ.get("FARGATE_CHUNK_SIZE_MB", "1024"),
        "SECRET_NAME": os.environ.get("SECRET_NAME"),
        "PGP_SECRET_NAME": os.environ.get("PGP_SECRET_NAME", ""),
        "EXECUTE_COPY_INTO": os.environ.get("FARGATE_EXECUTE_COPY_INTO", "false"),
    }
    
    # Convert to ECS task environment format
    task_env_list = [
        {"name": k, "value": v} 
        for k, v in task_env.items() 
        if v  # Only include non-empty values
    ]
    
    # Network configuration
    network_config = {
        "awsvpcConfiguration": {
            "subnets": [s.strip() for s in fargate_subnet_ids if s.strip()],
            "assignPublicIp": "ENABLED"  # Needed for S3 access if no NAT Gateway
        }
    }
    
    if fargate_security_group_ids[0]:  # Check if security groups provided
        network_config["awsvpcConfiguration"]["securityGroups"] = [
            s.strip() for s in fargate_security_group_ids if s.strip()
        ]
    
    try:
        # Start Fargate task
        response = ecs_client.run_task(
            cluster=fargate_cluster,
            taskDefinition=fargate_task_definition,
            launchType="FARGATE",
            platformVersion="LATEST",
            networkConfiguration=network_config,
            overrides={
                "containerOverrides": [
                    {
                        "name": "fargate-splitter",  # Must match container name in task definition
                        "environment": task_env_list
                    }
                ]
            },
            tags=[
                {"key": "SourceBucket", "value": bucket},
                {"key": "SourceKey", "value": key},
                {"key": "TargetTable", "value": parsed.table_name},
                {"key": "LoadId", "value": load_record.load_id},
                {"key": "LambdaFunction", "value": load_record.lambda_function},
            ]
        )
        
        # Get task ARN
        task_arn = response["tasks"][0]["taskArn"] if response["tasks"] else None
        
        if not task_arn:
            raise ValueError("Failed to start Fargate task - no task ARN returned")
        
        logger.info(f"Fargate task started: {task_arn}")
        
        # Update load record to track Fargate processing
        load_record.status = "PROCESSING_FARGATE"
        load_record.error_message = f"Processing by Fargate task: {task_arn}"
        
        # Log to Snowflake history
        client = SnowflakeClient(config)
        with client.connect():
            history = HistoryLogger(client._conn, config.admin_database, config.admin_schema)
            history.insert_loading(load_record)
            history.update_complete(load_record)
        
        return {
            "status": "FARGATE_TRIGGERED",
            "s3_key": key,
            "table": parsed.table_name,
            "fargate_task_arn": task_arn,
            "load_id": load_record.load_id,
            "message": "Large file processing delegated to Fargate"
        }
        
    except Exception as e:
        logger.error(f"Failed to start Fargate task: {e}")
        raise ValueError(f"Fargate task creation failed: {e}") from e


def _handle_fargate_completion(bucket: str, marker_key: str, parsed, config, load_record) -> dict:
    """
    Handle Fargate _COMPLETE marker by executing COPY INTO from chunks.
    
    Called when Lambda detects a _COMPLETE marker in the _chunks/ directory.
    """
    logger.info(f"Processing Fargate completion marker: {marker_key}")
    
    # Parse chunks prefix from marker key
    # e.g., "data/table1/_chunks/_COMPLETE" -> "data/table1/_chunks"
    chunks_prefix = marker_key.rsplit('/', 1)[0]
    
    try:
        # Read completion marker for metadata
        marker_response = s3_client.get_object(Bucket=bucket, Key=marker_key)
        marker_data = json.loads(marker_response['Body'].read().decode('utf-8'))
        
        expected_rows = marker_data.get('total_rows', 0)
        total_chunks = marker_data.get('chunks_created', 0)
        
        logger.info(
            f"Fargate completion details: {total_chunks} chunks, {expected_rows:,} total rows"
        )
        
        # Execute COPY INTO from all chunks
        client = SnowflakeClient(config)
        with client.connect():
            history = HistoryLogger(client._conn, config.admin_database, config.admin_schema)
            
            # Log initial LOADING status for the COPY operation
            load_record.status = "LOADING"
            load_record.error_message = f"Loading from Fargate chunks: {chunks_prefix}"
            history.insert_loading(load_record)
            
            try:
                # Check/create table if needed (same logic as normal Lambda)
                if not client.table_exists(parsed.table_name):
                    logger.info(f"Creating table {parsed.table_name} for Fargate chunks")
                    # For Fargate chunks, we assume CSV format
                    # Read manifest to get more details if needed
                    client.create_table_for_csv_chunks(parsed.table_name)
                
                # Execute COPY INTO with pattern to match only chunk files
                copy_result = client.copy_into(
                    table_name=parsed.table_name,
                    s3_relative_path=f"{chunks_prefix}/",
                    file_format="CSV",
                    compression="GZIP", 
                    copy_options={
                        "PATTERN": "chunk-.*\\.csv\\.gz"  # Only process chunk files
                    }
                )
                
                # Update load record
                load_record.rows_loaded = copy_result["rows_loaded"]
                load_record.rows_parsed = copy_result["rows_parsed"]
                load_record.errors_seen = copy_result["errors_seen"]
                load_record.copy_into_query_id = copy_result["query_id"]
                load_record.status = copy_result["status"]
                
                # Check row count matches Fargate processing
                if abs(load_record.rows_loaded - expected_rows) > expected_rows * 0.01:  # 1% tolerance
                    logger.warning(
                        f"Row count mismatch: Fargate reported {expected_rows}, "
                        f"COPY INTO loaded {load_record.rows_loaded}"
                    )
                
                history.update_complete(load_record)
                
                logger.info(
                    f"Fargate COPY complete: {load_record.rows_loaded:,} rows loaded "
                    f"from {total_chunks} chunks"
                )
                
                return {
                    "status": load_record.status,
                    "s3_key": marker_key,
                    "table": parsed.table_name,
                    "rows_loaded": load_record.rows_loaded,
                    "chunks_processed": total_chunks,
                    "fargate_chunks_prefix": chunks_prefix,
                    "load_id": load_record.load_id
                }
                
            except Exception as e:
                load_record.status = "FAILED"
                load_record.error_message = f"Fargate COPY INTO failed: {str(e)[:4000]}"
                history.update_complete(load_record)
                raise
    
    except Exception as e:
        logger.error(f"Failed to process Fargate completion: {e}")
        return {
            "status": "FAILED",
            "s3_key": marker_key,
            "error": f"Fargate completion processing failed: {e}",
            "load_id": load_record.load_id
        }


def _handle_fargate_failure(bucket: str, marker_key: str, parsed, config, load_record) -> dict:
    """
    Handle Fargate _FAILED marker by logging the failure and sending notifications.
    """
    logger.error(f"Fargate processing failed: {marker_key}")
    
    try:
        # Read failure marker for error details
        marker_response = s3_client.get_object(Bucket=bucket, Key=marker_key)
        failure_data = json.loads(marker_response['Body'].read().decode('utf-8'))
        
        error_message = failure_data.get('error', 'Unknown Fargate error')
        failed_at = failure_data.get('failed_at', 'Unknown time')
        
        logger.error(f"Fargate failure details: {error_message} (at {failed_at})")
        
        # Log failure to Snowflake
        load_record.status = "FAILED"
        load_record.error_message = f"Fargate processing failed: {error_message}"
        
        client = SnowflakeClient(config)
        with client.connect():
            history = HistoryLogger(client._conn, config.admin_database, config.admin_schema)
            history.insert_loading(load_record)
            history.update_complete(load_record)
            
            # Send failure notification
            try:
                notify_failure(
                    s3_key=marker_key,
                    table=parsed.table_name,
                    database=config.database,
                    error_message=f"Fargate processing failed: {error_message}",
                    load_id=load_record.load_id,
                )
            except Exception as notif_err:
                logger.error(f"Failed to send Fargate failure notification: {notif_err}")
        
        return {
            "status": "FAILED",
            "s3_key": marker_key,
            "table": parsed.table_name,
            "error": f"Fargate processing failed: {error_message}",
            "failed_at": failed_at,
            "load_id": load_record.load_id
        }
        
    except Exception as e:
        logger.error(f"Failed to process Fargate failure marker: {e}")
        return {
            "status": "FAILED",
            "s3_key": marker_key,
            "error": f"Failed to read Fargate failure details: {e}",
            "load_id": load_record.load_id
        }
