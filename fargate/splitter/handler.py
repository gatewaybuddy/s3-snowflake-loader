#!/usr/bin/env python3
"""
Fargate handler for large S3-to-Snowflake file processing.

Triggered by Lambda when files exceed size threshold (default 5GB) or are 
PGP-encrypted + large. Handles files that Lambda can't process due to 
/tmp size limits and 15-minute timeout.

Flow:
  1. Receive S3 bucket/key from environment variables
  2. Stream file from S3 (never hold full file in memory)  
  3. If PGP encrypted: streaming decrypt using GPG
  4. If compressed: streaming decompress
  5. Split at line boundaries into configurable chunks (default 1GB)
  6. GZIP each chunk
  7. Upload chunks to s3://{bucket}/{original_prefix}/_chunks/chunk-NNN.csv.gz
  8. Write _manifest.json tracking all chunks
  9. Write _COMPLETE marker on success or _FAILED on failure
  10. Optionally execute COPY INTO directly from Fargate

Environment Variables:
  - S3_BUCKET: Source bucket
  - S3_KEY: Source object key
  - CHUNK_SIZE_MB: Chunk size in MB (default: 1024 = 1GB)
  - SECRET_NAME: Secrets Manager secret for Snowflake config
  - PGP_SECRET_NAME: Secrets Manager secret for PGP keys (if encrypted)
  - DRY_RUN: Set to 'true' to skip actual uploads/COPY INTO
"""

import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from chunker import FileChunker, ChunkResult
from pgp_handler import decrypt_stream, is_pgp_encrypted
from uploader import S3Uploader, UploadResult

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# AWS clients
s3_client = boto3.client('s3')


class FargateProcessingError(Exception):
    """Raised when Fargate processing fails."""
    pass


def main():
    """Main entry point for Fargate task."""
    start_time = time.time()
    
    try:
        # Load configuration from environment
        config = _load_config()
        logger.info(f"Starting Fargate processing: s3://{config['bucket']}/{config['key']}")
        
        # Process the file
        result = process_file(config)
        
        duration = time.time() - start_time
        logger.info(
            f"Processing complete: {result['status']} in {duration:.1f}s. "
            f"Chunks: {result.get('chunks_created', 0)}, "
            f"Total rows: {result.get('total_rows', 0)}, "
            f"Total size: {result.get('total_size_mb', 0):.1f}MB"
        )
        
        # Write final status marker
        _write_status_marker(config, result)
        
        # Exit with appropriate code
        if result['status'] == 'SUCCESS':
            sys.exit(0)
        else:
            sys.exit(1)
            
    except Exception as e:
        duration = time.time() - start_time
        error_msg = f"Fargate processing failed after {duration:.1f}s: {e}"
        logger.error(f"{error_msg}\n{traceback.format_exc()}")
        
        # Try to write failure marker
        try:
            config = _load_config()
            _write_status_marker(config, {
                'status': 'FAILED',
                'error': str(e),
                'duration_seconds': duration
            })
        except Exception as marker_err:
            logger.error(f"Could not write failure marker: {marker_err}")
        
        sys.exit(1)


def process_file(config: Dict) -> Dict:
    """
    Process a single large file by streaming, decrypting, and chunking.
    
    Returns:
        Dict with processing results including status, chunks created, etc.
    """
    bucket = config['bucket']
    key = config['key']
    chunk_size_bytes = config['chunk_size_mb'] * 1024 * 1024
    dry_run = config.get('dry_run', False)
    
    # Get file info
    try:
        head_resp = s3_client.head_object(Bucket=bucket, Key=key)
        file_size = head_resp['ContentLength']
        logger.info(f"File size: {file_size:,} bytes ({file_size / (1024**3):.1f} GB)")
    except ClientError as e:
        raise FargateProcessingError(f"Could not access s3://{bucket}/{key}: {e}")
    
    # Create uploader
    chunks_prefix = _get_chunks_prefix(key)
    uploader = S3Uploader(bucket, chunks_prefix, dry_run=dry_run)
    
    # Initialize manifest
    manifest = {
        'original_file': key,
        'original_size_bytes': file_size,
        'processing_started': datetime.utcnow().isoformat() + 'Z',
        'chunk_size_bytes': chunk_size_bytes,
        'chunks': []
    }
    
    total_rows = 0
    total_size_bytes = 0
    chunk_count = 0
    
    try:
        # Stream and process file
        with _stream_file(bucket, key) as file_stream:
            # Check if PGP encrypted and decrypt if needed
            if is_pgp_encrypted(file_stream):
                logger.info("PGP encryption detected, setting up decryption stream")
                file_stream = decrypt_stream(file_stream, config.get('pgp_secret_name'))
            
            # Initialize chunker based on file extension
            chunker = FileChunker(key, chunk_size_bytes)
            
            # Process chunks
            for chunk_result in chunker.process_stream(file_stream):
                chunk_count += 1
                chunk_name = f"chunk-{chunk_count:06d}.csv.gz"
                
                logger.info(
                    f"Processing chunk {chunk_count}: {chunk_result.row_count:,} rows, "
                    f"{len(chunk_result.data) / (1024**2):.1f}MB compressed"
                )
                
                # Upload chunk
                if not dry_run:
                    upload_result = uploader.upload_chunk(chunk_name, chunk_result.data)
                    
                    if not upload_result.success:
                        raise FargateProcessingError(
                            f"Failed to upload chunk {chunk_name}: {upload_result.error}"
                        )
                else:
                    # Dry run - simulate upload
                    upload_result = UploadResult(
                        success=True,
                        s3_key=f"{chunks_prefix}/{chunk_name}",
                        size_bytes=len(chunk_result.data),
                        md5_hash="dry-run-md5"
                    )
                
                # Update manifest
                chunk_info = {
                    'chunk_name': chunk_name,
                    's3_key': upload_result.s3_key,
                    'row_count': chunk_result.row_count,
                    'size_bytes': upload_result.size_bytes,
                    'md5_hash': upload_result.md5_hash,
                    'status': 'UPLOADED' if upload_result.success else 'FAILED'
                }
                manifest['chunks'].append(chunk_info)
                
                total_rows += chunk_result.row_count
                total_size_bytes += upload_result.size_bytes
                
                # Update manifest incrementally
                if not dry_run:
                    _update_manifest(uploader, manifest)
        
        # Finalize manifest
        manifest['processing_completed'] = datetime.utcnow().isoformat() + 'Z'
        manifest['total_chunks'] = chunk_count
        manifest['total_rows'] = total_rows
        manifest['total_size_bytes'] = total_size_bytes
        
        if not dry_run:
            _update_manifest(uploader, manifest)
        
        logger.info(
            f"File processing complete: {chunk_count} chunks, {total_rows:,} rows, "
            f"{total_size_bytes / (1024**2):.1f}MB total"
        )
        
        # Execute COPY INTO if configured
        copy_result = None
        if config.get('execute_copy_into') and not dry_run:
            try:
                copy_result = _execute_snowflake_copy(config, chunks_prefix, total_rows)
            except Exception as copy_err:
                logger.error(f"COPY INTO failed (chunks still uploaded): {copy_err}")
                # Don't fail the entire process if COPY INTO fails
                # Chunks are uploaded and Lambda can retry the COPY later
        
        return {
            'status': 'SUCCESS',
            'chunks_created': chunk_count,
            'total_rows': total_rows,
            'total_size_mb': total_size_bytes / (1024**2),
            'chunks_prefix': chunks_prefix,
            'copy_result': copy_result,
            'manifest': manifest
        }
        
    except Exception as e:
        # Mark failed chunks in manifest
        for chunk in manifest.get('chunks', []):
            if chunk.get('status') != 'UPLOADED':
                chunk['status'] = 'FAILED'
                
        manifest['processing_failed'] = datetime.utcnow().isoformat() + 'Z'
        manifest['error'] = str(e)
        
        if not dry_run:
            try:
                _update_manifest(uploader, manifest)
            except Exception as manifest_err:
                logger.error(f"Could not update manifest with failure: {manifest_err}")
        
        raise FargateProcessingError(f"Processing failed: {e}") from e


def _load_config() -> Dict:
    """Load configuration from environment variables."""
    required_vars = ['S3_BUCKET', 'S3_KEY']
    
    config = {}
    for var in required_vars:
        value = os.environ.get(var)
        if not value:
            raise FargateProcessingError(f"Required environment variable {var} not set")
        config[var.lower()] = value
    
    # Optional variables with defaults
    config['chunk_size_mb'] = int(os.environ.get('CHUNK_SIZE_MB', '1024'))  # 1GB default
    config['secret_name'] = os.environ.get('SECRET_NAME')
    config['pgp_secret_name'] = os.environ.get('PGP_SECRET_NAME')
    config['execute_copy_into'] = os.environ.get('EXECUTE_COPY_INTO', 'false').lower() == 'true'
    config['dry_run'] = os.environ.get('DRY_RUN', 'false').lower() == 'true'
    
    return config


def _stream_file(bucket: str, key: str):
    """
    Stream file from S3 without loading into memory.
    Returns a file-like object that can be read in chunks.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body']
    except ClientError as e:
        raise FargateProcessingError(f"Could not stream s3://{bucket}/{key}: {e}")


def _get_chunks_prefix(original_key: str) -> str:
    """Generate the S3 prefix for chunks based on original file path."""
    # Remove filename, add _chunks subdirectory
    # e.g., "data/table1/file.csv" -> "data/table1/_chunks"
    path_parts = original_key.split('/')
    if len(path_parts) > 1:
        directory = '/'.join(path_parts[:-1])
        return f"{directory}/_chunks"
    else:
        return "_chunks"


def _update_manifest(uploader: S3Uploader, manifest: Dict) -> None:
    """Write/update the manifest.json file."""
    manifest_json = json.dumps(manifest, indent=2)
    manifest_bytes = manifest_json.encode('utf-8')
    
    result = uploader.upload_chunk('_manifest.json', manifest_bytes, content_type='application/json')
    if not result.success:
        logger.warning(f"Failed to update manifest: {result.error}")


def _write_status_marker(config: Dict, result: Dict) -> None:
    """Write _COMPLETE or _FAILED marker to S3."""
    bucket = config['bucket']
    key = config['key']
    chunks_prefix = _get_chunks_prefix(key)
    
    if result['status'] == 'SUCCESS':
        marker_key = f"{chunks_prefix}/_COMPLETE"
        marker_data = {
            'status': 'SUCCESS',
            'completed_at': datetime.utcnow().isoformat() + 'Z',
            'chunks_created': result.get('chunks_created', 0),
            'total_rows': result.get('total_rows', 0),
            'total_size_mb': result.get('total_size_mb', 0),
            'fargate_task': True
        }
    else:
        marker_key = f"{chunks_prefix}/_FAILED"
        marker_data = {
            'status': 'FAILED',
            'failed_at': datetime.utcnow().isoformat() + 'Z',
            'error': result.get('error', 'Unknown error'),
            'duration_seconds': result.get('duration_seconds'),
            'fargate_task': True
        }
    
    try:
        marker_json = json.dumps(marker_data, indent=2)
        s3_client.put_object(
            Bucket=bucket,
            Key=marker_key,
            Body=marker_json.encode('utf-8'),
            ContentType='application/json'
        )
        logger.info(f"Status marker written: s3://{bucket}/{marker_key}")
    except Exception as e:
        logger.error(f"Failed to write status marker: {e}")


def _execute_snowflake_copy(config: Dict, chunks_prefix: str, expected_rows: int) -> Optional[Dict]:
    """
    Execute COPY INTO Snowflake from the chunks prefix.
    This is optional - Lambda can also handle this later when it sees _COMPLETE.
    """
    if not config.get('secret_name'):
        logger.info("No SECRET_NAME configured, skipping COPY INTO")
        return None
    
    try:
        # Import here to avoid dependency if not needed
        from lambda.loader.config_loader import load_config
        from lambda.loader.snowflake_client import SnowflakeClient
        from lambda.loader.path_parser import parse_s3_key
        
        # Load Snowflake config
        sf_config = load_config(config['secret_name'])
        
        # Parse table info from original key
        parsed = parse_s3_key(config['key'], "")  # No prefix stripping in Fargate
        
        # Connect and execute COPY
        client = SnowflakeClient(sf_config)
        with client.connect():
            copy_result = client.copy_into(
                table_name=parsed.table_name,
                s3_relative_path=f"{chunks_prefix}/",
                file_format="CSV",  # Chunks are always CSV.gz
                compression="GZIP",
                copy_options={
                    'PATTERN': 'chunk-.*\\.csv\\.gz'  # Only process chunk files, not manifest
                }
            )
            
            logger.info(
                f"COPY INTO complete: {copy_result['rows_loaded']} rows loaded "
                f"({copy_result['rows_parsed']} parsed, {copy_result['errors_seen']} errors)"
            )
            
            # Verify row count matches expectation
            if copy_result['rows_loaded'] != expected_rows:
                logger.warning(
                    f"Row count mismatch: expected {expected_rows}, loaded {copy_result['rows_loaded']}"
                )
            
            return copy_result
    
    except Exception as e:
        logger.error(f"COPY INTO failed: {e}")
        raise


if __name__ == '__main__':
    main()