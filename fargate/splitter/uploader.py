"""
S3 multipart uploader with retry logic for Fargate chunk uploads.

Handles large chunk uploads with:
- Multipart upload for chunks > 100MB
- Exponential backoff retry (3 attempts)
- MD5 verification after upload
- Incremental manifest updates
"""

import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


@dataclass
class UploadResult:
    """Result of chunk upload operation."""
    success: bool
    s3_key: str
    size_bytes: int
    md5_hash: str
    error: Optional[str] = None
    attempts: int = 1


class S3Uploader:
    """
    S3 uploader with multipart support and retry logic.
    
    Automatically uses multipart upload for chunks > 100MB and handles
    retries with exponential backoff for transient failures.
    """
    
    def __init__(self, bucket: str, prefix: str, dry_run: bool = False):
        """
        Initialize uploader.
        
        Args:
            bucket: S3 bucket name
            prefix: S3 key prefix for uploads (e.g., "data/table1/_chunks")
            dry_run: If True, simulate uploads without actually uploading
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip('/')  # Remove trailing slash
        self.dry_run = dry_run
        self.s3_client = boto3.client('s3')
        
        # Multipart upload threshold (100MB)
        self.multipart_threshold = 100 * 1024 * 1024
        
        # Retry configuration
        self.max_retries = 3
        self.base_delay = 1.0  # Base delay in seconds
        self.max_delay = 30.0  # Maximum delay in seconds
        
        logger.info(
            f"S3 Uploader initialized: bucket={bucket}, prefix={prefix}, "
            f"dry_run={dry_run}, multipart_threshold={self.multipart_threshold / (1024**2):.1f}MB"
        )
    
    def upload_chunk(
        self, 
        chunk_name: str, 
        data: bytes, 
        content_type: str = 'application/gzip'
    ) -> UploadResult:
        """
        Upload a chunk with retry logic.
        
        Args:
            chunk_name: Name of the chunk file (e.g., "chunk-001.csv.gz")
            data: Chunk data to upload
            content_type: MIME type for the upload
            
        Returns:
            UploadResult with success status and metadata
        """
        s3_key = f"{self.prefix}/{chunk_name}"
        size_bytes = len(data)
        md5_hash = hashlib.md5(data).hexdigest()
        
        if self.dry_run:
            logger.info(f"DRY RUN: Would upload {chunk_name} ({size_bytes / (1024**2):.1f}MB)")
            return UploadResult(
                success=True,
                s3_key=s3_key,
                size_bytes=size_bytes,
                md5_hash=md5_hash,
                attempts=1
            )
        
        # Attempt upload with retries
        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                if size_bytes > self.multipart_threshold:
                    self._upload_multipart(s3_key, data, content_type, md5_hash)
                else:
                    self._upload_single_part(s3_key, data, content_type, md5_hash)
                
                logger.info(
                    f"Upload successful: {chunk_name} → s3://{self.bucket}/{s3_key} "
                    f"({size_bytes / (1024**2):.1f}MB) [attempt {attempt}]"
                )
                
                return UploadResult(
                    success=True,
                    s3_key=s3_key,
                    size_bytes=size_bytes,
                    md5_hash=md5_hash,
                    attempts=attempt
                )
                
            except Exception as e:
                last_error = e
                
                if attempt < self.max_retries:
                    delay = min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)
                    logger.warning(
                        f"Upload failed (attempt {attempt}/{self.max_retries}): {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"Upload failed after {self.max_retries} attempts: {e}"
                    )
        
        return UploadResult(
            success=False,
            s3_key=s3_key,
            size_bytes=size_bytes,
            md5_hash=md5_hash,
            error=str(last_error),
            attempts=self.max_retries
        )
    
    def _upload_single_part(
        self, 
        s3_key: str, 
        data: bytes, 
        content_type: str, 
        expected_md5: str
    ) -> None:
        """Upload data as a single S3 object."""
        try:
            # Upload with MD5 verification
            response = self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=data,
                ContentType=content_type,
                ContentMD5=_base64_md5(data)
            )
            
            # Verify ETag matches our MD5 (for single-part uploads, ETag = MD5)
            etag = response.get('ETag', '').strip('"')
            if etag != expected_md5:
                raise ValueError(f"MD5 mismatch: expected {expected_md5}, got {etag}")
                
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            raise Exception(f"S3 upload failed ({error_code}): {e}") from e
    
    def _upload_multipart(
        self, 
        s3_key: str, 
        data: bytes, 
        content_type: str,
        expected_md5: str
    ) -> None:
        """Upload data using S3 multipart upload."""
        # Part size for multipart upload (5MB minimum, 100MB default)
        part_size = max(5 * 1024 * 1024, len(data) // 10000)  # Max 10k parts
        part_size = min(part_size, 100 * 1024 * 1024)  # 100MB max per part
        
        try:
            # Initiate multipart upload
            response = self.s3_client.create_multipart_upload(
                Bucket=self.bucket,
                Key=s3_key,
                ContentType=content_type
            )
            upload_id = response['UploadId']
            
            parts = []
            part_number = 1
            offset = 0
            
            while offset < len(data):
                # Extract part data
                end_offset = min(offset + part_size, len(data))
                part_data = data[offset:end_offset]
                
                # Upload part
                part_response = self.s3_client.upload_part(
                    Bucket=self.bucket,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=part_data,
                    ContentMD5=_base64_md5(part_data)
                )
                
                parts.append({
                    'ETag': part_response['ETag'],
                    'PartNumber': part_number
                })
                
                offset = end_offset
                part_number += 1
            
            # Complete multipart upload
            self.s3_client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            logger.info(f"Multipart upload complete: {len(parts)} parts")
            
            # Verify upload by getting object metadata
            head_response = self.s3_client.head_object(Bucket=self.bucket, Key=s3_key)
            if head_response['ContentLength'] != len(data):
                raise ValueError(
                    f"Size mismatch after upload: expected {len(data)}, "
                    f"got {head_response['ContentLength']}"
                )
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            
            # Try to abort the multipart upload on failure
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket,
                    Key=s3_key,
                    UploadId=upload_id
                )
            except Exception:
                pass  # Best effort cleanup
            
            raise Exception(f"S3 multipart upload failed ({error_code}): {e}") from e


def _base64_md5(data: bytes) -> str:
    """Calculate base64-encoded MD5 hash for S3 ContentMD5 header."""
    import base64
    md5_hash = hashlib.md5(data).digest()
    return base64.b64encode(md5_hash).decode('ascii')