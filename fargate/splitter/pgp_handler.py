"""
PGP streaming decryption for Fargate large file processing.

Unlike the Lambda version that decrypts to /tmp files, this module provides
streaming decryption that works as a pipe - reading encrypted chunks and
yielding decrypted chunks without loading the entire file into memory.

Uses gnupg library with streaming operations to handle 50GB+ files.
Private key loaded from AWS Secrets Manager (same pattern as Lambda).
"""

import io
import logging
import os
import subprocess
import tempfile
import time
from typing import BinaryIO, Iterator, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Module-level cache for PGP key (avoid repeated Secrets Manager calls)
_cached_pgp_key: Optional[str] = None
_cached_pgp_passphrase: Optional[str] = None
_cache_timestamp: Optional[float] = None
_CACHE_TTL_SECONDS = 15 * 60  # 15 minutes


class PGPStreamDecryptionError(Exception):
    """Raised when PGP streaming decryption fails."""
    pass


def is_pgp_encrypted(file_stream: BinaryIO) -> bool:
    """
    Check if a file stream contains PGP-encrypted data.
    
    Reads the first few KB to check for PGP magic bytes or ASCII armor.
    Resets the stream position to the beginning.
    """
    # Read first 8KB to check for PGP signatures
    current_pos = file_stream.tell()
    try:
        header = file_stream.read(8192)
        file_stream.seek(current_pos)  # Reset position
        
        if not header:
            return False
        
        # Check for binary PGP magic bytes
        pgp_binary_signatures = [
            b'\x85',       # PGP packet tag for compressed data
            b'\x8c',       # PGP packet tag for encrypted data
            b'\x95',       # PGP packet tag for literal data
        ]
        
        for sig in pgp_binary_signatures:
            if header.startswith(sig):
                return True
        
        # Check for ASCII armor
        header_text = header.decode('ascii', errors='ignore')
        ascii_armor_markers = [
            '-----BEGIN PGP MESSAGE-----',
            '-----BEGIN PGP ENCRYPTED MESSAGE-----',
            '-----BEGIN PGP SIGNATURE-----'
        ]
        
        for marker in ascii_armor_markers:
            if marker in header_text:
                return True
        
        return False
        
    except Exception as e:
        logger.warning(f"Error checking PGP encryption: {e}")
        return False


def decrypt_stream(
    encrypted_stream: BinaryIO,
    pgp_secret_name: Optional[str] = None,
    chunk_size: int = 1024 * 1024  # 1MB chunks
) -> Iterator[bytes]:
    """
    Stream PGP decryption - yields decrypted chunks without loading full file.
    
    Args:
        encrypted_stream: Input stream containing PGP-encrypted data
        pgp_secret_name: Secrets Manager secret containing PGP private key
        chunk_size: Size of chunks to yield (default 1MB)
        
    Yields:
        bytes: Decrypted data chunks
        
    Raises:
        PGPStreamDecryptionError: If decryption setup or process fails
    """
    # Load PGP key from Secrets Manager
    pgp_key, passphrase = _load_pgp_key(pgp_secret_name)
    
    # Set up temporary GPG home directory
    with tempfile.TemporaryDirectory() as gnupg_home:
        try:
            # Import key into temporary GPG keyring
            _import_pgp_key(gnupg_home, pgp_key)
            
            # Start GPG decryption process
            gpg_process = _start_gpg_decrypt_process(gnupg_home, passphrase)
            
            try:
                # Stream data through GPG process
                yield from _stream_through_gpg(encrypted_stream, gpg_process, chunk_size)
                
            finally:
                # Ensure GPG process is terminated
                _cleanup_gpg_process(gpg_process)
                
        except Exception as e:
            raise PGPStreamDecryptionError(f"Streaming decryption failed: {e}") from e


def _load_pgp_key(secret_name: Optional[str] = None) -> tuple[str, str]:
    """
    Load PGP private key and passphrase from Secrets Manager.
    
    Same format as Lambda version:
    {
        "pgp_private_key": "-----BEGIN PGP PRIVATE KEY BLOCK-----\n...",
        "pgp_passphrase": "optional-passphrase"
    }
    
    Returns (private_key_ascii, passphrase).
    """
    global _cached_pgp_key, _cached_pgp_passphrase, _cache_timestamp
    
    # Check cache validity (15-minute TTL)
    current_time = time.time()
    if (_cached_pgp_key is not None and
        _cache_timestamp is not None and
        (current_time - _cache_timestamp) < _CACHE_TTL_SECONDS):
        return _cached_pgp_key, _cached_pgp_passphrase or ""
    
    # Cache expired or empty, fetch fresh key
    if secret_name is None:
        secret_name = os.environ.get("PGP_SECRET_NAME")
        if not secret_name:
            raise PGPStreamDecryptionError(
                "PGP_SECRET_NAME environment variable not set. "
                "Create a Secrets Manager secret with the PGP private key."
            )
    
    client = boto3.client("secretsmanager")
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "ResourceNotFoundException":
            raise PGPStreamDecryptionError(
                f"PGP secret '{secret_name}' not found in Secrets Manager."
            ) from e
        raise PGPStreamDecryptionError(
            f"Failed to load PGP key from Secrets Manager: {e}"
        ) from e
    
    import json
    secret_data = json.loads(response["SecretString"])
    
    pgp_key = secret_data.get("pgp_private_key")
    if not pgp_key:
        raise PGPStreamDecryptionError(
            f"Secret '{secret_name}' does not contain 'pgp_private_key' field."
        )
    
    passphrase = secret_data.get("pgp_passphrase", "")
    
    # Cache for subsequent calls
    _cached_pgp_key = pgp_key
    _cached_pgp_passphrase = passphrase
    _cache_timestamp = current_time
    
    logger.info(f"Loaded PGP key from {secret_name}")
    return pgp_key, passphrase


def _import_pgp_key(gnupg_home: str, pgp_key: str) -> None:
    """Import PGP private key into temporary GPG keyring."""
    # Write key to temporary file
    key_file = os.path.join(gnupg_home, "private.asc")
    with open(key_file, 'w') as f:
        f.write(pgp_key)
    
    # Import the key
    import_cmd = [
        'gpg', '--homedir', gnupg_home, '--batch', '--quiet',
        '--import', key_file
    ]
    
    result = subprocess.run(
        import_cmd,
        capture_output=True,
        text=True,
        timeout=30
    )
    
    if result.returncode != 0:
        raise PGPStreamDecryptionError(
            f"Failed to import PGP key: {result.stderr}"
        )
    
    # Clean up key file
    os.remove(key_file)
    
    logger.info("PGP private key imported successfully")


def _start_gpg_decrypt_process(gnupg_home: str, passphrase: str) -> subprocess.Popen:
    """Start GPG decryption process for streaming."""
    decrypt_cmd = [
        'gpg', '--homedir', gnupg_home, '--batch', '--quiet',
        '--decrypt', '--armor',
        '--pinentry-mode', 'loopback',
        '--passphrase-fd', '0'  # Read passphrase from stdin
    ]
    
    try:
        process = subprocess.Popen(
            decrypt_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0  # Unbuffered for streaming
        )
        
        # Send passphrase to GPG
        if passphrase:
            process.stdin.write((passphrase + '\n').encode('utf-8'))
        else:
            process.stdin.write(b'\n')  # Empty passphrase
        
        return process
        
    except Exception as e:
        raise PGPStreamDecryptionError(f"Failed to start GPG process: {e}") from e


def _stream_through_gpg(
    input_stream: BinaryIO,
    gpg_process: subprocess.Popen,
    chunk_size: int
) -> Iterator[bytes]:
    """
    Stream data through GPG process for decryption.
    
    Reads from input_stream, feeds to GPG stdin, reads decrypted output.
    """
    import threading
    import queue
    
    # Queue to pass data between threads
    output_queue = queue.Queue(maxsize=10)  # Limit memory usage
    exception_container = [None]
    
    def feed_gpg():
        """Thread to feed encrypted data to GPG stdin."""
        try:
            while True:
                chunk = input_stream.read(chunk_size)
                if not chunk:
                    break
                gpg_process.stdin.write(chunk)
            
            # Close stdin to signal end of data
            gpg_process.stdin.close()
            
        except Exception as e:
            exception_container[0] = e
            gpg_process.stdin.close()
    
    def read_gpg():
        """Thread to read decrypted data from GPG stdout."""
        try:
            while True:
                chunk = gpg_process.stdout.read(chunk_size)
                if not chunk:
                    break
                output_queue.put(chunk)
            
            # Signal end of output
            output_queue.put(None)
            
        except Exception as e:
            exception_container[0] = e
            output_queue.put(None)
    
    # Start threads
    feed_thread = threading.Thread(target=feed_gpg, daemon=True)
    read_thread = threading.Thread(target=read_gpg, daemon=True)
    
    feed_thread.start()
    read_thread.start()
    
    # Yield decrypted chunks
    try:
        while True:
            try:
                chunk = output_queue.get(timeout=60)  # 60 second timeout
                if chunk is None:  # End of stream
                    break
                yield chunk
            except queue.Empty:
                # Check if process is still running
                if gpg_process.poll() is not None:
                    # Process exited, check for errors
                    stderr_output = gpg_process.stderr.read()
                    if gpg_process.returncode != 0:
                        raise PGPStreamDecryptionError(
                            f"GPG process failed (exit {gpg_process.returncode}): "
                            f"{stderr_output.decode('utf-8', errors='ignore')}"
                        )
                    break
                
                # Check for exceptions in threads
                if exception_container[0]:
                    raise exception_container[0]
    
    finally:
        # Wait for threads to complete
        feed_thread.join(timeout=5)
        read_thread.join(timeout=5)


def _cleanup_gpg_process(gpg_process: subprocess.Popen) -> None:
    """Ensure GPG process is properly terminated."""
    try:
        # Try gentle termination first
        if gpg_process.poll() is None:
            gpg_process.terminate()
            
        # Wait for exit
        try:
            gpg_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            # Force kill if needed
            gpg_process.kill()
            gpg_process.wait(timeout=5)
    
    except Exception as e:
        logger.warning(f"Error cleaning up GPG process: {e}")


def invalidate_cache() -> None:
    """Force re-read of PGP key on next decryption."""
    global _cached_pgp_key, _cached_pgp_passphrase, _cache_timestamp
    _cached_pgp_key = None
    _cached_pgp_passphrase = None
    _cache_timestamp = None
    logger.info("PGP key cache invalidated")