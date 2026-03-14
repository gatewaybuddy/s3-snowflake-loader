"""
PGP decryption handler for encrypted file uploads.

Detects PGP-encrypted files and decrypts them using a private key
stored in AWS Secrets Manager. Uses python-gnupg for GPG operations.

Flow:
  1. Validator detects PGP encryption (magic bytes or ASCII armor)
  2. Handler calls decrypt_file() with S3 object body
  3. Decrypted cleartext is written to /tmp/<uuid>_<filename>
  4. Path to cleartext file is returned for downstream processing
  5. Cleartext is cleaned up after load completes

Security:
  - PGP private key loaded from Secrets Manager on first use (cached)
  - Decrypted files live in /tmp (Lambda's ephemeral storage, max 10GB)
  - Decryption is logged for audit trail (no cleartext in logs)
  - Passphrase for PGP key also stored in Secrets Manager
"""

import logging
import os
import tempfile
import time
import uuid
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Module-level cache for PGP key (avoid repeated Secrets Manager calls)
# Cache expires after 15 minutes for security
_cached_pgp_key: Optional[str] = None
_cached_pgp_passphrase: Optional[str] = None
_cache_timestamp: Optional[float] = None
_CACHE_TTL_SECONDS = 15 * 60  # 15 minutes


class PGPDecryptionError(Exception):
    """Raised when PGP decryption fails."""
    pass


def decrypt_file(
    encrypted_bytes: bytes,
    original_filename: str,
    pgp_secret_name: Optional[str] = None,
) -> str:
    """
    Decrypt a PGP-encrypted file.

    Args:
        encrypted_bytes: Raw bytes of the PGP-encrypted file.
        original_filename: Original filename (for naming the cleartext output).
        pgp_secret_name: Secrets Manager secret containing the PGP private key.
                         Defaults to PGP_SECRET_NAME env var.

    Returns:
        Path to the decrypted cleartext file in /tmp.

    Raises:
        PGPDecryptionError: If decryption fails for any reason.
    """
    try:
        import gnupg
    except ImportError:
        raise PGPDecryptionError(
            "python-gnupg is not installed. Add 'python-gnupg' to requirements.txt "
            "to enable PGP decryption. Alternatively, pre-decrypt files before upload."
        )

    # Load PGP key from Secrets Manager
    pgp_key, passphrase = _load_pgp_key(pgp_secret_name)

    # Set up GPG in /tmp (Lambda's writable directory)
    gnupg_home = os.path.join(tempfile.gettempdir(), ".gnupg")
    os.makedirs(gnupg_home, exist_ok=True)
    gpg = gnupg.GPG(gnupghome=gnupg_home)

    # Import the private key
    import_result = gpg.import_keys(pgp_key)
    if import_result.count == 0:
        raise PGPDecryptionError(
            f"Failed to import PGP private key. GPG output: {import_result.stderr}"
        )

    logger.info(
        f"Imported PGP key: {import_result.count} key(s), "
        f"fingerprints: {import_result.fingerprints}"
    )

    # Decrypt to a temp file
    output_filename = f"{uuid.uuid4().hex}_{_strip_pgp_extension(original_filename)}"
    output_path = os.path.join(tempfile.gettempdir(), output_filename)

    decrypted = gpg.decrypt(
        encrypted_bytes,
        passphrase=passphrase,
        output=output_path,
    )

    if not decrypted.ok:
        raise PGPDecryptionError(
            f"PGP decryption failed: {decrypted.status}. "
            f"Possible causes: wrong key, corrupted file, or missing passphrase. "
            f"GPG stderr: {decrypted.stderr}"
        )

    # Verify output file exists and has content
    if not os.path.exists(output_path):
        raise PGPDecryptionError(
            "Decryption appeared to succeed but output file was not created."
        )

    output_size = os.path.getsize(output_path)
    if output_size == 0:
        os.remove(output_path)
        raise PGPDecryptionError(
            "Decryption produced an empty file. The encrypted file may be corrupted."
        )

    logger.info(
        f"PGP decryption successful: {original_filename} → {output_path} "
        f"({output_size} bytes). "
        f"Audit: file decrypted by Lambda, original was PGP-encrypted."
    )

    return output_path


def cleanup_decrypted_file(file_path: str) -> None:
    """Remove a decrypted cleartext file from /tmp."""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Cleaned up decrypted file: {file_path}")
    except OSError as e:
        logger.warning(f"Failed to clean up {file_path}: {e}")


def _load_pgp_key(secret_name: Optional[str] = None) -> tuple[str, str]:
    """
    Load PGP private key and passphrase from Secrets Manager.

    Secret format (JSON):
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
            raise PGPDecryptionError(
                "PGP_SECRET_NAME environment variable not set. "
                "Create a Secrets Manager secret with the PGP private key and "
                "set PGP_SECRET_NAME in the Lambda environment."
            )

    client = boto3.client("secretsmanager")
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "ResourceNotFoundException":
            raise PGPDecryptionError(
                f"PGP secret '{secret_name}' not found in Secrets Manager. "
                f"Create it with: aws secretsmanager create-secret "
                f"--name {secret_name} --secret-string '{{\"pgp_private_key\": \"...\", \"pgp_passphrase\": \"...\"}}'"
            ) from e
        raise PGPDecryptionError(
            f"Failed to load PGP key from Secrets Manager: {e}"
        ) from e

    import json
    secret_data = json.loads(response["SecretString"])

    pgp_key = secret_data.get("pgp_private_key")
    if not pgp_key:
        raise PGPDecryptionError(
            f"Secret '{secret_name}' does not contain 'pgp_private_key' field. "
            f"Expected JSON format: {{\"pgp_private_key\": \"-----BEGIN PGP...\", "
            f"\"pgp_passphrase\": \"optional\"}}"
        )

    passphrase = secret_data.get("pgp_passphrase", "")

    # Cache for warm Lambda starts (with 15-minute TTL)
    _cached_pgp_key = pgp_key
    _cached_pgp_passphrase = passphrase
    _cache_timestamp = current_time

    logger.info(f"Loaded PGP key from {secret_name} (cached for {_CACHE_TTL_SECONDS // 60} minutes)")
    return pgp_key, passphrase


def _strip_pgp_extension(filename: str) -> str:
    """Remove PGP-related extensions from filename."""
    lower = filename.lower()
    for ext in (".pgp", ".gpg", ".asc", ".enc"):
        if lower.endswith(ext):
            return filename[: -len(ext)]
    return filename


def invalidate_cache() -> None:
    """Force re-read of PGP key on next decryption."""
    global _cached_pgp_key, _cached_pgp_passphrase, _cache_timestamp
    _cached_pgp_key = None
    _cached_pgp_passphrase = None
    _cache_timestamp = None
    logger.info("PGP key cache invalidated")
