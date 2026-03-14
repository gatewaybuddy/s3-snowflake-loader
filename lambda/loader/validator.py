"""
Pre-load file validation gate.

Runs BEFORE data reaches Snowflake. Catches bad files early to avoid
wasting warehouse compute on junk data and to provide clear error messages.

Validation checks (in order):
  1. Empty file detection (0 bytes)
  2. Size limit enforcement (configurable max, default 5GB)
  3. PGP detection and decryption
  4. ZIP/GZIP passthrough (note compression in metadata)
  5. Format sniffing (is the CSV valid? is the JSON parseable?)
  6. Poison file detection (unrecognized binary)
"""

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Optional

from format_detector import (
    AVRO_MAGIC,
    ORC_MAGIC,
    PARQUET_MAGIC,
    detect_format_from_content,
)

logger = logging.getLogger(__name__)

# Default max file size: 5GB (Lambda can stream via S3 range reads)
DEFAULT_MAX_SIZE_BYTES = 5 * 1024 * 1024 * 1024  # 5 GB

# PGP/GPG magic bytes and ASCII armor header
PGP_BINARY_MAGIC = b"\x85"        # Old-format PGP packet tag
PGP_BINARY_MAGIC_2 = b"\xc0"     # New-format PGP packet tag (0xC0-0xFF range)
PGP_ASCII_ARMOR = b"-----BEGIN PGP MESSAGE-----"
PGP_PUBLIC_KEY_HEADER = b"-----BEGIN PGP PUBLIC KEY BLOCK-----"

# Known binary format magic bytes (for poison file detection)
KNOWN_BINARY_MAGICS = {
    b"PAR1": "PARQUET",
    b"Obj\x01": "AVRO",
    b"ORC": "ORC",
    b"PK": "ZIP",
    b"\x1f\x8b": "GZIP",
    b"BZ": "BZ2",
    b"\x28\xb5\x2f\xfd": "ZSTD",
}

# Compression magic bytes (passthrough — Snowflake handles these)
COMPRESSION_MAGICS = {
    b"\x1f\x8b": "GZIP",
    b"BZ": "BZ2",
    b"\x28\xb5\x2f\xfd": "ZSTD",
}


@dataclass
class ValidationResult:
    """Result of file validation."""
    valid: bool = True
    error: Optional[str] = None         # Human-readable error message
    error_code: Optional[str] = None    # Machine-readable error code
    is_pgp_encrypted: bool = False      # File needs PGP decryption
    compression: Optional[str] = None   # Detected compression (GZIP, BZ2, ZSTD)
    detected_format: Optional[str] = None  # Format detected from content
    warnings: list[str] = field(default_factory=list)


def validate_file(
    head_bytes: bytes,
    file_size: int,
    file_extension: str,
    expected_format: str,
    max_size_bytes: Optional[int] = None,
) -> ValidationResult:
    """
    Run all validation checks on a file before loading.

    Args:
        head_bytes: First 64KB of the file content
        file_size: Total file size in bytes (from S3 metadata)
        file_extension: File extension (e.g., ".csv", ".json")
        expected_format: Expected format from path parsing (CSV, JSON, PARQUET, etc.)
        max_size_bytes: Maximum allowed file size (default: 5GB from env or constant)

    Returns:
        ValidationResult with valid=True if file passes all checks,
        or valid=False with error details.
    """
    if max_size_bytes is None:
        max_size_bytes = int(
            os.environ.get("MAX_FILE_SIZE_BYTES", DEFAULT_MAX_SIZE_BYTES)
        )

    result = ValidationResult()

    # 1. Empty file detection
    if file_size == 0:
        result.valid = False
        result.error = "File is empty (0 bytes). No data to load."
        result.error_code = "EMPTY_FILE"
        return result

    if not head_bytes:
        result.valid = False
        result.error = "Could not read file content. File may be empty or inaccessible."
        result.error_code = "UNREADABLE"
        return result

    # 2. Size limit
    if file_size > max_size_bytes:
        result.valid = False
        size_gb = file_size / (1024 ** 3)
        max_gb = max_size_bytes / (1024 ** 3)
        result.error = (
            f"File size ({size_gb:.2f} GB) exceeds maximum allowed size "
            f"({max_gb:.2f} GB). Adjust MAX_FILE_SIZE_BYTES to increase the limit."
        )
        result.error_code = "FILE_TOO_LARGE"
        return result

    # 3. PGP detection
    if _is_pgp_encrypted(head_bytes):
        result.is_pgp_encrypted = True
        result.detected_format = "PGP"
        logger.info("PGP-encrypted file detected — decryption required")
        # Don't fail here — let the handler route to pgp_handler
        return result

    # 4. Compression detection (passthrough)
    compression = _detect_compression(head_bytes)
    if compression:
        result.compression = compression
        result.warnings.append(
            f"Compression detected: {compression}. "
            f"Snowflake will decompress automatically during COPY INTO."
        )
        # Can't validate content inside compressed files without decompressing.
        # Trust Snowflake's COPY INTO to handle format validation.
        result.detected_format = expected_format
        return result

    # 5. Format sniffing validation
    format_result = _validate_format(head_bytes, file_extension, expected_format)
    if not format_result.valid:
        return format_result

    result.detected_format = format_result.detected_format
    result.warnings.extend(format_result.warnings)

    return result


def _is_pgp_encrypted(head_bytes: bytes) -> bool:
    """Detect PGP-encrypted content (binary or ASCII-armored)."""
    # ASCII-armored PGP
    if head_bytes.lstrip().startswith(PGP_ASCII_ARMOR):
        return True

    # Also detect PGP public key blocks (shouldn't be loaded as data)
    if head_bytes.lstrip().startswith(PGP_PUBLIC_KEY_HEADER):
        return True

    # Binary PGP packets: old-format tags (bit 7 set, bit 6 clear)
    # have tag byte 0x85-0x8D for common packet types.
    # New-format tags (bits 7 and 6 both set) start 0xC0+.
    if len(head_bytes) >= 2:
        first = head_bytes[0]
        # Old format: bit 7 set, not a known binary format
        if (first & 0x80) and not _is_known_binary(head_bytes):
            # Check for PGP session key packet (tag 1: 0x85)
            # or symmetric encrypted packet (tag 9: 0xC9, tag 18: 0xD2)
            if first in (0x85, 0xC9, 0xD2, 0xC1, 0xC5):
                return True

    return False


def _is_known_binary(head_bytes: bytes) -> bool:
    """Check if head_bytes match any known binary format."""
    for magic in KNOWN_BINARY_MAGICS:
        if head_bytes.startswith(magic):
            return True
    return False


def _detect_compression(head_bytes: bytes) -> Optional[str]:
    """Detect compression format from magic bytes."""
    for magic, comp_type in COMPRESSION_MAGICS.items():
        if head_bytes.startswith(magic):
            return comp_type
    return None


def _validate_format(
    head_bytes: bytes,
    file_extension: str,
    expected_format: str,
) -> ValidationResult:
    """
    Validate that file content matches expected format.
    Catches poison files (random binary that isn't a recognized format).
    """
    result = ValidationResult()

    # Binary formats: check magic bytes
    if expected_format in ("PARQUET", "AVRO", "ORC"):
        return _validate_binary_format(head_bytes, expected_format)

    # Text formats: try to decode and validate
    try:
        text = head_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            text = head_bytes.decode("latin-1")
            result.warnings.append(
                "File is not UTF-8 encoded. Decoded as Latin-1. "
                "Snowflake expects UTF-8; non-ASCII characters may be garbled."
            )
        except UnicodeDecodeError:
            result.valid = False
            result.error = (
                "File contains binary data that is not a recognized format "
                "(not Parquet, Avro, ORC, GZIP, BZ2, or ZSTD). "
                "Cannot load binary garbage as text data."
            )
            result.error_code = "POISON_FILE"
            return result

    # Check for poison: high ratio of non-printable characters
    if _is_binary_garbage(text):
        result.valid = False
        result.error = (
            "File appears to be binary data disguised as a text file. "
            "High ratio of non-printable characters detected. "
            "Ensure the file is a valid CSV, JSON, or other supported text format."
        )
        result.error_code = "POISON_FILE"
        return result

    # Validate specific text formats
    if expected_format == "JSON":
        return _validate_json(text)

    if expected_format == "CSV":
        return _validate_csv(text, file_extension)

    # Unknown text format — pass through with warning
    result.detected_format = expected_format
    result.warnings.append(
        f"Format '{expected_format}' not deeply validated. "
        f"Snowflake COPY INTO will report any format errors."
    )
    return result


def _validate_binary_format(
    head_bytes: bytes, expected_format: str
) -> ValidationResult:
    """Validate binary format files by checking magic bytes."""
    result = ValidationResult()

    magic_map = {
        "PARQUET": (PARQUET_MAGIC, 4),
        "AVRO": (AVRO_MAGIC, 4),
        "ORC": (ORC_MAGIC, 3),
    }

    expected_magic, magic_len = magic_map[expected_format]

    if head_bytes[:magic_len] == expected_magic:
        result.detected_format = expected_format
        return result

    # Magic doesn't match — check what it actually is
    detected = detect_format_from_content(head_bytes, "")
    if detected["type"] != "UNKNOWN":
        result.valid = False
        result.error = (
            f"File extension indicates {expected_format} but content "
            f"is actually {detected['type']}. Rename the file with the "
            f"correct extension or fix the file."
        )
        result.error_code = "FORMAT_MISMATCH"
    else:
        result.valid = False
        result.error = (
            f"File extension indicates {expected_format} but the file "
            f"does not have valid {expected_format} magic bytes. "
            f"The file may be corrupted or is not a valid {expected_format} file."
        )
        result.error_code = "INVALID_FORMAT"

    return result


def _validate_json(text: str) -> ValidationResult:
    """Validate JSON content (single object, array, or JSON Lines)."""
    result = ValidationResult()
    result.detected_format = "JSON"

    stripped = text.strip()
    if not stripped:
        result.valid = False
        result.error = "File contains only whitespace. No JSON data found."
        result.error_code = "EMPTY_CONTENT"
        return result

    # Try parsing as a single JSON document
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            json.loads(stripped)
            return result  # Valid JSON
        except json.JSONDecodeError:
            pass  # Might be partial read of a large file — try lines

    # Try parsing as JSON Lines
    lines = stripped.split("\n")
    valid_count = 0
    first_error = None

    for i, line in enumerate(lines[:20]):  # Check first 20 lines
        line = line.strip()
        if not line:
            continue
        try:
            json.loads(line)
            valid_count += 1
        except json.JSONDecodeError as e:
            if first_error is None:
                first_error = (i + 1, str(e))

    if valid_count == 0 and first_error:
        result.valid = False
        line_num, error_msg = first_error
        result.error = (
            f"File is not valid JSON. Parse error at line {line_num}: {error_msg}. "
            f"Ensure the file contains valid JSON objects, a JSON array, "
            f"or newline-delimited JSON (one object per line)."
        )
        result.error_code = "INVALID_JSON"
        return result

    if first_error and valid_count > 0:
        line_num, error_msg = first_error
        result.warnings.append(
            f"Mixed content detected: {valid_count} valid JSON lines found, "
            f"but line {line_num} failed to parse: {error_msg}. "
            f"Snowflake will report these as errors during COPY INTO."
        )

    return result


def _validate_csv(text: str, file_extension: str) -> ValidationResult:
    """Validate CSV content structure."""
    result = ValidationResult()
    result.detected_format = "CSV"

    lines = text.split("\n")
    non_empty = [l for l in lines if l.strip()]

    if not non_empty:
        result.valid = False
        result.error = "File contains no data lines. File appears to be empty."
        result.error_code = "EMPTY_CONTENT"
        return result

    if len(non_empty) == 1:
        result.warnings.append(
            "File contains only one line (likely a header with no data rows). "
            "COPY INTO will load 0 rows if SKIP_HEADER=1 is set."
        )

    # Check for consistent column count across first 20 lines
    # Use the format detector's delimiter detection
    detection = detect_format_from_content(text.encode("utf-8"), file_extension)
    dialect = detection.get("dialect", {})
    delim = dialect.get("field_delimiter", ",")
    if delim == "\\t":
        delim = "\t"

    col_counts = []
    for line in non_empty[:20]:
        col_counts.append(line.count(delim) + 1)

    if col_counts:
        # Check if column counts are consistent (ignoring first line which may be header)
        data_counts = col_counts[1:] if len(col_counts) > 1 else col_counts
        if data_counts:
            expected_cols = data_counts[0]
            inconsistent = [
                (i + 2, c)
                for i, c in enumerate(data_counts)
                if c != expected_cols
            ]
            if inconsistent and len(inconsistent) > len(data_counts) * 0.3:
                result.warnings.append(
                    f"Inconsistent column counts detected. Expected {expected_cols} "
                    f"columns but found varying counts. This may cause COPY INTO errors. "
                    f"Check delimiter detection or file formatting."
                )

    return result


def _is_binary_garbage(text: str, threshold: float = 0.10) -> bool:
    """
    Detect if text has too many non-printable characters.
    Returns True if the file looks like binary garbage.
    
    Threshold lowered to 10% — real CSV/JSON files have near-zero
    non-printable chars. Binary files decoded as latin-1 typically
    have 20-50%. 30% was too permissive and let garbage.bin through.
    """
    if not text:
        return False

    # Sample first 4KB
    sample = text[:4096]
    non_printable = sum(
        1 for c in sample
        if not c.isprintable() and c not in ("\n", "\r", "\t")
    )

    ratio = non_printable / len(sample)
    return ratio > threshold
