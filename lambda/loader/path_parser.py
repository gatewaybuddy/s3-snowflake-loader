"""
Parse S3 keys into table name, load mode, and file metadata.

Convention:
  s3://bucket/<prefix>/[any/nested/folders/]/<TABLE_NAME>.<MODE>/filename.ext
  
  - Parent folder of the file = table name + optional mode extension
  - Mode extensions: .T (truncate/reload), .A (append), .M (merge)
  - No extension = truncate/reload (default)
"""

import os
import re
from dataclasses import dataclass
from typing import Optional


# Valid load modes
LOAD_MODES = {
    "T": "TRUNCATE",
    "A": "APPEND",
    "M": "MERGE",
}

# File extensions we can handle
SUPPORTED_EXTENSIONS = {
    ".csv": "CSV",
    ".tsv": "CSV",       # Tab-separated → CSV format with tab delimiter
    ".txt": "CSV",       # Assume delimited text
    ".json": "JSON",
    ".jsonl": "JSON",    # JSON Lines
    ".ndjson": "JSON",   # Newline-delimited JSON
    ".parquet": "PARQUET",
    ".avro": "AVRO",
    ".orc": "ORC",
    ".gz": None,         # Compressed — check inner extension
    ".gzip": None,
    ".bz2": None,
    ".zst": None,
}


@dataclass
class ParsedPath:
    """Result of parsing an S3 key."""
    table_name: str           # Target table (uppercase)
    load_mode: str            # TRUNCATE | APPEND | MERGE
    file_name: str            # Original filename
    file_extension: str       # Detected extension (after compression)
    file_format: str          # CSV | JSON | PARQUET | AVRO | ORC
    compression: Optional[str]  # GZIP | BZ2 | ZSTD | None
    relative_path: str        # Full path relative to prefix (for COPY INTO)
    parent_folder: str        # Raw parent folder name


def parse_s3_key(s3_key: str, prefix: str) -> ParsedPath:
    """
    Parse an S3 key into routing information.
    
    Args:
        s3_key: Full S3 object key (e.g., "analytics/reports/REVENUE.A/revenue_2026.csv")
        prefix: The database prefix to strip (e.g., "analytics/")
    
    Returns:
        ParsedPath with table name, mode, format, etc.
    
    Raises:
        ValueError: If the path can't be parsed or format is unsupported.
    """
    # Strip the prefix
    if prefix and s3_key.startswith(prefix):
        relative = s3_key[len(prefix):]
    else:
        relative = s3_key
    
    # Split into parts
    parts = relative.rstrip("/").split("/")
    
    if len(parts) < 2:
        raise ValueError(
            f"S3 key must have at least a folder and a file. "
            f"Got: {s3_key} (relative: {relative})"
        )
    
    file_name = parts[-1]
    parent_folder = parts[-2]
    
    # Skip S3 "folder" markers
    if not file_name or file_name.endswith("/"):
        raise ValueError(f"S3 key appears to be a folder marker, not a file: {s3_key}")
    
    # Parse parent folder → table name + mode
    table_name, load_mode = _parse_folder_name(parent_folder)
    
    # Parse file extension → format + compression
    file_extension, file_format, compression = _detect_format(file_name)
    
    return ParsedPath(
        table_name=table_name,
        load_mode=load_mode,
        file_name=file_name,
        file_extension=file_extension,
        file_format=file_format,
        compression=compression,
        relative_path=relative,
        parent_folder=parent_folder,
    )


def _parse_folder_name(folder: str) -> tuple[str, str]:
    """
    Extract table name and load mode from a folder name.
    
    Examples:
        "CUSTOMERS.T"  → ("CUSTOMERS", "TRUNCATE")
        "REVENUE.A"    → ("REVENUE", "APPEND")
        "ORDERS.M"     → ("ORDERS", "MERGE")
        "DAILY_METRICS" → ("DAILY_METRICS", "TRUNCATE")
        "my_table"     → ("MY_TABLE", "TRUNCATE")
    """
    # Check for mode extension (last 2 chars = ".X" where X is a mode)
    if len(folder) > 2 and folder[-2] == ".":
        mode_char = folder[-1].upper()
        if mode_char in LOAD_MODES:
            table_name = folder[:-2].upper()
            return table_name, LOAD_MODES[mode_char]
    
    # No mode extension → default to TRUNCATE
    table_name = folder.upper()
    
    # Validate table name (Snowflake identifier rules)
    if not re.match(r'^[A-Z_][A-Z0-9_$]*$', table_name):
        # Try sanitizing: replace invalid chars with underscore
        sanitized = re.sub(r'[^A-Z0-9_$]', '_', table_name)
        sanitized = re.sub(r'^[0-9]', '_', sanitized)  # Can't start with digit
        table_name = sanitized
    
    return table_name, "TRUNCATE"


def _detect_format(file_name: str) -> tuple[str, str, Optional[str]]:
    """
    Detect file format and compression from filename.
    
    Returns: (extension, format, compression)
    
    Examples:
        "data.csv"           → (".csv", "CSV", None)
        "data.csv.gz"        → (".csv", "CSV", "GZIP")
        "data.json"          → (".json", "JSON", None)
        "data.parquet"       → (".parquet", "PARQUET", None)
        "data.tsv"           → (".tsv", "CSV", None)  # Tab-delimited
        "data.txt"           → (".txt", "CSV", None)   # Assume delimited
    """
    name_lower = file_name.lower()
    
    # Check for compression wrapper
    compression = None
    compression_map = {
        ".gz": "GZIP",
        ".gzip": "GZIP",
        ".bz2": "BZ2",
        ".zst": "ZSTD",
    }
    
    inner_name = name_lower
    for comp_ext, comp_type in compression_map.items():
        if name_lower.endswith(comp_ext):
            compression = comp_type
            inner_name = name_lower[:-len(comp_ext)]
            break
    
    # Get the actual format extension
    _, ext = os.path.splitext(inner_name)
    
    if ext in SUPPORTED_EXTENSIONS:
        file_format = SUPPORTED_EXTENSIONS[ext]
        if file_format is None:
            # Double-compressed or unknown inner format
            raise ValueError(f"Cannot determine format for file: {file_name}")
        return ext, file_format, compression
    
    # Unknown extension — try to detect from content later
    # For now, default to CSV
    if not ext:
        raise ValueError(f"File has no extension, cannot detect format: {file_name}")
    
    return ext, "CSV", compression  # Fallback to CSV


def detect_csv_dialect(first_line: str) -> dict:
    """
    Auto-detect CSV dialect from the first line of a file.
    
    Returns dict with Snowflake COPY INTO format options.
    """
    # Count potential delimiters
    delimiters = {
        ",": first_line.count(","),
        "|": first_line.count("|"),
        "\t": first_line.count("\t"),
        ";": first_line.count(";"),
    }
    
    # Pick the most common delimiter (must appear at least once)
    best_delim = max(delimiters, key=delimiters.get)
    if delimiters[best_delim] == 0:
        best_delim = ","  # Default
    
    # Check for quoting
    has_quotes = '"' in first_line
    
    result = {
        "field_delimiter": best_delim,
        "skip_header": 1,  # Assume header present
    }
    
    if has_quotes:
        result["field_optionally_enclosed_by"] = '"'
    
    # Tab-delimited specific
    if best_delim == "\t":
        result["field_delimiter"] = "\\t"
    
    return result
