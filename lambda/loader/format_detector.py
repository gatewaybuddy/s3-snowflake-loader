"""
Auto-detect file format by inspecting file content.

Used when:
- File extension is ambiguous (.txt)
- Control table doesn't specify a format
- Validating that extension matches actual content
"""

import json
import struct
from typing import Optional


# Magic bytes for binary formats
PARQUET_MAGIC = b"PAR1"
AVRO_MAGIC = b"Obj\x01"
ORC_MAGIC = b"ORC"


def detect_format_from_content(
    head_bytes: bytes,
    file_extension: str = "",
) -> dict:
    """
    Inspect file content to determine format and dialect.
    
    Args:
        head_bytes: First 8KB-64KB of the file
        file_extension: Hint from filename
    
    Returns:
        Dict with format info:
        {
            "type": "CSV" | "JSON" | "PARQUET" | "AVRO" | "ORC",
            "dialect": { ... },  # For CSV: delimiter, quoting, etc.
            "confidence": "high" | "medium" | "low",
        }
    """
    # Check binary formats first (magic bytes)
    if head_bytes[:4] == PARQUET_MAGIC:
        return {"type": "PARQUET", "dialect": {}, "confidence": "high"}
    
    if head_bytes[:4] == AVRO_MAGIC:
        return {"type": "AVRO", "dialect": {}, "confidence": "high"}
    
    if head_bytes[:3] == ORC_MAGIC:
        return {"type": "ORC", "dialect": {}, "confidence": "high"}
    
    # Try to decode as text
    try:
        text = head_bytes.decode("utf-8-sig")  # Handle BOM
    except UnicodeDecodeError:
        try:
            text = head_bytes.decode("latin-1")
        except UnicodeDecodeError:
            return {"type": "UNKNOWN", "dialect": {}, "confidence": "low"}
    
    # Check for JSON
    stripped = text.lstrip()
    if stripped.startswith("{") or stripped.startswith("["):
        return _detect_json_variant(stripped)
    
    # It's probably delimited text (CSV/TSV/PSV)
    return _detect_delimited(text, file_extension)


def _detect_json_variant(text: str) -> dict:
    """Detect JSON vs JSON Lines."""
    lines = text.strip().split("\n")
    
    if len(lines) == 1 or text.strip().startswith("["):
        # Single JSON object or JSON array
        return {"type": "JSON", "dialect": {"strip_outer_array": True}, "confidence": "high"}
    
    # Check if each line is valid JSON (JSON Lines / NDJSON)
    valid_lines = 0
    for line in lines[:10]:  # Check first 10 lines
        line = line.strip()
        if not line:
            continue
        try:
            json.loads(line)
            valid_lines += 1
        except json.JSONDecodeError:
            break
    
    if valid_lines >= 2:
        return {
            "type": "JSON",
            "dialect": {"strip_outer_array": False, "json_lines": True},
            "confidence": "high",
        }
    
    return {"type": "JSON", "dialect": {}, "confidence": "medium"}


def _detect_delimited(text: str, file_extension: str) -> dict:
    """
    Detect delimiter, quoting, and header for delimited text files.
    """
    lines = text.split("\n")
    non_empty = [l for l in lines if l.strip()]
    
    if not non_empty:
        return {"type": "CSV", "dialect": {}, "confidence": "low"}
    
    # Count delimiters across first 10 lines
    candidates = {
        ",": [],
        "|": [],
        "\t": [],
        ";": [],
    }
    
    for line in non_empty[:10]:
        for delim in candidates:
            # Count occurrences outside quotes
            candidates[delim].append(_count_unquoted(line, delim))
    
    # Best delimiter: consistent count across lines, highest count
    best_delim = ","
    best_score = 0
    
    for delim, counts in candidates.items():
        if not counts or max(counts) == 0:
            continue
        
        # Consistency: how similar are the counts across lines?
        avg = sum(counts) / len(counts)
        if avg == 0:
            continue
        
        variance = sum((c - avg) ** 2 for c in counts) / len(counts)
        consistency = 1.0 / (1.0 + variance)
        
        # Score = average count × consistency
        score = avg * consistency
        
        if score > best_score:
            best_score = score
            best_delim = delim
    
    # Detect header (first line often has different characteristics)
    has_header = _detect_header(non_empty, best_delim)
    
    # Detect quoting
    has_quotes = any('"' in line for line in non_empty[:5])
    
    dialect = {
        "field_delimiter": best_delim if best_delim != "\t" else "\\t",
        "skip_header": 1 if has_header else 0,
    }
    
    if has_quotes:
        dialect["field_optionally_enclosed_by"] = '"'
    
    # Confidence based on consistency
    confidence = "high" if best_score > 3 else "medium" if best_score > 1 else "low"
    
    # Extension hint boosts confidence
    if file_extension in (".csv", ".tsv", ".psv"):
        confidence = "high"
    
    return {"type": "CSV", "dialect": dialect, "confidence": confidence}


def _count_unquoted(line: str, delim: str) -> int:
    """Count delimiter occurrences outside quoted strings."""
    count = 0
    in_quotes = False
    prev = ""
    
    for char in line:
        if char == '"' and prev != "\\":
            in_quotes = not in_quotes
        elif char == delim and not in_quotes:
            count += 1
        prev = char
    
    return count


def _detect_header(lines: list[str], delim: str) -> bool:
    """
    Heuristic: first line is a header if:
    - It has no numeric-only fields (headers are usually text)
    - Subsequent lines have numeric fields
    """
    if len(lines) < 2:
        return True  # Assume header if only one line
    
    first_fields = lines[0].split(delim)
    second_fields = lines[1].split(delim) if len(lines) > 1 else []
    
    # Count numeric fields in first line vs second line
    first_numeric = sum(1 for f in first_fields if _is_numeric(f.strip().strip('"')))
    second_numeric = sum(1 for f in second_fields if _is_numeric(f.strip().strip('"')))
    
    # Header line typically has fewer numeric fields than data lines
    if first_numeric < second_numeric:
        return True
    
    # If first line fields look like identifiers (no spaces, short), probably header
    if all(len(f.strip().strip('"')) < 50 for f in first_fields):
        return True
    
    return True  # Default: assume header


def _is_numeric(s: str) -> bool:
    """Check if a string looks numeric."""
    if not s:
        return False
    try:
        float(s.replace(",", ""))
        return True
    except ValueError:
        return False
