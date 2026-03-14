"""Tests for file validation gate."""

import json
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lambda"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lambda", "loader"))

from loader.validator import (
    validate_file,
    _is_pgp_encrypted,
    _detect_compression,
    _is_binary_garbage,
    _validate_json,
    _validate_csv,
    ValidationResult,
)


class TestEmptyFileDetection:
    def test_zero_byte_file(self):
        result = validate_file(b"", 0, ".csv", "CSV")
        assert not result.valid
        assert result.error_code == "EMPTY_FILE"
        assert "empty" in result.error.lower()

    def test_no_head_bytes(self):
        result = validate_file(b"", 100, ".csv", "CSV")
        assert not result.valid
        assert result.error_code == "UNREADABLE"


class TestSizeLimits:
    def test_within_limit(self):
        content = b"id,name\n1,Alice\n"
        result = validate_file(content, 1000, ".csv", "CSV", max_size_bytes=5000)
        assert result.valid

    def test_exceeds_limit(self):
        content = b"id,name\n1,Alice\n"
        result = validate_file(content, 6000, ".csv", "CSV", max_size_bytes=5000)
        assert not result.valid
        assert result.error_code == "FILE_TOO_LARGE"
        assert "exceeds" in result.error.lower()

    def test_exactly_at_limit(self):
        content = b"id,name\n1,Alice\n"
        result = validate_file(content, 5000, ".csv", "CSV", max_size_bytes=5000)
        assert result.valid

    def test_default_5gb_limit(self):
        # Verify the constant
        from loader.validator import DEFAULT_MAX_SIZE_BYTES
        assert DEFAULT_MAX_SIZE_BYTES == 5 * 1024 * 1024 * 1024


class TestPGPDetection:
    def test_ascii_armored_pgp(self):
        content = b"-----BEGIN PGP MESSAGE-----\nVersion: GnuPG v2\n\njA0ECQMC..."
        assert _is_pgp_encrypted(content)

    def test_ascii_armored_with_whitespace(self):
        content = b"  \n-----BEGIN PGP MESSAGE-----\ndata..."
        assert _is_pgp_encrypted(content)

    def test_pgp_public_key_block(self):
        content = b"-----BEGIN PGP PUBLIC KEY BLOCK-----\nVersion: GnuPG..."
        assert _is_pgp_encrypted(content)

    def test_binary_pgp_old_format(self):
        # Old-format PGP packet tag 0x85 (session key packet)
        content = bytes([0x85, 0x01, 0x0C]) + b"\x00" * 100
        assert _is_pgp_encrypted(content)

    def test_not_pgp_csv(self):
        content = b"id,name,email\n1,Alice,alice@test.com\n"
        assert not _is_pgp_encrypted(content)

    def test_not_pgp_json(self):
        content = b'{"key": "value"}'
        assert not _is_pgp_encrypted(content)

    def test_not_pgp_parquet(self):
        # Parquet starts with PAR1 — should not be confused with PGP
        content = b"PAR1" + b"\x00" * 100
        assert not _is_pgp_encrypted(content)

    def test_pgp_file_triggers_validation_flag(self):
        content = b"-----BEGIN PGP MESSAGE-----\nVersion: GnuPG v2\n\njA0ECQMC..."
        result = validate_file(content, 1000, ".csv.pgp", "CSV")
        assert result.valid  # PGP files are valid — they just need decryption
        assert result.is_pgp_encrypted
        assert result.detected_format == "PGP"


class TestCompressionDetection:
    def test_gzip(self):
        content = b"\x1f\x8b\x08\x00" + b"\x00" * 100
        assert _detect_compression(content) == "GZIP"

    def test_bz2(self):
        content = b"BZ" + b"\x00" * 100
        assert _detect_compression(content) == "BZ2"

    def test_zstd(self):
        content = b"\x28\xb5\x2f\xfd" + b"\x00" * 100
        assert _detect_compression(content) == "ZSTD"

    def test_no_compression(self):
        content = b"id,name\n1,Alice\n"
        assert _detect_compression(content) is None

    def test_gzip_passthrough(self):
        content = b"\x1f\x8b\x08\x00" + b"\x00" * 100
        result = validate_file(content, 1000, ".csv.gz", "CSV")
        assert result.valid
        assert result.compression == "GZIP"
        assert any("decompress" in w.lower() for w in result.warnings)


class TestFormatValidation:
    def test_valid_csv(self):
        content = b"id,name,email\n1,Alice,alice@test.com\n2,Bob,bob@test.com\n"
        result = validate_file(content, len(content), ".csv", "CSV")
        assert result.valid
        assert result.detected_format == "CSV"

    def test_valid_json_object(self):
        content = b'{"key": "value", "num": 42}'
        result = validate_file(content, len(content), ".json", "JSON")
        assert result.valid
        assert result.detected_format == "JSON"

    def test_valid_json_array(self):
        content = b'[{"a": 1}, {"a": 2}]'
        result = validate_file(content, len(content), ".json", "JSON")
        assert result.valid

    def test_valid_json_lines(self):
        content = b'{"a": 1}\n{"a": 2}\n{"a": 3}\n'
        result = validate_file(content, len(content), ".jsonl", "JSON")
        assert result.valid

    def test_invalid_json(self):
        content = b"this is not json at all"
        result = validate_file(content, len(content), ".json", "JSON")
        assert not result.valid
        assert result.error_code == "INVALID_JSON"

    def test_valid_parquet(self):
        content = b"PAR1" + b"\x00" * 100
        result = validate_file(content, len(content), ".parquet", "PARQUET")
        assert result.valid
        assert result.detected_format == "PARQUET"

    def test_invalid_parquet(self):
        content = b"NOT_PARQUET" + b"\x00" * 100
        result = validate_file(content, len(content), ".parquet", "PARQUET")
        assert not result.valid
        assert result.error_code in ("FORMAT_MISMATCH", "INVALID_FORMAT")

    def test_valid_avro(self):
        content = b"Obj\x01" + b"\x00" * 100
        result = validate_file(content, len(content), ".avro", "AVRO")
        assert result.valid
        assert result.detected_format == "AVRO"

    def test_valid_orc(self):
        content = b"ORC" + b"\x00" * 100
        result = validate_file(content, len(content), ".orc", "ORC")
        assert result.valid
        assert result.detected_format == "ORC"


class TestPoisonFileDetection:
    def test_binary_garbage(self):
        # Random binary with control characters that don't match any known format
        # Mix of NUL, SOH, STX, etc. — clearly not text data
        content = bytes(range(0, 32)) * 200  # Control characters
        result = validate_file(content, len(content), ".csv", "CSV")
        # Should detect as binary garbage or unknown
        assert not result.valid
        assert result.error_code == "POISON_FILE"

    def test_is_binary_garbage_function(self):
        # Text with many non-printable characters
        garbage = "".join(chr(i) for i in range(1, 32) if i not in (9, 10, 13)) * 100
        assert _is_binary_garbage(garbage)

    def test_normal_text_not_garbage(self):
        text = "id,name,email\n1,Alice,alice@test.com\n2,Bob,bob@test.com\n"
        assert not _is_binary_garbage(text)

    def test_empty_text_not_garbage(self):
        assert not _is_binary_garbage("")


class TestCSVValidation:
    def test_header_only_csv(self):
        content = "id,name,email\n"
        result = _validate_csv(content, ".csv")
        assert result.valid
        assert any("one line" in w.lower() for w in result.warnings)

    def test_empty_csv(self):
        result = _validate_csv("", ".csv")
        assert not result.valid
        assert result.error_code == "EMPTY_CONTENT"

    def test_whitespace_only(self):
        result = _validate_csv("  \n  \n  ", ".csv")
        assert not result.valid
        assert result.error_code == "EMPTY_CONTENT"

    def test_consistent_columns(self):
        content = "a,b,c\n1,2,3\n4,5,6\n"
        result = _validate_csv(content, ".csv")
        assert result.valid
        assert not result.warnings


class TestJSONValidation:
    def test_empty_json(self):
        result = _validate_json("   ")
        assert not result.valid
        assert result.error_code == "EMPTY_CONTENT"

    def test_valid_object(self):
        result = _validate_json('{"key": "value"}')
        assert result.valid

    def test_valid_array(self):
        result = _validate_json('[{"a": 1}, {"a": 2}]')
        assert result.valid

    def test_invalid_json_content(self):
        result = _validate_json("this is not json")
        assert not result.valid
        assert result.error_code == "INVALID_JSON"

    def test_partial_json_lines(self):
        # Some valid, some invalid lines — should warn
        content = '{"a": 1}\n{"a": 2}\nbad line\n{"a": 4}\n'
        result = _validate_json(content)
        assert result.valid  # Some lines are valid
        assert result.warnings  # But there's a warning about bad lines


class TestIntegration:
    """End-to-end validation scenarios."""

    def test_normal_csv_workflow(self):
        content = b"id,name,amount\n1,Alice,100.50\n2,Bob,200.75\n3,Charlie,300.00\n"
        result = validate_file(content, len(content), ".csv", "CSV")
        assert result.valid
        assert result.detected_format == "CSV"
        assert not result.is_pgp_encrypted
        assert result.compression is None

    def test_pgp_encrypted_csv(self):
        content = b"-----BEGIN PGP MESSAGE-----\nVersion: GnuPG v2\n\nencrypted data"
        result = validate_file(content, len(content), ".csv.pgp", "CSV")
        assert result.valid
        assert result.is_pgp_encrypted

    def test_gzipped_csv(self):
        content = b"\x1f\x8b\x08\x00compressed csv data"
        result = validate_file(content, len(content), ".csv.gz", "CSV")
        assert result.valid
        assert result.compression == "GZIP"

    def test_zero_byte_rejection(self):
        result = validate_file(b"", 0, ".csv", "CSV")
        assert not result.valid
        assert result.error_code == "EMPTY_FILE"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
