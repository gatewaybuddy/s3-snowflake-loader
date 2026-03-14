"""Tests for file format auto-detection."""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lambda"))

from loader.format_detector import detect_format_from_content


class TestBinaryFormats:
    def test_parquet_magic(self):
        result = detect_format_from_content(b"PAR1" + b"\x00" * 100)
        assert result["type"] == "PARQUET"
        assert result["confidence"] == "high"

    def test_avro_magic(self):
        result = detect_format_from_content(b"Obj\x01" + b"\x00" * 100)
        assert result["type"] == "AVRO"

    def test_orc_magic(self):
        result = detect_format_from_content(b"ORC" + b"\x00" * 100)
        assert result["type"] == "ORC"


class TestJsonDetection:
    def test_json_object(self):
        result = detect_format_from_content(b'{"key": "value"}')
        assert result["type"] == "JSON"

    def test_json_array(self):
        result = detect_format_from_content(b'[{"a": 1}, {"a": 2}]')
        assert result["type"] == "JSON"
        assert result["dialect"].get("strip_outer_array") == True

    def test_json_lines(self):
        content = b'{"a": 1}\n{"a": 2}\n{"a": 3}\n'
        result = detect_format_from_content(content)
        assert result["type"] == "JSON"
        assert result["confidence"] == "high"

    def test_json_with_bom(self):
        result = detect_format_from_content(b'\xef\xbb\xbf{"key": "value"}')
        assert result["type"] == "JSON"


class TestDelimitedDetection:
    def test_comma_csv(self):
        content = b"id,name,email\n1,Alice,alice@test.com\n2,Bob,bob@test.com\n"
        result = detect_format_from_content(content)
        assert result["type"] == "CSV"
        assert result["dialect"]["field_delimiter"] == ","

    def test_pipe_delimited(self):
        content = b"id|name|email\n1|Alice|alice@test.com\n2|Bob|bob@test.com\n"
        result = detect_format_from_content(content)
        assert result["type"] == "CSV"
        assert result["dialect"]["field_delimiter"] == "|"

    def test_tab_delimited(self):
        content = b"id\tname\temail\n1\tAlice\talice@test.com\n"
        result = detect_format_from_content(content)
        assert result["type"] == "CSV"
        assert result["dialect"]["field_delimiter"] == "\\t"

    def test_semicolon_delimited(self):
        content = b"id;name;email\n1;Alice;alice@test.com\n"
        result = detect_format_from_content(content)
        assert result["type"] == "CSV"
        assert result["dialect"]["field_delimiter"] == ";"

    def test_quoted_csv(self):
        content = b'"id","name","email"\n"1","Alice","alice@test.com"\n'
        result = detect_format_from_content(content)
        assert result["type"] == "CSV"
        assert result["dialect"].get("field_optionally_enclosed_by") == '"'

    def test_header_detection(self):
        content = b"id,name,amount\n1,Alice,100.50\n2,Bob,200.75\n"
        result = detect_format_from_content(content)
        assert result["dialect"]["skip_header"] == 1

    def test_csv_extension_hint(self):
        content = b"some ambiguous content\n"
        result = detect_format_from_content(content, ".csv")
        assert result["confidence"] == "high"


class TestEdgeCases:
    def test_empty_content(self):
        result = detect_format_from_content(b"")
        assert result["type"] == "CSV"  # Fallback
        assert result["confidence"] == "low"

    def test_binary_gibberish(self):
        result = detect_format_from_content(bytes(range(256)))
        # Should handle gracefully
        assert result["type"] in ("CSV", "UNKNOWN")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
