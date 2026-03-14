"""Tests for S3 path parsing."""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lambda"))

from loader.path_parser import parse_s3_key, _parse_folder_name, _detect_format, detect_csv_dialect


class TestParseFolderName:
    def test_truncate_explicit(self):
        assert _parse_folder_name("CUSTOMERS.T") == ("CUSTOMERS", "TRUNCATE")

    def test_append(self):
        assert _parse_folder_name("REVENUE.A") == ("REVENUE", "APPEND")

    def test_merge(self):
        assert _parse_folder_name("ORDERS.M") == ("ORDERS", "MERGE")

    def test_no_mode_defaults_truncate(self):
        assert _parse_folder_name("DAILY_METRICS") == ("DAILY_METRICS", "TRUNCATE")

    def test_lowercase_uppercased(self):
        assert _parse_folder_name("my_table") == ("MY_TABLE", "TRUNCATE")

    def test_mode_case_insensitive(self):
        assert _parse_folder_name("DATA.t") == ("DATA", "TRUNCATE")
        assert _parse_folder_name("DATA.a") == ("DATA", "APPEND")

    def test_dot_in_name_not_mode(self):
        # ".X" where X is not a valid mode should keep the dot
        name, mode = _parse_folder_name("TABLE.Z")
        assert mode == "TRUNCATE"  # Z is not a mode

    def test_short_names(self):
        assert _parse_folder_name("A.T") == ("A", "TRUNCATE")
        assert _parse_folder_name("X") == ("X", "TRUNCATE")


class TestDetectFormat:
    def test_csv(self):
        ext, fmt, comp = _detect_format("data.csv")
        assert fmt == "CSV"
        assert comp is None

    def test_csv_gzipped(self):
        ext, fmt, comp = _detect_format("data.csv.gz")
        assert fmt == "CSV"
        assert comp == "GZIP"

    def test_json(self):
        ext, fmt, comp = _detect_format("events.json")
        assert fmt == "JSON"

    def test_jsonl(self):
        ext, fmt, comp = _detect_format("events.jsonl")
        assert fmt == "JSON"

    def test_parquet(self):
        ext, fmt, comp = _detect_format("data.parquet")
        assert fmt == "PARQUET"

    def test_tsv(self):
        ext, fmt, comp = _detect_format("data.tsv")
        assert fmt == "CSV"  # TSV is CSV with tab delimiter

    def test_bz2_compression(self):
        ext, fmt, comp = _detect_format("data.csv.bz2")
        assert comp == "BZ2"

    def test_zstd_compression(self):
        ext, fmt, comp = _detect_format("data.json.zst")
        assert fmt == "JSON"
        assert comp == "ZSTD"

    def test_no_extension_raises(self):
        with pytest.raises(ValueError):
            _detect_format("noext")


class TestParseS3Key:
    def test_simple_csv(self):
        result = parse_s3_key("analytics/CUSTOMERS.T/data.csv", "analytics/")
        assert result.table_name == "CUSTOMERS"
        assert result.load_mode == "TRUNCATE"
        assert result.file_format == "CSV"
        assert result.relative_path == "CUSTOMERS.T/data.csv"

    def test_nested_path(self):
        result = parse_s3_key(
            "analytics/reports/monthly/REVENUE.A/revenue_2026.csv",
            "analytics/"
        )
        assert result.table_name == "REVENUE"
        assert result.load_mode == "APPEND"

    def test_parquet(self):
        result = parse_s3_key("reporting/METRICS/data.parquet", "reporting/")
        assert result.table_name == "METRICS"
        assert result.file_format == "PARQUET"

    def test_compressed(self):
        result = parse_s3_key("analytics/LOGS.A/events.json.gz", "analytics/")
        assert result.file_format == "JSON"
        assert result.compression == "GZIP"

    def test_no_prefix(self):
        result = parse_s3_key("TABLE.T/file.csv", "")
        assert result.table_name == "TABLE"

    def test_too_short_raises(self):
        with pytest.raises(ValueError):
            parse_s3_key("file.csv", "")

    def test_folder_marker_raises(self):
        with pytest.raises(ValueError):
            parse_s3_key("analytics/CUSTOMERS.T/", "analytics/")


class TestDetectCsvDialect:
    def test_comma_delimited(self):
        result = detect_csv_dialect("id,name,email,phone")
        assert result["field_delimiter"] == ","

    def test_pipe_delimited(self):
        result = detect_csv_dialect("id|name|email|phone")
        assert result["field_delimiter"] == "|"

    def test_tab_delimited(self):
        result = detect_csv_dialect("id\tname\temail\tphone")
        assert result["field_delimiter"] == "\\t"

    def test_semicolon_delimited(self):
        result = detect_csv_dialect("id;name;email;phone")
        assert result["field_delimiter"] == ";"

    def test_quoted_fields(self):
        result = detect_csv_dialect('"id","name","email"')
        assert result.get("field_optionally_enclosed_by") == '"'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
