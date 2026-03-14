#!/usr/bin/env python3
"""
Unit tests for the file chunker module.

Tests streaming file processing, line boundary handling, and chunk generation
for various file formats (CSV, JSON Lines, JSON arrays).
"""

import csv
import gzip
import io
import json
import tempfile
import unittest
from unittest.mock import patch

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from splitter.chunker import FileChunker, ChunkResult


class TestFileChunker(unittest.TestCase):
    """Test cases for FileChunker class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.chunk_size = 1024  # 1KB for testing
    
    def test_format_detection(self):
        """Test file format detection from filenames."""
        test_cases = [
            ("data.csv", "CSV"),
            ("data.csv.gz", "CSV"),
            ("data.csv.pgp.gz", "CSV"),
            ("data.jsonl", "JSONL"),
            ("data.ndjson", "JSONL"),
            ("data.json", "JSON"),
            ("data.json.enc", "JSON"),
            ("data.parquet", "PARQUET"),
            ("unknown.txt", "CSV"),  # Default fallback
        ]
        
        for filename, expected_format in test_cases:
            with self.subTest(filename=filename):
                chunker = FileChunker(filename, self.chunk_size)
                self.assertEqual(chunker.file_format, expected_format)
    
    def test_csv_chunking_simple(self):
        """Test basic CSV chunking with small data."""
        csv_data = "name,age,city\nJohn,25,NYC\nJane,30,LA\nBob,35,Chicago\n"
        stream = io.BytesIO(csv_data.encode('utf-8'))
        
        chunker = FileChunker("test.csv", 50)  # Small chunk size to force splitting
        chunks = list(chunker.process_stream(stream))
        
        self.assertEqual(len(chunks), 2)  # Should split into 2 chunks
        
        # Verify first chunk
        chunk1 = chunks[0]
        self.assertIsInstance(chunk1, ChunkResult)
        self.assertTrue(chunk1.is_header_chunk)
        self.assertEqual(chunk1.chunk_number, 1)
        self.assertGreater(chunk1.row_count, 0)
        
        # Decompress and verify content
        decompressed = gzip.decompress(chunk1.data).decode('utf-8')
        lines = decompressed.strip().split('\n')
        self.assertEqual(lines[0], 'name,age,city')  # Header preserved
        
        # Verify second chunk also has header
        chunk2 = chunks[1]
        decompressed2 = gzip.decompress(chunk2.data).decode('utf-8')
        lines2 = decompressed2.strip().split('\n')
        self.assertEqual(lines2[0], 'name,age,city')  # Header preserved
    
    def test_csv_quoted_fields(self):
        """Test CSV chunking with quoted fields containing commas and newlines."""
        csv_data = '''name,description,value
"Smith, John","A person with
multiple lines
in description","100"
"Doe, Jane","Simple description","200"
"Brown, Bob","Another multi-line
description here","300"
'''
        stream = io.BytesIO(csv_data.encode('utf-8'))
        
        chunker = FileChunker("test.csv", 100)  # Small chunk to force splitting
        chunks = list(chunker.process_stream(stream))
        
        self.assertGreater(len(chunks), 1)
        
        # Verify that records are not split mid-field
        total_rows = sum(chunk.row_count for chunk in chunks)
        
        # Verify content integrity by parsing each chunk
        for chunk in chunks:
            decompressed = gzip.decompress(chunk.data).decode('utf-8')
            lines = decompressed.strip().split('\n')
            
            # Each chunk should have header + some data rows
            self.assertEqual(lines[0], 'name,description,value')
            
            # Parse the chunk as CSV to verify integrity
            chunk_reader = csv.reader(io.StringIO(decompressed))
            chunk_rows = list(chunk_reader)
            self.assertGreater(len(chunk_rows), 1)  # Header + at least one data row
        
        # Should have 3 data rows total (as reported by chunker)
        self.assertEqual(total_rows, 3)
    
    def test_jsonl_chunking(self):
        """Test JSON Lines chunking."""
        jsonl_data = '''{"name": "John", "age": 25}
{"name": "Jane", "age": 30}
{"name": "Bob", "age": 35}
{"name": "Alice", "age": 40}
'''
        stream = io.BytesIO(jsonl_data.encode('utf-8'))
        
        chunker = FileChunker("test.jsonl", 60)  # Small chunk size
        chunks = list(chunker.process_stream(stream))
        
        self.assertGreater(len(chunks), 1)
        
        # Verify each chunk contains valid JSON lines
        total_records = 0
        for chunk in chunks:
            decompressed = gzip.decompress(chunk.data).decode('utf-8')
            lines = [line.strip() for line in decompressed.strip().split('\n') if line.strip()]
            
            for line in lines:
                data = json.loads(line)  # Should parse without error
                self.assertIn('name', data)
                self.assertIn('age', data)
                total_records += 1
        
        self.assertEqual(total_records, 4)
    
    def test_json_array_chunking(self):
        """Test JSON array chunking."""
        json_data = [
            {"id": 1, "name": "John", "data": "x" * 100},
            {"id": 2, "name": "Jane", "data": "y" * 100},
            {"id": 3, "name": "Bob", "data": "z" * 100},
            {"id": 4, "name": "Alice", "data": "w" * 100},
        ]
        json_text = json.dumps(json_data)
        stream = io.BytesIO(json_text.encode('utf-8'))
        
        chunker = FileChunker("test.json", 200)  # Force splitting
        chunks = list(chunker.process_stream(stream))
        
        # Verify each chunk contains a valid JSON array
        total_records = 0
        for chunk in chunks:
            decompressed = gzip.decompress(chunk.data).decode('utf-8')
            array = json.loads(decompressed)
            
            self.assertIsInstance(array, list)
            for item in array:
                self.assertIn('id', item)
                self.assertIn('name', item)
                total_records += 1
        
        self.assertEqual(total_records, 4)
    
    def test_empty_file(self):
        """Test handling of empty files."""
        stream = io.BytesIO(b'')
        
        chunker = FileChunker("empty.csv", self.chunk_size)
        chunks = list(chunker.process_stream(stream))
        
        self.assertEqual(len(chunks), 0)
    
    def test_single_line_csv(self):
        """Test CSV with only header."""
        csv_data = "name,age,city\n"
        stream = io.BytesIO(csv_data.encode('utf-8'))
        
        chunker = FileChunker("test.csv", self.chunk_size)
        chunks = list(chunker.process_stream(stream))
        
        # Should not create chunks for header-only CSV
        self.assertEqual(len(chunks), 0)
    
    def test_large_single_line(self):
        """Test handling of lines larger than chunk size."""
        large_line = "name,description\n\"John\",\"" + "x" * 2000 + "\"\n"
        stream = io.BytesIO(large_line.encode('utf-8'))
        
        chunker = FileChunker("test.csv", 500)  # Smaller than the large line
        chunks = list(chunker.process_stream(stream))
        
        self.assertEqual(len(chunks), 1)
        
        # Verify large line is preserved intact
        decompressed = gzip.decompress(chunks[0].data).decode('utf-8')
        lines = decompressed.strip().split('\n')
        self.assertEqual(len(lines), 2)  # Header + data line
        self.assertIn("x" * 2000, lines[1])  # Large line intact
    
    def test_compression_efficiency(self):
        """Test that chunks are properly compressed."""
        # Create repetitive data that should compress well
        csv_data = "name,value\n"
        for i in range(100):
            csv_data += f"user{i:03d},{'data' * 10}\n"
        
        stream = io.BytesIO(csv_data.encode('utf-8'))
        
        chunker = FileChunker("test.csv", len(csv_data) // 2)  # Force 2 chunks
        chunks = list(chunker.process_stream(stream))
        
        for chunk in chunks:
            # Compressed size should be significantly smaller than original
            original_size = len(csv_data.encode('utf-8')) // 2
            compressed_size = len(chunk.data)
            compression_ratio = compressed_size / original_size
            
            self.assertLess(compression_ratio, 0.8)  # At least 20% compression
    
    def test_chunk_metadata(self):
        """Test that chunk metadata is accurate."""
        csv_data = "name,age\n"
        for i in range(50):
            csv_data += f"User{i},2{i}\n"
        
        stream = io.BytesIO(csv_data.encode('utf-8'))
        
        chunker = FileChunker("test.csv", 300)  # Force multiple chunks
        chunks = list(chunker.process_stream(stream))
        
        total_rows = 0
        for i, chunk in enumerate(chunks):
            # Verify chunk numbers are sequential
            self.assertEqual(chunk.chunk_number, i + 1)
            
            # Only first chunk should be header chunk
            self.assertEqual(chunk.is_header_chunk, i == 0)
            
            # Row count should be positive
            self.assertGreater(chunk.row_count, 0)
            
            # Data should be compressed bytes
            self.assertIsInstance(chunk.data, bytes)
            
            total_rows += chunk.row_count
        
        # Total rows should match input (excluding header)
        self.assertEqual(total_rows, 50)
    
    def test_binary_parquet_handling(self):
        """Test that binary files (like Parquet) are handled without splitting."""
        # Simulate binary Parquet data
        binary_data = b'\x50\x41\x52\x31' + b'\x00' * 1000  # PAR1 magic + data
        stream = io.BytesIO(binary_data)
        
        chunker = FileChunker("test.parquet", 200)  # Smaller than data size
        chunks = list(chunker.process_stream(stream))
        
        # Parquet should be processed as single chunk regardless of size
        self.assertEqual(len(chunks), 1)
        
        chunk = chunks[0]
        self.assertTrue(chunk.is_header_chunk)
        self.assertEqual(chunk.chunk_number, 1)
        # Row count is unknown for binary files
        self.assertEqual(chunk.row_count, 0)
    
    def test_unicode_handling(self):
        """Test proper handling of Unicode characters."""
        csv_data = "name,description\nJöhn,Ünicöde tëst\nJané,Españól tëxt\n"
        stream = io.BytesIO(csv_data.encode('utf-8'))
        
        chunker = FileChunker("test.csv", self.chunk_size)
        chunks = list(chunker.process_stream(stream))
        
        self.assertGreater(len(chunks), 0)
        
        # Verify Unicode is preserved
        decompressed = gzip.decompress(chunks[0].data).decode('utf-8')
        self.assertIn('Jöhn', decompressed)
        self.assertIn('Ünicöde', decompressed)
        self.assertIn('Españól', decompressed)


class TestChunkResult(unittest.TestCase):
    """Test cases for ChunkResult dataclass."""
    
    def test_chunk_result_creation(self):
        """Test ChunkResult object creation and attributes."""
        test_data = b'compressed data'
        
        result = ChunkResult(
            data=test_data,
            row_count=100,
            chunk_number=5,
            is_header_chunk=False
        )
        
        self.assertEqual(result.data, test_data)
        self.assertEqual(result.row_count, 100)
        self.assertEqual(result.chunk_number, 5)
        self.assertFalse(result.is_header_chunk)


if __name__ == '__main__':
    unittest.main()