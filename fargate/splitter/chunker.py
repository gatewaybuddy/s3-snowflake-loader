"""
File chunking logic for streaming large file processing.

Splits files at line boundaries into configurable chunks while streaming.
Never loads the full file into memory - processes line by line.

Supported formats:
- CSV: splits at newlines, handles quoted fields with embedded newlines
- JSON Lines: splits at newlines  
- Standard JSON array: parses and splits by records
- Parquet: skipped (Snowflake handles natively)

Each chunk:
- Respects line boundaries (never cuts mid-row)
- Includes header row (CSV) or is self-contained (JSON)
- Is GZIP compressed for upload
- Tracks accurate row counts
"""

import csv
import gzip
import io
import json
import logging
from dataclasses import dataclass
from typing import BinaryIO, Iterator, Optional, TextIO

logger = logging.getLogger(__name__)


@dataclass
class ChunkResult:
    """Result of processing a file chunk."""
    data: bytes  # GZIP-compressed chunk data
    row_count: int  # Number of rows in this chunk
    chunk_number: int  # Sequential chunk number (1-based)
    is_header_chunk: bool  # True if this is the first chunk with headers


class FileChunker:
    """
    Streaming file chunker that splits large files at line boundaries.
    
    Handles different file formats and ensures each chunk is self-contained
    with appropriate headers and proper compression.
    """
    
    def __init__(self, file_key: str, chunk_size_bytes: int):
        """
        Initialize chunker.
        
        Args:
            file_key: S3 key of the file being processed (used to detect format)
            chunk_size_bytes: Target chunk size (uncompressed)
        """
        self.file_key = file_key
        self.chunk_size_bytes = chunk_size_bytes
        self.file_format = self._detect_format(file_key)
        self.header_row: Optional[str] = None
        self.chunk_number = 0
        
        logger.info(
            f"Initialized chunker: format={self.file_format}, "
            f"target_size={chunk_size_bytes / (1024**2):.1f}MB"
        )
    
    def process_stream(self, input_stream: BinaryIO) -> Iterator[ChunkResult]:
        """
        Process input stream and yield compressed chunks.
        
        Args:
            input_stream: Binary stream to read from
            
        Yields:
            ChunkResult: Each chunk with compressed data and metadata
        """
        if self.file_format == "PARQUET":
            # Parquet files are not split - Snowflake handles them natively
            logger.info("Parquet format detected - copying entire file as single chunk")
            yield from self._process_binary_file(input_stream)
        elif self.file_format == "CSV":
            yield from self._process_csv_stream(input_stream)
        elif self.file_format == "JSONL":
            yield from self._process_jsonl_stream(input_stream)
        elif self.file_format == "JSON":
            yield from self._process_json_array_stream(input_stream)
        else:
            # Default to line-based processing
            logger.warning(f"Unknown format {self.file_format}, defaulting to line-based splitting")
            yield from self._process_text_stream(input_stream)
    
    def _detect_format(self, file_key: str) -> str:
        """Detect file format from extension."""
        lower_key = file_key.lower()
        
        # Remove compression extensions first
        for comp_ext in ['.gz', '.gzip', '.bz2', '.xz', '.zip']:
            if lower_key.endswith(comp_ext):
                lower_key = lower_key[:-len(comp_ext)]
                break
        
        # Remove PGP extensions
        for pgp_ext in ['.pgp', '.gpg', '.asc', '.enc']:
            if lower_key.endswith(pgp_ext):
                lower_key = lower_key[:-len(pgp_ext)]
                break
        
        # Detect format from final extension
        if lower_key.endswith('.csv'):
            return "CSV"
        elif lower_key.endswith('.jsonl') or lower_key.endswith('.ndjson'):
            return "JSONL"
        elif lower_key.endswith('.json'):
            return "JSON"
        elif lower_key.endswith('.parquet'):
            return "PARQUET"
        else:
            return "CSV"  # Default assumption
    
    def _process_binary_file(self, input_stream: BinaryIO) -> Iterator[ChunkResult]:
        """Process binary file (like Parquet) without splitting."""
        self.chunk_number += 1
        
        # Read entire file and compress
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wb') as gzf:
            while True:
                chunk = input_stream.read(1024 * 1024)  # 1MB chunks
                if not chunk:
                    break
                gzf.write(chunk)
        
        compressed_data = buffer.getvalue()
        
        logger.info(f"Binary file processed: {len(compressed_data) / (1024**2):.1f}MB compressed")
        
        yield ChunkResult(
            data=compressed_data,
            row_count=0,  # Unknown for binary files
            chunk_number=self.chunk_number,
            is_header_chunk=True
        )
    
    def _process_csv_stream(self, input_stream: BinaryIO) -> Iterator[ChunkResult]:
        """Process CSV stream with proper quoted field handling."""
        # Convert binary stream to text
        text_stream = io.TextIOWrapper(input_stream, encoding='utf-8-sig', errors='replace')
        
        # Use csv.reader to handle quoted fields properly
        csv_reader = csv.reader(text_stream)
        
        try:
            # Read header row
            header_row = next(csv_reader)
            self.header_row = ','.join(f'"{field}"' if ',' in field or '"' in field else field 
                                      for field in header_row)
            logger.info(f"CSV header: {len(header_row)} columns")
            
            current_chunk_lines = [self.header_row]
            current_chunk_size = len(self.header_row.encode('utf-8'))
            row_count = 0  # Don't count header in row count
            
            for row in csv_reader:
                # Convert row back to CSV line
                csv_line = ','.join(f'"{field}"' if ',' in field or '"' in field or '\n' in field 
                                  else field for field in row)
                
                line_size = len(csv_line.encode('utf-8')) + 1  # +1 for newline
                
                # Check if adding this line would exceed chunk size
                if (current_chunk_size + line_size > self.chunk_size_bytes and 
                    len(current_chunk_lines) > 1):  # Always include at least header + 1 row
                    
                    # Yield current chunk
                    yield self._create_chunk(current_chunk_lines, row_count)
                    
                    # Start new chunk with header
                    current_chunk_lines = [self.header_row, csv_line]
                    current_chunk_size = len(self.header_row.encode('utf-8')) + line_size
                    row_count = 1
                else:
                    # Add line to current chunk
                    current_chunk_lines.append(csv_line)
                    current_chunk_size += line_size
                    row_count += 1
            
            # Yield final chunk if it has data beyond header
            if len(current_chunk_lines) > 1:
                yield self._create_chunk(current_chunk_lines, row_count)
                
        except UnicodeDecodeError as e:
            logger.error(f"Unicode decode error in CSV processing: {e}")
            raise
        finally:
            text_stream.detach()  # Don't close the underlying binary stream
    
    def _process_jsonl_stream(self, input_stream: BinaryIO) -> Iterator[ChunkResult]:
        """Process JSON Lines (NDJSON) stream."""
        text_stream = io.TextIOWrapper(input_stream, encoding='utf-8-sig', errors='replace')
        
        try:
            current_chunk_lines = []
            current_chunk_size = 0
            row_count = 0
            
            for line in text_stream:
                line = line.strip()
                if not line:
                    continue
                
                # Validate JSON line
                try:
                    json.loads(line)
                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping invalid JSON line: {e}")
                    continue
                
                line_size = len(line.encode('utf-8')) + 1  # +1 for newline
                
                # Check if adding this line would exceed chunk size
                if (current_chunk_size + line_size > self.chunk_size_bytes and 
                    len(current_chunk_lines) > 0):
                    
                    # Yield current chunk
                    yield self._create_chunk(current_chunk_lines, row_count)
                    
                    # Start new chunk
                    current_chunk_lines = [line]
                    current_chunk_size = line_size
                    row_count = 1
                else:
                    # Add line to current chunk
                    current_chunk_lines.append(line)
                    current_chunk_size += line_size
                    row_count += 1
            
            # Yield final chunk
            if current_chunk_lines:
                yield self._create_chunk(current_chunk_lines, row_count)
                
        finally:
            text_stream.detach()
    
    def _process_json_array_stream(self, input_stream: BinaryIO) -> Iterator[ChunkResult]:
        """
        Process JSON array stream.
        
        Note: This loads the entire JSON into memory for parsing, which may not
        be suitable for very large files. For streaming JSON parsing, consider
        using ijson library, but it adds a dependency.
        """
        text_stream = io.TextIOWrapper(input_stream, encoding='utf-8-sig', errors='replace')
        
        try:
            # Load entire JSON (this is the memory limitation)
            json_data = json.load(text_stream)
            
            if not isinstance(json_data, list):
                raise ValueError("JSON file must contain an array of objects")
            
            logger.info(f"Loaded JSON array with {len(json_data)} records")
            
            current_chunk_objects = []
            current_chunk_size = 2  # Start with [] characters
            record_count = 0
            
            for obj in json_data:
                obj_json = json.dumps(obj, separators=(',', ':'))
                obj_size = len(obj_json.encode('utf-8')) + 1  # +1 for comma/newline
                
                # Check if adding this object would exceed chunk size
                if (current_chunk_size + obj_size > self.chunk_size_bytes and 
                    len(current_chunk_objects) > 0):
                    
                    # Create JSON array for current chunk
                    chunk_array = json.dumps(current_chunk_objects, separators=(',', ':'))
                    yield self._create_chunk([chunk_array], record_count)
                    
                    # Start new chunk
                    current_chunk_objects = [obj]
                    current_chunk_size = 2 + obj_size  # [] + object
                    record_count = 1
                else:
                    # Add object to current chunk
                    current_chunk_objects.append(obj)
                    current_chunk_size += obj_size
                    record_count += 1
            
            # Yield final chunk
            if current_chunk_objects:
                chunk_array = json.dumps(current_chunk_objects, separators=(',', ':'))
                yield self._create_chunk([chunk_array], record_count)
                
        finally:
            text_stream.detach()
    
    def _process_text_stream(self, input_stream: BinaryIO) -> Iterator[ChunkResult]:
        """Process generic text stream line by line."""
        text_stream = io.TextIOWrapper(input_stream, encoding='utf-8-sig', errors='replace')
        
        try:
            current_chunk_lines = []
            current_chunk_size = 0
            row_count = 0
            
            for line in text_stream:
                line = line.rstrip('\n\r')  # Remove line endings
                line_size = len(line.encode('utf-8')) + 1  # +1 for newline
                
                # Check if adding this line would exceed chunk size
                if (current_chunk_size + line_size > self.chunk_size_bytes and 
                    len(current_chunk_lines) > 0):
                    
                    # Yield current chunk
                    yield self._create_chunk(current_chunk_lines, row_count)
                    
                    # Start new chunk
                    current_chunk_lines = [line]
                    current_chunk_size = line_size
                    row_count = 1
                else:
                    # Add line to current chunk
                    current_chunk_lines.append(line)
                    current_chunk_size += line_size
                    row_count += 1
            
            # Yield final chunk
            if current_chunk_lines:
                yield self._create_chunk(current_chunk_lines, row_count)
                
        finally:
            text_stream.detach()
    
    def _create_chunk(self, lines: list[str], row_count: int) -> ChunkResult:
        """Create a compressed chunk from lines."""
        self.chunk_number += 1
        
        # Join lines with newlines
        chunk_text = '\n'.join(lines) + '\n'
        
        # GZIP compress
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wt', encoding='utf-8') as gzf:
            gzf.write(chunk_text)
        
        compressed_data = buffer.getvalue()
        
        logger.info(
            f"Created chunk {self.chunk_number}: {row_count:,} rows, "
            f"{len(chunk_text) / (1024**2):.1f}MB uncompressed → "
            f"{len(compressed_data) / (1024**2):.1f}MB compressed "
            f"({len(compressed_data) / len(chunk_text.encode('utf-8')) * 100:.1f}% ratio)"
        )
        
        return ChunkResult(
            data=compressed_data,
            row_count=row_count,
            chunk_number=self.chunk_number,
            is_header_chunk=(self.chunk_number == 1)
        )