"""
Pager: Disk I/O layer for the database.
Manages reading, writing, and caching of fixed-size pages.
"""

import os
from typing import Dict, Optional
from chidb.util import pack_uint32, unpack_uint32, assert_valid_page_id
from chidb.log import get_logger, log_page_read, log_page_write, log_page_allocate


# Database file header structure
DB_HEADER_SIZE = 100
MAGIC_NUMBER = b'chidb\x00\x00\x00'  # 8 bytes
DB_VERSION = 1

# Default page size
DEFAULT_PAGE_SIZE = 4096


class Pager:
    """
    The Pager manages all disk I/O operations.
    It treats the database file as an array of fixed-size pages.
    
    Page numbering: Pages are numbered starting from 0.
    Page 0 contains the database header.
    """
    
    def __init__(self, filename: str, page_size: int = DEFAULT_PAGE_SIZE):
        """
        Initialize the pager.
        
        Args:
            filename: Path to the database file
            page_size: Size of each page in bytes (must be power of 2, >= 512)
        """
        if page_size < 512 or (page_size & (page_size - 1)) != 0:
            raise ValueError("Page size must be a power of 2 and at least 512 bytes")
        
        self.filename = filename
        self.page_size = page_size
        self.page_cache: Dict[int, bytearray] = {}
        self.dirty_pages: set = set()
        self.file_handle: Optional[object] = None
        self.num_pages = 0
        self.logger = get_logger("pager")
        
        # Open or create the database file
        self._open_or_create()
    
    def _open_or_create(self) -> None:
        """Open existing database or create a new one."""
        file_exists = os.path.exists(self.filename) and os.path.getsize(self.filename) > 0
        
        if file_exists:
            self._open_existing()
        else:
            self._create_new()
    
    def _open_existing(self) -> None:
        """Open an existing database file."""
        self.file_handle = open(self.filename, 'r+b')
        
        # Read and validate header
        self.file_handle.seek(0)
        header = self.file_handle.read(DB_HEADER_SIZE)
        
        if len(header) < DB_HEADER_SIZE:
            raise ValueError("Invalid database file: header too short")
        
        # Check magic number
        magic = header[0:8]
        if magic != MAGIC_NUMBER:
            raise ValueError(f"Invalid database file: bad magic number")
        
        # Read page size
        stored_page_size = unpack_uint32(header, 8)
        if stored_page_size != self.page_size:
            self.logger.warning(
                f"Database page size ({stored_page_size}) differs from requested ({self.page_size}). "
                f"Using database page size."
            )
            self.page_size = stored_page_size
        
        # Calculate number of pages
        self.file_handle.seek(0, os.SEEK_END)
        file_size = self.file_handle.tell()
        self.num_pages = file_size // self.page_size
        
        self.logger.info(f"Opened database '{self.filename}' with {self.num_pages} pages")
    
    def _create_new(self) -> None:
        """Create a new database file."""
        self.file_handle = open(self.filename, 'w+b')
        
        # Create and write header page
        header = self._create_header()
        self.file_handle.write(header)
        self.file_handle.flush()
        
        self.num_pages = 1
        self.logger.info(f"Created new database '{self.filename}'")
    
    def _create_header(self) -> bytes:
        """Create the database file header."""
        header = bytearray(self.page_size)
        
        # Magic number (8 bytes)
        header[0:8] = MAGIC_NUMBER
        
        # Page size (4 bytes)
        header[8:12] = pack_uint32(self.page_size)
        
        # Version (4 bytes)
        header[12:16] = pack_uint32(DB_VERSION)
        
        # Number of pages (4 bytes) - will be updated as we add pages
        header[16:20] = pack_uint32(1)
        
        # Free page list head (4 bytes) - 0 means no free pages
        header[20:24] = pack_uint32(0)
        
        # Reserved space for future use
        # Bytes 24-99 are reserved
        
        return bytes(header)
    
    def read_page(self, page_id: int) -> bytearray:
        """
        Read a page from disk or cache.
        
        Args:
            page_id: The page number to read
            
        Returns:
            A bytearray containing the page data
        """
        assert_valid_page_id(page_id, self.num_pages - 1)
        
        # Check cache first
        if page_id in self.page_cache:
            log_page_read(page_id)
            return self.page_cache[page_id]
        
        # Read from disk
        self.file_handle.seek(page_id * self.page_size)
        data = self.file_handle.read(self.page_size)
        
        if len(data) != self.page_size:
            raise IOError(f"Failed to read complete page {page_id}")
        
        # Cache the page
        page_buffer = bytearray(data)
        self.page_cache[page_id] = page_buffer
        
        log_page_read(page_id)
        return page_buffer
    
    def write_page(self, page_id: int, data: bytes) -> None:
        """
        Write a page to disk.
        
        Args:
            page_id: The page number to write
            data: The page data (must be exactly page_size bytes)
        """
        assert_valid_page_id(page_id, self.num_pages - 1)
        
        if len(data) != self.page_size:
            raise ValueError(f"Page data must be exactly {self.page_size} bytes")
        
        # Update cache
        self.page_cache[page_id] = bytearray(data)
        self.dirty_pages.add(page_id)
        
        log_page_write(page_id)
    
    def allocate_page(self) -> int:
        """
        Allocate a new page.
        
        Returns:
            The page ID of the newly allocated page
        """
        new_page_id = self.num_pages
        self.num_pages += 1
        
        # Create empty page
        empty_page = bytearray(self.page_size)
        self.page_cache[new_page_id] = empty_page
        self.dirty_pages.add(new_page_id)
        
        # Update header with new page count
        header = self.read_page(0)
        header[16:20] = pack_uint32(self.num_pages)
        self.dirty_pages.add(0)
        
        log_page_allocate(new_page_id)
        return new_page_id
    
    def flush(self) -> None:
        """Write all dirty pages to disk."""
        if not self.dirty_pages:
            return
        
        for page_id in self.dirty_pages:
            if page_id in self.page_cache:
                self.file_handle.seek(page_id * self.page_size)
                self.file_handle.write(self.page_cache[page_id])
        
        self.file_handle.flush()
        self.dirty_pages.clear()
        
        self.logger.debug("Flushed all dirty pages to disk")
    
    def close(self) -> None:
        """Close the database file."""
        self.flush()
        
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
        
        self.page_cache.clear()
        self.logger.info(f"Closed database '{self.filename}'")
    
    def get_page_size(self) -> int:
        """Get the page size."""
        return self.page_size
    
    def get_num_pages(self) -> int:
        """Get the total number of pages."""
        return self.num_pages
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False