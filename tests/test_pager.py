"""
Tests for chidb/pager.py
"""

import pytest
import os
import tempfile
from chidb.pager import Pager, DEFAULT_PAGE_SIZE, MAGIC_NUMBER, DB_HEADER_SIZE
from chidb.util import unpack_uint32


@pytest.fixture
def temp_db_file():
    """Create a temporary database file."""
    fd, path = tempfile.mkstemp(suffix='.cdb')
    os.close(fd)
    yield path
    # Cleanup
    if os.path.exists(path):
        os.unlink(path)


class TestPagerCreation:
    """Test pager initialization and file creation."""
    
    def test_create_new_database(self, temp_db_file):
        pager = Pager(temp_db_file)
        
        assert pager.filename == temp_db_file
        assert pager.page_size == DEFAULT_PAGE_SIZE
        assert pager.num_pages == 1
        assert os.path.exists(temp_db_file)
        
        pager.close()
    
    def test_invalid_page_size(self, temp_db_file):
        # Page size must be power of 2
        with pytest.raises(ValueError):
            Pager(temp_db_file, page_size=1000)
        
        # Page size must be at least 512
        with pytest.raises(ValueError):
            Pager(temp_db_file, page_size=256)
    
    def test_valid_page_sizes(self, temp_db_file):
        for size in [512, 1024, 2048, 4096, 8192]:
            if os.path.exists(temp_db_file):
                os.unlink(temp_db_file)
            
            pager = Pager(temp_db_file, page_size=size)
            assert pager.page_size == size
            pager.close()
    
    def test_header_creation(self, temp_db_file):
        pager = Pager(temp_db_file)
        
        # Read header page
        header = pager.read_page(0)
        
        # Check magic number
        assert bytes(header[0:8]) == MAGIC_NUMBER
        
        # Check page size
        page_size = unpack_uint32(header, 8)
        assert page_size == DEFAULT_PAGE_SIZE
        
        # Check version
        version = unpack_uint32(header, 12)
        assert version == 1
        
        # Check num pages
        num_pages = unpack_uint32(header, 16)
        assert num_pages == 1
        
        pager.close()


class TestPagerOpenExisting:
    """Test opening existing database files."""
    
    def test_open_existing_database(self, temp_db_file):
        # Create database
        pager1 = Pager(temp_db_file)
        pager1.close()
        
        # Reopen it
        pager2 = Pager(temp_db_file)
        assert pager2.num_pages == 1
        assert pager2.page_size == DEFAULT_PAGE_SIZE
        pager2.close()
    
    def test_open_with_different_page_size(self, temp_db_file):
        # Create with 4096
        pager1 = Pager(temp_db_file, page_size=4096)
        pager1.close()
        
        # Try to open with 2048 - should use the stored page size (4096)
        pager2 = Pager(temp_db_file, page_size=2048)
        assert pager2.page_size == 4096
        pager2.close()
    
    def test_open_invalid_file(self, temp_db_file):
        # Write garbage to file
        with open(temp_db_file, 'wb') as f:
            f.write(b'not a valid database file')
        
        with pytest.raises(ValueError):
            Pager(temp_db_file)


class TestPageReadWrite:
    """Test reading and writing pages."""
    
    def test_read_page_zero(self, temp_db_file):
        pager = Pager(temp_db_file)
        
        # Should be able to read page 0 (header)
        page = pager.read_page(0)
        assert len(page) == DEFAULT_PAGE_SIZE
        assert bytes(page[0:8]) == MAGIC_NUMBER
        
        pager.close()
    
    def test_read_invalid_page(self, temp_db_file):
        pager = Pager(temp_db_file)
        
        # Only page 0 exists initially
        with pytest.raises(ValueError):
            pager.read_page(1)
        
        with pytest.raises(ValueError):
            pager.read_page(-1)
        
        pager.close()
    
    def test_write_page(self, temp_db_file):
        pager = Pager(temp_db_file)
        
        # Allocate a new page
        page_id = pager.allocate_page()
        assert page_id == 1
        
        # Write data to it
        test_data = b'X' * DEFAULT_PAGE_SIZE
        pager.write_page(page_id, test_data)
        
        # Read it back
        read_data = pager.read_page(page_id)
        assert bytes(read_data) == test_data
        
        pager.close()
    
    def test_write_wrong_size(self, temp_db_file):
        pager = Pager(temp_db_file)
        page_id = pager.allocate_page()
        
        # Try to write wrong size
        with pytest.raises(ValueError):
            pager.write_page(page_id, b'too short')
        
        pager.close()
    
    def test_page_caching(self, temp_db_file):
        pager = Pager(temp_db_file)
        
        # Read page 0 twice - should use cache
        page1 = pager.read_page(0)
        page2 = pager.read_page(0)
        
        # Should be the same object (cached)
        assert page1 is page2
        
        pager.close()


class TestPageAllocation:
    """Test page allocation."""
    
    def test_allocate_single_page(self, temp_db_file):
        pager = Pager(temp_db_file)
        
        assert pager.num_pages == 1
        
        page_id = pager.allocate_page()
        assert page_id == 1
        assert pager.num_pages == 2
        
        pager.close()
    
    def test_allocate_multiple_pages(self, temp_db_file):
        pager = Pager(temp_db_file)
        
        page_ids = []
        for i in range(5):
            page_id = pager.allocate_page()
            page_ids.append(page_id)
        
        assert page_ids == [1, 2, 3, 4, 5]
        assert pager.num_pages == 6
        
        pager.close()
    
    def test_allocated_page_is_empty(self, temp_db_file):
        pager = Pager(temp_db_file)
        
        page_id = pager.allocate_page()
        page = pager.read_page(page_id)
        
        # Should be all zeros
        assert all(b == 0 for b in page)
        
        pager.close()


class TestPagerFlush:
    """Test flushing dirty pages to disk."""
    
    def test_flush_writes_to_disk(self, temp_db_file):
        # Create and write data
        pager1 = Pager(temp_db_file)
        page_id = pager1.allocate_page()
        test_data = b'A' * DEFAULT_PAGE_SIZE
        pager1.write_page(page_id, test_data)
        pager1.flush()
        pager1.close()
        
        # Reopen and verify
        pager2 = Pager(temp_db_file)
        read_data = pager2.read_page(page_id)
        assert bytes(read_data) == test_data
        pager2.close()
    
    def test_close_flushes_automatically(self, temp_db_file):
        # Create and write data
        pager1 = Pager(temp_db_file)
        page_id = pager1.allocate_page()
        test_data = b'B' * DEFAULT_PAGE_SIZE
        pager1.write_page(page_id, test_data)
        pager1.close()  # Should flush automatically
        
        # Reopen and verify
        pager2 = Pager(temp_db_file)
        read_data = pager2.read_page(page_id)
        assert bytes(read_data) == test_data
        pager2.close()


class TestPagerContextManager:
    """Test context manager functionality."""
    
    def test_context_manager(self, temp_db_file):
        with Pager(temp_db_file) as pager:
            page_id = pager.allocate_page()
            test_data = b'C' * DEFAULT_PAGE_SIZE
            pager.write_page(page_id, test_data)
        
        # Should be closed and flushed
        # Verify by reopening
        with Pager(temp_db_file) as pager:
            read_data = pager.read_page(page_id)
            assert bytes(read_data) == test_data


class TestPagerMethods:
    """Test utility methods."""
    
    def test_get_page_size(self, temp_db_file):
        pager = Pager(temp_db_file)
        assert pager.get_page_size() == DEFAULT_PAGE_SIZE
        pager.close()
    
    def test_get_num_pages(self, temp_db_file):
        pager = Pager(temp_db_file)
        assert pager.get_num_pages() == 1
        
        pager.allocate_page()
        assert pager.get_num_pages() == 2
        
        pager.close()