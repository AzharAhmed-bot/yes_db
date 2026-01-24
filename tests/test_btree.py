"""
Tests for chidb/btree.py
"""

import pytest
import tempfile
import os
from chidb.pager import Pager
from chidb.btree import BTree, BTreeNode, NODE_TYPE_LEAF, NODE_TYPE_INTERNAL
from chidb.record import Record


@pytest.fixture
def temp_db():
    """Create a temporary database."""
    fd, path = tempfile.mkstemp(suffix='.cdb')
    os.close(fd)
    pager = Pager(path)
    yield pager
    pager.close()
    if os.path.exists(path):
        os.unlink(path)


class TestBTreeCreation:
    """Test B-tree creation."""
    
    def test_create_new_btree(self, temp_db):
        btree = BTree(temp_db)
        assert btree.root_page > 0
    
    def test_create_with_existing_root(self, temp_db):
        # Create first tree
        btree1 = BTree(temp_db)
        root = btree1.root_page
        
        # Create second tree using same root
        btree2 = BTree(temp_db, root_page=root)
        assert btree2.root_page == root


class TestBTreeInsertion:
    """Test inserting into B-tree."""
    
    def test_insert_single_record(self, temp_db):
        btree = BTree(temp_db)
        record = Record([42, "test"])
        
        btree.insert(1, record)
        
        # Verify it was inserted
        result = btree.search(1)
        assert result is not None
        assert result.get_value(0) == 42
        assert result.get_value(1) == "test"
    
    def test_insert_multiple_records(self, temp_db):
        btree = BTree(temp_db)
        
        for i in range(10):
            record = Record([i, f"value_{i}"])
            btree.insert(i, record)
        
        # Verify all were inserted
        for i in range(10):
            result = btree.search(i)
            assert result is not None
            assert result.get_value(0) == i
    
    def test_insert_out_of_order(self, temp_db):
        btree = BTree(temp_db)
        keys = [5, 2, 8, 1, 9, 3, 7, 4, 6]
        
        for key in keys:
            record = Record([key])
            btree.insert(key, record)
        
        # Verify all can be found
        for key in keys:
            result = btree.search(key)
            assert result is not None
            assert result.get_value(0) == key
    
    def test_insert_duplicate_key_updates(self, temp_db):
        btree = BTree(temp_db)
        
        # Insert initial record
        btree.insert(1, Record([100]))
        
        # Update with new record
        btree.insert(1, Record([200]))
        
        # Should have the updated value
        result = btree.search(1)
        assert result.get_value(0) == 200
    
    def test_insert_large_number_of_records(self, temp_db):
        btree = BTree(temp_db)
        
        # Insert many records to force splits
        for i in range(150):
            record = Record([i, f"data_{i}"])
            btree.insert(i, record)
        
        # Verify all can be found
        for i in range(150):
            result = btree.search(i)
            assert result is not None
            assert result.get_value(0) == i


class TestBTreeSearch:
    """Test searching in B-tree."""
    
    def test_search_empty_tree(self, temp_db):
        btree = BTree(temp_db)
        result = btree.search(1)
        assert result is None
    
    def test_search_existing_key(self, temp_db):
        btree = BTree(temp_db)
        btree.insert(42, Record(["found"]))
        
        result = btree.search(42)
        assert result is not None
        assert result.get_value(0) == "found"
    
    def test_search_non_existing_key(self, temp_db):
        btree = BTree(temp_db)
        btree.insert(1, Record(["data"]))
        
        result = btree.search(999)
        assert result is None
    
    def test_search_after_multiple_inserts(self, temp_db):
        btree = BTree(temp_db)
        
        for i in range(50):
            btree.insert(i * 2, Record([i]))
        
        # Search for existing keys
        result = btree.search(20)
        assert result is not None
        
        # Search for non-existing keys
        result = btree.search(21)
        assert result is None


class TestBTreeScan:
    """Test scanning B-tree."""
    
    def test_scan_empty_tree(self, temp_db):
        btree = BTree(temp_db)
        results = btree.scan()
        assert len(results) == 0
    
    def test_scan_single_record(self, temp_db):
        btree = BTree(temp_db)
        btree.insert(1, Record(["data"]))
        
        results = btree.scan()
        assert len(results) == 1
        assert results[0][0] == 1
        assert results[0][1].get_value(0) == "data"
    
    def test_scan_multiple_records_in_order(self, temp_db):
        btree = BTree(temp_db)
        
        # Insert in random order
        keys = [5, 2, 8, 1, 9, 3]
        for key in keys:
            btree.insert(key, Record([key]))
        
        # Scan should return in sorted order
        results = btree.scan()
        scanned_keys = [k for k, _ in results]
        assert scanned_keys == sorted(keys)
    
    def test_scan_after_many_inserts(self, temp_db):
        btree = BTree(temp_db)
        
        # Insert many records
        num_records = 100
        for i in range(num_records):
            btree.insert(i, Record([i]))
        
        # Scan all
        results = btree.scan()
        assert len(results) == num_records
        
        # Verify order
        for i, (key, record) in enumerate(results):
            assert key == i
            assert record.get_value(0) == i


class TestBTreeNode:
    """Test BTreeNode class."""
    
    def test_create_leaf_node(self, temp_db):
        btree = BTree(temp_db)
        
        # Root should be a leaf initially
        page_data = temp_db.read_page(btree.root_page)
        node = BTreeNode(btree.root_page, page_data, temp_db.get_page_size())
        
        assert node.is_leaf()
        assert not node.is_internal()
        assert node.num_keys == 0
    
    def test_node_after_insertion(self, temp_db):
        btree = BTree(temp_db)
        btree.insert(1, Record(["test"]))
        
        page_data = temp_db.read_page(btree.root_page)
        node = BTreeNode(btree.root_page, page_data, temp_db.get_page_size())
        
        assert node.num_keys == 1
    
    def test_find_key_index_empty_node(self, temp_db):
        btree = BTree(temp_db)
        page_data = temp_db.read_page(btree.root_page)
        node = BTreeNode(btree.root_page, page_data, temp_db.get_page_size())
        
        # Should return 0 for any key in empty node
        assert node.find_key_index(5) == 0
    
    def test_find_key_index_with_keys(self, temp_db):
        btree = BTree(temp_db)
        
        # Insert some keys
        for key in [10, 20, 30]:
            btree.insert(key, Record([key]))
        
        page_data = temp_db.read_page(btree.root_page)
        node = BTreeNode(btree.root_page, page_data, temp_db.get_page_size())
        
        # Test finding indices
        assert node.find_key_index(5) == 0  # Before first
        assert node.find_key_index(15) == 1  # Between first and second
        assert node.find_key_index(25) == 2  # Between second and third
        assert node.find_key_index(35) == 3  # After last


class TestBTreePersistence:
    """Test that B-tree data persists across sessions."""
    
    def test_persistence(self):
        fd, path = tempfile.mkstemp(suffix='.cdb')
        os.close(fd)
        
        try:
            # Create and populate tree
            pager1 = Pager(path)
            btree1 = BTree(pager1)
            root_page = btree1.root_page
            
            for i in range(20):
                btree1.insert(i, Record([i, f"value_{i}"]))
            
            pager1.close()
            
            # Reopen and verify
            pager2 = Pager(path)
            btree2 = BTree(pager2, root_page=root_page)
            
            for i in range(20):
                result = btree2.search(i)
                assert result is not None
                assert result.get_value(0) == i
                assert result.get_value(1) == f"value_{i}"
            
            pager2.close()
        
        finally:
            if os.path.exists(path):
                os.unlink(path)


class TestBTreeEdgeCases:
    """Test edge cases."""
    
    def test_insert_zero_key(self, temp_db):
        btree = BTree(temp_db)
        btree.insert(0, Record(["zero"]))
        
        result = btree.search(0)
        assert result is not None
        assert result.get_value(0) == "zero"
    
    def test_insert_large_key(self, temp_db):
        btree = BTree(temp_db)
        large_key = 1000000
        btree.insert(large_key, Record(["large"]))
        
        result = btree.search(large_key)
        assert result is not None
    
    def test_insert_record_with_many_fields(self, temp_db):
        btree = BTree(temp_db)
        values = list(range(50))
        record = Record(values)
        
        btree.insert(1, record)
        
        result = btree.search(1)
        assert result is not None
        assert len(result) == 50
    
    def test_insert_record_with_large_text(self, temp_db):
        btree = BTree(temp_db)
        large_text = "x" * 1000
        record = Record([large_text])
        
        btree.insert(1, record)
        
        result = btree.search(1)
        assert result is not None
        assert result.get_value(0) == large_text