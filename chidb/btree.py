"""
B-Tree implementation for table and index storage.
Provides persistent ordered key-value storage using B-tree data structure.
"""

from typing import Any, List, Optional, Tuple
from chidb.pager import Pager
from chidb.record import Record
from chidb.util import (
    pack_uint8, unpack_uint8,
    pack_uint16, unpack_uint16,
    pack_uint32, unpack_uint32,
    pack_varint, unpack_varint
)
from chidb.log import get_logger, log_btree_insert, log_btree_search, log_btree_split


# Node type constants
NODE_TYPE_INTERNAL = 1
NODE_TYPE_LEAF = 2

# Node header structure (at start of each page)
# [node_type: 1 byte][num_keys: 2 bytes][right_page: 4 bytes (internal only)]
NODE_HEADER_SIZE = 7


class BTreeNode:
    """
    Represents a B-tree node stored in a page.
    
    Node structure:
    - Header: node type, number of keys, rightmost child pointer (for internal nodes)
    - Cell pointer array: offsets to cells
    - Cells: key-value pairs (or key-child pairs for internal nodes)
    """
    
    def __init__(self, page_id: int, page_data: bytearray, page_size: int):
        """
        Initialize a B-tree node from page data.
        
        Args:
            page_id: The page ID of this node
            page_data: The page data
            page_size: Size of the page
        """
        self.page_id = page_id
        self.page_data = page_data
        self.page_size = page_size
        self.is_dirty = False
        
        # Parse header
        self.node_type = unpack_uint8(page_data, 0)
        self.num_keys = unpack_uint16(page_data, 1)
        
        if self.node_type == NODE_TYPE_INTERNAL:
            self.right_page = unpack_uint32(page_data, 3)
        else:
            self.right_page = None
    
    def is_leaf(self) -> bool:
        """Check if this is a leaf node."""
        return self.node_type == NODE_TYPE_LEAF
    
    def is_internal(self) -> bool:
        """Check if this is an internal node."""
        return self.node_type == NODE_TYPE_INTERNAL
    
    def get_cell_offset(self, index: int) -> int:
        """Get the offset of a cell in the page."""
        if index < 0 or index >= self.num_keys:
            raise IndexError(f"Cell index {index} out of range (0-{self.num_keys-1})")
        
        # Cell pointer array starts after header
        pointer_offset = NODE_HEADER_SIZE + (index * 2)
        return unpack_uint16(self.page_data, pointer_offset)
    
    def read_cell(self, index: int) -> Tuple[int, Any]:
        """
        Read a cell from the node.
        
        Returns:
            For leaf nodes: (key, record_data)
            For internal nodes: (key, child_page_id)
        """
        offset = self.get_cell_offset(index)
        
        # Read key
        key, consumed = unpack_varint(self.page_data, offset)
        offset += consumed
        
        if self.is_leaf():
            # Read record data length
            data_len, consumed = unpack_varint(self.page_data, offset)
            offset += consumed
            
            # Read record data
            record_data = bytes(self.page_data[offset:offset + data_len])
            return key, record_data
        else:
            # Read child page ID
            child_page = unpack_uint32(self.page_data, offset)
            return key, child_page
    
    def find_key_index(self, key: int) -> int:
        """
        Find the index where a key should be inserted or is located.
        Uses binary search.
        
        Returns:
            Index where key is or should be inserted
        """
        left, right = 0, self.num_keys
        
        while left < right:
            mid = (left + right) // 2
            mid_key, _ = self.read_cell(mid)
            
            if mid_key < key:
                left = mid + 1
            else:
                right = mid
        
        return left


class BTree:
    """
    B-Tree for storing key-record pairs.
    
    The B-tree provides:
    - Ordered storage by key
    - Efficient insertion and lookup
    - Range scans via cursor
    """
    
    def __init__(self, pager: Pager, root_page: Optional[int] = None):
        """
        Initialize a B-tree.
        
        Args:
            pager: The pager for disk I/O
            root_page: Page ID of root node (None to create new tree)
        """
        self.pager = pager
        self.logger = get_logger("btree")
        
        if root_page is None:
            # Create a new B-tree with a root leaf node
            self.root_page = self._create_leaf_node()
        else:
            self.root_page = root_page
    
    def _create_leaf_node(self) -> int:
        """
        Create a new leaf node.
        
        Returns:
            Page ID of the new node
        """
        page_id = self.pager.allocate_page()
        page_data = self.pager.read_page(page_id)
        
        # Initialize header
        page_data[0] = NODE_TYPE_LEAF
        page_data[1:3] = pack_uint16(0)  # num_keys = 0
        
        self.pager.write_page(page_id, bytes(page_data))
        return page_id
    
    def _create_internal_node(self) -> int:
        """
        Create a new internal node.
        
        Returns:
            Page ID of the new node
        """
        page_id = self.pager.allocate_page()
        page_data = self.pager.read_page(page_id)
        
        # Initialize header
        page_data[0] = NODE_TYPE_INTERNAL
        page_data[1:3] = pack_uint16(0)  # num_keys = 0
        page_data[3:7] = pack_uint32(0)  # right_page = 0
        
        self.pager.write_page(page_id, bytes(page_data))
        return page_id
    
    def _create_new_root(self, split_key: int, left_page: int, right_page: int) -> None:
        """
        Create a new root after the old root was split.
        
        Args:
            split_key: The key that separates left and right
            left_page: The old root (now left child)
            right_page: The new page (now right child)
        """
        new_root_id = self._create_internal_node()
        
        # Load the new root
        page_data = self.pager.read_page(new_root_id)
        new_root = BTreeNode(new_root_id, page_data, self.pager.get_page_size())
        
        # In our B-tree structure:
        # - The cell at index 0 contains (split_key, left_page)
        # - The right_page pointer contains right_page
        # This means: keys < split_key go to left_page, keys >= split_key go to right_page
        
        # Set the right pointer
        new_root.page_data[3:7] = pack_uint32(right_page)
        new_root.right_page = right_page
        
        # Insert the split key with left child pointer
        self._insert_internal_cell(new_root, 0, split_key, left_page)
        
        # Update root page
        self.root_page = new_root_id
        self.logger.info(f"Created new root at page {new_root_id}")
    
    def insert(self, key: int, record: Record) -> None:
        """
        Insert a key-record pair into the B-tree.
        
        Args:
            key: The key to insert
            record: The record to store
        """
        log_btree_insert(key)
        
        record_data = record.encode()
        split_result = self._insert_recursive(self.root_page, key, record_data)
        
        # If root was split, create a new root
        if split_result is not None:
            split_key, new_page = split_result
            self._create_new_root(split_key, self.root_page, new_page)
    
    def _insert_recursive(self, page_id: int, key: int, record_data: bytes) -> Optional[Tuple[int, int]]:
        """
        Recursively insert into the B-tree.
        
        Returns:
            None if no split occurred
            (split_key, new_page_id) if node was split
        """
        page_data = self.pager.read_page(page_id)
        node = BTreeNode(page_id, page_data, self.pager.get_page_size())
        
        if node.is_leaf():
            return self._insert_into_leaf(node, key, record_data)
        else:
            return self._insert_into_internal(node, key, record_data)
    
    def _insert_into_leaf(self, node: BTreeNode, key: int, record_data: bytes) -> Optional[Tuple[int, int]]:
        """
        Insert into a leaf node.
        
        Returns:
            None if no split, or (split_key, new_page_id) if split
        """
        # Find insertion point
        insert_idx = node.find_key_index(key)
        
        # Check if key already exists
        if insert_idx < node.num_keys:
            existing_key, _ = node.read_cell(insert_idx)
            if existing_key == key:
                # Update existing record
                self._update_leaf_cell(node, insert_idx, key, record_data)
                return None
        
        # Calculate cell size
        cell_size = len(pack_varint(key)) + len(pack_varint(len(record_data))) + len(record_data)
        
        # Check if we need to split
        if self._needs_split(node, cell_size):
            return self._split_leaf(node, key, record_data, insert_idx)
        
        # Insert into node
        self._insert_leaf_cell(node, insert_idx, key, record_data)
        return None
    
    def _insert_into_internal(self, node: BTreeNode, key: int, record_data: bytes) -> Optional[Tuple[int, int]]:
        """
        Insert into an internal node.
        
        Returns:
            None if no split, or (split_key, new_page_id) if split
        """
        # Find which child to descend to
        insert_idx = node.find_key_index(key)
        
        if insert_idx < node.num_keys:
            _, child_page = node.read_cell(insert_idx)
        else:
            child_page = node.right_page
        
        # Recursively insert into child
        split_result = self._insert_recursive(child_page, key, record_data)
        
        if split_result is None:
            return None
        
        # Child was split, insert the split key into this node
        split_key, new_page = split_result
        return self._insert_split_into_internal(node, split_key, new_page)
    
    def _insert_leaf_cell(self, node: BTreeNode, index: int, key: int, record_data: bytes) -> None:
        """Insert a cell into a leaf node at the specified index."""
        # Encode the cell
        cell_data = pack_varint(key) + pack_varint(len(record_data)) + record_data
        
        # Find free space at end of page
        free_offset = self._find_free_space(node)
        
        # Write cell data
        cell_offset = free_offset - len(cell_data)
        node.page_data[cell_offset:cell_offset + len(cell_data)] = cell_data
        
        # Shift cell pointers to make room
        pointer_start = NODE_HEADER_SIZE
        for i in range(node.num_keys, index, -1):
            old_offset = unpack_uint16(node.page_data, pointer_start + (i - 1) * 2)
            node.page_data[pointer_start + i * 2:pointer_start + i * 2 + 2] = pack_uint16(old_offset)
        
        # Write new cell pointer
        node.page_data[pointer_start + index * 2:pointer_start + index * 2 + 2] = pack_uint16(cell_offset)
        
        # Update num_keys
        node.num_keys += 1
        node.page_data[1:3] = pack_uint16(node.num_keys)
        
        # Write back to pager
        self.pager.write_page(node.page_id, bytes(node.page_data))
    
    def _update_leaf_cell(self, node: BTreeNode, index: int, key: int, record_data: bytes) -> None:
        """Update an existing cell in a leaf node."""
        # For simplicity, we'll delete and re-insert
        # In a production system, you'd check if the new record fits in the same space
        self._delete_cell(node, index)
        self._insert_leaf_cell(node, index, key, record_data)
    
    def _delete_cell(self, node: BTreeNode, index: int) -> None:
        """Delete a cell from a node."""
        # Shift cell pointers
        pointer_start = NODE_HEADER_SIZE
        for i in range(index, node.num_keys - 1):
            offset = unpack_uint16(node.page_data, pointer_start + (i + 1) * 2)
            node.page_data[pointer_start + i * 2:pointer_start + i * 2 + 2] = pack_uint16(offset)
        
        # Update num_keys
        node.num_keys -= 1
        node.page_data[1:3] = pack_uint16(node.num_keys)
    
    def _needs_split(self, node: BTreeNode, new_cell_size: int) -> bool:
        """Check if a node needs to be split before inserting a new cell."""
        # Simple heuristic: split if more than 75% full or too many keys
        used_space = self._calculate_used_space(node)
        return used_space + new_cell_size > (self.pager.get_page_size() * 3 // 4) or node.num_keys >= 100
    
    def _calculate_used_space(self, node: BTreeNode) -> int:
        """Calculate how much space is used in a node."""
        # Header + cell pointer array + cells
        return NODE_HEADER_SIZE + (node.num_keys * 2) + (self.pager.get_page_size() - self._find_free_space(node))
    
    def _find_free_space(self, node: BTreeNode) -> int:
        """Find the offset where free space begins (working backwards from end)."""
        if node.num_keys == 0:
            return self.pager.get_page_size()
        
        # Find the minimum cell offset
        min_offset = self.pager.get_page_size()
        for i in range(node.num_keys):
            offset = self.get_cell_offset(node, i)
            if offset < min_offset:
                min_offset = offset
        
        return min_offset
    
    def get_cell_offset(self, node: BTreeNode, index: int) -> int:
        """Get cell offset (wrapper for node method)."""
        return node.get_cell_offset(index)
    
    def _split_leaf(self, node: BTreeNode, key: int, record_data: bytes, insert_idx: int) -> Tuple[int, int]:
        """
        Split a leaf node.
        
        Returns:
            (split_key, new_page_id)
        """
        log_btree_split(node.page_id)
        
        # Create new node
        new_page_id = self._create_leaf_node()
        
        # Collect all keys and data including the new one
        all_cells = []
        for i in range(node.num_keys):
            k, data = node.read_cell(i)
            all_cells.append((k, data))
        
        all_cells.insert(insert_idx, (key, record_data))
        
        # Split point (middle)
        split_point = len(all_cells) // 2
        
        # Clear original node
        node.num_keys = 0
        node.page_data[1:3] = pack_uint16(0)
        
        # Insert first half into original node
        for i in range(split_point):
            k, data = all_cells[i]
            self._insert_leaf_cell(node, i, k, data)
        
        # Load new node
        new_page_data = self.pager.read_page(new_page_id)
        new_node = BTreeNode(new_page_id, new_page_data, self.pager.get_page_size())
        
        # Insert second half into new node
        for i in range(split_point, len(all_cells)):
            k, data = all_cells[i]
            self._insert_leaf_cell(new_node, i - split_point, k, data)
        
        # Return the first key of the new node as split key
        split_key, _ = all_cells[split_point]
        return split_key, new_page_id
    
    def _insert_split_into_internal(self, node: BTreeNode, split_key: int, new_page: int) -> Optional[Tuple[int, int]]:
        """Insert a split result into an internal node."""
        # Find insertion point
        insert_idx = node.find_key_index(split_key)
        
        # Create cell
        cell_data = pack_varint(split_key) + pack_uint32(new_page)
        
        # Check if we need to split
        if self._needs_split(node, len(cell_data)):
            return self._split_internal(node, split_key, new_page, insert_idx)
        
        # Insert the cell
        self._insert_internal_cell(node, insert_idx, split_key, new_page)
        return None
    
    def _insert_internal_cell(self, node: BTreeNode, index: int, key: int, child_page: int) -> None:
        """Insert a cell into an internal node."""
        cell_data = pack_varint(key) + pack_uint32(child_page)
        
        free_offset = self._find_free_space(node)
        cell_offset = free_offset - len(cell_data)
        node.page_data[cell_offset:cell_offset + len(cell_data)] = cell_data
        
        # Shift cell pointers
        pointer_start = NODE_HEADER_SIZE
        for i in range(node.num_keys, index, -1):
            old_offset = unpack_uint16(node.page_data, pointer_start + (i - 1) * 2)
            node.page_data[pointer_start + i * 2:pointer_start + i * 2 + 2] = pack_uint16(old_offset)
        
        # Write new cell pointer
        node.page_data[pointer_start + index * 2:pointer_start + index * 2 + 2] = pack_uint16(cell_offset)
        
        # Update num_keys
        node.num_keys += 1
        node.page_data[1:3] = pack_uint16(node.num_keys)
        
        self.pager.write_page(node.page_id, bytes(node.page_data))
    
    def _split_internal(self, node: BTreeNode, key: int, child_page: int, insert_idx: int) -> Tuple[int, int]:
        """Split an internal node."""
        log_btree_split(node.page_id)
        
        new_page_id = self._create_internal_node()
        
        # Collect all cells
        all_cells = []
        for i in range(node.num_keys):
            k, child = node.read_cell(i)
            all_cells.append((k, child))
        
        all_cells.insert(insert_idx, (key, child_page))
        
        split_point = len(all_cells) // 2
        
        # Clear and rebuild nodes
        node.num_keys = 0
        node.page_data[1:3] = pack_uint16(0)
        
        for i in range(split_point):
            k, child = all_cells[i]
            self._insert_internal_cell(node, i, k, child)
        
        new_page_data = self.pager.read_page(new_page_id)
        new_node = BTreeNode(new_page_id, new_page_data, self.pager.get_page_size())
        
        for i in range(split_point + 1, len(all_cells)):
            k, child = all_cells[i]
            self._insert_internal_cell(new_node, i - split_point - 1, k, child)
        
        split_key, _ = all_cells[split_point]
        return split_key, new_page_id
    
    def search(self, key: int) -> Optional[Record]:
        """
        Search for a key in the B-tree.
        
        Args:
            key: The key to search for
            
        Returns:
            The record if found, None otherwise
        """
        log_btree_search(key)
        return self._search_recursive(self.root_page, key)
    
    def _search_recursive(self, page_id: int, key: int) -> Optional[Record]:
        """Recursively search for a key."""
        page_data = self.pager.read_page(page_id)
        node = BTreeNode(page_id, page_data, self.pager.get_page_size())
        
        if node.is_leaf():
            # Find the key in leaf node
            idx = node.find_key_index(key)
            if idx < node.num_keys:
                found_key, record_data = node.read_cell(idx)
                if found_key == key:
                    return Record.decode(record_data)
            return None
        else:
            # Internal node - find which child to descend to
            idx = node.find_key_index(key)
            
            # If idx < num_keys, the key at idx is >= our search key
            # So we should go to the child pointer at idx (which points left)
            if idx < node.num_keys:
                _, child_page = node.read_cell(idx)
                return self._search_recursive(child_page, key)
            else:
                # Key is greater than all keys in this node
                # Go to the rightmost child
                if node.right_page and node.right_page > 0:
                    return self._search_recursive(node.right_page, key)
                return None
    
    def scan(self) -> List[Tuple[int, Record]]:
        """
        Scan all key-record pairs in order.
        
        Returns:
            List of (key, record) tuples in ascending key order
        """
        results = []
        self._scan_recursive(self.root_page, results)
        return results
    
    def _scan_recursive(self, page_id: int, results: List[Tuple[int, Record]]) -> None:
        """Recursively scan the tree."""
        page_data = self.pager.read_page(page_id)
        node = BTreeNode(page_id, page_data, self.pager.get_page_size())
        
        if node.is_leaf():
            # Add all records from this leaf
            for i in range(node.num_keys):
                key, record_data = node.read_cell(i)
                record = Record.decode(record_data)
                results.append((key, record))
        else:
            # Internal node - scan children in order
            for i in range(node.num_keys):
                key, child_page = node.read_cell(i)
                self._scan_recursive(child_page, results)
            
            # Don't forget the rightmost child
            if node.right_page:
                self._scan_recursive(node.right_page, results)
    
    def get_root_page(self) -> int:
        """Get the root page ID."""
        return self.root_page
    
    def delete(self, key: int) -> bool:
        """
        Delete a key-record pair from the B-tree.
        
        Args:
            key: The key to delete
            
        Returns:
            True if deleted, False if key not found
        """
        return self._delete_recursive(self.root_page, key)
    
    def _delete_recursive(self, page_id: int, key: int) -> bool:
        """
        Recursively delete a key from the B-tree.
        
        Returns:
            True if key was found and deleted
        """
        page_data = self.pager.read_page(page_id)
        node = BTreeNode(page_id, page_data, self.pager.get_page_size())
        
        if node.is_leaf():
            # Find the key
            idx = node.find_key_index(key)
            
            if idx < node.num_keys:
                found_key, _ = node.read_cell(idx)
                if found_key == key:
                    # Delete this cell
                    self._delete_cell(node, idx)
                    self.pager.write_page(node.page_id, bytes(node.page_data))
                    return True
            
            return False
        else:
            # Internal node - find which child to descend to
            idx = node.find_key_index(key)
            
            if idx < node.num_keys:
                _, child_page = node.read_cell(idx)
                return self._delete_recursive(child_page, key)
            else:
                if node.right_page and node.right_page > 0:
                    return self._delete_recursive(node.right_page, key)
                return False
    
    def update(self, key: int, record: Record) -> bool:
        """
        Update a record in the B-tree.
        
        Args:
            key: The key to update
            record: The new record data
            
        Returns:
            True if updated, False if key not found
        """
        # Simple implementation: delete then insert
        if self._delete_recursive(self.root_page, key):
            self.insert(key, record)
            return True
        return False