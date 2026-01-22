"""
Tests for chidb/util.py
"""

import pytest
from chidb.util import (
    pack_uint8, unpack_uint8,
    pack_uint16, unpack_uint16,
    pack_uint32, unpack_uint32,
    pack_uint64, unpack_uint64,
    pack_varint, unpack_varint, bytes_required_varint,
    align_to, assert_valid_page_id, bytes_to_hex
)


class TestIntegerPacking:
    """Test fixed-size integer packing/unpacking."""
    
    def test_uint8(self):
        assert pack_uint8(0) == b'\x00'
        assert pack_uint8(255) == b'\xff'
        assert pack_uint8(127) == b'\x7f'
        
        assert unpack_uint8(b'\x00') == 0
        assert unpack_uint8(b'\xff') == 255
        assert unpack_uint8(b'\x7f') == 127
    
    def test_uint16(self):
        assert pack_uint16(0) == b'\x00\x00'
        assert pack_uint16(256) == b'\x01\x00'
        assert pack_uint16(65535) == b'\xff\xff'
        
        assert unpack_uint16(b'\x00\x00') == 0
        assert unpack_uint16(b'\x01\x00') == 256
        assert unpack_uint16(b'\xff\xff') == 65535
    
    def test_uint32(self):
        assert pack_uint32(0) == b'\x00\x00\x00\x00'
        assert pack_uint32(16777216) == b'\x01\x00\x00\x00'
        assert pack_uint32(4294967295) == b'\xff\xff\xff\xff'
        
        assert unpack_uint32(b'\x00\x00\x00\x00') == 0
        assert unpack_uint32(b'\x01\x00\x00\x00') == 16777216
        assert unpack_uint32(b'\xff\xff\xff\xff') == 4294967295
    
    def test_uint64(self):
        assert pack_uint64(0) == b'\x00' * 8
        assert pack_uint64(72057594037927936) == b'\x01' + b'\x00' * 7
        
        assert unpack_uint64(b'\x00' * 8) == 0
        assert unpack_uint64(b'\x01' + b'\x00' * 7) == 72057594037927936
    
    def test_unpack_with_offset(self):
        data = b'\xff\xff\x12\x34\xff\xff'
        assert unpack_uint16(data, 2) == 0x1234
        assert unpack_uint8(data, 0) == 255
        assert unpack_uint8(data, 5) == 255


class TestVarint:
    """Test variable-length integer encoding."""
    
    def test_small_values(self):
        # Values < 128 should encode to 1 byte
        assert pack_varint(0) == b'\x00'
        assert pack_varint(1) == b'\x01'
        assert pack_varint(127) == b'\x7f'
    
    def test_medium_values(self):
        # Values >= 128 need multiple bytes
        assert pack_varint(128) == b'\x80\x01'
        assert pack_varint(255) == b'\xff\x01'
        assert pack_varint(300) == b'\xac\x02'
    
    def test_large_values(self):
        assert pack_varint(16384) == b'\x80\x80\x01'
        assert pack_varint(1000000) == b'\xc0\x84\x3d'
    
    def test_unpack_varint(self):
        assert unpack_varint(b'\x00') == (0, 1)
        assert unpack_varint(b'\x7f') == (127, 1)
        assert unpack_varint(b'\x80\x01') == (128, 2)
        assert unpack_varint(b'\xff\x01') == (255, 2)
        assert unpack_varint(b'\xac\x02') == (300, 2)
        assert unpack_varint(b'\xc0\x84\x3d') == (1000000, 3)
    
    def test_unpack_with_offset(self):
        data = b'\xff\xff\x80\x01\xff'
        value, consumed = unpack_varint(data, 2)
        assert value == 128
        assert consumed == 2
    
    def test_varint_roundtrip(self):
        for value in [0, 1, 127, 128, 255, 1000, 10000, 1000000]:
            packed = pack_varint(value)
            unpacked, _ = unpack_varint(packed)
            assert unpacked == value
    
    def test_bytes_required(self):
        assert bytes_required_varint(0) == 1
        assert bytes_required_varint(127) == 1
        assert bytes_required_varint(128) == 2
        assert bytes_required_varint(16383) == 2
        assert bytes_required_varint(16384) == 3
    
    def test_negative_varint_raises(self):
        with pytest.raises(ValueError):
            pack_varint(-1)
        
        with pytest.raises(ValueError):
            bytes_required_varint(-1)
    
    def test_incomplete_varint_raises(self):
        # A varint that indicates continuation but has no more bytes
        with pytest.raises(ValueError):
            unpack_varint(b'\x80')


class TestHelperFunctions:
    """Test utility helper functions."""
    
    def test_align_to(self):
        assert align_to(0, 4) == 0
        assert align_to(1, 4) == 4
        assert align_to(4, 4) == 4
        assert align_to(5, 4) == 8
        assert align_to(100, 16) == 112
        assert align_to(128, 16) == 128
    
    def test_assert_valid_page_id(self):
        # Should not raise
        assert_valid_page_id(0)
        assert_valid_page_id(100)
        assert_valid_page_id(5, max_page=10)
        
        # Should raise
        with pytest.raises(ValueError):
            assert_valid_page_id(-1)
        
        with pytest.raises(ValueError):
            assert_valid_page_id(15, max_page=10)
    
    def test_bytes_to_hex(self):
        assert bytes_to_hex(b'\x00\x01\x02') == '000102'
        assert bytes_to_hex(b'\xff\xfe\xfd') == 'fffefd'
        
        # Test truncation
        long_data = b'\x00' * 20
        result = bytes_to_hex(long_data, max_bytes=8)
        assert '0000000000000000' in result
        assert '20 bytes' in result