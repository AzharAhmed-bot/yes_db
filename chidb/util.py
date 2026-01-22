"""
Utility functions for binary encoding/decoding and data manipulation.
This module provides low-level helpers used throughout the database.
"""

import struct
from typing import Tuple


def pack_uint8(value: int) -> bytes:
    """Pack an unsigned 8-bit integer into bytes."""
    return struct.pack('>B', value)


def unpack_uint8(data: bytes, offset: int = 0) -> int:
    """Unpack an unsigned 8-bit integer from bytes."""
    return struct.unpack_from('>B', data, offset)[0]


def pack_uint16(value: int) -> bytes:
    """Pack an unsigned 16-bit integer into bytes (big-endian)."""
    return struct.pack('>H', value)


def unpack_uint16(data: bytes, offset: int = 0) -> int:
    """Unpack an unsigned 16-bit integer from bytes (big-endian)."""
    return struct.unpack_from('>H', data, offset)[0]


def pack_uint32(value: int) -> bytes:
    """Pack an unsigned 32-bit integer into bytes (big-endian)."""
    return struct.pack('>I', value)


def unpack_uint32(data: bytes, offset: int = 0) -> int:
    """Unpack an unsigned 32-bit integer from bytes (big-endian)."""
    return struct.unpack_from('>I', data, offset)[0]


def pack_uint64(value: int) -> bytes:
    """Pack an unsigned 64-bit integer into bytes (big-endian)."""
    return struct.pack('>Q', value)


def unpack_uint64(data: bytes, offset: int = 0) -> int:
    """Unpack an unsigned 64-bit integer from bytes (big-endian)."""
    return struct.unpack_from('>Q', data, offset)[0]


def pack_varint(value: int) -> bytes:
    """
    Pack an integer as a variable-length integer.
    Uses a compact encoding where smaller numbers take fewer bytes.
    
    Format: Each byte stores 7 bits of data + 1 continuation bit.
    If the high bit is set, more bytes follow.
    """
    if value < 0:
        raise ValueError("Varint encoding only supports non-negative integers")
    
    result = bytearray()
    
    while value >= 0x80:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    
    result.append(value & 0x7F)
    return bytes(result)


def unpack_varint(data: bytes, offset: int = 0) -> Tuple[int, int]:
    """
    Unpack a variable-length integer from bytes.
    
    Returns:
        Tuple of (value, bytes_consumed)
    """
    value = 0
    shift = 0
    pos = offset
    
    while pos < len(data):
        byte = data[pos]
        value |= (byte & 0x7F) << shift
        pos += 1
        
        if (byte & 0x80) == 0:
            return value, pos - offset
        
        shift += 7
    
    raise ValueError("Incomplete varint encoding")


def bytes_required_varint(value: int) -> int:
    """Calculate how many bytes are needed to encode a varint."""
    if value < 0:
        raise ValueError("Varint encoding only supports non-negative integers")
    
    if value == 0:
        return 1
    
    count = 0
    while value > 0:
        count += 1
        value >>= 7
    
    return count


def align_to(value: int, alignment: int) -> int:
    """Align a value up to the nearest multiple of alignment."""
    remainder = value % alignment
    if remainder == 0:
        return value
    return value + (alignment - remainder)


def assert_valid_page_id(page_id: int, max_page: int = None) -> None:
    """Assert that a page ID is valid."""
    if page_id < 0:
        raise ValueError(f"Page ID must be non-negative, got {page_id}")
    if max_page is not None and page_id > max_page:
        raise ValueError(f"Page ID {page_id} exceeds maximum {max_page}")


def bytes_to_hex(data: bytes, max_bytes: int = 16) -> str:
    """Convert bytes to a readable hex string (for debugging)."""
    if len(data) > max_bytes:
        return data[:max_bytes].hex() + f"... ({len(data)} bytes total)"
    return data.hex()