"""
Record encoding and decoding.
Handles serialization of database rows to binary format.
"""

from typing import Any, List, Tuple, Optional
from enum import IntEnum
from chidb.util import (
    pack_uint8, unpack_uint8,
    pack_uint32, unpack_uint32,
    pack_varint, unpack_varint
)


class DataType(IntEnum):
    """Data types supported in records."""
    NULL = 0
    INTEGER = 1
    FLOAT = 2
    TEXT = 3
    BLOB = 4


class Record:
    """
    Represents a database record (row).
    
    A record consists of:
    - Header: contains column count and type information
    - Data: the actual column values
    
    Format:
    [header_size: varint][num_columns: varint][type1: varint][type2: varint]...[data1][data2]...
    """
    
    def __init__(self, values: List[Any]):
        """
        Create a record from a list of values.
        
        Args:
            values: List of column values (can be None, int, float, str, or bytes)
        """
        self.values = values
    
    def encode(self) -> bytes:
        """
        Encode the record to binary format.
        
        Returns:
            Binary representation of the record
        """
        num_columns = len(self.values)
        
        # Build header with type information
        header_parts = []
        header_parts.append(pack_varint(num_columns))
        
        # Encode type information for each column
        type_info = []
        for value in self.values:
            type_code = self._get_type_code(value)
            type_info.append(pack_varint(type_code))
        
        header_parts.extend(type_info)
        header = b''.join(header_parts)
        
        # Add header size at the beginning
        header_with_size = pack_varint(len(header)) + header
        
        # Encode the actual data
        data_parts = []
        for value in self.values:
            encoded_value = self._encode_value(value)
            data_parts.append(encoded_value)
        
        data = b''.join(data_parts)
        
        return header_with_size + data
    
    def _get_type_code(self, value: Any) -> int:
        """Get the type code for a value."""
        if value is None:
            return DataType.NULL
        elif isinstance(value, bool):
            # Bool is a subclass of int, handle it separately
            return DataType.INTEGER
        elif isinstance(value, int):
            return DataType.INTEGER
        elif isinstance(value, float):
            return DataType.FLOAT
        elif isinstance(value, str):
            return DataType.TEXT
        elif isinstance(value, bytes):
            return DataType.BLOB
        else:
            raise TypeError(f"Unsupported type: {type(value)}")
    
    def _encode_value(self, value: Any) -> bytes:
        """Encode a single value to bytes."""
        if value is None:
            return b''
        elif isinstance(value, bool):
            # Encode bool as 0 or 1
            return pack_uint8(1 if value else 0)
        elif isinstance(value, int):
            # Use varint encoding for integers
            if value < 0:
                # For negative numbers, use 4-byte signed encoding
                return pack_uint32(value & 0xFFFFFFFF)
            return pack_varint(value)
        elif isinstance(value, float):
            # Encode float as 8 bytes (double precision)
            import struct
            return struct.pack('>d', value)
        elif isinstance(value, str):
            # Encode text as UTF-8 with length prefix
            text_bytes = value.encode('utf-8')
            return pack_varint(len(text_bytes)) + text_bytes
        elif isinstance(value, bytes):
            # Encode blob with length prefix
            return pack_varint(len(value)) + value
        else:
            raise TypeError(f"Cannot encode type: {type(value)}")
    
    @staticmethod
    def decode(data: bytes) -> 'Record':
        """
        Decode a record from binary format.
        
        Args:
            data: Binary data to decode
            
        Returns:
            Decoded Record object
        """
        offset = 0
        
        # Read header size
        header_size, consumed = unpack_varint(data, offset)
        offset += consumed
        
        # Read number of columns
        num_columns, consumed = unpack_varint(data, offset)
        offset += consumed
        
        # Read type codes
        type_codes = []
        for _ in range(num_columns):
            type_code, consumed = unpack_varint(data, offset)
            offset += consumed
            type_codes.append(type_code)
        
        # Now we're at the data section
        # offset should be at header_size + size_of_header_size_varint
        
        values = []
        for type_code in type_codes:
            value, consumed = Record._decode_value(data, offset, type_code)
            values.append(value)
            offset += consumed
        
        return Record(values)
    
    @staticmethod
    def _decode_value(data: bytes, offset: int, type_code: int) -> Tuple[Any, int]:
        """
        Decode a single value from bytes.
        
        Returns:
            Tuple of (value, bytes_consumed)
        """
        if type_code == DataType.NULL:
            return None, 0
        
        elif type_code == DataType.INTEGER:
            # Try varint first for positive numbers
            try:
                value, consumed = unpack_varint(data, offset)
                return value, consumed
            except:
                # If varint fails, try 4-byte encoding
                value = unpack_uint32(data, offset)
                # Convert back from unsigned to signed if needed
                if value > 0x7FFFFFFF:
                    value = value - 0x100000000
                return value, 4
        
        elif type_code == DataType.FLOAT:
            import struct
            value = struct.unpack_from('>d', data, offset)[0]
            return value, 8
        
        elif type_code == DataType.TEXT:
            length, consumed = unpack_varint(data, offset)
            offset += consumed
            text_bytes = data[offset:offset + length]
            text = text_bytes.decode('utf-8')
            return text, consumed + length
        
        elif type_code == DataType.BLOB:
            length, consumed = unpack_varint(data, offset)
            offset += consumed
            blob = bytes(data[offset:offset + length])
            return blob, consumed + length
        
        else:
            raise ValueError(f"Unknown type code: {type_code}")
    
    def get_values(self) -> List[Any]:
        """Get the list of values in this record."""
        return self.values
    
    def get_value(self, index: int) -> Any:
        """Get a specific value by column index."""
        if index < 0 or index >= len(self.values):
            raise IndexError(f"Column index {index} out of range")
        return self.values[index]
    
    def __len__(self) -> int:
        """Get the number of columns."""
        return len(self.values)
    
    def __repr__(self) -> str:
        return f"Record({self.values})"
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, Record):
            return False
        return self.values == other.values


def calculate_record_size(values: List[Any]) -> int:
    """
    Calculate the size in bytes of a record without encoding it.
    Useful for space estimation.
    
    Args:
        values: List of values that would go into the record
        
    Returns:
        Estimated size in bytes
    """
    record = Record(values)
    encoded = record.encode()
    return len(encoded)