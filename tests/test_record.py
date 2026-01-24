"""
Tests for chidb/record.py
"""

import pytest
from chidb.record import Record, DataType, calculate_record_size


class TestRecordBasics:
    """Test basic record creation and access."""
    
    def test_create_empty_record(self):
        record = Record([])
        assert len(record) == 0
        assert record.get_values() == []
    
    def test_create_single_value_record(self):
        record = Record([42])
        assert len(record) == 1
        assert record.get_value(0) == 42
    
    def test_create_multi_value_record(self):
        record = Record([1, "hello", 3.14])
        assert len(record) == 3
        assert record.get_value(0) == 1
        assert record.get_value(1) == "hello"
        assert record.get_value(2) == 3.14
    
    def test_get_value_out_of_range(self):
        record = Record([1, 2, 3])
        
        with pytest.raises(IndexError):
            record.get_value(5)
        
        with pytest.raises(IndexError):
            record.get_value(-1)
    
    def test_record_equality(self):
        record1 = Record([1, 2, 3])
        record2 = Record([1, 2, 3])
        record3 = Record([1, 2, 4])
        
        assert record1 == record2
        assert record1 != record3
    
    def test_record_repr(self):
        record = Record([1, "test"])
        assert "Record" in repr(record)
        assert "1" in repr(record)
        assert "test" in repr(record)


class TestRecordEncoding:
    """Test encoding various data types."""
    
    def test_encode_null(self):
        record = Record([None])
        encoded = record.encode()
        assert len(encoded) > 0
    
    def test_encode_integer(self):
        record = Record([42])
        encoded = record.encode()
        assert len(encoded) > 0
    
    def test_encode_negative_integer(self):
        record = Record([-42])
        encoded = record.encode()
        assert len(encoded) > 0
    
    def test_encode_float(self):
        record = Record([3.14159])
        encoded = record.encode()
        assert len(encoded) > 0
    
    def test_encode_text(self):
        record = Record(["hello world"])
        encoded = record.encode()
        assert len(encoded) > 0
    
    def test_encode_empty_text(self):
        record = Record([""])
        encoded = record.encode()
        assert len(encoded) > 0
    
    def test_encode_blob(self):
        record = Record([b'\x00\x01\x02\xff'])
        encoded = record.encode()
        assert len(encoded) > 0
    
    def test_encode_boolean(self):
        record = Record([True, False])
        encoded = record.encode()
        assert len(encoded) > 0
    
    def test_encode_mixed_types(self):
        record = Record([42, "test", 3.14, None, b'blob'])
        encoded = record.encode()
        assert len(encoded) > 0
    
    def test_encode_large_integer(self):
        record = Record([1000000])
        encoded = record.encode()
        assert len(encoded) > 0
    
    def test_encode_unicode_text(self):
        record = Record(["Hello 世界 🌍"])
        encoded = record.encode()
        assert len(encoded) > 0
    
    def test_encode_unsupported_type(self):
        with pytest.raises(TypeError):
            record = Record([{'key': 'value'}])
            record.encode()


class TestRecordDecoding:
    """Test decoding records."""
    
    def test_decode_null(self):
        original = Record([None])
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        assert decoded == original
        assert decoded.get_value(0) is None
    
    def test_decode_integer(self):
        original = Record([42])
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        assert decoded == original
        assert decoded.get_value(0) == 42
    
    def test_decode_negative_integer(self):
        original = Record([-42])
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        assert decoded.get_value(0) == -42
    
    def test_decode_float(self):
        original = Record([3.14159])
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        assert abs(decoded.get_value(0) - 3.14159) < 0.00001
    
    def test_decode_text(self):
        original = Record(["hello world"])
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        assert decoded == original
        assert decoded.get_value(0) == "hello world"
    
    def test_decode_empty_text(self):
        original = Record([""])
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        assert decoded.get_value(0) == ""
    
    def test_decode_blob(self):
        original = Record([b'\x00\x01\x02\xff'])
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        assert decoded == original
        assert decoded.get_value(0) == b'\x00\x01\x02\xff'
    
    def test_decode_boolean(self):
        original = Record([True, False])
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        # Booleans are encoded as integers
        assert decoded.get_value(0) in [0, 1]
        assert decoded.get_value(1) in [0, 1]
    
    def test_decode_mixed_types(self):
        original = Record([42, "test", 3.14, None, b'data'])
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        assert decoded.get_value(0) == 42
        assert decoded.get_value(1) == "test"
        assert abs(decoded.get_value(2) - 3.14) < 0.01
        assert decoded.get_value(3) is None
        assert decoded.get_value(4) == b'data'
    
    def test_decode_unicode_text(self):
        original = Record(["Hello 世界 🌍"])
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        assert decoded.get_value(0) == "Hello 世界 🌍"
    
    def test_decode_multiple_integers(self):
        original = Record([1, 2, 3, 4, 5])
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        assert decoded.get_values() == [1, 2, 3, 4, 5]
    
    def test_decode_empty_record(self):
        original = Record([])
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        assert len(decoded) == 0


class TestRecordRoundtrip:
    """Test encoding and decoding round-trip."""
    
    def test_roundtrip_simple(self):
        values = [1, "hello", 3.14]
        original = Record(values)
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        assert len(decoded) == len(original)
        assert decoded.get_value(0) == 1
        assert decoded.get_value(1) == "hello"
        assert abs(decoded.get_value(2) - 3.14) < 0.01
    
    def test_roundtrip_with_nulls(self):
        values = [None, 42, None, "test", None]
        original = Record(values)
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        assert decoded.get_value(0) is None
        assert decoded.get_value(1) == 42
        assert decoded.get_value(2) is None
        assert decoded.get_value(3) == "test"
        assert decoded.get_value(4) is None
    
    def test_roundtrip_large_text(self):
        large_text = "x" * 10000
        original = Record([large_text])
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        assert decoded.get_value(0) == large_text
    
    def test_roundtrip_large_blob(self):
        large_blob = b'\xff' * 10000
        original = Record([large_blob])
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        assert decoded.get_value(0) == large_blob
    
    def test_roundtrip_many_columns(self):
        values = list(range(100))
        original = Record(values)
        encoded = original.encode()
        decoded = Record.decode(encoded)
        
        assert decoded.get_values() == values


class TestCalculateRecordSize:
    """Test record size calculation."""
    
    def test_size_single_integer(self):
        size = calculate_record_size([42])
        assert size > 0
        
        # Verify it matches actual encoding
        record = Record([42])
        encoded = record.encode()
        assert len(encoded) == size
    
    def test_size_multiple_values(self):
        values = [1, "hello", 3.14]
        size = calculate_record_size(values)
        
        record = Record(values)
        encoded = record.encode()
        assert len(encoded) == size
    
    def test_size_with_null(self):
        values = [None, 42]
        size = calculate_record_size(values)
        assert size > 0
    
    def test_size_empty_record(self):
        size = calculate_record_size([])
        assert size > 0  # Still has header


class TestDataType:
    """Test DataType enum."""
    
    def test_data_type_values(self):
        assert DataType.NULL == 0
        assert DataType.INTEGER == 1
        assert DataType.FLOAT == 2
        assert DataType.TEXT == 3
        assert DataType.BLOB == 4