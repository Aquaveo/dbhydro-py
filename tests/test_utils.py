"""Tests for utility functions."""

import pytest
from dataclasses import dataclass, field
from typing import Optional

from dbhydro_py.utils import dataclass_from_dict


@dataclass
class SimpleTestClass: 
    """Simple test dataclass."""
    name: str
    value: int


@dataclass  
class DataClassWithMetadata:
    """Test dataclass with field metadata."""
    display_name: str = field(metadata={"json_key": "displayName"})
    item_count: int = field(metadata={"json_key": "itemCount"})
    description: Optional[str] = None


class TestDataclassFromDict:
    """Test cases for dataclass_from_dict utility."""
    
    def test_simple_conversion(self):
        """Test basic dataclass conversion."""
        data = {"name": "test", "value": 42}
        result = dataclass_from_dict(SimpleTestClass, data)
        
        assert isinstance(result, SimpleTestClass)
        assert result.name == "test"
        assert result.value == 42
    
    def test_with_json_key_metadata(self):
        """Test conversion with field metadata mapping."""
        data = {
            "displayName": "Test Item",
            "itemCount": 5,
            "description": "A test item"
        }
        
        result = dataclass_from_dict(DataClassWithMetadata, data)
        
        assert result.display_name == "Test Item"
        assert result.item_count == 5
        assert result.description == "A test item"
    
    def test_missing_optional_fields(self):
        """Test handling of missing optional fields."""
        data = {
            "displayName": "Test Item",
            "itemCount": 5
            # description is missing but optional
        }
        
        result = dataclass_from_dict(DataClassWithMetadata, data)
        
        assert result.display_name == "Test Item"
        assert result.item_count == 5
        assert result.description is None
    
    def test_non_dataclass_raises_error(self):
        """Test that non-dataclass raises TypeError."""
        class NotADataclass:
            pass
        
        with pytest.raises(TypeError, match="is not a dataclass"):
            dataclass_from_dict(NotADataclass, {})