"""Utility functions for data processing and dataclass operations."""

# Standard library imports
from dataclasses import fields, is_dataclass
from datetime import datetime
from typing import Any, get_args, get_origin


def dataclass_from_dict(cls: type, data: dict[str, Any]) -> Any:
    """Recursively convert a dict into a dataclass instance.
    
    Supports field mapping via metadata with 'json_key' for handling
    API responses with different field naming conventions.
    
    Args:
        cls: The dataclass type to instantiate.
        data: The dictionary containing the data.
    """
    if not is_dataclass(cls):
        raise TypeError(f'{cls} is not a dataclass')

    kwargs: dict[str, Any] = {}

    for f in fields(cls):
        field_name = f.name
        field_type = f.type
        
        # Check for json_key in field metadata, otherwise use field name
        json_key = f.metadata.get('json_key', field_name)
        value = data.get(json_key)

        if value is None:
            kwargs[field_name] = None if get_origin(field_type) is not list else []
            continue

        origin = get_origin(field_type)

        # Case 1: field is a list[Something]
        if origin is list:
            inner_type = get_args(field_type)[0]

            if is_dataclass(inner_type):
                kwargs[field_name] = [
                    _create_dataclass_instance(inner_type, item) for item in value  # type: ignore
                ]
            else:
                kwargs[field_name] = list(value)
            continue

        # Case 2: field is a nested dataclass
        if is_dataclass(field_type):
            kwargs[field_name] = _create_dataclass_instance(field_type, value)  # type: ignore
            continue

        # Case 3: primitive types (str, int, float, etc)
        kwargs[field_name] = value

    return cls(**kwargs)


def _create_dataclass_instance(cls: type, data: dict) -> Any:
    """Create a dataclass instance, using custom from_dict if available.
    
    Args:
        cls: The dataclass type to instantiate.
        data: The dictionary containing the data.
    """
    if hasattr(cls, 'from_dict') and callable(getattr(cls, 'from_dict')):
        return cls.from_dict(data)
    return dataclass_from_dict(cls, data)
