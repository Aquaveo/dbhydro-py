# Standard library imports
from dataclasses import dataclass
from typing import Any

@dataclass
class Result:
    """Generic result wrapper for REST adapter responses."""
    status_code: int
    message: str
    data: dict[str, Any]