# Standard library imports
from dataclasses import dataclass, field

# Local imports
from dbhydro_py.utils import dataclass_from_dict

@dataclass
class Status:
    """Common status information for API responses."""
    status_code: int = field(metadata={'json_key': 'statusCode'})
    message: str = field(metadata={'json_key': 'statusMessage'})
    elapsed_time: float = field(metadata={'json_key': 'elapsedTime'})

@dataclass
class ApiResponseBase:
    """Base class for all API responses containing common status information."""
    status: Status
    
    @classmethod
    def from_dict_base(cls, data: dict) -> 'ApiResponseBase':
        """Base implementation that handles status mapping automatically."""
        return dataclass_from_dict(cls, data)  # type: ignore
