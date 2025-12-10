"""DBHydro Python client library for accessing South Florida Water Management District data."""

__license__ = "MIT"
__version__ = '0.0.1'

# Main API client - the primary entry point users need
from .api import DbHydroApi

# Common response models users will work with
from .models import TimeSeriesResponse, Status
from .models.responses import PointResponse

# REST adapters for customization
from .rest_adapters import RestAdapterRequests

# Main exception for error handling
from .exceptions import DbHydroException

__all__ = [
    'DbHydroApi',
    'TimeSeriesResponse', 
    'Status',
    'PointResponse',
    'RestAdapterRequests',
    'DbHydroException',
]