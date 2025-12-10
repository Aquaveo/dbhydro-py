"""REST adapter implementations for HTTP communication."""

from .rest_adapter_base import RestAdapterBase
from .rest_adapter_requests import RestAdapterRequests

__all__ = ['RestAdapterBase', 'RestAdapterRequests']