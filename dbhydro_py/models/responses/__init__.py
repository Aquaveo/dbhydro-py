"""API response models for DBHydro endpoints."""

from .base import Status, ApiResponseBase
from .time_series import TimeSeriesResponse, TimeSeriesEntry
from .aggregate import AggregateResponse, AggregateInterval, Timespan, Tag
from .interpolate import InterpolateResponse, InterpolateEntry, InterpolateTag
from .point import PointResponse, Point
from .synchronize import SynchronizeResponse, SynchronizeEntry, SynchronizeValue
from .water_quality import WaterQualityValue, WaterQualityResponse

__all__ = [
    'Status', 
    'ApiResponseBase', 
    'TimeSeriesResponse', 
    'TimeSeriesEntry', 
    'AggregateResponse', 
    'AggregateInterval', 
    'Timespan', 
    'Tag', 
    'InterpolateResponse', 
    'InterpolateEntry', 
    'InterpolateTag', 
    'PointResponse', 
    'Point', 
    'SynchronizeResponse', 
    'SynchronizeEntry', 
    'SynchronizeValue',
    'WaterQualityValue',
    'WaterQualityResponse'
]