"""Response models for DBHydro tsarithmetic endpoint."""

from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING, Any

from dbhydro_py.models.responses.base import Status
from dbhydro_py.utils import dataclass_from_dict

if TYPE_CHECKING:
    import pandas as pd

# Hierarchy of dataclasses representing the Point Response structure
"""
PointResponse
│
├── status
│     ├── status_code
│     ├── status_message
│     └── elapsed_time
│
└── points (list)
      │
      └── [0] (a single point - can be null)
            ├── value (optional)
            ├── timestamp (optional)
            ├── ms_since_epoch (optional)
            └── quality_code (optional)
"""


@dataclass
class Point:
    """Single point data from tsarithmetic endpoint."""
    # The actual structure will depend on what a valid response looks like
    # For now, keeping flexible with Optional fields based on common patterns
    value: Optional[float] = field(default=None, metadata={'json_key': 'value'})
    timestamp: Optional[int] = field(default=None, metadata={'json_key': 'timestamp'}) 
    ms_since_epoch: Optional[int] = field(default=None, metadata={'json_key': 'msSinceEpoch'})
    quality_code: Optional[str] = field(default=None, metadata={'json_key': 'qualityCode'})
    
    @classmethod
    def from_dict(cls, data: dict | None) -> 'Point':
        """Create Point from dictionary."""
        if data is None:
            # Handle null points in the response
            return cls()
        return dataclass_from_dict(cls, data)  # type: ignore


@dataclass
class PointResponse:
    """Response from the tsarithmetic endpoint."""
    status: Status = field(metadata={'json_key': 'status'})
    points: list[Optional[Point]] = field(default_factory=list, metadata={'json_key': 'points'})
    
    @classmethod
    def from_dict(cls, data: dict) -> 'PointResponse':
        """Create PointResponse from dictionary."""
        # Use dataclass_from_dict but handle point conversion
        result: PointResponse = dataclass_from_dict(cls, data)
        
        # Convert dictionary points to Point objects if needed
        if result.points:
            converted_points: list[Point | None] = []
            for point_item in result.points:
                if point_item is None:
                    converted_points.append(None)
                elif isinstance(point_item, dict):
                    # Convert dictionary to Point object
                    converted_points.append(Point.from_dict(point_item))
                elif isinstance(point_item, Point):
                    # Already a Point object
                    converted_points.append(point_item)
            result.points = converted_points
        
        return result
    
    def to_dataframe(self, include_metadata: bool = False) -> 'pd.DataFrame':
        """Convert point data to pandas DataFrame.
        
        Args:
            include_metadata (bool): If True, includes additional metadata columns.
                                   If False (default), only includes essential columns.
        
        Returns:
            pd.DataFrame: DataFrame with point data.
            
        Raises:
            ImportError: If pandas is not installed.
            
        Example:
            >>> response = api.get_time_series_arithmetic('TS123', '2023-01-01 12:00:00')
            >>> df = response.to_dataframe()
            >>> print(df.head())
               value quality_code  timestamp
            0   12.5            A  1672574400000
        """
        # Import pandas here allowing it to be an optional dependency
        try:
            import pandas as pd
        except ImportError:
            raise ImportError('pandas is required for DataFrame conversion. Install with: pip install pandas')
        
        # Handle case where points is empty or contains only None values
        valid_points = [point for point in self.points if point is not None]
        
        if not valid_points:
            # Return empty DataFrame with expected columns
            columns = ['value', 'timestamp'] if not include_metadata else ['value', 'timestamp', 'ms_since_epoch', 'quality_code']
            return pd.DataFrame(columns=columns)
        
        # Convert points to records
        records = []
        for point in valid_points:
            record: dict[str, Any] = {}
            
            # Essential columns
            record['value'] = point.value
            record['timestamp'] = point.timestamp
            
            # Additional metadata if requested
            if include_metadata:
                record['ms_since_epoch'] = point.ms_since_epoch
                record['quality_code'] = point.quality_code
            
            records.append(record)
        
        return pd.DataFrame(records)
    
    def get_valid_points(self) -> list[Point]:
        """Get list of valid (non-None) points.
        
        Returns:
            list[Point]: List of valid Point objects.
        """
        return [point for point in self.points if point is not None]
    
    def get_values(self) -> list[float]:
        """Get list of all non-null values.
        
        Returns:
            list[float]: List of values, excluding None values.
        """
        return [point.value for point in self.get_valid_points() if point.value is not None]
    
    def get_quality_codes(self) -> list[str]:
        """Get list of all unique quality codes.
        
        Returns:
            list[str]: List of unique quality codes, sorted.
        """
        quality_codes = set()
        for point in self.get_valid_points():
            if point.quality_code is not None:
                quality_codes.add(point.quality_code)
        return sorted(quality_codes)
    
    def get_timestamps(self) -> list[int]:
        """Get list of all timestamps.
        
        Returns:
            list[int]: List of timestamps, excluding None values.
        """
        timestamps = []
        for point in self.get_valid_points():
            if point.timestamp is not None:
                timestamps.append(point.timestamp)
            elif point.ms_since_epoch is not None:
                timestamps.append(point.ms_since_epoch)
        return sorted(timestamps)
    
    def get_value_range(self) -> tuple[float, float] | None:
        """Get min/max value range.
        
        Returns:
            tuple[float, float] | None: (min, max) values or None if no data.
        """
        values = self.get_values()
        return (min(values), max(values)) if values else None
    
    def get_timestamp_range(self) -> tuple[int, int] | None:
        """Get timestamp range.
        
        Returns:
            tuple[int, int] | None: (earliest, latest) timestamp or None if no data.
        """
        timestamps = self.get_timestamps()
        return (min(timestamps), max(timestamps)) if timestamps else None
    
    def filter_by_quality(self, quality_codes: list[str]) -> 'PointResponse':
        """Filter points by quality codes.
        
        Args:
            quality_codes (list[str]): List of quality codes to include.
            
        Returns:
            PointResponse: New response containing only points with specified quality codes.
        """
        filtered_points: list[Point | None] = []
        for point in self.points:
            if point is None:
                continue
            if point.quality_code in quality_codes:
                filtered_points.append(point)
        
        return PointResponse(status=self.status, points=filtered_points)
    
    def get_average_value(self) -> float | None:
        """Get average of all values.
        
        Returns:
            float | None: Average value or None if no valid values.
        """
        values = self.get_values()
        return sum(values) / len(values) if values else None
    
    def get_quality_summary(self) -> dict[str, int]:
        """Get count of points by quality code.
        
        Returns:
            dict[str, int]: Dictionary mapping quality codes to their counts.
        """
        quality_counts: dict[str, int] = {}
        for point in self.get_valid_points():
            if point.quality_code is not None:
                quality_counts[point.quality_code] = quality_counts.get(point.quality_code, 0) + 1
        return quality_counts
    
    def get_data_count(self) -> int:
        """Get total number of valid data points.
        
        Returns:
            int: Count of valid (non-None) points.
        """
        return len(self.get_valid_points())
    
    def get_null_count(self) -> int:
        """Get count of null/None points.
        
        Returns:
            int: Count of None points in the response.
        """
        return sum(1 for point in self.points if point is None)
    
    def has_data(self) -> bool:
        """Check if the response contains any valid data points.
        
        Returns:
            bool: True if there are valid data points, False otherwise.
        """
        return len(self.get_valid_points()) > 0
    
    def has_null_points(self) -> bool:
        """Check if the response contains any null points.
        
        Returns:
            bool: True if there are null points, False otherwise.
        """
        return self.get_null_count() > 0
    
    def get_latest_value(self) -> float | None:
        """Get the value from the point with the latest timestamp.
        
        Returns:
            float | None: Latest value or None if no valid data.
        """
        valid_points = self.get_valid_points()
        if not valid_points:
            return None
        
        # Find point with latest timestamp
        latest_point = max(
            valid_points,
            key=lambda p: p.timestamp or p.ms_since_epoch or 0
        )
        return latest_point.value
    
    def get_earliest_value(self) -> float | None:
        """Get the value from the point with the earliest timestamp.
        
        Returns:
            float | None: Earliest value or None if no valid data.
        """
        valid_points = self.get_valid_points()
        if not valid_points:
            return None
        
        # Find point with earliest timestamp
        earliest_point = min(
            valid_points,
            key=lambda p: p.timestamp or p.ms_since_epoch or float('inf')
        )
        return earliest_point.value
    
    def get_points_by_quality(self) -> dict[str, list[Point]]:
        """Group points by quality code.
        
        Returns:
            dict[str, list[Point]]: Dictionary mapping quality codes to their points.
        """
        result: dict[str, list[Point]] = {}
        for point in self.get_valid_points():
            if point.quality_code is not None:
                if point.quality_code not in result:
                    result[point.quality_code] = []
                result[point.quality_code].append(point)
        return result