# Standard library imports
from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    # Only import pandas for type checking (optional import at runtime)
    import pandas as pd

# Local imports
from dbhydro_py.models.responses.base import ApiResponseBase
from dbhydro_py.utils import dataclass_from_dict

# Hierarchy of dataclasses representing the Aggregate Response structure
"""
AggregateResponse
│
└── intervals  (list)
      │
      └── [0]  (a single interval)
            │
            ├── end_millis_since_epoch
            ├── statistic_type
            ├── timespan
            │     ├── scalar
            │     └── unit_of_time
            ├── start_millis_since_epoch
            ├── value
            ├── key (station/site identifier)
            ├── tag
            │     └── tag
            ├── end_date
            ├── start_date
            ├── key_type
            ├── quality_code
            ├── percent_available
            └── origin
"""


@dataclass
class Tag:
    """Tag information for aggregate data."""
    tag: Optional[str] = field(metadata={'json_key': 'tag'})
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Tag':
        """Create Tag from dictionary."""
        return dataclass_from_dict(cls, data)  # type: ignore


@dataclass
class Timespan:
    """Timespan definition for aggregate calculations."""
    scalar: int = field(metadata={'json_key': 'scalar'})
    unit_of_time: str = field(metadata={'json_key': 'unitOfTime'})
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Timespan':
        """Create Timespan from dictionary."""
        return dataclass_from_dict(cls, data)  # type: ignore


@dataclass
class AggregateInterval:
    """Single aggregate interval with statistical calculation."""
    end_millis_since_epoch: int = field(metadata={'json_key': 'endMilliSinceEpoch'})
    statistic_type: str = field(metadata={'json_key': 'statisticType'})
    timespan: Timespan = field(metadata={'json_key': 'timespan'})
    start_millis_since_epoch: int = field(metadata={'json_key': 'startMilliSinceEpoch'})
    value: float = field(metadata={'json_key': 'value'})
    key: str = field(metadata={'json_key': 'key'})
    tag: Tag = field(metadata={'json_key': 'tag'})
    end_date: str = field(metadata={'json_key': 'endDate'})
    start_date: str = field(metadata={'json_key': 'startDate'})
    key_type: str = field(metadata={'json_key': 'keyType'})
    quality_code: str = field(metadata={'json_key': 'qualityCode'})
    percent_available: float = field(metadata={'json_key': 'percentAvailable'})
    origin: str = field(metadata={'json_key': 'origin'})
    
    @classmethod
    def from_dict(cls, data: dict) -> 'AggregateInterval':
        """Create AggregateInterval from dictionary."""
        # Use dataclass_from_dict directly - it will handle nested objects properly
        return dataclass_from_dict(cls, data)  # type: ignore


@dataclass
class AggregateResponse:
    """Response from the aggregate endpoint containing statistical intervals."""
    intervals: list[AggregateInterval] = field(default_factory=list, metadata={'json_key': 'intervals'})
    
    @classmethod
    def from_dict(cls, data: dict) -> 'AggregateResponse':
        """Create AggregateResponse from dictionary."""
        # Use dataclass_from_dict - intervals will always be a list due to default_factory
        result: AggregateResponse = dataclass_from_dict(cls, data)
        return result
    
    def to_dataframe(self, include_metadata: bool = False) -> 'pd.DataFrame':
        """Convert aggregate data to pandas DataFrame.
        
        Args:
            include_metadata (bool): If True, includes additional metadata columns.
                                   If False (default), only includes essential columns.
        
        Returns:
            pd.DataFrame: DataFrame with aggregate interval data.
            
        Raises:
            ImportError: If pandas is not installed.
            
        Example:
            >>> response = api.get_aggregate('S123-R', '2023-01-01', '2023-01-02', 'MAX', 'DAY')
            >>> df = response.to_dataframe()
            >>> print(df.head())
                   start_date     end_date statistic_type  value key quality_code
            0  2023-01-01...  2023-01-02...            MAX   12.5  S123-R         A
        """
        # Import pandas here allowing it to be an optional dependency
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                'pandas is required for to_dataframe(). Install with: pip install pandas'
            )
        
        # Collect all data from intervals
        records = []
        for interval in self.intervals:
            # Convert milliseconds to datetime
            start_datetime = pd.to_datetime(interval.start_millis_since_epoch, unit='ms')
            end_datetime = pd.to_datetime(interval.end_millis_since_epoch, unit='ms')
            
            record = {
                'start_datetime': start_datetime,
                'end_datetime': end_datetime,
                'statistic_type': interval.statistic_type,
                'value': interval.value,
                'key': interval.key,
                'quality_code': interval.quality_code,
            }
            
            # Add metadata if requested
            if include_metadata:
                record.update({
                    'start_date': interval.start_date,
                    'end_date': interval.end_date,
                    'key_type': interval.key_type,
                    'percent_available': interval.percent_available,
                    'origin': interval.origin,
                    'timespan_scalar': interval.timespan.scalar,
                    'timespan_unit': interval.timespan.unit_of_time,
                    'tag': interval.tag.tag,
                    'start_millis': interval.start_millis_since_epoch,
                    'end_millis': interval.end_millis_since_epoch,
                })
            
            records.append(record)
        
        # Create DataFrame
        df = pd.DataFrame(records)
        
        # Ensure consistent DataFrame structure even when empty
        if df.empty:
            # Define the basic columns that will always be present
            basic_columns = ['start_datetime', 'end_datetime', 'statistic_type', 'value', 'key', 'quality_code']
            
            # Add metadata columns if requested
            if include_metadata:
                basic_columns.extend([
                    'start_date', 'end_date', 'key_type', 'percent_available', 'origin',
                    'timespan_scalar', 'timespan_unit', 'tag', 'start_millis', 'end_millis'
                ])
            
            # Create empty DataFrame with proper column types
            df = pd.DataFrame(columns=basic_columns)
            df = df.astype({
                'start_datetime': 'datetime64[ns]',
                'end_datetime': 'datetime64[ns]',
                'statistic_type': 'object',
                'value': 'float64',
                'key': 'object',
                'quality_code': 'object'
            })
        else:
            # Sort by start time for chronological order
            df = df.sort_values('start_datetime').reset_index(drop=True)
        
        return df
    
    def get_keys(self) -> list[str]:
        """Get list of all unique keys in the response.
        
        Returns:
            list[str]: List of unique keys, sorted.
        """
        keys = set(interval.key for interval in self.intervals)
        return sorted(keys)
    
    def get_statistic_types(self) -> list[str]:
        """Get list of all unique statistic types in the response.
        
        Returns:
            list[str]: List of unique statistic types, sorted.
        """
        stat_types = set(interval.statistic_type for interval in self.intervals)
        return sorted(stat_types)
    
    def get_values(self) -> list[float]:
        """Get list of all aggregated values in the response.
        
        Returns:
            list[float]: List of all aggregate values.
        """
        return [interval.value for interval in self.intervals]
    
    def get_quality_codes(self) -> list[str]:
        """Get list of all unique quality codes in the response.
        
        Returns:
            list[str]: List of unique quality codes, sorted.
        """
        quality_codes = set(interval.quality_code for interval in self.intervals)
        return sorted(quality_codes)
    
    def get_origins(self) -> list[str]:
        """Get list of all unique data origins in the response.
        
        Returns:
            list[str]: List of unique origins, sorted.
        """
        origins = set(interval.origin for interval in self.intervals)
        return sorted(origins)
    
    def get_timespans(self) -> list[tuple[int, str]]:
        """Get list of all unique timespan definitions.
        
        Returns:
            list[tuple[int, str]]: List of (scalar, unit_of_time) tuples, sorted.
        """
        timespans = set((interval.timespan.scalar, interval.timespan.unit_of_time) for interval in self.intervals)
        return sorted(timespans)
    
    def get_timestamp_range(self) -> tuple[int, int] | None:
        """Get overall timestamp range across all intervals.
        
        Returns:
            tuple[int, int] | None: (earliest_start, latest_end) or None if no data.
        """
        if not self.intervals:
            return None
        
        starts = [interval.start_millis_since_epoch for interval in self.intervals]
        ends = [interval.end_millis_since_epoch for interval in self.intervals]
        return (min(starts), max(ends))
    
    def get_value_range(self) -> tuple[float, float] | None:
        """Get min/max value range across all intervals.
        
        Returns:
            tuple[float, float] | None: (min, max) values or None if no data.
        """
        values = self.get_values()
        return (min(values), max(values)) if values else None
    
    def filter_by_key(self, keys: list[str]) -> 'AggregateResponse':
        """Filter intervals by specific keys.
        
        Args:
            keys (list[str]): List of keys to include.
            
        Returns:
            AggregateResponse: New response containing only intervals with specified keys.
        """
        filtered_intervals = [interval for interval in self.intervals if interval.key in keys]
        return AggregateResponse(intervals=filtered_intervals)
    
    def filter_by_statistic(self, statistic_types: list[str]) -> 'AggregateResponse':
        """Filter intervals by statistic types.
        
        Args:
            statistic_types (list[str]): List of statistic types to include (e.g., ['MAX', 'MIN']).
            
        Returns:
            AggregateResponse: New response containing only intervals with specified statistic types.
        """
        filtered_intervals = [interval for interval in self.intervals if interval.statistic_type in statistic_types]
        return AggregateResponse(intervals=filtered_intervals)
    
    def filter_by_quality(self, quality_codes: list[str]) -> 'AggregateResponse':
        """Filter intervals by quality codes.
        
        Args:
            quality_codes (list[str]): List of quality codes to include.
            
        Returns:
            AggregateResponse: New response containing only intervals with specified quality codes.
        """
        filtered_intervals = [interval for interval in self.intervals if interval.quality_code in quality_codes]
        return AggregateResponse(intervals=filtered_intervals)
    
    def filter_by_origin(self, origins: list[str]) -> 'AggregateResponse':
        """Filter intervals by origins.
        
        Args:
            origins (list[str]): List of origins to include.
            
        Returns:
            AggregateResponse: New response containing only intervals from specified origins.
        """
        filtered_intervals = [interval for interval in self.intervals if interval.origin in origins]
        return AggregateResponse(intervals=filtered_intervals)
    
    def get_intervals_for_key(self, key: str) -> list[AggregateInterval]:
        """Get all intervals for a specific key.
        
        Args:
            key (str): The key to retrieve intervals for.
            
        Returns:
            list[AggregateInterval]: List of intervals for the specified key, sorted by start time.
        """
        key_intervals = [interval for interval in self.intervals if interval.key == key]
        return sorted(key_intervals, key=lambda i: i.start_millis_since_epoch)
    
    def get_intervals_by_statistic(self, statistic_type: str) -> list[AggregateInterval]:
        """Get all intervals for a specific statistic type.
        
        Args:
            statistic_type (str): The statistic type to retrieve intervals for.
            
        Returns:
            list[AggregateInterval]: List of intervals for the specified statistic type.
        """
        return [interval for interval in self.intervals if interval.statistic_type == statistic_type]
    
    def get_latest_values_by_key(self) -> dict[str, float]:
        """Get the most recent value for each key.
        
        Returns:
            dict[str, float]: Dictionary mapping keys to their latest values.
        """
        latest_values = {}
        for key in self.get_keys():
            key_intervals = self.get_intervals_for_key(key)
            if key_intervals:
                latest_interval = max(key_intervals, key=lambda i: i.end_millis_since_epoch)
                latest_values[key] = latest_interval.value
        return latest_values
    
    def get_earliest_values_by_key(self) -> dict[str, float]:
        """Get the earliest value for each key.
        
        Returns:
            dict[str, float]: Dictionary mapping keys to their earliest values.
        """
        earliest_values = {}
        for key in self.get_keys():
            key_intervals = self.get_intervals_for_key(key)
            if key_intervals:
                earliest_interval = min(key_intervals, key=lambda i: i.start_millis_since_epoch)
                earliest_values[key] = earliest_interval.value
        return earliest_values
    
    def get_value_ranges_by_key(self) -> dict[str, tuple[float, float]]:
        """Get value range for each key.
        
        Returns:
            dict[str, tuple[float, float]]: Dictionary mapping keys to (min, max) tuples.
        """
        ranges = {}
        for key in self.get_keys():
            key_intervals = self.get_intervals_for_key(key)
            if key_intervals:
                values = [interval.value for interval in key_intervals]
                ranges[key] = (min(values), max(values))
        return ranges
    
    def get_average_values_by_key(self) -> dict[str, float]:
        """Get average value for each key across all its intervals.
        
        Returns:
            dict[str, float]: Dictionary mapping keys to their average values.
        """
        averages = {}
        for key in self.get_keys():
            key_intervals = self.get_intervals_for_key(key)
            if key_intervals:
                values = [interval.value for interval in key_intervals]
                averages[key] = sum(values) / len(values)
        return averages
    
    def get_quality_summary(self) -> dict[str, int]:
        """Get count of intervals by quality code.
        
        Returns:
            dict[str, int]: Dictionary mapping quality codes to their counts.
        """
        quality_counts: dict[str, int] = {}
        for interval in self.intervals:
            quality_counts[interval.quality_code] = quality_counts.get(interval.quality_code, 0) + 1
        return quality_counts
    
    def get_statistic_summary(self) -> dict[str, int]:
        """Get count of intervals by statistic type.
        
        Returns:
            dict[str, int]: Dictionary mapping statistic types to their counts.
        """
        stat_counts: dict[str, int] = {}
        for interval in self.intervals:
            stat_counts[interval.statistic_type] = stat_counts.get(interval.statistic_type, 0) + 1
        return stat_counts
    
    def get_data_count(self) -> int:
        """Get total number of aggregate intervals.
        
        Returns:
            int: Total count of intervals.
        """
        return len(self.intervals)
    
    def get_data_counts_by_key(self) -> dict[str, int]:
        """Get count of intervals for each key.
        
        Returns:
            dict[str, int]: Dictionary mapping keys to their interval counts.
        """
        return {key: len(self.get_intervals_for_key(key)) for key in self.get_keys()}
    
    def has_data(self) -> bool:
        """Check if the response contains any data.
        
        Returns:
            bool: True if there are intervals, False otherwise.
        """
        return len(self.intervals) > 0
    
    def get_intervals_by_key_and_statistic(self) -> dict[str, dict[str, list[AggregateInterval]]]:
        """Group intervals by key and statistic type.
        
        Returns:
            dict[str, dict[str, list[AggregateInterval]]]: Nested dictionary with key -> statistic_type -> intervals.
        """
        result = {}
        for key in self.get_keys():
            key_data: dict[str, list[AggregateInterval]] = {}
            key_intervals = self.get_intervals_for_key(key)
            for interval in key_intervals:
                if interval.statistic_type not in key_data:
                    key_data[interval.statistic_type] = []
                key_data[interval.statistic_type].append(interval)
            if key_data:
                result[key] = key_data
        return result
    
    def get_tagged_intervals(self) -> list[AggregateInterval]:
        """Get intervals that have non-empty tag information.
        
        Returns:
            list[AggregateInterval]: List of intervals with non-null tag values.
        """
        return [interval for interval in self.intervals if interval.tag and interval.tag.tag]
    
    def get_time_coverage_by_key(self) -> dict[str, tuple[int, int]]:
        """Get time coverage (start to end) for each key.
        
        Returns:
            dict[str, tuple[int, int]]: Dictionary mapping keys to (earliest_start, latest_end) timestamps.
        """
        coverage = {}
        for key in self.get_keys():
            key_intervals = self.get_intervals_for_key(key)
            if key_intervals:
                starts = [interval.start_millis_since_epoch for interval in key_intervals]
                ends = [interval.end_millis_since_epoch for interval in key_intervals]
                coverage[key] = (min(starts), max(ends))
        return coverage
