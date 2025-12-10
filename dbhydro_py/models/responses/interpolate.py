"""Response models for DBHydro interpolate endpoint."""

from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

# Local imports
from dbhydro_py.utils import dataclass_from_dict

if TYPE_CHECKING:
    import pandas as pd

# Hierarchy of dataclasses representing the Interpolate Response structure
"""
InterpolateResponse
│
└── entries (list)
      │
      └── [0] (a single interpolated entry)
            ├── origin
            ├── key
            ├── key_type
            ├── ms_since_epoch
            ├── value
            ├── tag
            │     └── tag (optional string)
            ├── quality_code
            ├── percent_available
            ├── time
            └── date
"""


@dataclass
class InterpolateTag:
    """Tag information for interpolated data."""
    tag: Optional[str] = field(metadata={'json_key': 'tag'})
    
    @classmethod
    def from_dict(cls, data: dict) -> 'InterpolateTag':
        """Create InterpolateTag from dictionary."""
        return dataclass_from_dict(cls, data)  # type: ignore


@dataclass
class InterpolateEntry:
    """Single interpolated data point."""
    origin: str = field(metadata={'json_key': 'origin'})
    key: str = field(metadata={'json_key': 'key'})
    key_type: str = field(metadata={'json_key': 'keyType'})
    ms_since_epoch: int = field(metadata={'json_key': 'msSinceEpoch'})
    value: float = field(metadata={'json_key': 'value'})
    tag: InterpolateTag = field(metadata={'json_key': 'tag'})
    quality_code: str = field(metadata={'json_key': 'qualityCode'})
    percent_available: float = field(metadata={'json_key': 'percentAvailable'})
    time: int = field(metadata={'json_key': 'time'})
    date: int = field(metadata={'json_key': 'date'})
    
    @classmethod
    def from_dict(cls, data: dict) -> 'InterpolateEntry':
        """Create InterpolateEntry from dictionary."""
        return dataclass_from_dict(cls, data)  # type: ignore


@dataclass
class InterpolateResponse:
    """Response from the interpolate endpoint containing interpolated data points."""
    entries: list[InterpolateEntry] = field(default_factory=list, metadata={'json_key': 'list'})
    
    @classmethod
    def from_dict(cls, data: dict) -> 'InterpolateResponse':
        """Create InterpolateResponse from dictionary."""
        # Use dataclass_from_dict - entries will always be a list due to default_factory
        result: InterpolateResponse = dataclass_from_dict(cls, data)
        return result
    
    def to_dataframe(self, include_metadata: bool = False) -> 'pd.DataFrame':
        """Convert interpolated data to pandas DataFrame.
        
        Args:
            include_metadata (bool): If True, includes additional metadata columns.
                                   If False (default), only includes essential columns.
        
        Returns:
            pd.DataFrame: DataFrame with interpolated data.
            
        Raises:
            ImportError: If pandas is not installed.
            
        Example:
            >>> response = api.get_interpolate('S123-R', '2023-01-01 12:00:00')
            >>> df = response.to_dataframe()
            >>> print(df.head())
                    key  value quality_code       origin
            0  S123-R   12.5            A  MANIPULATED
        """
        # Import pandas here allowing it to be an optional dependency
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                'pandas is required for DataFrame conversion. '
                'Install it with: pip install pandas'
            )
        
        if not self.entries:
            # Return empty DataFrame with expected columns
            columns = ['key', 'value', 'quality_code', 'origin']
            if include_metadata:
                columns.extend(['key_type', 'ms_since_epoch', 'percent_available', 'time', 'date', 'tag'])
            return pd.DataFrame(columns=columns)
        
        # Convert entries to list of dictionaries
        data_rows = []
        for entry in self.entries:
            row = {
                'key': entry.key,
                'value': entry.value,
                'quality_code': entry.quality_code,
                'origin': entry.origin
            }
            
            if include_metadata:
                row.update({
                    'key_type': entry.key_type,
                    'ms_since_epoch': entry.ms_since_epoch,
                    'percent_available': entry.percent_available,
                    'time': entry.time,
                    'date': entry.date,
                    'tag': entry.tag.tag if entry.tag else None
                })
            
            data_rows.append(row)
        
        return pd.DataFrame(data_rows)
    
    def get_keys(self) -> list[str]:
        """Get list of all unique keys in the response.
        
        Returns:
            list[str]: List of unique keys, sorted.
        """
        keys = set(entry.key for entry in self.entries)
        return sorted(keys)
    
    def get_values(self) -> list[float]:
        """Get list of all values in the response.
        
        Returns:
            list[float]: List of all interpolated values.
        """
        return [entry.value for entry in self.entries]
    
    def get_quality_codes(self) -> list[str]:
        """Get list of all unique quality codes in the response.
        
        Returns:
            list[str]: List of unique quality codes, sorted.
        """
        quality_codes = set(entry.quality_code for entry in self.entries)
        return sorted(quality_codes)
    
    def get_origins(self) -> list[str]:
        """Get list of all unique data origins in the response.
        
        Returns:
            list[str]: List of unique origins, sorted.
        """
        origins = set(entry.origin for entry in self.entries)
        return sorted(origins)
    
    def get_key_types(self) -> list[str]:
        """Get list of all unique key types in the response.
        
        Returns:
            list[str]: List of unique key types, sorted.
        """
        key_types = set(entry.key_type for entry in self.entries)
        return sorted(key_types)
    
    def get_timestamps(self) -> list[int]:
        """Get list of all timestamps in the response, sorted.
        
        Returns:
            list[int]: List of ms_since_epoch timestamps, sorted.
        """
        return sorted(entry.ms_since_epoch for entry in self.entries)
    
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
    
    def filter_by_key(self, keys: list[str]) -> 'InterpolateResponse':
        """Filter entries by specific keys.
        
        Args:
            keys (list[str]): List of keys to include.
            
        Returns:
            InterpolateResponse: New response containing only entries with specified keys.
        """
        filtered_entries = [entry for entry in self.entries if entry.key in keys]
        return InterpolateResponse(entries=filtered_entries)
    
    def filter_by_quality(self, quality_codes: list[str]) -> 'InterpolateResponse':
        """Filter entries by quality codes.
        
        Args:
            quality_codes (list[str]): List of quality codes to include.
            
        Returns:
            InterpolateResponse: New response containing only entries with specified quality codes.
        """
        filtered_entries = [entry for entry in self.entries if entry.quality_code in quality_codes]
        return InterpolateResponse(entries=filtered_entries)
    
    def filter_by_origin(self, origins: list[str]) -> 'InterpolateResponse':
        """Filter entries by origins.
        
        Args:
            origins (list[str]): List of origins to include.
            
        Returns:
            InterpolateResponse: New response containing only entries from specified origins.
        """
        filtered_entries = [entry for entry in self.entries if entry.origin in origins]
        return InterpolateResponse(entries=filtered_entries)
    
    def get_entries_for_key(self, key: str) -> list[InterpolateEntry]:
        """Get all entries for a specific key.
        
        Args:
            key (str): The key to retrieve entries for.
            
        Returns:
            list[InterpolateEntry]: List of entries for the specified key.
        """
        return [entry for entry in self.entries if entry.key == key]
    
    def get_latest_value_by_key(self) -> dict[str, float]:
        """Get the most recent value for each key.
        
        Returns:
            dict[str, float]: Dictionary mapping keys to their latest values.
        """
        latest_values = {}
        for key in self.get_keys():
            key_entries = self.get_entries_for_key(key)
            if key_entries:
                latest_entry = max(key_entries, key=lambda e: e.ms_since_epoch)
                latest_values[key] = latest_entry.value
        return latest_values
    
    def get_earliest_value_by_key(self) -> dict[str, float]:
        """Get the earliest value for each key.
        
        Returns:
            dict[str, float]: Dictionary mapping keys to their earliest values.
        """
        earliest_values = {}
        for key in self.get_keys():
            key_entries = self.get_entries_for_key(key)
            if key_entries:
                earliest_entry = min(key_entries, key=lambda e: e.ms_since_epoch)
                earliest_values[key] = earliest_entry.value
        return earliest_values
    
    def get_average_values_by_key(self) -> dict[str, float]:
        """Get average value for each key.
        
        Returns:
            dict[str, float]: Dictionary mapping keys to their average values.
        """
        averages = {}
        for key in self.get_keys():
            key_entries = self.get_entries_for_key(key)
            if key_entries:
                values = [entry.value for entry in key_entries]
                averages[key] = sum(values) / len(values)
        return averages
    
    def get_value_ranges_by_key(self) -> dict[str, tuple[float, float]]:
        """Get value range for each key.
        
        Returns:
            dict[str, tuple[float, float]]: Dictionary mapping keys to (min, max) tuples.
        """
        ranges = {}
        for key in self.get_keys():
            key_entries = self.get_entries_for_key(key)
            if key_entries:
                values = [entry.value for entry in key_entries]
                ranges[key] = (min(values), max(values))
        return ranges
    
    def get_quality_summary(self) -> dict[str, int]:
        """Get count of entries by quality code.
        
        Returns:
            dict[str, int]: Dictionary mapping quality codes to their counts.
        """
        quality_counts: dict[str, int] = {}
        for entry in self.entries:
            quality_counts[entry.quality_code] = quality_counts.get(entry.quality_code, 0) + 1
        return quality_counts
    
    def get_data_count(self) -> int:
        """Get total number of interpolated data points.
        
        Returns:
            int: Total count of entries.
        """
        return len(self.entries)
    
    def get_data_counts_by_key(self) -> dict[str, int]:
        """Get count of entries for each key.
        
        Returns:
            dict[str, int]: Dictionary mapping keys to their entry counts.
        """
        return {key: len(self.get_entries_for_key(key)) for key in self.get_keys()}
    
    def has_data(self) -> bool:
        """Check if the response contains any data.
        
        Returns:
            bool: True if there are entries, False otherwise.
        """
        return len(self.entries) > 0
    
    def get_entries_by_key_and_quality(self) -> dict[str, dict[str, list[InterpolateEntry]]]:
        """Group entries by key and quality code.
        
        Returns:
            dict[str, dict[str, list[InterpolateEntry]]]: Nested dictionary with key -> quality_code -> entries.
        """
        result = {}
        for key in self.get_keys():
            key_data: dict[str, list[InterpolateEntry]] = {}
            key_entries = self.get_entries_for_key(key)
            for entry in key_entries:
                if entry.quality_code not in key_data:
                    key_data[entry.quality_code] = []
                key_data[entry.quality_code].append(entry)
            if key_data:
                result[key] = key_data
        return result
    
    def get_tagged_entries(self) -> list[InterpolateEntry]:
        """Get entries that have non-empty tag information.
        
        Returns:
            list[InterpolateEntry]: List of entries with non-null tag values.
        """
        return [entry for entry in self.entries if entry.tag and entry.tag.tag]