"""Synchronize endpoint response models for DBHydro API."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

# Local imports
from dbhydro_py.models.responses.base import ApiResponseBase
from dbhydro_py.utils import dataclass_from_dict

if TYPE_CHECKING:
    import pandas as pd

# Hierarchy of dataclasses representing the Synchronize Response structure
"""
SynchronizeResponse
│
└── stations (dict)
      │
      └── [station_id] (e.g. "S6-H")
            │
            └── values (list)
                  │
                  └── [0] (a single data point)
                        ├── ms_since_epoch
                        ├── value
                        ├── quality_code
                        ├── percent_available
                        ├── origin
                        ├── key_type
                        └── tag (dict)
"""


@dataclass
class SynchronizeValue:
    """Single data point for a station in synchronized data."""
    ms_since_epoch: int = field(metadata={'json_key': 'msSinceEpoch'})
    value: float = field(metadata={'json_key': 'value'})
    quality_code: str = field(metadata={'json_key': 'qualityCode'})
    percent_available: float = field(metadata={'json_key': 'percentAvailable'})
    origin: str = field(metadata={'json_key': 'origin'})
    key_type: str = field(metadata={'json_key': 'keyType'})
    tag: dict = field(metadata={'json_key': 'tag'})
    
    @classmethod
    def from_dict(cls, data: dict) -> 'SynchronizeValue':
        """Create SynchronizeValue from dictionary."""
        return dataclass_from_dict(cls, data)  # type: ignore


@dataclass
class SynchronizeEntry:
    """Station data containing metadata and list of synchronized values."""
    station_id: str
    key: str = field(metadata={'json_key': 'key'})
    values: list[SynchronizeValue] = field(default_factory=list)
    
    @classmethod
    def from_dict(cls, station_id: str, station_data: dict[str, dict]) -> 'SynchronizeEntry':
        """Create SynchronizeEntry from station data grouped by timestamp."""
        values = []
        key = None
        
        # Extract values from each timestamp
        for timestamp_data in station_data.values():
            if isinstance(timestamp_data, dict):
                # Get the key from first entry (should be same for all)
                if key is None:
                    key = timestamp_data.get('key', station_id)
                
                value = SynchronizeValue.from_dict(timestamp_data)
                values.append(value)
        
        return cls(station_id=station_id, key=key or station_id, values=values)


@dataclass
class SynchronizeResponse:
    """Response from the synchronize endpoint containing synchronized data points across multiple stations."""
    stations: dict[str, SynchronizeEntry] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'SynchronizeResponse':
        """Create SynchronizeResponse from dictionary."""
        # The data is a nested dictionary: timestamp -> station_id -> entry_data
        # We need to restructure it to: station_id -> list of values
        
        # Group data by station
        station_data: dict[str, dict[str, dict]] = {}
        for timestamp_key, stations_at_time in data.items():
            if not isinstance(stations_at_time, dict):
                continue
                
            for station_id, entry_data in stations_at_time.items():
                if isinstance(entry_data, dict):
                    if station_id not in station_data:
                        station_data[station_id] = {}
                    station_data[station_id][timestamp_key] = entry_data
        
        # Convert to SynchronizeEntry objects
        stations = {}
        for station_id, timestamps_data in station_data.items():
            stations[station_id] = SynchronizeEntry.from_dict(station_id, timestamps_data)
        
        return cls(stations=stations)
    
    def to_dataframe(self, include_metadata: bool = False) -> 'pd.DataFrame':
        """Convert synchronize response to a pandas DataFrame.
        
        Args:
            include_metadata (bool): If True, includes additional metadata columns.
                                   If False (default), only includes essential columns.
        
        Returns:
            pd.DataFrame: DataFrame with columns for station_id, timestamp, value, and quality_code.
            
        Raises:
            ImportError: If pandas is not installed.
            
        Example:
            >>> response = api.get_synchronize(['S6-H', 'S6P-3'], '2023-01-01', '2023-01-02')
            >>> df = response.to_dataframe()
            >>> print(df.head())
                 station_id   ms_since_epoch  value quality_code
            0         S6-H  1449794818000   7.76            A
            1        S6P-3  1449794818000   8.12            A
        """
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError(
                'pandas is required for to_dataframe(). Install with: pip install pandas'
            ) from e
        
        if not self.stations:
            # Return empty DataFrame with expected columns
            columns = ['station_id', 'ms_since_epoch', 'value', 'quality_code']
            if include_metadata:
                columns.extend(['key', 'origin', 'key_type', 'tag', 'percent_available'])
            return pd.DataFrame(columns=columns)
        
        # Flatten the station-grouped structure for DataFrame
        rows = []
        for station_id, entry in self.stations.items():
            for value in entry.values:
                row = {
                    'station_id': station_id,
                    'ms_since_epoch': value.ms_since_epoch,
                    'value': value.value,
                    'quality_code': value.quality_code
                }
                
                if include_metadata:
                    row.update({
                        'key': entry.key,
                        'origin': value.origin,
                        'key_type': value.key_type,
                        'tag': value.tag,
                        'percent_available': value.percent_available
                    })
                
                rows.append(row)
        
        return pd.DataFrame(rows)
    
    def get_stations(self) -> set[str]:
        """Get all station IDs in the response.
        
        Returns:
            set[str]: Set of station IDs.
        """
        return set(self.stations.keys())
    
    def get_timestamps(self) -> list[int]:
        """Get all unique timestamps in the response, sorted.
        
        Returns:
            list[int]: List of ms_since_epoch timestamps, sorted.
        """
        timestamps = set()
        for entry in self.stations.values():
            for value in entry.values:
                timestamps.add(value.ms_since_epoch)
        return sorted(timestamps)
    
    def get_station_data(self, station_id: str) -> SynchronizeEntry | None:
        """Get all data for a specific station.
        
        Args:
            station_id (str): The station ID to retrieve data for.
            
        Returns:
            SynchronizeEntry | None: Entry for the station, or None if not found.
        """
        return self.stations.get(station_id)
    
    def get_values_at_timestamp(self, ms_since_epoch: int) -> dict[str, SynchronizeValue]:
        """Get all station values for a specific timestamp.
        
        Args:
            ms_since_epoch (int): The timestamp to retrieve data for.
            
        Returns:
            dict[str, SynchronizeValue]: Dictionary mapping station IDs to values at the timestamp.
        """
        result = {}
        for station_id, entry in self.stations.items():
            for value in entry.values:
                if value.ms_since_epoch == ms_since_epoch:
                    result[station_id] = value
                    break
        return result
    
    def get_station_values(self, station_id: str) -> list[SynchronizeValue]:
        """Get all values for a specific station.
        
        Args:
            station_id (str): The station ID to retrieve values for.
            
        Returns:
            list[SynchronizeValue]: List of values for the station, empty if not found.
        """
        entry = self.stations.get(station_id)
        return entry.values if entry else []
    
    def get_station_ids(self) -> list[str]:
        """Get list of all station IDs in the response.
        
        Returns:
            list[str]: List of station IDs.
        """
        return list(self.stations.keys())
    
    def get_quality_codes(self) -> list[str]:
        """Get list of all unique quality codes in the response.
        
        Returns:
            list[str]: List of unique quality codes.
        """
        quality_codes = set()
        for entry in self.stations.values():
            for value in entry.values:
                quality_codes.add(value.quality_code)
        return sorted(quality_codes)
    
    def get_origins(self) -> list[str]:
        """Get list of all unique data origins in the response.
        
        Returns:
            list[str]: List of unique origins.
        """
        origins = set()
        for entry in self.stations.values():
            for value in entry.values:
                origins.add(value.origin)
        return sorted(origins)
    
    def get_value_ranges(self) -> dict[str, tuple[float, float]]:
        """Get min/max value ranges for each station.
        
        Returns:
            dict[str, tuple[float, float]]: Dictionary mapping station IDs to (min, max) tuples.
        """
        ranges = {}
        for station_id, entry in self.stations.items():
            values = [v.value for v in entry.values if v.value is not None]
            if values:
                ranges[station_id] = (min(values), max(values))
        return ranges
    
    def get_latest_values(self) -> dict[str, float]:
        """Get the most recent value for each station.
        
        Returns:
            dict[str, float]: Dictionary mapping station IDs to their latest values.
        """
        latest_values = {}
        for station_id, entry in self.stations.items():
            if entry.values:
                # Find the value with the latest timestamp
                latest_value = max(entry.values, key=lambda v: v.ms_since_epoch)
                if latest_value.value is not None:
                    latest_values[station_id] = latest_value.value
        return latest_values
    
    def get_earliest_values(self) -> dict[str, float]:
        """Get the earliest value for each station.
        
        Returns:
            dict[str, float]: Dictionary mapping station IDs to their earliest values.
        """
        earliest_values = {}
        for station_id, entry in self.stations.items():
            if entry.values:
                # Find the value with the earliest timestamp
                earliest_value = min(entry.values, key=lambda v: v.ms_since_epoch)
                if earliest_value.value is not None:
                    earliest_values[station_id] = earliest_value.value
        return earliest_values
    
    def filter_by_quality(self, quality_codes: list[str]) -> 'SynchronizeResponse':
        """Filter data to only include specific quality codes.
        
        Args:
            quality_codes (list[str]): List of quality codes to include (e.g., ['A', 'P']).
            
        Returns:
            SynchronizeResponse: New response containing only data with specified quality codes.
        """
        filtered_stations = {}
        for station_id, entry in self.stations.items():
            filtered_values = [v for v in entry.values if v.quality_code in quality_codes]
            if filtered_values:
                filtered_stations[station_id] = SynchronizeEntry(
                    station_id=station_id,
                    key=entry.key,
                    values=filtered_values
                )
        return SynchronizeResponse(stations=filtered_stations)
    
    def filter_by_origin(self, origins: list[str]) -> 'SynchronizeResponse':
        """Filter data to only include specific origins.
        
        Args:
            origins (list[str]): List of origins to include.
            
        Returns:
            SynchronizeResponse: New response containing only data from specified origins.
        """
        filtered_stations = {}
        for station_id, entry in self.stations.items():
            filtered_values = [v for v in entry.values if v.origin in origins]
            if filtered_values:
                filtered_stations[station_id] = SynchronizeEntry(
                    station_id=station_id,
                    key=entry.key,
                    values=filtered_values
                )
        return SynchronizeResponse(stations=filtered_stations)
    
    def get_timestamp_range(self) -> tuple[int, int] | None:
        """Get the overall timestamp range across all data.
        
        Returns:
            tuple[int, int] | None: (earliest_ms, latest_ms) or None if no data.
        """
        timestamps = self.get_timestamps()
        return (timestamps[0], timestamps[-1]) if timestamps else None
    
    def get_quality_summary(self) -> dict[str, int]:
        """Get count of observations by quality code.
        
        Returns:
            dict[str, int]: Dictionary mapping quality codes to their counts.
        """
        quality_counts: dict[str, int] = {}
        for entry in self.stations.values():
            for value in entry.values:
                quality_counts[value.quality_code] = quality_counts.get(value.quality_code, 0) + 1
        return quality_counts
    
    def get_data_count(self) -> int:
        """Get total number of data points across all stations.
        
        Returns:
            int: Total count of observations.
        """
        return sum(len(entry.values) for entry in self.stations.values())
    
    def get_data_counts_by_station(self) -> dict[str, int]:
        """Get count of observations for each station.
        
        Returns:
            dict[str, int]: Dictionary mapping station IDs to their observation counts.
        """
        return {station_id: len(entry.values) for station_id, entry in self.stations.items()}
    
    def has_data(self) -> bool:
        """Check if the response contains any data.
        
        Returns:
            bool: True if there is any data, False otherwise.
        """
        return any(entry.values for entry in self.stations.values())
    
    def get_average_values(self) -> dict[str, float]:
        """Get average value for each station.
        
        Returns:
            dict[str, float]: Dictionary mapping station IDs to their average values.
        """
        averages = {}
        for station_id, entry in self.stations.items():
            values = [v.value for v in entry.values if v.value is not None]
            if values:
                averages[station_id] = sum(values) / len(values)
        return averages
    
    def get_values_by_station_and_quality(self) -> dict[str, dict[str, list[float]]]:
        """Group values by station and quality code.
        
        Returns:
            dict[str, dict[str, list[float]]]: Nested dictionary with station_id -> quality_code -> values.
        """
        result = {}
        for station_id, entry in self.stations.items():
            station_data: dict[str, list[float]] = {}
            for value in entry.values:
                if value.value is not None:
                    if value.quality_code not in station_data:
                        station_data[value.quality_code] = []
                    station_data[value.quality_code].append(value.value)
            if station_data:
                result[station_id] = station_data
        return result