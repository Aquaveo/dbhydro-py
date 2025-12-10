# Standard library imports
from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    # Only import pandas for type checking (optional import at runtime)
    import pandas as pd

# Local imports
from dbhydro_py.models.responses.base import ApiResponseBase, Status
from dbhydro_py.utils import dataclass_from_dict

# Hierarchy of dataclasses representing the Time Series Response structure
"""
TimeSeriesResponse
│
├── time_series  (list)
│     │
│     └── [0]  (a single timeseries)
│           │
│           ├── source_info
│           │     ├── site_name
│           │     ├── site_code
│           │     │     ├── network
│           │     │     ├── agency_code
│           │     │     └── value
│           │     └── geo_location
│           │           └── geog_location
│           │                 ├── type
│           │                 ├── srs
│           │                 ├── latitude
│           │                 └── longitude
│           │
│           ├── period_of_record
│           │     ├── por_begin_date
│           │     ├── por_last_date
│           │     ├── provisional_begin_date
│           │     ├── provisional_last_date
│           │     ├── approved_begin_date
│           │     └── approved_last_date
│           │
│           ├── name
│           ├── description
│           ├── application_name
│           ├── recorder_class
│           ├── current_status
│           ├── time_series_id
│           ├── reference_elevation
│           │     └── values  (list)
│           │
│           ├── parameter
│           │     ├── parameter_code
│           │     │     ├── parameter_id
│           │     │     └── value
│           │     ├── parameter_name
│           │     ├── parameter_description
│           │     ├── unit
│           │     │     └── unit_code
│           │     └── no_data_value
│           │
│           ├── values  (list of observation points)
│           │     └── [0]
│           │           ├── qualifier
│           │           ├── quality_code
│           │           ├── date_time
│           │           ├── value
│           │           └── percent_available
│           │
│           └── summary  (optional - present in real-time responses)
│                 ├── min
│                 ├── max
│                 ├── mean
│                 ├── median
│                 ├── stddev
│                 └── num_of_records
│
└── status
      ├── status_code
      ├── status_message
      └── elapsed_time
"""

# -------------------------------
# GeoLocation Models
# -------------------------------

@dataclass
class GeogLocation:
    type: str
    srs: str
    latitude: float
    longitude: float


@dataclass
class GeoLocation:
    geog_location: GeogLocation = field(metadata={'json_key': 'geogLocation'})


# -------------------------------
# Source Info Models
# -------------------------------

@dataclass
class SiteCode:
    network: str
    agency_code: str = field(metadata={'json_key': 'agencyCode'})
    value: str


@dataclass
class SourceInfo:
    site_name: str = field(metadata={'json_key': 'siteName'})
    site_code: SiteCode = field(metadata={'json_key': 'siteCode'})
    geo_location: GeoLocation = field(metadata={'json_key': 'geoLocation'})


# -------------------------------
# Period of Record
# -------------------------------

@dataclass
class PeriodOfRecord:
    por_begin_date: Optional[str] = field(metadata={'json_key': 'porBeginDate'})
    por_last_date: Optional[str] = field(metadata={'json_key': 'porLastDate'})
    provisional_begin_date: Optional[str] = field(metadata={'json_key': 'provisionalBeginDate'})
    provisional_last_date: Optional[str] = field(metadata={'json_key': 'provisionalLastDate'})
    approved_begin_date: Optional[str] = field(metadata={'json_key': 'approvedBeginDate'})
    approved_last_date: Optional[str] = field(metadata={'json_key': 'approvedLastDate'})


# -------------------------------
# Parameter Models
# -------------------------------

@dataclass
class ParameterCode:
    parameter_id: str = field(metadata={'json_key': 'parameterID'})
    value: str


@dataclass
class Unit:
    unit_code: str = field(metadata={'json_key': 'unitCode'})


@dataclass
class Parameter:
    parameter_code: ParameterCode = field(metadata={'json_key': 'parameterCode'})
    parameter_name: str = field(metadata={'json_key': 'parameterName'})
    parameter_description: str = field(metadata={'json_key': 'parameterDescription'})
    unit: Unit
    no_data_value: float = field(metadata={'json_key': 'noDataValue'})


# -------------------------------
# Observation Value
# -------------------------------

@dataclass
class ObservationValue:
    qualifier: Optional[str]
    quality_code: Optional[str] = field(metadata={'json_key': 'qualityCode'})
    date_time: str = field(metadata={'json_key': 'dateTime'})
    value: Optional[float]
    percent_available: Optional[float] = field(metadata={'json_key': 'percentAvailable'})


# -------------------------------
# Summary Statistics (for real-time data)
# -------------------------------

@dataclass
class Summary:
    min: Optional[float]
    max: Optional[float]
    mean: Optional[float]
    median: Optional[float]
    stddev: Optional[float]
    num_of_records: Optional[int] = field(metadata={'json_key': 'numOfRecords'})


# -------------------------------
# Time Series (One Timeseries Entry)
# -------------------------------

@dataclass
class ReferenceElevation:
    values: list[float] = field(default_factory=list)


@dataclass
class TimeSeriesEntry:
    source_info: SourceInfo = field(metadata={'json_key': 'sourceInfo'})
    period_of_record: PeriodOfRecord = field(metadata={'json_key': 'periodOfRecord'})

    name: str
    description: str
    application_name: Optional[str] = field(metadata={'json_key': 'applicationName'})
    recorder_class: Optional[str] = field(metadata={'json_key': 'recorderClass'})
    current_status: Optional[str] = field(metadata={'json_key': 'currentStatus'})
    time_series_id: str = field(metadata={'json_key': 'timeSeriesId'})

    reference_elevation: ReferenceElevation = field(metadata={'json_key': 'referenceElevation'})
    parameter: Parameter

    values: list[ObservationValue] = field(default_factory=list)
    summary: Optional[Summary] = field(default=None)  # Present in real-time responses
    
    @classmethod
    def from_dict(cls, data: dict) -> 'TimeSeriesEntry':
        """Create TimeSeriesEntry from dict, ensuring values is never None and handling summary field."""
        # Ensure values is an empty list if None or missing
        if data.get('values') is None:
            data = data.copy()  # Don't modify the original
            data['values'] = []
        
        # Handle summary field conversion if present
        summary_data = data.get('summary')
        if summary_data is not None:
            # Create a copy of data and replace summary with Summary instance
            data = data.copy()
            data['summary'] = Summary(
                min=summary_data.get('min'),
                max=summary_data.get('max'),
                mean=summary_data.get('mean'),
                median=summary_data.get('median'),
                stddev=summary_data.get('stddev'),
                num_of_records=summary_data.get('numOfRecords')
            )
        
        return dataclass_from_dict(cls, data)  # type: ignore


# -------------------------------
# Root Response
# -------------------------------

@dataclass
class TimeSeriesResponse(ApiResponseBase):
    time_series: list[TimeSeriesEntry] = field(metadata={'json_key': 'timeSeries'}, default_factory=list)

    @classmethod
    def from_dict(cls, data: dict) -> 'TimeSeriesResponse':
        """
        Create a TimeSeriesResponse instance from a dictionary.
        
        Args:
            data (dict): The dictionary containing the time series response data.
            
        Returns:
            TimeSeriesResponse: The populated TimeSeriesResponse instance.
        """
        # Ensure timeSeries is an empty list if None or missing
        if data.get('timeSeries') is None:
            data = data.copy()  # Don't modify the original
            data['timeSeries'] = []
        
        return dataclass_from_dict(cls, data)  # type: ignore
    
    def to_dataframe(self, include_metadata: bool = False) -> 'pd.DataFrame':
        """Convert time series data to pandas DataFrame.
        
        Args:
            include_metadata (bool): If True, includes site info and parameter details as columns.
                                   If False (default), only includes datetime, value, and site_code.
        
        Returns:
            pd.DataFrame: DataFrame with time series data.
            
        Raises:
            ImportError: If pandas is not installed.
            
        Example:
            >>> response = api.get_time_series(['S123-R'], '2023-01-01', '2023-01-02')
            >>> df = response.to_dataframe()
            >>> print(df.head())
                        datetime  value site_code quality_code
            0  2023-01-01 00:00:00   12.5     S123-R         A
            1  2023-01-01 01:00:00   12.3     S123-R         A
        """
        # Import pandas here allowing it to be an optional dependency
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                'pandas is required for to_dataframe(). Install with: pip install pandas'
            )
        
        # Collect all data points from all time series
        records = []
        for time_series in self.time_series:
            # Get site identifier and parameter info
            site_code = time_series.source_info.site_code.value
            site_name = time_series.source_info.site_name
            parameter_code = time_series.parameter.parameter_code.value
            parameter_name = time_series.parameter.parameter_name
            unit_code = time_series.parameter.unit.unit_code
            
            # Extract observation values
            for observation in time_series.values:
                # Handle the non-standard datetime format (colon before milliseconds)
                try:
                    datetime_value = pd.to_datetime(observation.date_time)
                except (ValueError, pd.errors.ParserError):
                    # Fix the non-standard milliseconds format (:000 -> .000)
                    fixed_datetime = observation.date_time.replace(':000', '.000')
                    datetime_value = pd.to_datetime(fixed_datetime)
                
                record = {
                    'datetime': datetime_value,
                    'value': observation.value,
                    'site_code': site_code,
                }
                
                # Add quality information if available
                if observation.quality_code:
                    record['quality_code'] = observation.quality_code
                if observation.qualifier:
                    record['qualifier'] = observation.qualifier
                
                # Add metadata if requested
                if include_metadata:
                    record.update({
                        'site_name': site_name,
                        'parameter_code': parameter_code,
                        'parameter_name': parameter_name,
                        'unit_code': unit_code,
                    })
                
                records.append(record)
        
        # Create DataFrame and set datetime as index
        df = pd.DataFrame(records)
        
        # Ensure consistent DataFrame structure even when empty
        if df.empty:
            # Define the basic columns that will always be present
            basic_columns = ['datetime', 'value', 'site_code']
            
            # Add optional quality columns if any time series has them
            if any(hasattr(ts, 'values') and any(obs.quality_code for obs in ts.values) 
                   for ts in self.time_series):
                basic_columns.append('quality_code')
            if any(hasattr(ts, 'values') and any(obs.qualifier for obs in ts.values) 
                   for ts in self.time_series):
                basic_columns.append('qualifier')
            
            # Add metadata columns if requested
            if include_metadata:
                basic_columns.extend(['site_name', 'parameter_code', 'parameter_name', 'unit_code'])
            
            # Create empty DataFrame with proper column types
            df = pd.DataFrame(columns=basic_columns)
            df = df.astype({
                'datetime': 'datetime64[ns]',
                'value': 'float64',
                'site_code': 'object'
            })
            
            # Set datetime as index for single site (consistent with non-empty case)
            if len(self.time_series) == 1:
                df = df.set_index('datetime')
        else:
            df = df.sort_values('datetime').reset_index(drop=True)
            
            # For single site, can make datetime the index for easier time series analysis
            if len(self.time_series) == 1:
                df = df.set_index('datetime')
        
        return df

    def get_site_codes(self) -> list[str]:
        """Get list of all site codes in the response.
        
        Returns:
            list[str]: List of site codes (e.g., ['S123-R', 'S124-R']).
        """
        return [ts.source_info.site_code.value for ts in self.time_series]

    def get_site_names(self) -> list[str]:
        """Get list of all site names in the response.
        
        Returns:
            list[str]: List of site names.
        """
        return [ts.source_info.site_name for ts in self.time_series]

    def get_parameter_codes(self) -> list[str]:
        """Get list of all parameter codes in the response.
        
        Returns:
            list[str]: List of parameter codes (e.g., ['00065', '00060']).
        """
        return [ts.parameter.parameter_code.value for ts in self.time_series]

    def get_parameter_names(self) -> list[str]:
        """Get list of all parameter names in the response.
        
        Returns:
            list[str]: List of parameter names (e.g., ['Gage height', 'Discharge']).
        """
        return [ts.parameter.parameter_name for ts in self.time_series]

    def get_data_for_site(self, site_code: str) -> 'TimeSeriesEntry | None':
        """Get time series data for a specific site.
        
        Args:
            site_code (str): The site code to retrieve data for.
            
        Returns:
            TimeSeriesEntry | None: The time series entry for the site, or None if not found.
        """
        for ts in self.time_series:
            if ts.source_info.site_code.value == site_code:
                return ts
        return None

    def get_latest_values(self) -> dict[str, float]:
        """Get the most recent value for each site.
        
        Returns:
            dict[str, float]: Dictionary mapping site codes to their latest values.
        """
        latest_values = {}
        for ts in self.time_series:
            if ts.values:
                # Values are assumed to be in chronological order
                latest_value = ts.values[-1].value
                if latest_value is not None:
                    latest_values[ts.source_info.site_code.value] = latest_value
        return latest_values

    def get_value_ranges(self) -> dict[str, tuple[float, float]]:
        """Get min/max value ranges for each site.
        
        Returns:
            dict[str, tuple[float, float]]: Dictionary mapping site codes to (min, max) tuples.
        """
        ranges = {}
        for ts in self.time_series:
            if ts.values:
                valid_values = [obs.value for obs in ts.values if obs.value is not None]
                if valid_values:
                    ranges[ts.source_info.site_code.value] = (min(valid_values), max(valid_values))
        return ranges

    def get_date_range(self) -> tuple[str, str] | None:
        """Get the overall date range of all data in the response.
        
        Returns:
            tuple[str, str] | None: Tuple of (earliest_date, latest_date) or None if no data.
        """
        all_dates = []
        for ts in self.time_series:
            for obs in ts.values:
                if obs.date_time:
                    all_dates.append(obs.date_time)
        
        if not all_dates:
            return None
        
        # Sort dates (they're in string format but should sort correctly if ISO format)
        all_dates.sort()
        return (all_dates[0], all_dates[-1])

    def get_quality_summary(self) -> dict[str, dict[str, int]]:
        """Get summary of quality codes by site.
        
        Returns:
            dict[str, dict[str, int]]: Dictionary mapping site codes to quality code counts.
        """
        quality_summary = {}
        for ts in self.time_series:
            site_code = ts.source_info.site_code.value
            quality_counts: dict[str, int] = {}
            
            for obs in ts.values:
                if obs.quality_code:
                    quality_counts[obs.quality_code] = quality_counts.get(obs.quality_code, 0) + 1
            
            if quality_counts:
                quality_summary[site_code] = quality_counts
        
        return quality_summary

    def filter_by_quality(self, quality_codes: list[str]) -> 'TimeSeriesResponse':
        """Create a new response with only observations having specified quality codes.
        
        Args:
            quality_codes (list[str]): List of quality codes to include (e.g., ['A', 'P']).
            
        Returns:
            TimeSeriesResponse: New response with filtered data.
        """
        filtered_time_series = []
        
        for ts in self.time_series:
            # Filter values for this time series
            filtered_values = [
                obs for obs in ts.values 
                if obs.quality_code in quality_codes
            ]
            
            if filtered_values:
                # Create new TimeSeriesEntry with filtered values
                filtered_ts = TimeSeriesEntry(
                    source_info=ts.source_info,
                    period_of_record=ts.period_of_record,
                    name=ts.name,
                    description=ts.description,
                    application_name=ts.application_name,
                    recorder_class=ts.recorder_class,
                    current_status=ts.current_status,
                    time_series_id=ts.time_series_id,
                    reference_elevation=ts.reference_elevation,
                    parameter=ts.parameter,
                    values=filtered_values,
                    summary=ts.summary
                )
                filtered_time_series.append(filtered_ts)
        
        # Create new response with filtered time series
        return TimeSeriesResponse(
            status=self.status,
            time_series=filtered_time_series
        )

    def has_data(self) -> bool:
        """Check if the response contains any observation data.
        
        Returns:
            bool: True if there are any observations, False otherwise.
        """
        return any(ts.values for ts in self.time_series)

    def get_data_count(self) -> int:
        """Get total number of observations across all sites.
        
        Returns:
            int: Total count of observations.
        """
        return sum(len(ts.values) for ts in self.time_series)

    def get_data_counts_by_site(self) -> dict[str, int]:
        """Get count of observations for each site.
        
        Returns:
            dict[str, int]: Dictionary mapping site codes to observation counts.
        """
        return {
            ts.source_info.site_code.value: len(ts.values) 
            for ts in self.time_series
        }
