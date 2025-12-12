# Standard library imports
from datetime import datetime
from typing import Literal, cast
import warnings

# Local imports
from dbhydro_py.exceptions import DbHydroException
from dbhydro_py.models.responses import TimeSeriesResponse, PointResponse, SynchronizeResponse
from dbhydro_py.models.responses.aggregate import AggregateResponse
from dbhydro_py.models.responses.interpolate import InterpolateResponse
from dbhydro_py.models.responses.time_series import PeriodOfRecord
from dbhydro_py.rest_adapters.rest_adapter_base import RestAdapterBase
from dbhydro_py.utils import dataclass_from_dict

# Get package version dynamically
try:
    from importlib.metadata import version
    __version__ = version('dbhydro-py')
except ImportError:
    # Fallback for Python < 3.8 or if package not installed
    __version__ = 'unknown'


class DbHydroApi:
    """Client for interacting with the South Florida Water Management District's DBHydro API.
    
    Provides methods to retrieve time series data, real-time monitoring data, and other
    hydrological information from the DBHydro database.
    
    Args:
        rest_adapter (RestAdapterBase): HTTP client adapter for making API requests.
        client_id (str): API client ID for authentication.
        client_secret (str): API client secret for authentication.
        api_version (int, optional): API version to use. Defaults to 1.
    
    Examples:
        >>> # Using custom adapter
        >>> api = DbHydroApi(custom_adapter, "client_id", "secret")
        >>> 
        >>> # Using default adapter (recommended)
        >>> api = DbHydroApi.with_default_adapter("client_id", "secret")
    """
    
    def __init__(self, rest_adapter: RestAdapterBase, client_id: str, client_secret: str, api_version: int = 1):
        self.rest_adapter = rest_adapter
        self._client_secret = client_secret
        self._client_id = client_id
        
        self.base_url = f'https://dataservice-proxy.api.sfwmd.gov/v{api_version}/ext/data/'
     
    @classmethod
    def with_default_adapter(cls, client_id: str, client_secret: str, api_version: int = 1) -> 'DbHydroApi':
        """Create a DbHydroApi instance with the default RestAdapterRequests adapter.
        
        Args:
            client_id (str): The client ID for authentication.
            client_secret (str): The client secret for authentication.
            api_version (int, optional): The API version to use. Defaults to 1.
        
        Returns:
            DbHydroApi: A new instance of DbHydroApi using the default RestAdapterRequests.
        """
        from dbhydro_py.rest_adapters import RestAdapterRequests
        return cls(RestAdapterRequests(), client_id, client_secret, api_version)
     
    def _check_api_response(self, response_data: dict, http_status_code: int) -> None:
        """Check API response for errors and raise enhanced DbHydroException if found.
        
        Args:
            response_data (dict): The JSON response data.
            http_status_code (int): The HTTP status code.
            
        Raises:
            DbHydroException: If API-level or HTTP-level error is detected.
        """
        # First check for API-level error details
        api_error = self._extract_api_error(response_data)
        if api_error:
            raise DbHydroException(
                message=f"API request failed: {api_error['message']}",
                http_status_code=http_status_code,
                api_status_code=api_error.get('status_code'),
                api_status_message=api_error.get('message'),
                elapsed_time=api_error.get('elapsed_time')
            )
        
        # If no API-level error but HTTP status indicates error, throw basic HTTP error
        if http_status_code < 200 or http_status_code >= 300:
            raise DbHydroException(
                message=f"HTTP request failed with status {http_status_code}",
                http_status_code=http_status_code
            )

    def _extract_api_error(self, response_data: dict) -> dict | None:
        """Extract API-level error information from response JSON.
        
        This method looks for error patterns in common DBHydro response structures.
        
        Args:
            response_data (dict): The JSON response data.
            
        Returns:
            dict | None: Dictionary with error details or None if no API error found.
        """
        try:
            # DBHydro APIs typically wrap responses in endpoint-specific objects
            # Look for status objects in any top-level response wrapper
            for key, value in response_data.items():
                if isinstance(value, dict) and 'status' in value:
                    status = value['status']
                    if isinstance(status, dict):
                        status_code = status.get('statusCode')
                        status_message = status.get('statusMessage', '')
                        
                        # Consider it an error if status code is present and not 2xx
                        if status_code and (status_code < 200 or status_code >= 300):
                            return {
                                'message': status_message,
                                'status_code': status_code,
                                'elapsed_time': status.get('elapsedTime')
                            }
                        
                        # Also check for error-like messages even with 2xx codes
                        if 'error' in status_message.lower():
                            return {
                                'message': status_message,
                                'status_code': status_code,
                                'elapsed_time': status.get('elapsedTime')
                            }
                            
        except (AttributeError, KeyError, TypeError):
            # If the response structure is unexpected, return None
            pass
            
        return None

    def _parse_date(self, date_input: datetime | str) -> str:
        """Parse date input and return API-formatted string.
        
        Accepts multiple formats and auto-completes missing time information:
        - datetime objects -> formatted string
        - "2025-12-04" -> "2025-12-0400:00:00:000"
        - "2025-12-04 10:30" -> "2025-12-0410:30:00:000"
        - "2025-12-04T10:30" -> "2025-12-0410:30:00:000"
        - "2025-12-0410:30" -> "2025-12-0410:30:00:000"
        - "2025-12-0410:30:45:123" -> "2025-12-0410:30:45:123"
        
        Args:
            date_input (datetime | str): The input date.
            
        Returns:
            str: The date formatted as "YYYY-MM-DDHH:MM:SS:SSS".
        """
        # Datetime object given
        if isinstance(date_input, datetime):
            # Convert datetime to API format: YYYY-MM-DDHH:MM:SS:SSS
            return date_input.strftime('%Y-%m-%d%H:%M:%S:%f')[:-3]
        
        # String given
        date_str = str(date_input).strip()
        
        # Handle formats with T separator and convert to API format (remove T)
        if 'T' in date_str:
            date_part, time_part = date_str.split('T', 1)
            date_str = date_part + time_part  # Remove T separator
        
        # Handle formats with space separator
        elif ' ' in date_str and len(date_str) > 10:
            date_part, time_part = date_str.split(' ', 1)
            date_str = date_part + time_part  # Remove space separator
        
        # Check if we have time information (string longer than date part)
        if len(date_str) > 10:
            # Extract date and time parts
            date_part = date_str[:10]  # YYYY-MM-DD
            time_part = date_str[10:]  # Everything after date
            
            # Count colons to determine time completeness
            colon_count = time_part.count(':')
            
            # Hours, minutes, seconds, and milliseconds already present
            if colon_count == 3:
                return date_str
            # Missing milliseconds
            elif colon_count == 2:
                return f"{date_str}:000"
            # Missing seconds and milliseconds
            elif colon_count == 1:
                return f"{date_str}:00:000"
            # Hours only - missing minutes, seconds, milliseconds
            else:
                return f"{date_str}:00:00:000"
        
        # Only date given: 2025-12-04 -> add default time (start of day)
        elif len(date_str) == 10 and date_str.count('-') == 2:
            return f"{date_str}00:00:00:000"
        
        raise ValueError(
            f"Invalid date format: '{date_str}'. "
            f"Expected formats: 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM', 'YYYY-MM-DDTHH:MM', "
            f"'YYYY-MM-DDHH:MM', 'YYYY-MM-DD HH:MM:SS', 'YYYY-MM-DD HH:MM:SS:SSS', or datetime object."
        )

    def _handle_date_parameters(self, date_start: datetime | str, date_end: datetime | str) -> tuple[str, str]:
        """Puts date parameters into the correct format and validates the range.
        
        Args:
            date_start (datetime | str): The start date in API format.
            date_end (datetime | str): The end date in API format.
        """
        # Parse and format dates for API
        datetime_start = self._parse_date(date_start)
        datetime_end = self._parse_date(date_end)
        
        # Validate date range
        if datetime.strptime(datetime_start, '%Y-%m-%d%H:%M:%S:%f') > datetime.strptime(datetime_end, '%Y-%m-%d%H:%M:%S:%f'):
            raise ValueError("The 'date_start' must be earlier or equal to 'date_end'.")
        
        return datetime_start, datetime_end

    def _validate_calculation_parameters(
        self, 
        calculation: str | None, 
        timespan_unit: str | None, 
        timespan_value: int | None = None,
        valid_calculations: set[str] | None = None, 
        valid_timespan_units: set[str] | None = None
    ) -> None:
        """Validate calculation, timespan unit, and timespan value parameters.
        
        Args:
            calculation (str | None): The calculation type.
            timespan_unit (str | None): The timespan unit.
            timespan_value (int | None): The timespan value (must be positive if provided).
            valid_calculations (set[str] | None): Allowed calculation types. Uses default if None.
            valid_timespan_units (set[str] | None): Allowed timespan units. Uses default if None.
            
        Raises:
            ValueError: If validation fails.
        """
        # Set defaults if not provided
        if valid_calculations is None:
            valid_calculations = {'MEAN', 'MAX', 'MIN', 'SUM'}
        if valid_timespan_units is None:
            valid_timespan_units = {'YEAR', 'MONTH', 'WEEK', 'DAY', 'HOUR', 'MINUTE', 'SECOND'}
        
        if calculation is None:
            # timespan_unit must be None if calculation is None
            if timespan_unit is not None:
                raise ValueError("If 'calculation' is None, 'timespan_unit' must also be None.")
        else:
            # Validate calculation type
            if calculation not in valid_calculations:
                raise ValueError(f"Invalid calculation type: '{calculation}'. Must be one of: {', '.join(sorted(valid_calculations))}.")
            
            # timespan_unit must be provided if calculation is given
            if timespan_unit is None:
                raise ValueError("If 'calculation' is provided, 'timespan_unit' must also be provided.")
            
            # Validate timespan_unit
            if timespan_unit not in valid_timespan_units:
                raise ValueError(f"Invalid timespan_unit: '{timespan_unit}'. Must be one of: {', '.join(sorted(valid_timespan_units))}.")
        
        # Validate timespan_value if provided
        if timespan_value is not None:
            if not isinstance(timespan_value, int):
                raise ValueError(f"Invalid timespan_value: {timespan_value}. Must be a positive integer.")
            if timespan_value <= 0:
                raise ValueError(f"Invalid timespan_value: {timespan_value}. Must be a positive integer.")

    def _perform_request(self, full_url: str, params: dict) -> dict:
        """Helper method to perform GET requests using the REST adapter.
        
        Args:
            full_url (str): The full API endpoint URL.
            params (dict): The query parameters for the request.
            
        Returns:
            dict: The raw response data from the GET request.
        """
        # Add API-specific headers
        headers = {
            'User-Agent': f'dbhydro-py/{__version__}'
        }
        
        # Make the GET request
        result = self.rest_adapter.get(endpoint=full_url, params=params, headers=headers)
        
        # Check for network-level errors (status_code=0 indicates RequestException from adapter)
        if result.status_code == 0:
            raise DbHydroException(result.message)
        
        # Check for API-level errors first (handles both HTTP errors and API-level errors)
        self._check_api_response(result.data, result.status_code)
        
        # Return the full response data for endpoint-specific processing
        return result.data
    
    def get_time_series(
        self,
        site_ids: list[str],
        date_start: datetime | str,
        date_end: datetime | str,
        calculation: Literal['MEAN', 'MAX', 'MIN', 'SUM'] | None = None,
        timespan_unit: Literal['YEAR', 'MONTH', 'WEEK', 'DAY', 'HOUR', 'MINUTE', 'SECOND'] | None = None,
        timespan_value: int = 1
    ) -> TimeSeriesResponse:
        """
        Retrieve time series data from the DBHydro API using the timeseries endpoint.
        
        Args:
            site_ids (list[str]): List of site IDs to retrieve data for. E.g., ['S123-R'].
            date_start (datetime | str): Start date. Accepts multiple formats:
                - datetime object
                - "YYYY-MM-DD" (time defaults to 00:00:00:000)
                - "YYYY-MM-DD HH:MM" (seconds/milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS" (milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS:SSS" (full format)
                - Also accepts ISO format with T separator (converted automatically)
            date_end (datetime | str): End date (same formats as date_start).
            calculation (Literal['MEAN', 'MAX', 'MIN', 'SUM'] | None, optional): Calculation type for the time series data. If None, no calculation is applied.
            timespan_unit (Literal['YEAR', 'MONTH', 'WEEK', 'DAY', 'HOUR', 'MINUTE', 'SECOND'] | None, optional): Unit of time for the timespan. Must be provided if calculation is given.
            timespan_value (int, optional): Value for the timespan unit.
            
        Returns:
            TimeSeriesResponse: Parsed response containing time series data and status.
        """
        # Build the request URL
        endpoint = 'timeseries'
        full_url = f'{self.base_url}{endpoint}'
        
        # Validate site_ids
        if not site_ids:
            raise ValueError("The 'site_ids' list cannot be empty.")
        
        for site_id in site_ids:
            if not isinstance(site_id, str) or not site_id.strip():
                raise ValueError(f"Invalid site ID: '{site_id}'. Each site ID must be a non-empty string.")
        
        # Handle and validate date parameters
        datetime_start, datetime_end = self._handle_date_parameters(date_start, date_end)
        
        # Validate calculation parameters
        self._validate_calculation_parameters(
            calculation=calculation,
            timespan_unit=timespan_unit,
            timespan_value=timespan_value
        )
        
        # Build the request parameters
        params = {
            'names': ','.join(site_ids),
            'beginDateTime': datetime_start,
            'endDateTime': datetime_end,
            'calculation': calculation,
            'timespanUnit': timespan_unit,
            'timespanValue': timespan_value,
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'format': 'json'
        }
        
        # Make the request using the helper
        response_data = self._perform_request(full_url, params)
        
        # Extract the time series response
        time_series_data = response_data.get('timeSeriesResponse', {})
        return TimeSeriesResponse.from_dict(time_series_data)

    def get_daily_data(
        self,
        identifiers: list[str],
        identifier_type: Literal['timeseries', 'station', 'id'],
        date_start: datetime | str,
        date_end: datetime | str,
        requested_datum: Literal['NGVD29', 'NAVD88'] = 'NGVD29',
        include_summary: bool = False
    ) -> TimeSeriesResponse:
        """
        Retrieve daily data from the DBHydro API using the dailydata endpoint.
        
        Args:
            identifiers (list[str]): The identifier values based on the identifier_type.
            identifier_type (Literal['timeseries', 'station', 'id']): The type of identifier provided.
            date_start (datetime | str): Start date. Accepts multiple formats:
                - datetime object
                - "YYYY-MM-DD" (time defaults to 00:00:00:000)
                - "YYYY-MM-DD HH:MM" (seconds/milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS" (milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS:SSS" (full format)
                - Also accepts ISO format with T separator (converted automatically)
            date_end (datetime | str): End date (same formats as date_start).
            requested_datum (Literal['NGVD29', 'NAVD88'], optional): Datum for elevation data. One of 'NGVD29', 'NAVD88' Default is 'NGVD29'.
            include_summary (bool, optional): Whether to include summary statistics. Default is False.
                NOTE: Setting this to True currently causes API 503 errors (known API bug).
            
        Returns:
            TimeSeriesResponse: Parsed response containing daily time series data.
        """
        # Build the request URL
        endpoint = 'dailydata'
        full_url = f'{self.base_url}{endpoint}'
        
        # validate identifiers
        if not identifiers:
            raise ValueError("The 'identifiers' list cannot be empty.")
        
        for identifier in identifiers:
            if not isinstance(identifier, str) or not identifier.strip():
                raise ValueError(f"Invalid identifier: '{identifier}'. Each identifier must be a non-empty string.")
        
        # Validate identifier_type at runtime
        valid_identifier_types = {'timeseries', 'station', 'id'}
        if identifier_type not in valid_identifier_types:
            raise ValueError(f"Invalid identifier_type: '{identifier_type}'. Must be one of: {', '.join(valid_identifier_types)}.")
        
        # Validate requested_datum at runtime  
        valid_datums = {'NGVD29', 'NAVD88'}
        if requested_datum not in valid_datums:
            raise ValueError(f"Invalid requested_datum: '{requested_datum}'. Must be one of: {', '.join(valid_datums)}.")
        
        # Handle and validate date parameters
        datetime_start, datetime_end = self._handle_date_parameters(date_start, date_end)
        
        # Handle API bug: includeSummary=Y causes 503 errors
        if include_summary:
            warnings.warn(
                'includeSummary=True currently causes API 503 errors. '
                'Setting to False to avoid request failure.',
                UserWarning
            )
            include_summary = False
        
        # Build the request parameters
        params = {
            identifier_type: ','.join(identifiers),
            'beginDateTime': datetime_start,
            'endDateTime': datetime_end,
            'format': 'json',
            'requestedDatum': requested_datum,
            'includeSummary': 'Y' if include_summary else 'N',
            'client_id': self._client_id,
            'client_secret': self._client_secret
        }
        
        # Make the request using the helper
        response_data = self._perform_request(full_url, params)
        
        # Extract the time series response (same structure as regular timeseries)
        time_series_data = response_data.get('timeSeriesResponse', {})
        return TimeSeriesResponse.from_dict(time_series_data)
    
    def get_aggregate(
        self,
        station_id: str,
        date_start: datetime | str,
        date_end: datetime | str,
        calculation: Literal['MEAN', 'MAX', 'MIN', 'SUM', 'MEDI'] | None = None,
        timespan_unit: Literal['YEAR', 'MONTH', 'WEEK', 'DAY', 'HOUR', 'MINUTE', 'SECOND'] | None = None,
        timespan_value: int = 1
    ) -> AggregateResponse:
        """Retrieve aggregate statistical data from the DBHydro API using the aggregate endpoint.
        
        Args:
            station_id (str): The station ID to retrieve data for.
            date_start (datetime | str): Start date. Accepts multiple formats:
                - datetime object
                - "YYYY-MM-DD" (time defaults to 00:00:00:000)
                - "YYYY-MM-DD HH:MM" (seconds/milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS" (milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS:SSS" (full format)
                - Also accepts ISO format with T separator (converted automatically)
            date_end (datetime | str): End date (same formats as date_start).
            calculation (Literal['MEAN', 'MAX', 'MIN', 'SUM', 'MEDI'] | None, optional): Calculation type for the time series data. If None, no calculation is applied.
            timespan_unit (Literal['YEAR', 'MONTH', 'WEEK', 'DAY', 'HOUR', 'MINUTE', 'SECOND'] | None, optional): Unit of time for the timespan. Must be provided if calculation is given.
            timespan_value (int, optional): Value for the timespan unit.
            
        Returns:
            AggregateResponse: Parsed response containing aggregate intervals with statistical calculations.
        """
        # Build the request URL
        endpoint = 'aggregate'
        full_url = f'{self.base_url}{endpoint}'
        
        # Validate station_id
        if not isinstance(station_id, str) or not station_id.strip():
            raise ValueError(f"Invalid station ID: '{station_id}'. Station ID must be a non-empty string.")
        
        # Handle and validate date parameters
        datetime_start, datetime_end = self._handle_date_parameters(date_start, date_end)
        
        # Validate calculation parameters
        self._validate_calculation_parameters(
            calculation=calculation,
            timespan_unit=timespan_unit,
            timespan_value=timespan_value,
            valid_calculations={'MEAN', 'MAX', 'MIN', 'SUM', 'MEDI'},
            valid_timespan_units={'YEAR', 'MONTH', 'WEEK', 'DAY', 'HOUR', 'MINUTE', 'SECOND'}
        )
        
        # Build the request parameters
        params = {
            'stationId': station_id,
            'beginDateTime': datetime_start,
            'endDateTime': datetime_end,
            'calculation': calculation,
            'timespanUnit': timespan_unit,
            'timespanValue': timespan_value,
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'format': 'json'
        }
        
        # Make the request using the helper
        response_data = self._perform_request(full_url, params)
        
        # Return the aggregate response directly
        return AggregateResponse.from_dict(response_data)

    def get_interpolate(self, station_id: str, date_time: datetime | str) -> InterpolateResponse:
        """Retrieve interpolated value for a specific station and datetime.
        
        Args:
            station_id (str): The site ID to retrieve data for.
            date_time (datetime | str): The date and time for interpolation. Accepts multiple formats:
                - datetime object
                - "YYYY-MM-DD" (time defaults to 00:00:00:000)
                - "YYYY-MM-DD HH:MM" (seconds/milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS" (milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS:SSS" (full format)
                - Also accepts ISO format with T separator (converted automatically)
        
        Returns:
            InterpolateResponse: Parsed response containing interpolated data points.
        """
        # Build the request URL
        endpoint = 'interpolate'
        full_url = f'{self.base_url}{endpoint}'
        
        # Validate station_id
        if not isinstance(station_id, str) or not station_id.strip():
            raise ValueError(f"Invalid station ID: '{station_id}'. Station ID must be a non-empty string.")
        
        # Handle and validate date_time parameter
        datetime_value = self._parse_date(date_time)
        
        # Build the request parameters
        params = {
            'stationId': station_id,
            'dateTime': datetime_value,
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'format': 'json'
        }
        
        # Make the request using the helper
        response_data = self._perform_request(full_url, params)
        
        # Return the interpolate response directly
        return InterpolateResponse.from_dict(response_data)

    def get_real_time(
        self,
        identifiers: list[str],
        identifier_type: Literal['sites', 'timeseries'],
        status: str | None = None
    ) -> TimeSeriesResponse:
        """Retrieve real-time data from the DBHydro API using the realtime endpoint.

        Args:
            identifiers (list[str]): The identifier values based on the identifier_type.
            identifier_type (Literal['sites', 'timeseries']): The type of identifier provided.
            status (str | None, optional): Filter by status (optional). Values such as 'A', 'I', 'D'. Note: Has no effect when identifier_type is 'timeseries'.
            
        Returns:
            TimeSeriesResponse: Parsed response containing real-time data.
        """
        # Build the request URL
        endpoint = 'realtime'
        full_url = f'{self.base_url}{endpoint}'
        
        # Validate identifiers
        if not identifiers:
            raise ValueError("The 'identifier' list cannot be empty.")
        
        for id_value in identifiers:
            if not isinstance(id_value, str) or not id_value.strip():
                raise ValueError(f"Invalid identifier: '{id_value}'. Each identifier must be a non-empty string.")
        
        # Validate identifier_type at runtime
        valid_identifier_types = {'sites', 'timeseries'}
        if identifier_type not in valid_identifier_types:
            raise ValueError(f"Invalid identifier_type: '{identifier_type}'. Must be one of: {', '.join(valid_identifier_types)}.")
        
        # Build the request parameters
        params = {
            identifier_type: ','.join(identifiers),
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'format': 'json'
        }
        
        # Add status parameter if provided
        if status is not None:
            params['status'] = status
        
        # Make the request using the helper
        response_data = self._perform_request(full_url, params)
        
        # Extract the time series response
        time_series_data = response_data.get('timeSeriesResponse', {})
        return TimeSeriesResponse.from_dict(time_series_data)

    def get_period_of_record(self, station_id: str) -> PeriodOfRecord:
        """
        Retrieve the period of record for a specific station using the por endpoint.
        
        Provides the start and end dates for the given station.
        
        Args:
            station_id (str): The station ID to retrieve the period of record for.
        
        Returns:
            PeriodOfRecord: Parsed response containing the period of record information.
        """
        # Build the request URL
        endpoint = 'por'
        full_url = f'{self.base_url}{endpoint}'
        
        # Validate station_id
        if not isinstance(station_id, str) or not station_id.strip():
            raise ValueError(f"Invalid station ID: '{station_id}'. Station ID must be a non-empty string.")
        
        # Build the request parameters
        params = {
            'stationId': station_id,
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'format': 'json'
        }
        
        # Make the request using the helper
        response_data = self._perform_request(full_url, params)
        
        # Extract the period of record response
        por_data = response_data.get('periodOfRecord', {})
        return cast(PeriodOfRecord, dataclass_from_dict(PeriodOfRecord, por_data))

    def get_nexrad_pixel_data(
        self,
        pixel_ids: list[str],
        date_start: datetime | str,
        date_end: datetime | str,
        frequency: Literal['H', 'D', 'M', 'Y', 'E'],
        include_zero: bool = False
    ) -> TimeSeriesResponse:
        """
        Retrieve NEXRAD pixel data from the DBHydro API using the nexrad endpoint.
        
        Args:
            pixel_ids (list[str]): List of pixel IDs to retrieve data for.
            date_start (datetime | str): Start date. Accepts multiple formats:
                - datetime object
                - "YYYY-MM-DD" (time defaults to 00:00:00:000)
                - "YYYY-MM-DD HH:MM" (seconds/milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS" (milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS:SSS" (full format)
                - Also accepts ISO format with T separator (converted automatically)
            date_end (datetime | str): End date (same formats as date_start).
            frequency (Literal['H', 'D', 'M', 'Y', 'E']): Frequency of the data.
            include_zero (bool, optional): Whether to include zero values in the data. Default is False.
        
        Returns:
            TimeSeriesResponse: Parsed response containing NEXRAD pixel data.
        """
        # Build the request URL
        endpoint = 'nexrad'
        full_url = f'{self.base_url}{endpoint}'
        
        # Validate pixel_ids
        if len(pixel_ids) == 0:
            raise ValueError("At least one pixel_id must be provided.")
        
        for pixel_id in pixel_ids:
            if not isinstance(pixel_id, str) or not pixel_id.strip():
                raise ValueError(f"Invalid pixel_id: '{pixel_id}'. Must be a non-empty string.")
        
        # Handle and validate date parameters
        datetime_start, datetime_end = self._handle_date_parameters(date_start, date_end)
        
        # Validate frequency
        valid_frequencies = {'H', 'D', 'M', 'Y', 'E'}
        if frequency not in valid_frequencies:
            raise ValueError(f"Invalid frequency: '{frequency}'. Must be one of: {', '.join(valid_frequencies)}.")
        
        # Build the request parameters
        params = {
            'pixelId': ','.join(pixel_ids),
            'polygonType': 0,
            'beginDateTime': datetime_start,
            'endDateTime': datetime_end,
            'frequency': frequency,
            'incZero': 'Y' if include_zero else 'N',
            'format': 'json',
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'format': 'json'
        }
        
        # Make the request using the helper
        response_data = self._perform_request(full_url, params)
        
        # Extract and return the NEXRAD pixel data
        time_series_data = response_data.get('timeSeriesResponse', {})
        return TimeSeriesResponse.from_dict(time_series_data)

    def get_nexrad_polygon_data(
        self,
        identifiers: list[str],
        identifier_type: Literal['polygonId', 'polygonName'],
        polygon_type: Literal[1, 2, 3, 4, 5, 6, 7, 8, 9],
        date_start: datetime | str,
        date_end: datetime | str,
        frequency: Literal['15', 'H', 'D', 'M', 'Y', 'E'],
        include_zero: bool = False
    ) -> TimeSeriesResponse:
        """
        Retrieve NEXRAD polygon data from the DBHydro API using the nexrad endpoint.
        
        Args:
            identifiers (list[str]): List of polygon IDs or names to retrieve data for.
            identifier_type (Literal['polygonId', 'polygonName']): The type of identifiers provided.
            date_start (datetime | str): Start date. Accepts multiple formats:
                - datetime object
                - "YYYY-MM-DD" (time defaults to 00:00:00:000)
                - "YYYY-MM-DD HH:MM" (seconds/milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS" (milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS:SSS" (full format)
                - Also accepts ISO format with T separator (converted automatically)
            date_end (datetime | str): End date (same formats as date_start).
            frequency (Literal['15', 'H', 'D', 'M', 'Y', 'E']): Frequency of the data.
            include_zero (bool, optional): Whether to include zero values in the data. Default is False.
            
        Returns:
            TimeSeriesResponse: Parsed response containing NEXRAD polygon data.
        """
        # Build the request URL
        endpoint = 'nexrad'
        full_url = f'{self.base_url}{endpoint}'
        
        # Validate identifiers
        if len(identifiers) == 0:
            raise ValueError("At least one identifier must be provided.")
        
        for identifier in identifiers:
            if not isinstance(identifier, str) or not identifier.strip():
                raise ValueError(f"Invalid identifier: '{identifier}'. Must be a non-empty string.")
        
        # Validate identifier_type
        valid_identifier_types = {'polygonId', 'polygonName'}
        if identifier_type not in valid_identifier_types:
            raise ValueError(f"Invalid identifier_type: '{identifier_type}'. Must be one of: {', '.join(valid_identifier_types)}.")
        
        # Validate polygon_type
        if polygon_type < 1 or polygon_type > 9:
            raise ValueError("Invalid polygon_type. Must be an integer between 1 and 9 inclusive.")
        
        # Handle and validate date parameters
        datetime_start, datetime_end = self._handle_date_parameters(date_start, date_end)
        
        # Validate frequency
        valid_frequencies = {'15', 'H', 'D', 'M', 'Y', 'E'}
        if frequency not in valid_frequencies:
            raise ValueError(f"Invalid frequency: '{frequency}'. Must be one of: {', '.join(valid_frequencies)}.")
        
        # Build the request parameters
        params = {
            identifier_type: ','.join(identifiers),
            'polygonType': polygon_type,
            'beginDateTime': datetime_start,
            'endDateTime': datetime_end,
            'frequency': frequency,
            'incZero': 'Y' if include_zero else 'N',
            'format': 'json',
            'client_id': self._client_id,
            'client_secret': self._client_secret,
        }
        
        # Make the request using the helper
        response_data = self._perform_request(full_url, params)
        
        # Extract and return the NEXRAD polygon data
        time_series_data = response_data.get('timeSeriesResponse', {})
        return TimeSeriesResponse.from_dict(time_series_data)

    def get_time_series_arithmetic(
        self,
        id: str,
        timestamp: datetime | str
    ) -> PointResponse:
        """
        Retrieve time series arithmetic data from the DBHydro API using the tsarithmetic endpoint.
        
        Args:
            id (str): The time series identifier for the arithmetic calculation.
            timestamp (datetime | str): The timestamp for the calculation. Accepts multiple formats:
                - datetime object
                - "YYYY-MM-DD" (time defaults to 00:00:00:000)
                - "YYYY-MM-DD HH:MM" (seconds/milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS" (milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS:SSS" (full format)
                - Also accepts ISO format with T separator (converted automatically)
                
        Returns:
            PointResponse: Parsed response containing arithmetic calculation results.
        """
        # Build the request URL
        endpoint = 'tsarithmetic'
        full_url = f'{self.base_url}{endpoint}'
        
        # Validate id
        if not isinstance(id, str) or not id.strip():
            raise ValueError(f"Invalid id: '{id}'. ID must be a non-empty string.")
        
        # Handle and validate timestamp parameter
        datetime_value = self._parse_date(timestamp)
        
        # Build the request parameters
        params = {
            'id': id,
            'timestamp': datetime_value,
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'format': 'json'
        }
        
        # Make the request using the helper
        response_data = self._perform_request(full_url, params)
        
        # Extract the point response
        point_data = response_data.get('pointResponse', {})
        return PointResponse.from_dict(point_data)
    
    def get_synchronize(
        self,
        time_series_names: list[str],
        date_start: datetime | str,
        date_end: datetime | str,
        requested_datum: Literal['NGVD29', 'NAVD88'] | None = None
    ) -> SynchronizeResponse:
        """
        Retrieve data for each unique timestamp for the given time series within the given date range using the synchronize endpoint.
        
        Args:
            time_series_names (list[str]): List of time series names (station ids).
            date_start (datetime | str): Start date. Accepts multiple formats:
                - datetime object
                - "YYYY-MM-DD" (time defaults to 00:00:00:000)
                - "YYYY-MM-DD HH:MM" (seconds/milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS" (milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS:SSS" (full format)
                - Also accepts ISO format with T separator (converted automatically)
            date_end (datetime | str): End date (same formats as date_start).
            requested_datum (Literal['NGVD29', 'NAVD88'] | None, optional): Desired Datum of results. One of 'NGVD29', 'NAVD88'. Default is the District's default datum.
        """
        # Build the request URL
        endpoint = 'synchronize'
        full_url = f'{self.base_url}{endpoint}'
        
        # Validate time_series_names
        if not time_series_names:
            raise ValueError("The 'time_series_names' list cannot be empty.")
        
        for name in time_series_names:
            if not isinstance(name, str) or not name.strip():
                raise ValueError(f"Invalid time series name: '{name}'. Each name must be a non-empty string.")
        
        # Handle and validate date parameters
        datetime_start, datetime_end = self._handle_date_parameters(date_start, date_end)
        
        # Validate requested_datum at runtime if provided
        valid_datums = {'NGVD29', 'NAVD88'}
        if requested_datum is not None and requested_datum not in valid_datums:
            raise ValueError(f"Invalid requested_datum: '{requested_datum}'. Must be one of: {', '.join(valid_datums)}.")

        # Build the request parameters
        params = {
            'timeseries': ','.join(time_series_names),
            'beginDateTime': datetime_start,
            'endDateTime': datetime_end,
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'format': 'json'
        }
        
        # Add requested_datum if provided
        if requested_datum is not None:
            params['requestedDatum'] = requested_datum
        
        # Make the request using the helper
        response_data = self._perform_request(full_url, params)
        
        # Return the synchronize response
        return SynchronizeResponse.from_dict(response_data)
    
    def get_water_quality(
        self,
        project_code: str | None = None,
        test_number: int | None = None,
        station: str | None = None,
        date_start: datetime | str | None = None,
        date_end: datetime | str | None = None,
        exclude_flagged_results: bool = False
    ) -> None:
        """
        Retrieve water quality data from the DBHydro API using the waterquality endpoint.
        
        Args:
            project_code (str | None, optional): The project code to filter results. Ex: '8SQM'
            test_number (int | None, optional): The test number to filter results. Ex: 7
            station (str | None, optional): The station ID to filter results. Ex: G211
            date_start (datetime | str | None, optional): Start date. Accepts multiple formats:
                - datetime object
                - "YYYY-MM-DD" (time defaults to 00:00:00:000)
                - "YYYY-MM-DD HH:MM" (seconds/milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS" (milliseconds auto-added)
                - "YYYY-MM-DD HH:MM:SS:SSS" (full format)
                - Also accepts ISO format with T separator (converted automatically)
            date_end (datetime | str | None, optional): End date (same formats as date_start).
            exclude_flagged_results (bool, optional): Whether to exclude flagged results. Default is False.
            
        Returns:
            None: Placeholder for actual response model once defined.
        """
        
        # Build the request URL
        endpoint = 'waterquality'
        full_url = f'{self.base_url}{endpoint}'
        
        # Validate at least one search parameter is provided
        has_project_code = project_code and project_code.strip()
        has_test_number = test_number is not None
        has_station = station and station.strip()
        
        if not any([has_project_code, has_test_number, has_station]):
            raise ValueError("At least one search parameter is required: project_code, test_number, or station")
        
        # Validate test_number parameter if provided
        if test_number is not None and not isinstance(test_number, int):
            raise ValueError("test_number must be an integer")
        
        # Date validation (both required if either provided)
        if (date_start is None) != (date_end is None):
            raise ValueError("Both date_start and date_end must be provided together")
        
        datetime_start, datetime_end = self._handle_date_parameters(date_start, date_end) if date_start and date_end else (None, None)
        
        # Build the request parameters
        params = {
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'format': 'json'
        }
        
        # Add optional parameters
        if project_code:
            params['projectCode'] = project_code
        
        if test_number is not None:
            params['testNumber'] = str(test_number)
        
        if station:
            params['station'] = station
        
        if datetime_start and datetime_end:
            params['beginDateTime'] = datetime_start
            params['endDateTime'] = datetime_end
        
        if exclude_flagged_results:
            params['excludeFlaggedResults'] = 'Y' if exclude_flagged_results else 'N'
        
        # Raise NotImplementedError as this endpoint doesn't seem to properly validate client credentials
        raise NotImplementedError("The waterquality endpoint is not currently implemented due to API credential validation issues.")
        
        
        
        
        