"""Tests for synchronize endpoint in DbHydroApi."""

import pytest
from unittest.mock import Mock
from datetime import datetime

from dbhydro_py.api import DbHydroApi
from dbhydro_py.exceptions import DbHydroException
from dbhydro_py.models.responses.synchronize import SynchronizeResponse
from dbhydro_py.models.transport import Result


class TestSynchronizeEndpoint:
    """Test cases for synchronize endpoint."""
    
    def test_get_synchronize_validation_errors(self, api_client):
        """Test parameter validation in get_synchronize."""
        # Test non-list time_series_names parameter
        with pytest.raises(ValueError, match="The 'time_series_names' must be a list of strings"):
            api_client.get_synchronize(
                time_series_names="S123-R",  # String instead of list
                date_start="2023-01-01",
                date_end="2023-01-02"
            )
        
        # Test empty time_series_names list
        with pytest.raises(ValueError, match="The 'time_series_names' list cannot be empty"):
            api_client.get_synchronize(
                time_series_names=[],
                date_start="2023-01-01",
                date_end="2023-01-02"
            )

    @pytest.fixture
    def sample_synchronize_response(self):
        """Sample synchronize response data."""
        return {
            "1449794818000": {
                "S6-H": {
                    "origin": "MANIPULATED",
                    "key": "S6-H", 
                    "keyType": "station_id",
                    "msSinceEpoch": 1449794818000,
                    "value": 7.76,
                    "tag": {},
                    "qualityCode": "A",
                    "percentAvailable": 0
                },
                "S6P-3": {
                    "origin": "MANIPULATED",
                    "key": "S6P-3",
                    "keyType": "station_id", 
                    "msSinceEpoch": 1449794818000,
                    "value": 8.12,
                    "tag": {},
                    "qualityCode": "A",
                    "percentAvailable": 0
                }
            }
        }

    def test_get_synchronize_success(self, api_client, sample_synchronize_response):
        """Test successful synchronize request."""
        # Mock successful response
        mock_result = Result(
            status_code=200,
            message="Success",
            data=sample_synchronize_response
        )
        api_client.rest_adapter.get.return_value = mock_result
        
        # Call the method
        response = api_client.get_synchronize(
            time_series_names=["S6-H", "S6P-3"],
            date_start="2023-01-01",
            date_end="2023-01-02"
        )
        
        # Verify response
        assert isinstance(response, SynchronizeResponse)
        assert len(response.stations) == 2
        assert "S6-H" in response.stations
        assert "S6P-3" in response.stations
        
        # Verify API call
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        
        # Check URL (endpoint is passed as kwarg)
        assert "synchronize" in call_args.kwargs["endpoint"]
        
        # Check parameters  
        params = call_args.kwargs["params"]
        assert params["timeseries"] == "S6-H,S6P-3"
        assert params["beginDateTime"] == "2023-01-0100:00:00:000"
        assert params["endDateTime"] == "2023-01-0200:00:00:000"
        assert params["client_id"] == "test_client_id"
        assert params["client_secret"] == "test_client_secret"
        assert params["format"] == "json"

    def test_get_synchronize_with_datum(self, api_client, sample_synchronize_response):
        """Test synchronize request with requested datum."""
        mock_result = Result(
            status_code=200,
            message="Success", 
            data=sample_synchronize_response
        )
        api_client.rest_adapter.get.return_value = mock_result
        
        api_client.get_synchronize(
            time_series_names=["S6-H"],
            date_start="2023-01-01",
            date_end="2023-01-02",
            requested_datum="NAVD88"
        )
        
        # Check that datum parameter was included
        call_args = api_client.rest_adapter.get.call_args
        params = call_args.kwargs["params"]
        assert params["requestedDatum"] == "NAVD88"

    def test_get_synchronize_empty_time_series_names(self, api_client):
        """Test that empty time_series_names raises ValueError."""
        with pytest.raises(ValueError, match="The 'time_series_names' list cannot be empty"):
            api_client.get_synchronize(
                time_series_names=[],
                date_start="2023-01-01", 
                date_end="2023-01-02"
            )

    def test_get_synchronize_invalid_time_series_names(self, api_client):
        """Test that invalid time series names raise ValueError."""
        # Test with empty string
        with pytest.raises(ValueError, match="Invalid time series name"):
            api_client.get_synchronize(
                time_series_names=["S6-H", ""],
                date_start="2023-01-01",
                date_end="2023-01-02"
            )
        
        # Test with whitespace only
        with pytest.raises(ValueError, match="Invalid time series name"):
            api_client.get_synchronize(
                time_series_names=["S6-H", "   "],
                date_start="2023-01-01",
                date_end="2023-01-02"
            )
        
        # Test with non-string
        with pytest.raises(ValueError, match="Invalid time series name"):
            api_client.get_synchronize(
                time_series_names=["S6-H", 123],
                date_start="2023-01-01",
                date_end="2023-01-02"
            )

    def test_get_synchronize_invalid_datum(self, api_client):
        """Test that invalid requested_datum raises ValueError."""
        with pytest.raises(ValueError, match="Invalid requested_datum"):
            api_client.get_synchronize(
                time_series_names=["S6-H"],
                date_start="2023-01-01",
                date_end="2023-01-02",
                requested_datum="INVALID"
            )

    def test_get_synchronize_date_validation(self, api_client):
        """Test date validation in synchronize requests."""
        # Test with end date before start date
        with pytest.raises(ValueError, match="The 'date_start' must be earlier or equal to 'date_end'"):
            api_client.get_synchronize(
                time_series_names=["S6-H"],
                date_start="2023-01-02",
                date_end="2023-01-01"
            )

    def test_get_synchronize_datetime_objects(self, api_client, sample_synchronize_response):
        """Test synchronize request with datetime objects."""
        mock_result = Result(
            status_code=200,
            message="Success",
            data=sample_synchronize_response
        )
        api_client.rest_adapter.get.return_value = mock_result
        
        start_dt = datetime(2023, 1, 1, 12, 30, 45)
        end_dt = datetime(2023, 1, 2, 15, 45, 30)
        
        api_client.get_synchronize(
            time_series_names=["S6-H"],
            date_start=start_dt,
            date_end=end_dt
        )
        
        # Check converted date formats
        call_args = api_client.rest_adapter.get.call_args
        params = call_args.kwargs["params"]
        assert params["beginDateTime"] == "2023-01-0112:30:45:000"
        assert params["endDateTime"] == "2023-01-0215:45:30:000"

    def test_get_synchronize_api_error(self, api_client):
        """Test synchronize request with API error response."""
        # Mock error response
        mock_result = Result(
            status_code=400,
            message="Bad Request",
            data=None
        )
        api_client.rest_adapter.get.return_value = mock_result
        
        with pytest.raises(DbHydroException):
            api_client.get_synchronize(
                time_series_names=["S6-H"],
                date_start="2023-01-01",
                date_end="2023-01-02"
            )

    def test_get_synchronize_network_error(self, api_client):
        """Test synchronize request with network error."""
        # Mock network exception
        api_client.rest_adapter.get.side_effect = Exception("Network error")
        
        with pytest.raises(Exception, match="Network error"):
            api_client.get_synchronize(
                time_series_names=["S6-H"],
                date_start="2023-01-01", 
                date_end="2023-01-02"
            )

    def test_get_synchronize_empty_response(self, api_client):
        """Test synchronize request with empty response."""
        mock_result = Result(
            status_code=200,
            message="Success",
            data={}
        )
        api_client.rest_adapter.get.return_value = mock_result
        
        response = api_client.get_synchronize(
            time_series_names=["NONEXISTENT"],
            date_start="2023-01-01",
            date_end="2023-01-02"
        )
        
        assert isinstance(response, SynchronizeResponse)
        assert len(response.stations) == 0

    def test_get_synchronize_parameter_formatting(self, api_client, sample_synchronize_response):
        """Test that parameters are formatted correctly."""
        mock_result = Result(
            status_code=200,
            message="Success",
            data=sample_synchronize_response
        )
        api_client.rest_adapter.get.return_value = mock_result
        
        # Test multiple time series names
        time_series_names = ["S6-H", "S6P-3", "S6-T", "S7-H"]
        api_client.get_synchronize(
            time_series_names=time_series_names,
            date_start="2023-01-01",
            date_end="2023-01-02"
        )
        
        call_args = api_client.rest_adapter.get.call_args
        params = call_args.kwargs["params"]
        
        # Check comma-separated time series names
        assert params["timeseries"] == "S6-H,S6P-3,S6-T,S7-H"

    def test_get_synchronize_date_string_formats(self, api_client, sample_synchronize_response):
        """Test various date string formats."""
        mock_result = Result(
            status_code=200,
            message="Success",
            data=sample_synchronize_response
        )
        api_client.rest_adapter.get.return_value = mock_result
        
        # Test different date formats
        test_cases = [
            ("2023-01-01", "2023-01-0100:00:00:000"),
            ("2023-01-01 12:30", "2023-01-0112:30:00:000"),
            ("2023-01-01T12:30:45", "2023-01-0112:30:45:000"),
            ("2023-01-0112:30:45:123", "2023-01-0112:30:45:123"),
        ]
        
        for input_date, expected_output in test_cases:
            api_client.get_synchronize(
                time_series_names=["S6-H"],
                date_start=input_date,
                date_end=input_date
            )
            
            call_args = api_client.rest_adapter.get.call_args
            params = call_args.kwargs["params"]
            assert params["beginDateTime"] == expected_output
            assert params["endDateTime"] == expected_output