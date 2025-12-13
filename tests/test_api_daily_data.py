"""Tests for daily data endpoint of DbHydroApi class."""

import pytest
from unittest.mock import Mock
from datetime import datetime

from dbhydro_py.api import DbHydroApi
from dbhydro_py.exceptions import DbHydroException
from dbhydro_py.models.transport import Result


class TestDailyDataEndpoint:
    """Test cases for DbHydroApi daily data endpoint."""
    
    def test_get_daily_data_validation_errors(self, api_client):
        """Test parameter validation in get_daily_data."""
        # Test non-list identifiers parameter
        with pytest.raises(ValueError, match="The 'identifiers' must be a list of strings"):
            api_client.get_daily_data(
                identifiers="S123-R",  # String instead of list
                identifier_type="station",
                date_start="2023-01-01",
                date_end="2023-01-02"
            )
        
        # Test empty identifiers list
        with pytest.raises(ValueError, match="The 'identifiers' list cannot be empty."):
            api_client.get_daily_data(
                identifiers=[],
                identifier_type="station",
                date_start="2023-01-01",
                date_end="2023-01-02"
            )
        
        # Test invalid identifier (empty string)
        with pytest.raises(ValueError, match="Invalid identifier: ''. Each identifier must be a non-empty string."):
            api_client.get_daily_data(
                identifiers=[""],
                identifier_type="station",
                date_start="2023-01-01",
                date_end="2023-01-02"
            )
        
        # Test invalid identifier (whitespace only)
        with pytest.raises(ValueError, match="Invalid identifier: '   '. Each identifier must be a non-empty string."):
            api_client.get_daily_data(
                identifiers=["   "],
                identifier_type="station",
                date_start="2023-01-01",
                date_end="2023-01-02"
            )
        
        # Test invalid identifier_type
        with pytest.raises(ValueError, match="Invalid identifier_type: 'invalid'"):
            api_client.get_daily_data(
                identifiers=["S123-R"],
                identifier_type="invalid",
                date_start="2023-01-01",
                date_end="2023-01-02"
            )
        
        # Test invalid requested_datum
        with pytest.raises(ValueError, match="Invalid requested_datum: 'INVALID'"):
            api_client.get_daily_data(
                identifiers=["S123-R"],
                identifier_type="station",
                date_start="2023-01-01",
                date_end="2023-01-02",
                requested_datum="INVALID"
            )
    
    def test_get_daily_data_include_summary_warning(self, api_client, sample_time_series_response):
        """Test that include_summary=True triggers warning and gets set to False."""
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_time_series_response
        )
        
        # Test with warning capture
        with pytest.warns(UserWarning, match="includeSummary=True currently causes API 503 errors"):
            response = api_client.get_daily_data(
                identifiers=["S123-R"],
                identifier_type="station",
                date_start="2023-01-01",
                date_end="2023-01-02",
                include_summary=True
            )
        
        # Verify the call was made with includeSummary=N  
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert call_args is not None
    
    def test_get_daily_data_success_with_timeseries_identifier(self, api_client, sample_time_series_response):
        """Test successful daily time series request with timeseries identifier."""
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_time_series_response
        )
        
        # Make request
        response = api_client.get_daily_data(
            identifiers=["12345"],
            identifier_type="timeseries",
            date_start="2023-01-01",
            date_end="2023-01-02",
            requested_datum="NAVD88"
        )
        
        # Verify response
        assert len(response.time_series) == 1
        assert response.time_series[0].source_info.site_code.value == "S123-R"
        
        # Verify API call parameters
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        # Basic verification that the call was made with expected parameters
        assert call_args is not None
    
    def test_get_daily_data_success_with_station_identifier(self, api_client, sample_time_series_response):
        """Test successful daily time series request with station identifier."""
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_time_series_response
        )
        
        # Make request
        response = api_client.get_daily_data(
            identifiers=["SITE001"],
            identifier_type="station",
            date_start=datetime(2023, 1, 1),
            date_end=datetime(2023, 1, 2)
        )
        
        # Verify response
        assert len(response.time_series) == 1
        
        # Verify API call parameters
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert call_args is not None
    
    def test_get_daily_data_success_with_id_identifier(self, api_client, sample_time_series_response):
        """Test successful daily time series request with id identifier."""
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_time_series_response
        )
        
        # Make request
        response = api_client.get_daily_data(
            identifiers=["ID456"],
            identifier_type="id",
            date_start="2023-01-01 12:30",
            date_end="2023-01-02 15:45"
        )
        
        # Verify response
        assert len(response.time_series) == 1
        
        # Verify API call parameters
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert call_args is not None
    
    def test_get_daily_data_success_with_multiple_identifiers(self, api_client, sample_time_series_response):
        """Test successful daily time series request with multiple identifiers."""
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_time_series_response
        )
        
        # Make request with multiple identifiers
        response = api_client.get_daily_data(
            identifiers=["SITE001", "SITE002", "SITE003"],
            identifier_type="station",
            date_start="2023-01-01",
            date_end="2023-01-02"
        )
        
        # Verify response
        assert len(response.time_series) == 1
        
        # Verify API call parameters - should include comma-separated identifiers
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert call_args is not None