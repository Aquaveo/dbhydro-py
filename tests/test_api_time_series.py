"""Tests for time series endpoint of DbHydroApi class."""

import pytest
from unittest.mock import Mock
from datetime import datetime

from dbhydro_py.api import DbHydroApi
from dbhydro_py.exceptions import DbHydroException
from dbhydro_py.models.transport import Result


class TestTimeSeriesEndpoint:
    """Test cases for DbHydroApi time series endpoint."""
    
    def test_get_time_series_validation_errors(self, api_client):
        """Test parameter validation in get_time_series."""
        # Test non-list site_ids parameter
        with pytest.raises(ValueError, match="The 'site_ids' must be a list of strings"):
            api_client.get_time_series(
                site_ids="S123-R",  # String instead of list
                date_start="2023-01-01",
                date_end="2023-01-02"
            )
        
        # Test empty site_ids list
        with pytest.raises(ValueError, match="The 'site_ids' list cannot be empty"):
            api_client.get_time_series(
                site_ids=[],
                date_start="2023-01-01",
                date_end="2023-01-02"
            )
        
        # Test invalid site_id (empty string)
        with pytest.raises(ValueError, match="Invalid site ID: ''. Each site ID must be a non-empty string"):
            api_client.get_time_series(
                site_ids=["S123-R", ""],
                date_start="2023-01-01",
                date_end="2023-01-02"
            )
        
        # Test invalid site_id (whitespace only)
        with pytest.raises(ValueError, match="Invalid site ID: '   '. Each site ID must be a non-empty string"):
            api_client.get_time_series(
                site_ids=["S123-R", "   "],
                date_start="2023-01-01",
                date_end="2023-01-02"
            )
        
        # Test invalid site_id (non-string type)
        with pytest.raises(ValueError, match="Invalid site ID: '123'. Each site ID must be a non-empty string"):
            api_client.get_time_series(
                site_ids=["S123-R", 123],
                date_start="2023-01-01",
                date_end="2023-01-02"
            )
        
        # Test calculation without timespan_unit
        with pytest.raises(ValueError, match="If 'calculation' is provided, 'timespan_unit' must also be provided"):
            api_client.get_time_series(
                site_ids=["S123-R"],
                date_start="2023-01-01",
                date_end="2023-01-02",
                calculation="MEAN"
            )
        
        # Test invalid calculation
        with pytest.raises(ValueError, match="Invalid calculation type"):
            api_client.get_time_series(
                site_ids=["S123-R"],
                date_start="2023-01-01",
                date_end="2023-01-02",
                calculation="INVALID",
                timespan_unit="DAY"
            )
    
    def test_get_time_series_success(self, api_client, sample_time_series_response):
        """Test successful time series request."""
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_time_series_response
        )
        
        # Make request
        response = api_client.get_time_series(
            site_ids=["S123-R"],
            date_start="2023-01-01",
            date_end="2023-01-02"
        )
        
        # Verify response
        assert len(response.time_series) == 1
        assert response.time_series[0].source_info.site_code.value == "S123-R"
        assert len(response.time_series[0].values) == 2
        
        # Verify API call was made correctly
        api_client.rest_adapter.get.assert_called_once()
    
    def test_get_time_series_valid_site_ids(self, api_client, sample_time_series_response):
        """Test that valid site_ids are accepted."""
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_time_series_response
        )
        
        # Test various valid site ID formats
        valid_site_ids = [
            ["S123-R"],                    # Single site
            ["S123-R", "C43S65"],         # Multiple sites
            ["SITE_001", "123ABC"],       # Alphanumeric
            ["G-3566"],                   # With dash
            [" S123-R "],                 # With whitespace (should be stripped)
        ]
        
        for site_ids in valid_site_ids:
            # Should not raise any exception
            response = api_client.get_time_series(
                site_ids=site_ids,
                date_start="2023-01-01",
                date_end="2023-01-02"
            )
            
            # Verify response is returned successfully
            assert response is not None
            assert hasattr(response, 'time_series')