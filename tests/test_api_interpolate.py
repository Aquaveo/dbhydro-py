"""Tests for interpolate endpoint of DbHydroApi class."""

import pytest
from unittest.mock import Mock
from datetime import datetime

from dbhydro_py.api import DbHydroApi
from dbhydro_py.exceptions import DbHydroException
from dbhydro_py.models.transport import Result


class TestInterpolateEndpoint:
    """Test cases for DbHydroApi interpolate endpoint."""
    
    def test_get_interpolate_validation_errors(self, api_client):
        """Test parameter validation in get_interpolate."""
        # Test invalid station_id (empty string)
        with pytest.raises(ValueError, match="Invalid station ID: ''. Station ID must be a non-empty string."):
            api_client.get_interpolate(
                station_id="",
                date_time="2023-01-01 12:00:00"
            )
        
        # Test invalid station_id (whitespace only)
        with pytest.raises(ValueError, match="Invalid station ID: '   '. Station ID must be a non-empty string."):
            api_client.get_interpolate(
                station_id="   ",
                date_time="2023-01-01 12:00:00"
            )

    def test_get_interpolate_success(self, api_client):
        """Test successful interpolate request."""
        # Mock interpolate response
        sample_interpolate_response = {
            "list": [
                {
                    "origin": "DBHYDRO",
                    "key": "STATION001",
                    "keyType": "SITE",
                    "msSinceEpoch": 1672574400000,
                    "value": 12.5,
                    "tag": {
                        "tag": "MEAN"
                    },
                    "qualityCode": "G",
                    "percentAvailable": 100.0,
                    "time": 120000000,
                    "date": 20230101
                }
            ]
        }
        
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_interpolate_response
        )
        
        # Make request with datetime object
        response = api_client.get_interpolate(
            station_id="STATION001",
            date_time=datetime(2023, 1, 1, 12, 0, 0)
        )
        
        # Verify response structure
        assert response.entries is not None
        assert len(response.entries) == 1
        assert response.entries[0].key == "STATION001"
        
        # Verify API call was made
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert call_args is not None

    def test_get_interpolate_success_with_string_datetime(self, api_client):
        """Test successful interpolate request with string datetime."""
        # Mock interpolate response
        sample_interpolate_response = {
            "list": [
                {
                    "origin": "DBHYDRO",
                    "key": "SITE-123",
                    "keyType": "SITE", 
                    "msSinceEpoch": 1672576200000,
                    "value": 15.2,
                    "tag": {
                        "tag": "MEAN"
                    },
                    "qualityCode": "G",
                    "percentAvailable": 95.5,
                    "time": 123000000,
                    "date": 20230101
                }
            ]
        }
        
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_interpolate_response
        )
        
        # Make request with string datetime
        response = api_client.get_interpolate(
            station_id="SITE-123",
            date_time="2023-01-01 12:30"
        )
        
        # Verify response structure
        assert response.entries is not None
        assert len(response.entries) == 1
        assert response.entries[0].key == "SITE-123"
        
        # Verify API call was made
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert call_args is not None