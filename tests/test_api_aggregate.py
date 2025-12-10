"""Tests for aggregate endpoint of DbHydroApi class."""

import pytest
from unittest.mock import Mock
from datetime import datetime

from dbhydro_py.api import DbHydroApi
from dbhydro_py.exceptions import DbHydroException
from dbhydro_py.models.transport import Result


class TestAggregateEndpoint:
    """Test cases for DbHydroApi aggregate endpoint."""
    
    def test_get_aggregate_validation_errors(self, api_client):
        """Test parameter validation in get_aggregate."""
        # Test invalid station_id (empty string)
        with pytest.raises(ValueError, match="Invalid station ID: ''. Station ID must be a non-empty string."):
            api_client.get_aggregate(
                station_id="",
                date_start="2023-01-01",
                date_end="2023-01-02"
            )
        
        # Test invalid station_id (whitespace only)
        with pytest.raises(ValueError, match="Invalid station ID: '   '. Station ID must be a non-empty string."):
            api_client.get_aggregate(
                station_id="   ",
                date_start="2023-01-01", 
                date_end="2023-01-02"
            )
        
        # Test invalid calculation type
        with pytest.raises(ValueError, match="Invalid calculation type: 'INVALID'"):
            api_client.get_aggregate(
                station_id="STATION001",
                date_start="2023-01-01",
                date_end="2023-01-02", 
                calculation="INVALID",
                timespan_unit="DAY"
            )
        
        # Test calculation provided but timespan_unit missing
        with pytest.raises(ValueError, match="If 'calculation' is provided, 'timespan_unit' must also be provided."):
            api_client.get_aggregate(
                station_id="STATION001",
                date_start="2023-01-01",
                date_end="2023-01-02",
                calculation="MEAN"
            )
        
        # Test timespan_unit provided but calculation missing
        with pytest.raises(ValueError, match="If 'calculation' is None, 'timespan_unit' must also be None."):
            api_client.get_aggregate(
                station_id="STATION001",
                date_start="2023-01-01",
                date_end="2023-01-02",
                timespan_unit="DAY"
            )

    def test_get_aggregate_success(self, api_client):
        """Test successful aggregate request."""
        # Mock aggregate response
        sample_aggregate_response = {
            "intervals": [
                {
                    "timespan": {
                        "beginTime": "2023-01-0100:00:00:000",
                        "endTime": "2023-01-0200:00:00:000"
                    },
                    "tags": [
                        {
                            "tag": "MEAN",
                            "value": 12.5
                        }
                    ]
                }
            ]
        }
        
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_aggregate_response
        )
        
        # Make request  
        response = api_client.get_aggregate(
            station_id="STATION001",
            date_start="2023-01-01",
            date_end="2023-01-02",
            calculation="MEAN",
            timespan_unit="DAY"
        )
        
        # Verify response structure
        assert response.intervals is not None
        assert len(response.intervals) == 1
        
        # Verify API call was made
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert call_args is not None

    def test_get_aggregate_success_without_calculation(self, api_client):
        """Test successful aggregate request without calculation parameters."""
        # Mock aggregate response
        sample_aggregate_response = {
            "intervals": []
        }
        
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_aggregate_response
        )
        
        # Make request without calculation parameters
        response = api_client.get_aggregate(
            station_id="STATION001",
            date_start="2023-01-01",
            date_end="2023-01-02"
        )
        
        # Verify response structure
        assert response.intervals is not None
        assert len(response.intervals) == 0
        
        # Verify API call was made
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert call_args is not None