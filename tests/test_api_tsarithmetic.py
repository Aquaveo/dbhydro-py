"""Tests for tsarithmetic API endpoint."""

import pytest
from datetime import datetime
from unittest.mock import Mock

from dbhydro_py.api import DbHydroApi
from dbhydro_py.models.responses.point import PointResponse
from dbhydro_py.exceptions import DbHydroException


class TestTimeSeriesArithmeticApi:
    """Test cases for tsarithmetic endpoint."""
    
    @pytest.fixture
    def api_client(self):
        """Create DbHydroApi client for testing."""
        mock_adapter = Mock()
        return DbHydroApi(
            rest_adapter=mock_adapter,
            client_id="test_client",
            client_secret="test_secret"
        )
    
    def test_get_time_series_arithmetic_validation_errors(self, api_client):
        """Test parameter validation in get_time_series_arithmetic."""
        # Test empty id
        with pytest.raises(ValueError, match="Invalid id: ''. ID must be a non-empty string."):
            api_client.get_time_series_arithmetic(
                id="",
                timestamp="2023-01-01 12:00:00"
            )
        
        # Test whitespace-only id  
        with pytest.raises(ValueError, match="Invalid id: '   '. ID must be a non-empty string."):
            api_client.get_time_series_arithmetic(
                id="   ",
                timestamp="2023-01-01 12:00:00"
            )
        
        # Note: Date format validation is lenient in the current implementation,
        # so we don't test for date format errors here
    
    def test_get_time_series_arithmetic_success_with_valid_data(self, api_client):
        """Test successful retrieval with valid point data."""
        # Mock successful response with valid point
        mock_response = {
            "pointResponse": {
                "status": {
                    "statusCode": 200,
                    "statusMessage": "Success",
                    "elapsedTime": 0.045123
                },
                "points": [
                    {
                        "value": 15.75,
                        "timestamp": 1672574400000,
                        "msSinceEpoch": 1672574400000,
                        "qualityCode": "A"
                    }
                ]
            }
        }
        
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=mock_response
        )
        
        response = api_client.get_time_series_arithmetic(
            id="TS123",
            timestamp="2023-01-01 12:00:00"
        )
        
        # Verify response structure
        assert isinstance(response, PointResponse)
        assert response.status.status_code == 200
        assert response.status.message == "Success"
        assert len(response.points) == 1
        assert response.points[0] is not None
        assert response.points[0].value == 15.75
        assert response.points[0].quality_code == "A"
    
    def test_get_time_series_arithmetic_success_with_null_point(self, api_client):
        """Test successful retrieval with null point (invalid identifier scenario)."""
        # Mock response with null point (like your example) - but with successful status to avoid exception
        mock_response = {
            "pointResponse": {
                "status": {
                    "statusCode": 200,
                    "statusMessage": "Success",
                    "elapsedTime": 0.001376235
                },
                "points": [None]
            }
        }
        
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=mock_response
        )
        
        response = api_client.get_time_series_arithmetic(
            id="INVALID_TS",
            timestamp="2023-01-01 12:00:00"
        )
        
        # Verify response structure
        assert isinstance(response, PointResponse)
        assert response.status.status_code == 200
        assert response.status.message == "Success"
        assert len(response.points) == 1
        assert response.points[0] is None
    
    def test_get_time_series_arithmetic_with_datetime_object(self, api_client):
        """Test with datetime object input."""
        mock_response = {
            "pointResponse": {
                "status": {
                    "statusCode": 200,
                    "statusMessage": "Success",
                    "elapsedTime": 0.045123
                },
                "points": [
                    {
                        "value": 12.5,
                        "timestamp": 1672574400000
                    }
                ]
            }
        }
        
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=mock_response
        )
        
        dt = datetime(2023, 1, 1, 12, 0, 0)
        response = api_client.get_time_series_arithmetic(
            id="TS456",
            timestamp=dt
        )
        
        assert isinstance(response, PointResponse)
        assert response.status.status_code == 200
        assert len(response.points) == 1
        assert response.points[0].value == 12.5
        
        # Verify the API was called with properly formatted timestamp
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert call_args[1]['params']['timestamp'] == '2023-01-0112:00:00:000'
    
    def test_get_time_series_arithmetic_url_construction(self, api_client):
        """Test proper URL construction."""
        mock_response = {
            "pointResponse": {
                "status": {
                    "statusCode": 200,
                    "statusMessage": "Success",
                    "elapsedTime": 0.01
                },
                "points": []
            }
        }
        
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=mock_response
        )
        
        api_client.get_time_series_arithmetic(
            id="TS789",
            timestamp="2023-06-15T14:30:00"
        )
        
        # Verify URL and parameters
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        
        # Check URL
        expected_url = "https://dataservice-proxy.api.sfwmd.gov/v1/ext/data/tsarithmetic"
        assert call_args[1]['endpoint'] == expected_url
        
        # Check parameters
        params = call_args[1]['params']
        assert params['id'] == 'TS789'
        assert params['timestamp'] == '2023-06-1514:30:00:000'
        assert params['client_id'] == 'test_client'
        assert params['client_secret'] == 'test_secret'
        assert params['format'] == 'json'
    
    def test_get_time_series_arithmetic_api_error_handling(self, api_client):
        """Test API error handling."""
        # Mock API error response
        mock_response = {
            "pointResponse": {
                "status": {
                    "statusCode": 500,
                    "statusMessage": "Internal server error",
                    "elapsedTime": 0.001
                },
                "points": []
            }
        }
        
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=mock_response
        )
        
        with pytest.raises(DbHydroException) as exc_info:
            api_client.get_time_series_arithmetic(
                id="TS_ERROR",
                timestamp="2023-01-01"
            )
        
        assert "Internal server error" in str(exc_info.value)
        assert exc_info.value.api_status_code == 500

    def test_get_time_series_arithmetic_invalid_identifier_error(self, api_client):
        """Test handling of invalid timeseries identifier as API error."""
        # Mock the actual response you're seeing
        mock_response = {
            "pointResponse": {
                "status": {
                    "statusCode": 1,
                    "statusMessage": "Invalid timeseries identifier.",
                    "elapsedTime": 0.001376235
                },
                "points": [None]
            }
        }
        
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=mock_response
        )
        
        # This should raise an exception because status code 1 is considered an error
        with pytest.raises(DbHydroException) as exc_info:
            api_client.get_time_series_arithmetic(
                id="INVALID_TS",
                timestamp="2023-01-01 12:00:00"
            )
        
        assert "Invalid timeseries identifier" in str(exc_info.value)
        assert exc_info.value.api_status_code == 1
    
    def test_get_time_series_arithmetic_http_error_handling(self, api_client):
        """Test HTTP error handling."""
        # Mock HTTP error
        api_client.rest_adapter.get.return_value = Mock(
            status_code=404,
            data={"error": "Not found"}
        )
        
        with pytest.raises(DbHydroException) as exc_info:
            api_client.get_time_series_arithmetic(
                id="TS_HTTP_ERROR",
                timestamp="2023-01-01"
            )
        
        assert exc_info.value.http_status_code == 404
    
    def test_get_time_series_arithmetic_empty_points_response(self, api_client):
        """Test response with empty points array."""
        mock_response = {
            "pointResponse": {
                "status": {
                    "statusCode": 200,
                    "statusMessage": "No data available",
                    "elapsedTime": 0.002
                },
                "points": []
            }
        }
        
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=mock_response
        )
        
        response = api_client.get_time_series_arithmetic(
            id="TS_EMPTY",
            timestamp="2023-01-01"
        )
        
        assert isinstance(response, PointResponse)
        assert response.status.status_code == 200
        assert len(response.points) == 0
    
    def test_get_time_series_arithmetic_multiple_date_formats(self, api_client):
        """Test various supported date formats."""
        mock_response = {
            "pointResponse": {
                "status": {
                    "statusCode": 200,
                    "statusMessage": "Success",
                    "elapsedTime": 0.01
                },
                "points": [{"value": 10.0}]
            }
        }
        
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=mock_response
        )
        
        test_cases = [
            ("2023-12-04", "2023-12-0400:00:00:000"),
            ("2023-12-04 10:30", "2023-12-0410:30:00:000"),
            ("2023-12-04T10:30", "2023-12-0410:30:00:000"),
            ("2023-12-0410:30:45", "2023-12-0410:30:45:000"),
            ("2023-12-0410:30:45:123", "2023-12-0410:30:45:123")
        ]
        
        for input_date, expected_format in test_cases:
            api_client.rest_adapter.get.reset_mock()
            
            api_client.get_time_series_arithmetic(
                id="TS_DATE_TEST",
                timestamp=input_date
            )
            
            call_args = api_client.rest_adapter.get.call_args
            actual_timestamp = call_args[1]['params']['timestamp']
            assert actual_timestamp == expected_format, f"Failed for input: {input_date}"