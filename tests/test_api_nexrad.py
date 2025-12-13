"""Tests for NEXRAD API methods."""

import pytest
from unittest.mock import Mock
from datetime import datetime

from dbhydro_py.api import DbHydroApi
from dbhydro_py.exceptions import DbHydroException
from dbhydro_py.models.transport import Result


class TestNexradApi:
    """Test cases for NEXRAD API methods."""

    @pytest.fixture
    def api_client(self):
        """Create API client with mocked REST adapter."""
        mock_adapter = Mock()
        return DbHydroApi(
            rest_adapter=mock_adapter,
            client_id="test_client_id",
            client_secret="test_client_secret"
        )
    
    def test_get_nexrad_pixel_data_validation_errors(self, api_client):
        """Test parameter validation in get_nexrad_pixel_data."""
        # Test non-list pixel_ids parameter
        with pytest.raises(ValueError, match="The 'pixel_ids' must be a list of strings"):
            api_client.get_nexrad_pixel_data(
                pixel_ids="PIXEL123",  # String instead of list
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="D"
            )
        
        # Test empty pixel_ids list
        with pytest.raises(ValueError, match="At least one pixel_id must be provided"):
            api_client.get_nexrad_pixel_data(
                pixel_ids=[],
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="D"
            )
    
    def test_get_nexrad_polygon_data_validation_errors(self, api_client):
        """Test parameter validation in get_nexrad_polygon_data."""
        # Test non-list identifiers parameter
        with pytest.raises(ValueError, match="The 'identifiers' must be a list of strings"):
            api_client.get_nexrad_polygon_data(
                identifiers="POLYGON123",  # String instead of list
                identifier_type="polygonId",
                polygon_type=1,
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="D"
            )
        
        # Test empty identifiers list
        with pytest.raises(ValueError, match="At least one identifier must be provided"):
            api_client.get_nexrad_polygon_data(
                identifiers=[],
                identifier_type="polygonId",
                polygon_type=1,
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="D"
            )

    @pytest.fixture
    def sample_nexrad_response(self):
        """Sample NEXRAD time series response."""
        return {
            "timeSeriesResponse": {
                "status": {
                    "statusCode": 200,
                    "statusMessage": "Success",
                    "elapsedTime": 0.150
                },
                "timeSeries": [
                    {
                        "sourceInfo": {
                            "siteName": "Test Pixel Site",
                            "siteCode": {
                                "network": "NEXRAD",
                                "agencyCode": "NEXRAD",
                                "value": "PIXEL-123"
                            },
                            "geoLocation": {
                                "geogLocation": {
                                    "type": "Point",
                                    "srs": "EPSG:4326",
                                    "latitude": 26.123,
                                    "longitude": -80.456
                                }
                            }
                        },
                        "values": [
                            {
                                "value": [
                                    {
                                        "value": "2.5",
                                        "qualifiers": "A",
                                        "dateTime": "2023-01-0100:00:00:000"
                                    },
                                    {
                                        "value": "3.1", 
                                        "qualifiers": "A",
                                        "dateTime": "2023-01-0101:00:00:000"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        }

    # NEXRAD Pixel Data Tests
    def test_get_nexrad_pixel_data_validation_errors(self, api_client):
        """Test parameter validation in get_nexrad_pixel_data."""
        # Test empty pixel_ids list
        with pytest.raises(ValueError, match="At least one pixel_id must be provided."):
            api_client.get_nexrad_pixel_data(
                pixel_ids=[],
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="H"
            )
        
        # Test invalid pixel_id (empty string)
        with pytest.raises(ValueError, match="Invalid pixel_id: ''. Must be a non-empty string."):
            api_client.get_nexrad_pixel_data(
                pixel_ids=[""],
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="H"
            )
        
        # Test invalid pixel_id (whitespace only)
        with pytest.raises(ValueError, match="Invalid pixel_id: '   '. Must be a non-empty string."):
            api_client.get_nexrad_pixel_data(
                pixel_ids=["   "],
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="H"
            )
        
        # Test invalid frequency
        with pytest.raises(ValueError, match="Invalid frequency: 'INVALID'"):
            api_client.get_nexrad_pixel_data(
                pixel_ids=["PIXEL-123"],
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="INVALID"
            )

    def test_get_nexrad_pixel_data_success(self, api_client, sample_nexrad_response):
        """Test successful NEXRAD pixel data request."""
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_nexrad_response
        )
        
        # Make request
        response = api_client.get_nexrad_pixel_data(
            pixel_ids=["PIXEL-123"],
            date_start=datetime(2023, 1, 1),
            date_end=datetime(2023, 1, 2),
            frequency="H",
            include_zero=False
        )
        
        # Verify response structure
        assert response.time_series is not None
        assert len(response.time_series) == 1
        assert response.time_series[0].source_info.site_code.value == "PIXEL-123"
        
        # Verify API call parameters
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert call_args is not None
        
        # Verify request parameters
        params = call_args[1]['params']
        assert params['pixelId'] == "PIXEL-123"
        assert params['frequency'] == "H"
        assert params['incZero'] == "N"
        assert params['polygonType'] == 0

    def test_get_nexrad_pixel_data_with_multiple_pixels(self, api_client, sample_nexrad_response):
        """Test NEXRAD pixel data request with multiple pixel IDs."""
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_nexrad_response
        )
        
        # Make request with multiple pixel IDs
        response = api_client.get_nexrad_pixel_data(
            pixel_ids=["PIXEL-123", "PIXEL-456", "PIXEL-789"],
            date_start="2023-01-01",
            date_end="2023-01-02",
            frequency="D",
            include_zero=True
        )
        
        # Verify response
        assert response.time_series is not None
        
        # Verify API call parameters - should include comma-separated pixel IDs
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        params = call_args[1]['params']
        assert params['pixelId'] == "PIXEL-123,PIXEL-456,PIXEL-789"
        assert params['incZero'] == "Y"

    def test_get_nexrad_pixel_data_all_frequencies(self, api_client, sample_nexrad_response):
        """Test NEXRAD pixel data request with different frequencies."""
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_nexrad_response
        )
        
        # Test all valid frequencies
        valid_frequencies = ['H', 'D', 'M', 'Y', 'E']
        for frequency in valid_frequencies:
            api_client.rest_adapter.get.reset_mock()
            
            response = api_client.get_nexrad_pixel_data(
                pixel_ids=["PIXEL-123"],
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency=frequency
            )
            
            # Verify response
            assert response.time_series is not None
            
            # Verify frequency parameter
            call_args = api_client.rest_adapter.get.call_args
            params = call_args[1]['params']
            assert params['frequency'] == frequency

    # NEXRAD Polygon Data Tests
    def test_get_nexrad_polygon_data_validation_errors(self, api_client):
        """Test parameter validation in get_nexrad_polygon_data."""
        # Test empty identifiers list
        with pytest.raises(ValueError, match="At least one identifier must be provided."):
            api_client.get_nexrad_polygon_data(
                identifiers=[],
                identifier_type="polygonId",
                polygon_type=1,
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="H"
            )
        
        # Test invalid identifier (empty string)
        with pytest.raises(ValueError, match="Invalid identifier: ''. Must be a non-empty string."):
            api_client.get_nexrad_polygon_data(
                identifiers=[""],
                identifier_type="polygonId",
                polygon_type=1,
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="H"
            )
        
        # Test invalid identifier (whitespace only)
        with pytest.raises(ValueError, match="Invalid identifier: '   '. Must be a non-empty string."):
            api_client.get_nexrad_polygon_data(
                identifiers=["   "],
                identifier_type="polygonId",
                polygon_type=1,
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="H"
            )
        
        # Test invalid identifier_type
        with pytest.raises(ValueError, match=r"Invalid identifier_type: 'invalid'\. Must be one of: .*"):
            api_client.get_nexrad_polygon_data(
                identifiers=["POLYGON-123"],
                identifier_type="invalid",
                polygon_type=1,
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="H"
            )
        
        # Test invalid polygon_type (too low)
        with pytest.raises(ValueError, match="Invalid polygon_type. Must be an integer between 1 and 9 inclusive."):
            api_client.get_nexrad_polygon_data(
                identifiers=["POLYGON-123"],
                identifier_type="polygonId",
                polygon_type=0,
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="H"
            )
        
        # Test invalid polygon_type (too high)
        with pytest.raises(ValueError, match="Invalid polygon_type. Must be an integer between 1 and 9 inclusive."):
            api_client.get_nexrad_polygon_data(
                identifiers=["POLYGON-123"],
                identifier_type="polygonId",
                polygon_type=10,
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="H"
            )
        
        # Test invalid frequency
        with pytest.raises(ValueError, match="Invalid frequency: 'INVALID'"):
            api_client.get_nexrad_polygon_data(
                identifiers=["POLYGON-123"],
                identifier_type="polygonId",
                polygon_type=1,
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="INVALID"
            )

    def test_get_nexrad_polygon_data_success_with_polygon_id(self, api_client, sample_nexrad_response):
        """Test successful NEXRAD polygon data request with polygon ID."""
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_nexrad_response
        )
        
        # Make request
        response = api_client.get_nexrad_polygon_data(
            identifiers=["POLYGON-123"],
            identifier_type="polygonId",
            polygon_type=5,
            date_start=datetime(2023, 1, 1),
            date_end=datetime(2023, 1, 2),
            frequency="15",
            include_zero=False
        )
        
        # Verify response structure
        assert response.time_series is not None
        assert len(response.time_series) == 1
        
        # Verify API call parameters
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert call_args is not None
        
        # Verify request parameters
        params = call_args[1]['params']
        assert params['polygonId'] == "POLYGON-123"
        assert params['polygonType'] == 5
        assert params['frequency'] == "15"
        assert params['incZero'] == "N"

    def test_get_nexrad_polygon_data_success_with_polygon_name(self, api_client, sample_nexrad_response):
        """Test successful NEXRAD polygon data request with polygon name."""
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_nexrad_response
        )
        
        # Make request
        response = api_client.get_nexrad_polygon_data(
            identifiers=["Test Polygon"],
            identifier_type="polygonName",
            polygon_type=3,
            date_start="2023-01-01",
            date_end="2023-01-02",
            frequency="H",
            include_zero=True
        )
        
        # Verify response structure
        assert response.time_series is not None
        
        # Verify API call parameters
        call_args = api_client.rest_adapter.get.call_args
        params = call_args[1]['params']
        assert params['polygonName'] == "Test Polygon"
        assert params['polygonType'] == 3
        assert params['incZero'] == "Y"

    def test_get_nexrad_polygon_data_with_multiple_identifiers(self, api_client, sample_nexrad_response):
        """Test NEXRAD polygon data request with multiple identifiers."""
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_nexrad_response
        )
        
        # Make request with multiple identifiers
        response = api_client.get_nexrad_polygon_data(
            identifiers=["Polygon A", "Polygon B", "Polygon C"],
            identifier_type="polygonName",
            polygon_type=7,
            date_start="2023-01-01",
            date_end="2023-01-02",
            frequency="M"
        )
        
        # Verify response
        assert response.time_series is not None
        
        # Verify API call parameters - should include comma-separated identifiers
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        params = call_args[1]['params']
        assert params['polygonName'] == "Polygon A,Polygon B,Polygon C"

    def test_get_nexrad_polygon_data_all_polygon_types(self, api_client, sample_nexrad_response):
        """Test NEXRAD polygon data request with different polygon types."""
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_nexrad_response
        )
        
        # Test all valid polygon types (1-9)
        for polygon_type in range(1, 10):
            api_client.rest_adapter.get.reset_mock()
            
            response = api_client.get_nexrad_polygon_data(
                identifiers=["POLYGON-123"],
                identifier_type="polygonId",
                polygon_type=polygon_type,
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="H"
            )
            
            # Verify response
            assert response.time_series is not None
            
            # Verify polygon type parameter
            call_args = api_client.rest_adapter.get.call_args
            params = call_args[1]['params']
            assert params['polygonType'] == polygon_type

    def test_get_nexrad_polygon_data_all_frequencies(self, api_client, sample_nexrad_response):
        """Test NEXRAD polygon data request with different frequencies."""
        # Setup mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_nexrad_response
        )
        
        # Test all valid frequencies for polygon data
        valid_frequencies = ['15', 'H', 'D', 'M', 'Y', 'E']
        for frequency in valid_frequencies:
            api_client.rest_adapter.get.reset_mock()
            
            response = api_client.get_nexrad_polygon_data(
                identifiers=["POLYGON-123"],
                identifier_type="polygonId",
                polygon_type=1,
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency=frequency
            )
            
            # Verify response
            assert response.time_series is not None
            
            # Verify frequency parameter
            call_args = api_client.rest_adapter.get.call_args
            params = call_args[1]['params']
            assert params['frequency'] == frequency

    # Date validation tests (common to both functions)
    def test_nexrad_date_validation_errors(self, api_client):
        """Test date validation in NEXRAD methods."""
        # Test invalid date range
        with pytest.raises(ValueError, match="The 'date_start' must be earlier or equal to 'date_end'."):
            api_client.get_nexrad_pixel_data(
                pixel_ids=["PIXEL-123"],
                date_start="2023-01-02",
                date_end="2023-01-01",
                frequency="H"
            )
        
        with pytest.raises(ValueError, match="The 'date_start' must be earlier or equal to 'date_end'."):
            api_client.get_nexrad_polygon_data(
                identifiers=["POLYGON-123"],
                identifier_type="polygonId",
                polygon_type=1,
                date_start="2023-01-02",
                date_end="2023-01-01",
                frequency="H"
            )

    def test_nexrad_api_error_handling(self, api_client):
        """Test API error handling in NEXRAD methods."""
        # Setup error response
        error_response = {
            "timeSeriesResponse": {
                "status": {
                    "statusCode": 404,
                    "statusMessage": "No data found for specified parameters",
                    "elapsedTime": 0.050
                }
            }
        }
        
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=error_response
        )
        
        # Test pixel data error handling
        with pytest.raises(DbHydroException, match="API request failed"):
            api_client.get_nexrad_pixel_data(
                pixel_ids=["NONEXISTENT"],
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="H"
            )
        
        # Test polygon data error handling
        with pytest.raises(DbHydroException, match="API request failed"):
            api_client.get_nexrad_polygon_data(
                identifiers=["NONEXISTENT"],
                identifier_type="polygonId",
                polygon_type=1,
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="H"
            )

    def test_nexrad_http_error_handling(self, api_client):
        """Test HTTP error handling in NEXRAD methods."""
        # Setup HTTP error response
        api_client.rest_adapter.get.return_value = Result(
            status_code=500,
            message="Internal Server Error",
            data={}
        )
        
        # Test pixel data HTTP error handling
        with pytest.raises(DbHydroException, match="HTTP request failed with status 500"):
            api_client.get_nexrad_pixel_data(
                pixel_ids=["PIXEL-123"],
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="H"
            )
        
        # Test polygon data HTTP error handling
        with pytest.raises(DbHydroException, match="HTTP request failed with status 500"):
            api_client.get_nexrad_polygon_data(
                identifiers=["POLYGON-123"],
                identifier_type="polygonId",
                polygon_type=1,
                date_start="2023-01-01",
                date_end="2023-01-02",
                frequency="H"
            )