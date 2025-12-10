import pytest
from unittest.mock import Mock, patch

from dbhydro_py.api import DbHydroApi
from dbhydro_py.exceptions import DbHydroException
from dbhydro_py.models.responses.time_series import PeriodOfRecord


class TestApiPeriodOfRecord:
    """Test cases for the get_period_of_record API method."""

    @pytest.fixture
    def mock_rest_adapter(self):
        """Create a mock REST adapter."""
        return Mock()

    @pytest.fixture
    def api(self, mock_rest_adapter):
        """Create DbHydroApi instance with mock adapter."""
        return DbHydroApi(mock_rest_adapter, "test_client", "test_secret")

    def test_get_period_of_record_success(self, api, mock_rest_adapter):
        """Test successful period of record retrieval."""
        # Mock response data
        mock_response_data = {
            "periodOfRecord": {
                "porBeginDate": "1997-03-18T00:00:00:000",
                "porLastDate": "2025-12-08T15:00:00:000",
                "provisionalBeginDate": "2025-12-08T00:00:00:000",
                "provisionalLastDate": "2025-12-08T15:00:00:000",
                "approvedBeginDate": "1997-03-18T00:00:00:000",
                "approvedLastDate": "2025-12-07T23:59:59:000"
            }
        }

        # Configure mock
        mock_result = Mock()
        mock_result.data = mock_response_data
        mock_result.status_code = 200
        mock_rest_adapter.get.return_value = mock_result

        # Call the method
        result = api.get_period_of_record("S123-R")

        # Verify the result
        assert isinstance(result, PeriodOfRecord)
        assert result.por_begin_date == "1997-03-18T00:00:00:000"
        assert result.por_last_date == "2025-12-08T15:00:00:000"
        assert result.provisional_begin_date == "2025-12-08T00:00:00:000"
        assert result.provisional_last_date == "2025-12-08T15:00:00:000"
        assert result.approved_begin_date == "1997-03-18T00:00:00:000"
        assert result.approved_last_date == "2025-12-07T23:59:59:000"

        # Verify the API call
        mock_rest_adapter.get.assert_called_once()
        call_args = mock_rest_adapter.get.call_args
        
        # Check URL
        expected_url = "https://dataservice-proxy.api.sfwmd.gov/v1/ext/data/por"
        assert call_args[1]['endpoint'] == expected_url
        
        # Check parameters
        params = call_args[1]['params']
        assert params['stationId'] == "S123-R"
        assert params['client_id'] == "test_client"
        assert params['client_secret'] == "test_secret"
        assert params['format'] == "json"

    def test_get_period_of_record_with_null_values(self, api, mock_rest_adapter):
        """Test period of record response with null values."""
        # Mock response data with some null values
        mock_response_data = {
            "periodOfRecord": {
                "porBeginDate": None,
                "porLastDate": None,
                "provisionalBeginDate": "2025-12-08T00:00:00:000",
                "provisionalLastDate": "2025-12-08T15:00:00:000",
                "approvedBeginDate": None,
                "approvedLastDate": None
            }
        }

        # Configure mock
        mock_result = Mock()
        mock_result.data = mock_response_data
        mock_result.status_code = 200
        mock_rest_adapter.get.return_value = mock_result

        # Call the method
        result = api.get_period_of_record("TEST-SITE")

        # Verify the result handles null values correctly
        assert isinstance(result, PeriodOfRecord)
        assert result.por_begin_date is None
        assert result.por_last_date is None
        assert result.provisional_begin_date == "2025-12-08T00:00:00:000"
        assert result.provisional_last_date == "2025-12-08T15:00:00:000"
        assert result.approved_begin_date is None
        assert result.approved_last_date is None

    def test_get_period_of_record_empty_response(self, api, mock_rest_adapter):
        """Test period of record with empty periodOfRecord object."""
        # Mock response data with empty period of record
        mock_response_data = {
            "periodOfRecord": {}
        }

        # Configure mock
        mock_result = Mock()
        mock_result.data = mock_response_data
        mock_result.status_code = 200
        mock_rest_adapter.get.return_value = mock_result

        # Call the method
        result = api.get_period_of_record("EMPTY-SITE")

        # Verify the result with all None values
        assert isinstance(result, PeriodOfRecord)
        assert result.por_begin_date is None
        assert result.por_last_date is None
        assert result.provisional_begin_date is None
        assert result.provisional_last_date is None
        assert result.approved_begin_date is None
        assert result.approved_last_date is None

    def test_get_period_of_record_missing_period_of_record(self, api, mock_rest_adapter):
        """Test period of record when periodOfRecord key is missing."""
        # Mock response data without periodOfRecord key
        mock_response_data = {
            "someOtherData": "value"
        }

        # Configure mock
        mock_result = Mock()
        mock_result.data = mock_response_data
        mock_result.status_code = 200
        mock_rest_adapter.get.return_value = mock_result

        # Call the method
        result = api.get_period_of_record("MISSING-SITE")

        # Verify the result with all None values (empty dict passed to dataclass_from_dict)
        assert isinstance(result, PeriodOfRecord)
        assert result.por_begin_date is None
        assert result.por_last_date is None
        assert result.provisional_begin_date is None
        assert result.provisional_last_date is None
        assert result.approved_begin_date is None
        assert result.approved_last_date is None

    def test_get_period_of_record_http_error(self, api, mock_rest_adapter):
        """Test period of record request with HTTP error."""
        # Configure mock to return HTTP error
        mock_result = Mock()
        mock_result.data = {"error": "Not found"}
        mock_result.status_code = 404
        mock_rest_adapter.get.return_value = mock_result

        # Verify that HTTP error is raised
        with pytest.raises(DbHydroException) as exc_info:
            api.get_period_of_record("NONEXISTENT-SITE")
        
        assert "HTTP request failed with status 404" in str(exc_info.value)
        assert exc_info.value.http_status_code == 404

    def test_get_period_of_record_api_error(self, api, mock_rest_adapter):
        """Test period of record request with API-level error."""
        # Configure mock to return API error
        mock_response_data = {
            "periodOfRecord": {
                "status": {
                    "statusCode": 400,
                    "statusMessage": "Invalid station ID",
                    "elapsedTime": 0.123
                }
            }
        }
        
        mock_result = Mock()
        mock_result.data = mock_response_data
        mock_result.status_code = 200  # HTTP OK but API error
        mock_rest_adapter.get.return_value = mock_result

        # Verify that API error is raised
        with pytest.raises(DbHydroException) as exc_info:
            api.get_period_of_record("INVALID-SITE")
        
        assert "API request failed: Invalid station ID" in str(exc_info.value)
        assert exc_info.value.api_status_code == 400
        assert exc_info.value.api_status_message == "Invalid station ID"
        assert exc_info.value.elapsed_time == 0.123

    def test_get_period_of_record_different_station_formats(self, api, mock_rest_adapter):
        """Test period of record with different station ID formats."""
        # Mock response data
        mock_response_data = {
            "periodOfRecord": {
                "porBeginDate": "2020-01-01T00:00:00:000",
                "porLastDate": "2025-12-08T15:00:00:000",
                "provisionalBeginDate": None,
                "provisionalLastDate": None,
                "approvedBeginDate": "2020-01-01T00:00:00:000",
                "approvedLastDate": "2025-12-08T15:00:00:000"
            }
        }

        # Configure mock
        mock_result = Mock()
        mock_result.data = mock_response_data
        mock_result.status_code = 200
        mock_rest_adapter.get.return_value = mock_result

        # Test various station ID formats
        station_ids = ["S123-R", "C43S65", "SITE_001", "123456"]
        
        for station_id in station_ids:
            result = api.get_period_of_record(station_id)
            
            # Verify the result is consistent
            assert isinstance(result, PeriodOfRecord)
            assert result.por_begin_date == "2020-01-01T00:00:00:000"
            assert result.por_last_date == "2025-12-08T15:00:00:000"
            
            # Verify the station ID was passed correctly
            call_args = mock_rest_adapter.get.call_args
            params = call_args[1]['params']
            assert params['stationId'] == station_id

    def test_get_period_of_record_url_construction(self, api, mock_rest_adapter):
        """Test that the URL is constructed correctly."""
        # Mock response
        mock_result = Mock()
        mock_result.data = {"periodOfRecord": {}}
        mock_result.status_code = 200
        mock_rest_adapter.get.return_value = mock_result

        # Call the method
        api.get_period_of_record("TEST-SITE")

        # Verify URL construction
        call_args = mock_rest_adapter.get.call_args
        expected_url = "https://dataservice-proxy.api.sfwmd.gov/v1/ext/data/por"
        assert call_args[1]['endpoint'] == expected_url

    def test_get_period_of_record_api_version(self, mock_rest_adapter):
        """Test period of record with different API version."""
        # Create API with different version
        api = DbHydroApi(mock_rest_adapter, "test_client", "test_secret", api_version=2)
        
        # Mock response
        mock_result = Mock()
        mock_result.data = {"periodOfRecord": {}}
        mock_result.status_code = 200
        mock_rest_adapter.get.return_value = mock_result

        # Call the method
        api.get_period_of_record("TEST-SITE")

        # Verify URL includes correct API version
        call_args = mock_rest_adapter.get.call_args
        expected_url = "https://dataservice-proxy.api.sfwmd.gov/v2/ext/data/por"
        assert call_args[1]['endpoint'] == expected_url

    def test_get_period_of_record_parameter_validation(self, api):
        """Test station_id parameter validation."""
        # Test invalid station_id (empty string)
        with pytest.raises(ValueError, match="Invalid station ID: ''. Station ID must be a non-empty string."):
            api.get_period_of_record("")
        
        # Test invalid station_id (whitespace only)
        with pytest.raises(ValueError, match="Invalid station ID: '   '. Station ID must be a non-empty string."):
            api.get_period_of_record("   ")
        
        # Verify method signature
        import inspect
        sig = inspect.signature(api.get_period_of_record)
        assert 'station_id' in sig.parameters
        assert sig.parameters['station_id'].annotation == str
        assert sig.return_annotation == PeriodOfRecord