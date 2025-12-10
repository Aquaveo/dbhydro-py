"""Tests for DbHydroApi class."""

import pytest
from unittest.mock import Mock
from datetime import datetime

from dbhydro_py.api import DbHydroApi
from dbhydro_py.exceptions import DbHydroException
from dbhydro_py.models.transport import Result


class TestDbHydroApi:
    """Test cases for DbHydroApi."""
    
    def test_init(self):
        """Test API client initialization."""
        mock_adapter = Mock()
        api = DbHydroApi(
            rest_adapter=mock_adapter,
            client_id="test_id",
            client_secret="test_secret"
        )
        
        assert api.rest_adapter == mock_adapter
        assert api._client_id == "test_id"
        assert api._client_secret == "test_secret"
        assert "v1" in api.base_url
    
    def test_parse_date_datetime_object(self, api_client):
        """Test date parsing with datetime object."""
        dt = datetime(2023, 1, 1, 12, 30, 45, 123000)
        result = api_client._parse_date(dt)
        assert result == "2023-01-0112:30:45:123"
    
    def test_parse_date_string_formats(self, api_client):
        """Test date parsing with various string formats."""
        test_cases = [
            ("2023-01-01", "2023-01-0100:00:00:000"),
            ("2023-01-01 12:30", "2023-01-0112:30:00:000"),
            ("2023-01-01T12:30:45", "2023-01-0112:30:45:000"),
            ("2023-01-0112:30:45:123", "2023-01-0112:30:45:123"),
        ]
        
        for input_date, expected in test_cases:
            result = api_client._parse_date(input_date)
            assert result == expected
    
    def test_parse_date_invalid_format(self, api_client):
        """Test date parsing with invalid format raises error."""
        with pytest.raises(ValueError, match="Invalid date format"):
            api_client._parse_date("")
    
    def test_handle_date_parameters_valid_range(self, api_client):
        """Test date parameter handling with valid date range."""
        start, end = api_client._handle_date_parameters("2023-01-01", "2023-01-02")
        assert start == "2023-01-0100:00:00:000"
        assert end == "2023-01-0200:00:00:000"
    
    def test_handle_date_parameters_same_date(self, api_client):
        """Test date parameter handling with same start and end date."""
        start, end = api_client._handle_date_parameters("2023-01-01", "2023-01-01")
        assert start == "2023-01-0100:00:00:000"
        assert end == "2023-01-0100:00:00:000"
    
    def test_handle_date_parameters_invalid_range(self, api_client):
        """Test date parameter handling with invalid date range."""
        with pytest.raises(ValueError, match="'date_start' must be earlier or equal to 'date_end'"):
            api_client._handle_date_parameters("2023-01-02", "2023-01-01")

    def test_validate_calculation_parameters(self, api_client):
        """Test calculation parameter validation helper method."""
        # Valid parameters should pass
        api_client._validate_calculation_parameters('MEAN', 'DAY', 1)
        
        # None calculation with None timespan_unit should pass
        api_client._validate_calculation_parameters(None, None, None)
        
        # Invalid calculation should fail
        with pytest.raises(ValueError, match="Invalid calculation type"):
            api_client._validate_calculation_parameters('INVALID', 'DAY', 1)
        
        # Invalid timespan_unit should fail
        with pytest.raises(ValueError, match="Invalid timespan_unit"):
            api_client._validate_calculation_parameters('MEAN', 'INVALID', 1)
        
        # Invalid timespan_value should fail 
        with pytest.raises(ValueError, match="Invalid timespan_value"):
            api_client._validate_calculation_parameters('MEAN', 'DAY', 0)
        
        # Negative timespan_value should fail
        with pytest.raises(ValueError, match="Invalid timespan_value"):
            api_client._validate_calculation_parameters('MEAN', 'DAY', -1)
        
        # Non-integer timespan_value should fail
        with pytest.raises(ValueError, match="Invalid timespan_value"):
            api_client._validate_calculation_parameters('MEAN', 'DAY', 1.5)
        
        with pytest.raises(ValueError, match="Invalid timespan_value"):
            api_client._validate_calculation_parameters('MEAN', 'DAY', "1")
        
        # Calculation without timespan_unit should fail
        with pytest.raises(ValueError, match="If 'calculation' is provided, 'timespan_unit' must also be provided"):
            api_client._validate_calculation_parameters('MEAN', None, 1)
        
        # timespan_unit without calculation should fail
        with pytest.raises(ValueError, match="If 'calculation' is None, 'timespan_unit' must also be None"):
            api_client._validate_calculation_parameters(None, 'DAY', 1)
    
    def test_validate_calculation_parameters_custom_sets(self, api_client):
        """Test validation helper with custom validation sets."""
        custom_calcs = {'AVERAGE', 'MAXIMUM'}
        custom_units = {'DAILY', 'MONTHLY'}
        
        # Should pass with custom valid values
        api_client._validate_calculation_parameters(
            'AVERAGE', 'DAILY', 1, custom_calcs, custom_units
        )
        
        # Should fail with default value not in custom set
        with pytest.raises(ValueError, match="Invalid calculation type"):
            api_client._validate_calculation_parameters(
                'MEAN', 'DAILY', 1, custom_calcs, custom_units
            )
        
        # Should fail with invalid timespan_unit in custom set
        with pytest.raises(ValueError, match="Invalid timespan_unit"):
            api_client._validate_calculation_parameters(
                'AVERAGE', 'DAY', 1, custom_calcs, custom_units
            )
    
    def test_network_error_handling(self, api_client):
        """Test that network errors from REST adapter are properly handled."""
        from dbhydro_py.models.transport import Result
        from dbhydro_py.exceptions import DbHydroException
        
        # Mock a network error response (status_code=0)
        api_client.rest_adapter.get.return_value = Result(
            status_code=0, 
            message="Request failed: Connection failed", 
            data={}
        )
        
        # Should raise DbHydroException for network errors
        with pytest.raises(DbHydroException, match="Request failed: Connection failed"):
            api_client._perform_request("https://test.com/api", {})