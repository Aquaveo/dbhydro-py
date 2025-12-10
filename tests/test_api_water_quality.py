"""Tests for water quality endpoint in DbHydroApi."""

import pytest
from unittest.mock import Mock
from datetime import datetime

from dbhydro_py.api import DbHydroApi
from dbhydro_py.exceptions import DbHydroException


class TestWaterQualityEndpoint:
    """Test cases for water quality endpoint."""

    def test_get_water_quality_not_implemented(self, api_client):
        """Test that get_water_quality raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="The waterquality endpoint is not currently implemented"):
            api_client.get_water_quality(
                project_code="8SQM"
            )

    def test_get_water_quality_no_search_parameters(self, api_client):
        """Test that no search parameters raises ValueError."""
        with pytest.raises(ValueError, match="At least one search parameter is required"):
            api_client.get_water_quality()

    def test_get_water_quality_project_code_only(self, api_client):
        """Test that project_code alone is sufficient."""
        with pytest.raises(NotImplementedError):
            api_client.get_water_quality(project_code="8SQM")

    def test_get_water_quality_test_number_only(self, api_client):
        """Test that test_number alone is sufficient."""
        with pytest.raises(NotImplementedError):
            api_client.get_water_quality(test_number=7)

    def test_get_water_quality_station_only(self, api_client):
        """Test that station alone is sufficient."""
        with pytest.raises(NotImplementedError):
            api_client.get_water_quality(station="G211")

    def test_get_water_quality_all_search_parameters(self, api_client):
        """Test that all search parameters can be used together."""
        with pytest.raises(NotImplementedError):
            api_client.get_water_quality(
                project_code="8SQM",
                test_number=7,
                station="G211"
            )

    def test_get_water_quality_invalid_test_number_type(self, api_client):
        """Test that non-integer test_number raises ValueError."""
        with pytest.raises(ValueError, match="test_number must be an integer"):
            api_client.get_water_quality(test_number="7")
        
        with pytest.raises(ValueError, match="test_number must be an integer"):
            api_client.get_water_quality(test_number=7.5)

    def test_get_water_quality_date_start_without_date_end(self, api_client):
        """Test that providing only date_start raises ValueError."""
        with pytest.raises(ValueError, match="Both date_start and date_end must be provided together"):
            api_client.get_water_quality(
                project_code="8SQM",
                date_start="2023-01-01"
            )

    def test_get_water_quality_date_end_without_date_start(self, api_client):
        """Test that providing only date_end raises ValueError."""
        with pytest.raises(ValueError, match="Both date_start and date_end must be provided together"):
            api_client.get_water_quality(
                project_code="8SQM",
                date_end="2023-01-02"
            )

    def test_get_water_quality_valid_date_range(self, api_client):
        """Test that valid date range passes validation."""
        with pytest.raises(NotImplementedError):  # Should get to NotImplementedError, not date validation error
            api_client.get_water_quality(
                project_code="8SQM",
                date_start="2023-01-01",
                date_end="2023-01-02"
            )

    def test_get_water_quality_invalid_date_range(self, api_client):
        """Test that invalid date range raises ValueError."""
        with pytest.raises(ValueError, match="The 'date_start' must be earlier or equal to 'date_end'"):
            api_client.get_water_quality(
                project_code="8SQM",
                date_start="2023-01-02",
                date_end="2023-01-01"
            )

    def test_get_water_quality_datetime_objects(self, api_client):
        """Test with datetime objects."""
        start_dt = datetime(2023, 1, 1, 12, 30, 45)
        end_dt = datetime(2023, 1, 2, 15, 45, 30)
        
        with pytest.raises(NotImplementedError):
            api_client.get_water_quality(
                station="G211",
                date_start=start_dt,
                date_end=end_dt
            )

    def test_get_water_quality_exclude_flagged_results_true(self, api_client):
        """Test exclude_flagged_results=True parameter."""
        with pytest.raises(NotImplementedError):
            api_client.get_water_quality(
                project_code="8SQM",
                exclude_flagged_results=True
            )

    def test_get_water_quality_exclude_flagged_results_false(self, api_client):
        """Test exclude_flagged_results=False parameter (default)."""
        with pytest.raises(NotImplementedError):
            api_client.get_water_quality(
                project_code="8SQM",
                exclude_flagged_results=False
            )

    def test_get_water_quality_complex_scenario(self, api_client):
        """Test complex scenario with all parameters."""
        with pytest.raises(NotImplementedError):
            api_client.get_water_quality(
                project_code="8SQM",
                test_number=7,
                station="G211",
                date_start="2023-09-25",
                date_end="2023-09-26",
                exclude_flagged_results=True
            )

    def test_get_water_quality_empty_strings_not_valid(self, api_client):
        """Test that empty strings don't count as valid parameters."""
        with pytest.raises(ValueError, match="At least one search parameter is required"):
            api_client.get_water_quality(
                project_code="",
                station="",
                test_number=None
            )

    def test_get_water_quality_whitespace_strings_not_valid(self, api_client):
        """Test that whitespace-only strings don't count as valid parameters."""
        with pytest.raises(ValueError, match="At least one search parameter is required"):
            api_client.get_water_quality(
                project_code="   ",
                station="   ",
                test_number=None
            )

    def test_get_water_quality_various_date_formats(self, api_client):
        """Test various date string formats are accepted."""
        date_formats = [
            ("2023-01-01", "2023-01-02"),
            ("2023-01-01 12:30", "2023-01-02 15:45"),
            ("2023-01-01T12:30:45", "2023-01-02T15:45:30"),
            ("2023-01-0112:30:45:123", "2023-01-0215:45:30:456"),
        ]
        
        for start_date, end_date in date_formats:
            with pytest.raises(NotImplementedError):  # Should reach NotImplementedError, not date parsing error
                api_client.get_water_quality(
                    project_code="8SQM",
                    date_start=start_date,
                    date_end=end_date
                )

    def test_get_water_quality_parameter_combinations(self, api_client):
        """Test various valid parameter combinations."""
        valid_combinations = [
            {"project_code": "8SQM"},
            {"test_number": 7},
            {"station": "G211"},
            {"project_code": "8SQM", "test_number": 7},
            {"project_code": "8SQM", "station": "G211"},
            {"test_number": 7, "station": "G211"},
            {"project_code": "8SQM", "test_number": 7, "station": "G211"},
        ]
        
        for params in valid_combinations:
            with pytest.raises(NotImplementedError):
                api_client.get_water_quality(**params)

    def test_get_water_quality_case_sensitivity_assumptions(self, api_client):
        """Test that string parameters are passed as-is (case sensitivity handled by API)."""
        # Test various cases - the method should accept them and let the API handle case sensitivity
        test_cases = [
            {"project_code": "8sqm"},  # lowercase
            {"project_code": "8SQM"},  # uppercase
            {"station": "g211"},       # lowercase
            {"station": "G211"},       # uppercase
        ]
        
        for params in test_cases:
            with pytest.raises(NotImplementedError):
                api_client.get_water_quality(**params)

    def test_get_water_quality_zero_test_number(self, api_client):
        """Test that test_number=0 is valid."""
        with pytest.raises(NotImplementedError):
            api_client.get_water_quality(test_number=0)

    def test_get_water_quality_negative_test_number(self, api_client):
        """Test that negative test_number is accepted (let API validate)."""
        with pytest.raises(NotImplementedError):
            api_client.get_water_quality(test_number=-1)

    def test_get_water_quality_large_test_number(self, api_client):
        """Test that large test_number is accepted (let API validate)."""
        with pytest.raises(NotImplementedError):
            api_client.get_water_quality(test_number=999999)