"""Tests for water quality endpoint in DbHydroApi."""

import pytest
from datetime import datetime

from dbhydro_py.models.transport import Result


class TestWaterQualityEndpoint:
    """Test cases for water quality endpoint."""

    def test_get_water_quality_no_search_parameters(self, api_client):
        """Test that no search parameters raises ValueError."""
        with pytest.raises(ValueError, match="At least one search parameter is required: project_codes, test_numbers, or stations."):
            api_client.get_water_quality()

    def test_get_water_quality_project_codes_only_success(self, api_client, sample_water_quality_api_response):
        """Test that project_codes alone is sufficient and returns a model with values."""
        # Setup Mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_water_quality_api_response
        )

        # Make Request
        response = api_client.get_water_quality(project_codes=["8SQM"])

        # Verify Response is not None, correct type, and has values
        from dbhydro_py.models.responses.water_quality import WaterQualityResponse, WaterQualityValue
        assert response is not None
        assert isinstance(response, WaterQualityResponse)
        assert len(response.values) > 0
        for v in response.values:
            assert isinstance(v, WaterQualityValue)

        # Verify API call parameters
        api_client.rest_adapter.get.assert_called_once()
        call_args, call_kwargs = api_client.rest_adapter.get.call_args
        params = call_kwargs.get('params', {})
        assert 'projectCode' in params
        assert params['projectCode'] == ["8SQM"] or params['projectCode'] == "8SQM"
        

    def test_get_water_quality_test_numbers_only(self, api_client, sample_water_quality_api_response):
        """Test that test_numbers alone is sufficient."""
        # Setup Mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_water_quality_api_response
        )

        # Make Request
        response = api_client.get_water_quality(test_numbers=[7])

        # Verify Response is not None, correct type, and has values
        from dbhydro_py.models.responses.water_quality import WaterQualityResponse, WaterQualityValue
        assert response is not None
        assert isinstance(response, WaterQualityResponse)
        assert len(response.values) > 0
        for v in response.values:
            assert isinstance(v, WaterQualityValue)

        # Verify API call parameters
        api_client.rest_adapter.get.assert_called_once()
        call_args, call_kwargs = api_client.rest_adapter.get.call_args
        params = call_kwargs.get('params', {})
        assert 'testNumber' in params
        assert params['testNumber'] == ["7"] or params['testNumber'] == "7" 
    
    def test_get_water_quality_test_numbers_as_string(self, api_client, sample_water_quality_api_response):
        """Test that test_numbers alone is sufficient."""
        # Setup Mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_water_quality_api_response
        )

        # Make Request
        response = api_client.get_water_quality(test_numbers=["7"])

        # Verify Response is not None, correct type, and has values
        from dbhydro_py.models.responses.water_quality import WaterQualityResponse, WaterQualityValue
        assert response is not None
        assert isinstance(response, WaterQualityResponse)
        assert len(response.values) > 0
        for v in response.values:
            assert isinstance(v, WaterQualityValue)

        # Verify API call parameters
        api_client.rest_adapter.get.assert_called_once()
        call_args, call_kwargs = api_client.rest_adapter.get.call_args
        params = call_kwargs.get('params', {})
        assert 'testNumber' in params
        assert params['testNumber'] == ["7"] or params['testNumber'] == "7" 

    def test_get_water_quality_stations_only(self, api_client, sample_water_quality_api_response):
        """Test that stations alone is sufficient."""
        # Setup Mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_water_quality_api_response
        )

        # Make Request
        response = api_client.get_water_quality(stations=["G211"])

        # Verify Response is not None, correct type, and has values
        from dbhydro_py.models.responses.water_quality import WaterQualityResponse, WaterQualityValue
        assert response is not None
        assert isinstance(response, WaterQualityResponse)
        assert len(response.values) > 0
        for v in response.values:
            assert isinstance(v, WaterQualityValue)

        # Verify API call parameters
        api_client.rest_adapter.get.assert_called_once()
        call_args, call_kwargs = api_client.rest_adapter.get.call_args
        params = call_kwargs.get('params', {})
        assert 'station' in params
        assert params['station'] == ["G211"] or params['station'] == "G211"

    def test_get_water_quality_all_search_parameters(self, api_client, sample_water_quality_api_response):
        """Test that all search parameters can be used together."""
        # Setup Mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_water_quality_api_response
        )

        # Make Request
        response = api_client.get_water_quality(
            project_codes=["8SQM"],
            test_numbers=[7],
            stations=["G211"]
        )

        # Verify Response is not None, correct type, and has values
        from dbhydro_py.models.responses.water_quality import WaterQualityResponse, WaterQualityValue
        assert response is not None
        assert isinstance(response, WaterQualityResponse)
        assert len(response.values) > 0
        for v in response.values:
            assert isinstance(v, WaterQualityValue)

        # Verify API call parameters
        api_client.rest_adapter.get.assert_called_once()
        call_args, call_kwargs = api_client.rest_adapter.get.call_args
        params = call_kwargs.get('params', {})
        assert 'station' in params
        assert params['projectCode'] == ["8SQM"] or params['projectCode'] == "8SQM"
        assert params['testNumber'] == ["7"] or params['testNumber'] == "7"
        assert params['station'] == ["G211"] or params['station'] == "G211"

    def test_get_water_quality_invalid_test_number_type(self, api_client):
        """Test that non-integer test_number raises ValueError."""
        with pytest.raises(ValueError, match="Each test number must be an integer."):
            api_client.get_water_quality(test_numbers="a")
        
        with pytest.raises(ValueError, match="Each test number must be an integer."):
            api_client.get_water_quality(test_numbers=["a"])

        with pytest.raises(ValueError, match="Each test number must be an integer."):
            api_client.get_water_quality(test_numbers="")
        
        with pytest.raises(ValueError, match="Each test number must be an integer."):
            api_client.get_water_quality(test_numbers=[""])
        
        with pytest.raises(ValueError, match="If provided, test_numbers must contain at least one value."):
            api_client.get_water_quality(test_numbers=[])

    def test_get_water_quality_date_start_without_date_end(self, api_client):
        """Test that providing only date_start raises ValueError."""
        with pytest.raises(ValueError, match="Both date_start and date_end must be provided together"):
            api_client.get_water_quality(
                project_codes="8SQM",
                date_start="2023-01-01"
            )

    def test_get_water_quality_date_end_without_date_start(self, api_client):
        """Test that providing only date_end raises ValueError."""
        with pytest.raises(ValueError, match="Both date_start and date_end must be provided together"):
            api_client.get_water_quality(
                project_codes="8SQM",
                date_end="2023-01-02"
            )

    def test_get_water_quality_valid_date_range(self, api_client, sample_water_quality_api_response):
        """Test that valid date range passes validation."""
        # Setup Mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_water_quality_api_response
        )
        
        # Make Request
        response = api_client.get_water_quality(
            project_codes="8SQM",
            date_start="2023-01-01",
            date_end="2023-01-02"
        )
        
        assert response is not None
        assert hasattr(response, "values")
        api_client.rest_adapter.get.assert_called_once()

    def test_get_water_quality_invalid_date_range(self, api_client):
        """Test that invalid date range raises ValueError."""
        with pytest.raises(ValueError, match="The 'date_start' must be earlier or equal to 'date_end'"):
            api_client.get_water_quality(
                project_codes="8SQM",
                date_start="2023-01-02",
                date_end="2023-01-01"
            )

    def test_get_water_quality_datetime_objects(self, api_client, sample_water_quality_api_response):
        """Test with datetime objects."""
        # Setup Mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_water_quality_api_response
        )
        
        # Setup datetime objects
        start_dt = datetime(2023, 1, 1, 12, 30, 45)
        end_dt = datetime(2023, 1, 2, 15, 45, 30)
        
        # Make Request
        response = api_client.get_water_quality(
            stations="G211",
            date_start=start_dt,
            date_end=end_dt
        )
        
        # Verify Response
        assert response is not None
        assert hasattr(response, "values")
        api_client.rest_adapter.get.assert_called_once()

    def test_get_water_quality_exclude_flagged_results_true(self, api_client, sample_water_quality_api_response):
        """Test exclude_flagged_results=True parameter."""
        # Setup Mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_water_quality_api_response
        )

        # Make Request
        response = api_client.get_water_quality(
            project_codes="8SQM",
            exclude_flagged_results=True
        )

        # Verify Response
        assert response is not None
        assert hasattr(response, "values")
        api_client.rest_adapter.get.assert_called_once()
        call_args, call_kwargs = api_client.rest_adapter.get.call_args
        params = call_kwargs.get('params', {})
        assert params.get('excludeFlaggedResults') == 'Y'

    def test_get_water_quality_exclude_flagged_results_false(self, api_client, sample_water_quality_api_response):
        """Test exclude_flagged_results=False parameter (default)."""
        # Setup Mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_water_quality_api_response
        )

        # Make Request
        response = api_client.get_water_quality(
            project_codes="8SQM",
            exclude_flagged_results=False
        )

        # Verify Response
        assert response is not None
        assert hasattr(response, "values")
        api_client.rest_adapter.get.assert_called_once()
        call_args, call_kwargs = api_client.rest_adapter.get.call_args
        params = call_kwargs.get('params', {})
        
        # Should not include excludeFlaggedResults or should be 'N'
        assert params.get('excludeFlaggedResults', 'N') == 'N'

    def test_get_water_quality_complex_scenario(self, api_client, sample_water_quality_api_response):
        """Test complex scenario with all parameters."""
        # Setup Mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_water_quality_api_response
        )

        # Make Request
        response = api_client.get_water_quality(
            project_codes="8SQM",
            test_numbers=7,
            stations="G211",
            date_start="2023-09-25",
            date_end="2023-09-26",
            exclude_flagged_results=True
        )

        # Verify Response
        assert response is not None
        assert hasattr(response, "values")
        api_client.rest_adapter.get.assert_called_once()
        call_args, call_kwargs = api_client.rest_adapter.get.call_args
        params = call_kwargs.get('params', {})
        assert params.get('projectCode') == '8SQM'
        assert params.get('testNumber') == '7'
        assert params.get('station') == 'G211'
        assert params.get('beginDateTime') == '2023-09-2500:00:00:000'
        assert params.get('endDateTime') == '2023-09-2600:00:00:000'
        assert params.get('excludeFlaggedResults') == 'Y'

    def test_get_water_quality_various_date_formats(self, api_client):
        """Test various date string formats are accepted."""
        # Setup Mock
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=[]
        )
        
        # Test various date formats
        date_formats = [
            ("2023-01-01", "2023-01-02"),
            ("2023-01-01 12:30", "2023-01-02 15:45"),
            ("2023-01-01T12:30:45", "2023-01-02T15:45:30"),
            ("2023-01-0112:30:45:123", "2023-01-0215:45:30:456"),
        ]
        
        # Make Requests
        for start_date, end_date in date_formats:
            api_client.get_water_quality(
                project_codes="8SQM",
                date_start=start_date,
                date_end=end_date
            )
        
        # Verify API call count
        assert api_client.rest_adapter.get.call_count == len(date_formats)

    def test_get_water_quality_parameter_combinations(self, api_client, sample_water_quality_api_response):
        """Test various valid parameter combinations."""
        valid_combinations = [
            {"project_codes": "8SQM"},
            {"test_numbers": 7},
            {"stations": "G211"},
            {"project_codes": "8SQM", "test_numbers": 7},
            {"project_codes": "8SQM", "stations": "G211"},
            {"test_numbers": 7, "stations": "G211"},
            {"project_codes": "8SQM", "test_numbers": 7, "stations": "G211"},
        ]
        for params in valid_combinations:
            api_client.rest_adapter.get.reset_mock()
            api_client.rest_adapter.get.return_value = Result(
                status_code=200,
                message="OK",
                data=sample_water_quality_api_response
            )
            response = api_client.get_water_quality(**params)
            assert response is not None
            assert hasattr(response, "values")
            api_client.rest_adapter.get.assert_called_once()

    def test_get_water_quality_case_sensitivity_assumptions(self, api_client, sample_water_quality_api_response):
        """Test that string parameters are passed as-is (case sensitivity handled by API)."""
        test_cases = [
            {"project_codes": "8sqm"},  # lowercase
            {"project_codes": "8SQM"},  # uppercase
            {"stations": "g211"},       # lowercase
            {"stations": "G211"},       # uppercase
        ]
        for params in test_cases:
            api_client.rest_adapter.get.reset_mock()
            api_client.rest_adapter.get.return_value = Result(
                status_code=200,
                message="OK",
                data=sample_water_quality_api_response
            )
            response = api_client.get_water_quality(**params)
            assert response is not None
            assert hasattr(response, "values")
            api_client.rest_adapter.get.assert_called_once()

    def test_get_water_quality_zero_test_number(self, api_client, sample_water_quality_api_response):
        """Test that test_number=0 is valid."""
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_water_quality_api_response
        )
        response = api_client.get_water_quality(test_numbers=0)
        assert response is not None
        assert hasattr(response, "values")
        api_client.rest_adapter.get.assert_called_once()

    def test_get_water_quality_negative_test_number(self, api_client, sample_water_quality_api_response):
        """Test that negative test_number is accepted (let API validate)."""
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_water_quality_api_response
        )
        response = api_client.get_water_quality(test_numbers=-1)
        assert response is not None
        assert hasattr(response, "values")
        api_client.rest_adapter.get.assert_called_once()

    def test_get_water_quality_large_test_number(self, api_client, sample_water_quality_api_response):
        """Test that large test_number is accepted (let API validate)."""
        api_client.rest_adapter.get.return_value = Result(
            status_code=200,
            message="OK",
            data=sample_water_quality_api_response
        )
        response = api_client.get_water_quality(test_numbers=999999)
        assert response is not None
        assert hasattr(response, "values")
        api_client.rest_adapter.get.assert_called_once()