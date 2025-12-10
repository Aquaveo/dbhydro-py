"""Tests for time series response models."""

import pytest
from unittest.mock import patch, Mock

from dbhydro_py.models.responses.time_series import TimeSeriesResponse, TimeSeriesEntry


class TestTimeSeriesResponse:
    """Test cases for TimeSeriesResponse."""
    
    def test_from_dict(self, sample_time_series_response):
        """Test creating TimeSeriesResponse from dictionary."""
        response_data = sample_time_series_response["timeSeriesResponse"]
        response = TimeSeriesResponse.from_dict(response_data)
        
        assert response.status.status_code == 200
        assert response.status.message == "Success"
        assert len(response.time_series) == 1
        assert response.time_series[0].source_info.site_code.value == "S123-R"
    
    @patch('pandas.DataFrame')
    def test_to_dataframe_without_pandas_raises_error(self, mock_df, sample_time_series_response):
        """Test that missing pandas raises ImportError."""
        # Mock pandas import failure
        with patch('builtins.__import__', side_effect=ImportError):
            response_data = sample_time_series_response["timeSeriesResponse"]
            response = TimeSeriesResponse.from_dict(response_data)
            
            with pytest.raises(ImportError, match="pandas is required"):
                response.to_dataframe()
    
    def test_to_dataframe_with_data(self, sample_time_series_response):
        """Test DataFrame conversion with actual data."""
        pytest.importorskip("pandas")  # Skip if pandas not available
        
        response_data = sample_time_series_response["timeSeriesResponse"]
        response = TimeSeriesResponse.from_dict(response_data)
        
        df = response.to_dataframe()
        
        assert not df.empty
        assert len(df) == 2  # Two data points
        assert 'value' in df.columns
        assert 'site_code' in df.columns
        assert df['site_code'].iloc[0] == 'S123-R'
    
    def test_to_dataframe_empty_data(self):
        """Test DataFrame conversion with empty data."""
        pytest.importorskip("pandas")
        
        # Create response with no time series data
        empty_response = TimeSeriesResponse(
            status=Mock(status_code=200, message="Success", elapsed_time=0.1),
            time_series=[]
        )
        
        df = empty_response.to_dataframe()
        
        assert df.empty
        assert 'datetime' in df.columns or df.index.name == 'datetime'
        assert 'value' in df.columns
        assert 'site_code' in df.columns
    
    def test_to_dataframe_empty_values_but_has_columns(self):
        """Test DataFrame conversion when time_series exists but has no data values."""
        pytest.importorskip("pandas")
        
        # Mock the nested structure for a time series with metadata but no values
        mock_source_info = Mock()
        mock_source_info.site_code.value = "S123-R"
        mock_source_info.site_name = "Test Site"
        
        mock_parameter = Mock()
        mock_parameter.parameter_code.value = "62610"
        mock_parameter.parameter_name = "Groundwater Level"
        mock_parameter.unit.unit_code = "ft"
        
        mock_time_series = Mock()
        mock_time_series.source_info = mock_source_info
        mock_time_series.parameter = mock_parameter
        mock_time_series.values = []  # Empty values list
        
        # Create response with time series metadata but no data points
        response_with_empty_values = TimeSeriesResponse(
            status=Mock(status_code=200, message="Success", elapsed_time=0.1),
            time_series=[mock_time_series]
        )
        
        # Test basic DataFrame
        df = response_with_empty_values.to_dataframe()
        
        assert df.empty  # No data rows
        assert len(df.columns) > 0  # But has column structure
        assert df.index.name == 'datetime'  # Single site gets datetime index
        assert 'value' in df.columns
        assert 'site_code' in df.columns
        
        # Test DataFrame with metadata
        df_with_meta = response_with_empty_values.to_dataframe(include_metadata=True)
        
        assert df_with_meta.empty  # No data rows
        assert len(df_with_meta.columns) > len(df.columns)  # More columns with metadata
        assert 'site_name' in df_with_meta.columns
        assert 'parameter_name' in df_with_meta.columns
        assert 'unit_code' in df_with_meta.columns


class TestTimeSeriesEntry:
    """Test cases for TimeSeriesEntry."""
    
    def test_from_dict_with_null_values(self):
        """Test that null values are converted to empty list."""
        data = {
            "sourceInfo": {"siteName": "Test", "siteCode": {"value": "TEST"}, "geoLocation": {}},
            "periodOfRecord": {},
            "name": "Test",
            "description": "Test",
            "timeSeriesId": "123",
            "referenceElevation": {"values": []},
            "parameter": {
                "parameterCode": {"value": "TEST"},
                "parameterName": "Test",
                "parameterDescription": "Test",
                "unit": {"unitCode": "ft"},
                "noDataValue": -999
            },
            "values": None  # This should be converted to empty list
        }
        
        entry = TimeSeriesEntry.from_dict(data)
        assert entry.values == []
        assert isinstance(entry.values, list)


class TestTimeSeriesResponseConvenienceMethods:
    """Test cases for TimeSeriesResponse convenience methods."""
    
    @pytest.fixture
    def multi_site_response(self):
        """Create a response with multiple sites for testing."""
        from dbhydro_py.models.responses.time_series import (
            TimeSeriesResponse, TimeSeriesEntry, SourceInfo, SiteCode, 
            GeoLocation, GeogLocation, PeriodOfRecord, Parameter, 
            ParameterCode, Unit, ObservationValue, ReferenceElevation
        )
        from dbhydro_py.models.responses.base import Status
        
        # Create mock data for two sites
        site1_source = SourceInfo(
            site_name="Site One",
            site_code=SiteCode(network="SFWMD", agency_code="SFWMD", value="S123-R"),
            geo_location=GeoLocation(
                geog_location=GeogLocation(type="Point", srs="EPSG:4326", latitude=25.1, longitude=-80.1)
            )
        )
        
        site2_source = SourceInfo(
            site_name="Site Two", 
            site_code=SiteCode(network="SFWMD", agency_code="SFWMD", value="S124-R"),
            geo_location=GeoLocation(
                geog_location=GeogLocation(type="Point", srs="EPSG:4326", latitude=25.2, longitude=-80.2)
            )
        )
        
        param1 = Parameter(
            parameter_code=ParameterCode(parameter_id="62610", value="62610"),
            parameter_name="Groundwater Level",
            parameter_description="Depth to water level, feet below land surface",
            unit=Unit(unit_code="ft"),
            no_data_value=-999.0
        )
        
        param2 = Parameter(
            parameter_code=ParameterCode(parameter_id="00065", value="00065"),
            parameter_name="Gage Height",
            parameter_description="Gage height, feet",
            unit=Unit(unit_code="ft"),
            no_data_value=-999.0
        )
        
        values1 = [
            ObservationValue(
                qualifier=None,
                quality_code="A",
                date_time="2023-01-01T12:00:00:000",
                value=10.5,
                percent_available=100.0
            ),
            ObservationValue(
                qualifier=None,
                quality_code="P",
                date_time="2023-01-01T13:00:00:000",
                value=10.8,
                percent_available=100.0
            ),
            ObservationValue(
                qualifier=None,
                quality_code="A",
                date_time="2023-01-01T14:00:00:000",
                value=None,  # Missing value
                percent_available=0.0
            )
        ]
        
        values2 = [
            ObservationValue(
                qualifier=None,
                quality_code="A",
                date_time="2023-01-01T12:00:00:000",
                value=5.2,
                percent_available=100.0
            ),
            ObservationValue(
                qualifier="e",
                quality_code="E",
                date_time="2023-01-01T13:00:00:000",
                value=5.7,
                percent_available=100.0
            )
        ]
        
        ts1 = TimeSeriesEntry(
            source_info=site1_source,
            period_of_record=PeriodOfRecord(
                por_begin_date=None,
                por_last_date=None,
                provisional_begin_date=None,
                provisional_last_date=None,
                approved_begin_date=None,
                approved_last_date=None
            ),
            name="Site One Groundwater",
            description="Groundwater level at Site One",
            application_name=None,
            recorder_class=None,
            current_status=None,
            time_series_id="123",
            reference_elevation=ReferenceElevation(),
            parameter=param1,
            values=values1
        )

        ts2 = TimeSeriesEntry(
            source_info=site2_source,
            period_of_record=PeriodOfRecord(
                por_begin_date=None,
                por_last_date=None,
                provisional_begin_date=None,
                provisional_last_date=None,
                approved_begin_date=None,
                approved_last_date=None
            ),
            name="Site Two Gage Height",
            description="Gage height at Site Two",
            application_name=None,
            recorder_class=None,
            current_status=None,
            time_series_id="124",
            reference_elevation=ReferenceElevation(),
            parameter=param2,
            values=values2
        )

        return TimeSeriesResponse(
            status=Status(status_code=200, message="Success", elapsed_time=0.1),
            time_series=[ts1, ts2]
        )
    
    def test_get_site_codes(self, multi_site_response):
        """Test getting all site codes."""
        site_codes = multi_site_response.get_site_codes()
        assert site_codes == ["S123-R", "S124-R"]
    
    def test_get_site_names(self, multi_site_response):
        """Test getting all site names."""
        site_names = multi_site_response.get_site_names()
        assert site_names == ["Site One", "Site Two"]
    
    def test_get_parameter_codes(self, multi_site_response):
        """Test getting all parameter codes."""
        param_codes = multi_site_response.get_parameter_codes()
        assert param_codes == ["62610", "00065"]
    
    def test_get_parameter_names(self, multi_site_response):
        """Test getting all parameter names."""
        param_names = multi_site_response.get_parameter_names()
        assert param_names == ["Groundwater Level", "Gage Height"]
    
    def test_get_data_for_site(self, multi_site_response):
        """Test getting data for a specific site."""
        site_data = multi_site_response.get_data_for_site("S123-R")
        assert site_data is not None
        assert site_data.source_info.site_name == "Site One"
        assert len(site_data.values) == 3
        
        # Test non-existent site
        no_site = multi_site_response.get_data_for_site("NONEXISTENT")
        assert no_site is None
    
    def test_get_latest_values(self, multi_site_response):
        """Test getting latest values for each site."""
        latest = multi_site_response.get_latest_values()
        # Site 1 last value is None, so it should be excluded
        # Site 2 last value is 5.7
        assert latest == {"S124-R": 5.7}
    
    def test_get_value_ranges(self, multi_site_response):
        """Test getting value ranges for each site."""
        ranges = multi_site_response.get_value_ranges()
        assert ranges["S123-R"] == (10.5, 10.8)  # Only non-None values
        assert ranges["S124-R"] == (5.2, 5.7)
    
    def test_get_date_range(self, multi_site_response):
        """Test getting overall date range."""
        date_range = multi_site_response.get_date_range()
        assert date_range == ("2023-01-01T12:00:00:000", "2023-01-01T14:00:00:000")
    
    def test_get_quality_summary(self, multi_site_response):
        """Test getting quality code summary."""
        quality_summary = multi_site_response.get_quality_summary()
        expected = {
            "S123-R": {"A": 2, "P": 1},
            "S124-R": {"A": 1, "E": 1}
        }
        assert quality_summary == expected
    
    def test_filter_by_quality(self, multi_site_response):
        """Test filtering by quality codes."""
        approved_only = multi_site_response.filter_by_quality(["A"])
        
        assert len(approved_only.time_series) == 2  # Both sites have 'A' quality data
        
        # Check site 1 has only 'A' quality data (2 observations)
        site1_data = approved_only.get_data_for_site("S123-R")
        assert len(site1_data.values) == 2
        assert all(obs.quality_code == "A" for obs in site1_data.values)
        
        # Check site 2 has only 'A' quality data (1 observation)  
        site2_data = approved_only.get_data_for_site("S124-R")
        assert len(site2_data.values) == 1
        assert site2_data.values[0].quality_code == "A"
        
        # Test filtering for quality that only exists in one site
        estimated_only = multi_site_response.filter_by_quality(["E"])
        assert len(estimated_only.time_series) == 1
        assert estimated_only.get_site_codes() == ["S124-R"]
    
    def test_filter_by_quality_no_matches(self, multi_site_response):
        """Test filtering with no matching quality codes."""
        no_matches = multi_site_response.filter_by_quality(["X"])
        assert len(no_matches.time_series) == 0
        assert no_matches.get_site_codes() == []
    
    def test_has_data(self, multi_site_response):
        """Test checking if response has data."""
        assert multi_site_response.has_data() is True
        
        # Test empty response
        empty_response = TimeSeriesResponse(
            status=Mock(status_code=200),
            time_series=[]
        )
        assert empty_response.has_data() is False
    
    def test_get_data_count(self, multi_site_response):
        """Test getting total data count."""
        total_count = multi_site_response.get_data_count()
        assert total_count == 5  # 3 from site1 + 2 from site2
    
    def test_get_data_counts_by_site(self, multi_site_response):
        """Test getting data counts by site."""
        counts = multi_site_response.get_data_counts_by_site()
        expected = {"S123-R": 3, "S124-R": 2}
        assert counts == expected
    
    def test_empty_response_methods(self):
        """Test convenience methods with empty response."""
        empty_response = TimeSeriesResponse(
            status=Mock(status_code=200),
            time_series=[]
        )
        
        assert empty_response.get_site_codes() == []
        assert empty_response.get_site_names() == []
        assert empty_response.get_parameter_codes() == []
        assert empty_response.get_parameter_names() == []
        assert empty_response.get_data_for_site("ANY") is None
        assert empty_response.get_latest_values() == {}
        assert empty_response.get_value_ranges() == {}
        assert empty_response.get_date_range() is None
        assert empty_response.get_quality_summary() == {}
        assert empty_response.has_data() is False
        assert empty_response.get_data_count() == 0
        assert empty_response.get_data_counts_by_site() == {}
        
        # Test filtering empty response
        filtered = empty_response.filter_by_quality(["A"])
        assert len(filtered.time_series) == 0