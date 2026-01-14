"""Tests for water quality response models."""

import pytest
from unittest.mock import patch, Mock

from dbhydro_py.models.responses.water_quality import WaterQualityResponse, WaterQualityValue

class TestWaterQualityResponse:
    """Test cases for WaterQualityResponse."""

    def test_from_dict(self, sample_water_quality_api_response):
        """Test creating WaterQualityResponse from dictionary."""
        response = WaterQualityResponse.from_dict({"values": sample_water_quality_api_response})
        assert isinstance(response, WaterQualityResponse)
        assert len(response.values) == len(sample_water_quality_api_response)
        assert isinstance(response.values[0], WaterQualityValue)
        assert hasattr(response.values[0], "station")

    @patch('pandas.DataFrame')
    def test_to_dataframe_without_pandas_raises_error(self, mock_df, sample_water_quality_api_response):
        """Test that missing pandas raises ImportError."""
        with patch('builtins.__import__', side_effect=ImportError):
            response = WaterQualityResponse.from_dict({"values": sample_water_quality_api_response})
            with pytest.raises(ImportError, match="pandas is required"):
                response.to_dataframe()

    def test_to_dataframe_with_data(self, sample_water_quality_api_response):
        """Test DataFrame conversion with actual data."""
        pytest.importorskip("pandas")
        response = WaterQualityResponse.from_dict({"values": sample_water_quality_api_response})
        df = response.to_dataframe()
        assert not df.empty
        assert len(df) == len(sample_water_quality_api_response)
        assert df['station'].iloc[0] == sample_water_quality_api_response[0]['station']

    def test_to_dataframe_with_metadata(self, sample_water_quality_api_response):
        """Test DataFrame conversion with metadata columns."""
        pytest.importorskip("pandas")
        
        # List of expected columns when include_metadata=True
        expected_columns = [
            'station', 'project_code', 'date_collected', 'date_collected_str',
            'sample_type', 'program_type', 'matrix', 'collect_method',
            'first_trigger_date', 'first_trigger_date_str', 'depth', 'depth_units',
            'test_number', 'parameter', 'data_type', 'value', 'remark_code', 'flag',
            'sig_fig_value', 'uncertainty', 'dilution', 'mdl', 'pql', 'rdl',
            'units', 'n_dec', 'bdl', 'quality_code', 'sample_id', 'up_down_stream',
            'discharge', 'weather', 'dcs_meters', 'total_depth', 'upper_depth',
            'lower_depth', 'latitude', 'longitude', 'collection_agency', 'work_lab',
            'source', 'owner', 'validator', 'validation_level', 'sampling_purpose',
            'data_investigation', 'receive_date', 'receive_date_str',
            'measure_date', 'measure_date_str', 'method', 'filtration_date',
            'filtration_date_str', 'sample_comment', 'result_comment',
            'collection_span', 'lims_number', 'storet_code'
        ]
        
        # Make request
        response = WaterQualityResponse.from_dict({"values": sample_water_quality_api_response})
        df = response.to_dataframe(include_metadata=True)

        # Verify all expected columns are present
        assert len(df.columns) == len(expected_columns)
        for col in expected_columns:
            assert col in df.columns, f"Missing column: {col}"

    def test_to_dataframe_without_metadata(self, sample_water_quality_api_response):
        """Test DataFrame conversion without metadata columns."""
        pytest.importorskip("pandas")
        response = WaterQualityResponse.from_dict({"values": sample_water_quality_api_response})
        df = response.to_dataframe(include_metadata=False)
    
        assert len(df.columns) == 6
        assert 'station' in df.columns
        assert 'parameter' in df.columns
        assert 'value' in df.columns
        assert 'sig_fig_value' in df.columns
        assert 'date_collected_str' in df.columns
        assert 'units' in df.columns
