"""Tests for point response models."""

import pytest
from unittest.mock import patch

from dbhydro_py.models.responses.point import Point, PointResponse
from dbhydro_py.models.responses.base import Status


class TestPointModels:
    """Test cases for Point and PointResponse models."""
    
    def test_point_from_dict(self):
        """Test Point creation from dictionary."""
        data = {
            "value": 15.75,
            "timestamp": 1672574400000,
            "msSinceEpoch": 1672574400000,
            "qualityCode": "A"
        }
        
        point = Point.from_dict(data)
        
        assert point.value == 15.75
        assert point.timestamp == 1672574400000
        assert point.ms_since_epoch == 1672574400000
        assert point.quality_code == "A"
    
    def test_point_from_dict_with_null(self):
        """Test Point creation from None (null point in API response)."""
        point = Point.from_dict(None)
        
        assert point.value is None
        assert point.timestamp is None
        assert point.ms_since_epoch is None
        assert point.quality_code is None
    
    def test_point_from_dict_partial_data(self):
        """Test Point creation with partial data."""
        data = {
            "value": 12.5
            # Missing other fields
        }
        
        point = Point.from_dict(data)
        
        assert point.value == 12.5
        assert point.timestamp is None
        assert point.ms_since_epoch is None
        assert point.quality_code is None
    
    def test_point_response_from_dict(self):
        """Test PointResponse creation from dictionary."""
        data = {
            "status": {
                "statusCode": 200,
                "statusMessage": "Success",
                "elapsedTime": 0.045123
            },
            "points": [
                {
                    "value": 15.75,
                    "timestamp": 1672574400000,
                    "qualityCode": "A"
                },
                None  # Null point
            ]
        }
        
        response = PointResponse.from_dict(data)
        
        assert isinstance(response.status, Status)
        assert response.status.status_code == 200
        assert response.status.message == "Success"
        assert response.status.elapsed_time == 0.045123
        
        assert len(response.points) == 2
        assert response.points[0] is not None
        assert response.points[0].value == 15.75
        assert response.points[0].quality_code == "A"
        assert response.points[1] is None
    
    def test_point_response_with_empty_points(self):
        """Test PointResponse with empty points array."""
        data = {
            "status": {
                "statusCode": 200,
                "statusMessage": "No data",
                "elapsedTime": 0.001
            },
            "points": []
        }
        
        response = PointResponse.from_dict(data)
        
        assert response.status.status_code == 200
        assert len(response.points) == 0
    
    def test_point_response_missing_points(self):
        """Test PointResponse when points key is missing."""
        data = {
            "status": {
                "statusCode": 200,
                "statusMessage": "Success",
                "elapsedTime": 0.001
            }
            # Missing "points" key
        }
        
        response = PointResponse.from_dict(data)
        
        assert response.status.status_code == 200
        assert len(response.points) == 0  # Should default to empty list
    
    def test_to_dataframe_basic(self):
        """Test basic DataFrame conversion."""
        response = PointResponse(
            status=Status(status_code=200, message="Success", elapsed_time=0.01),
            points=[
                Point(value=15.75, timestamp=1672574400000, ms_since_epoch=1672574400000, quality_code="A"),
                Point(value=20.25, timestamp=1672574460000, ms_since_epoch=1672574460000, quality_code="B")
            ]
        )
        
        with patch('pandas.DataFrame') as mock_dataframe:
            df = response.to_dataframe()
            
            # Verify DataFrame was called with correct data
            mock_dataframe.assert_called_once()
            call_args = mock_dataframe.call_args[0][0]
            
            assert len(call_args) == 2
            assert call_args[0]['value'] == 15.75
            assert call_args[0]['timestamp'] == 1672574400000
            assert call_args[1]['value'] == 20.25
            assert call_args[1]['timestamp'] == 1672574460000
            
            # Should not include metadata columns by default
            assert 'ms_since_epoch' not in call_args[0]
            assert 'quality_code' not in call_args[0]
    
    def test_to_dataframe_with_metadata(self):
        """Test DataFrame conversion with metadata."""
        response = PointResponse(
            status=Status(status_code=200, message="Success", elapsed_time=0.01),
            points=[
                Point(value=15.75, timestamp=1672574400000, ms_since_epoch=1672574400000, quality_code="A")
            ]
        )
        
        with patch('pandas.DataFrame') as mock_dataframe:
            df = response.to_dataframe(include_metadata=True)
            
            # Verify DataFrame was called with metadata columns
            mock_dataframe.assert_called_once()
            call_args = mock_dataframe.call_args[0][0]
            
            record = call_args[0]
            assert record['value'] == 15.75
            assert record['timestamp'] == 1672574400000
            assert record['ms_since_epoch'] == 1672574400000
            assert record['quality_code'] == "A"
    
    def test_to_dataframe_with_null_points(self):
        """Test DataFrame conversion with null points."""
        response = PointResponse(
            status=Status(status_code=200, message="Success", elapsed_time=0.01),
            points=[
                Point(value=15.75, timestamp=1672574400000),
                None,  # Null point should be filtered out
                Point(value=20.25, timestamp=1672574460000)
            ]
        )
        
        with patch('pandas.DataFrame') as mock_dataframe:
            df = response.to_dataframe()
            
            # Verify only valid points are included
            mock_dataframe.assert_called_once()
            call_args = mock_dataframe.call_args[0][0]
            
            assert len(call_args) == 2  # Null point should be filtered out
            assert call_args[0]['value'] == 15.75
            assert call_args[1]['value'] == 20.25
    
    def test_to_dataframe_empty_response(self):
        """Test DataFrame conversion with empty/null points."""
        response = PointResponse(
            status=Status(status_code=200, message="No data", elapsed_time=0.01),
            points=[]
        )
        
        with patch('pandas.DataFrame') as mock_dataframe:
            df = response.to_dataframe()
            
            # Should create empty DataFrame with expected columns
            mock_dataframe.assert_called_once()
            call_args = mock_dataframe.call_args
            
            # Should be called with columns parameter for empty DataFrame
            assert 'columns' in call_args[1]
            expected_columns = ['value', 'timestamp']
            assert call_args[1]['columns'] == expected_columns
    
    @pytest.mark.skip(reason="Pandas import mocking is complex in test environment")
    def test_to_dataframe_without_pandas_raises_error(self):
        """Test DataFrame conversion raises ImportError when pandas not available."""
        # This functionality is tested manually but hard to mock in test environment
        pass


class TestPointResponseConvenienceMethods:
    """Test convenience methods for PointResponse."""

    @pytest.fixture
    def sample_response(self):
        """Create a response with mixed data for testing."""
        from dbhydro_py.models.responses.point import PointResponse, Point
        from dbhydro_py.models.responses.base import Status
        
        points = [
            Point(value=10.5, timestamp=1672574400000, ms_since_epoch=1672574400000, quality_code="A"),
            Point(value=12.3, timestamp=1672574460000, ms_since_epoch=1672574460000, quality_code="A"),
            Point(value=15.7, timestamp=1672574520000, ms_since_epoch=1672574520000, quality_code="P"),
            None,  # Null point
            Point(value=None, timestamp=1672574640000, ms_since_epoch=1672574640000, quality_code="M"),
            Point(value=18.2, timestamp=1672574700000, ms_since_epoch=1672574700000, quality_code="E"),
        ]
        
        status = Status(status_code=200, message="Success", elapsed_time=0.1)
        return PointResponse(status=status, points=points)

    def test_get_valid_points(self, sample_response):
        """Test getting valid (non-None) points."""
        valid = sample_response.get_valid_points()
        assert len(valid) == 5  # 6 total points - 1 None point
        assert all(point is not None for point in valid)

    def test_get_values(self, sample_response):
        """Test getting all non-null values."""
        values = sample_response.get_values()
        assert values == [10.5, 12.3, 15.7, 18.2]  # Excludes None value

    def test_get_quality_codes(self, sample_response):
        """Test getting unique quality codes."""
        quality_codes = sample_response.get_quality_codes()
        assert quality_codes == ["A", "E", "M", "P"]  # Sorted

    def test_get_timestamps(self, sample_response):
        """Test getting timestamps."""
        timestamps = sample_response.get_timestamps()
        expected = [1672574400000, 1672574460000, 1672574520000, 1672574640000, 1672574700000]
        assert timestamps == expected

    def test_get_value_range(self, sample_response):
        """Test getting value range."""
        value_range = sample_response.get_value_range()
        assert value_range == (10.5, 18.2)

    def test_get_value_range_empty(self):
        """Test getting value range with no valid values."""
        from dbhydro_py.models.responses.point import PointResponse
        from dbhydro_py.models.responses.base import Status
        
        status = Status(status_code=200, message="Success", elapsed_time=0.1)
        response = PointResponse(status=status, points=[])
        
        assert response.get_value_range() is None

    def test_get_timestamp_range(self, sample_response):
        """Test getting timestamp range."""
        timestamp_range = sample_response.get_timestamp_range()
        assert timestamp_range == (1672574400000, 1672574700000)

    def test_get_timestamp_range_empty(self):
        """Test getting timestamp range with no data."""
        from dbhydro_py.models.responses.point import PointResponse
        from dbhydro_py.models.responses.base import Status
        
        status = Status(status_code=200, message="Success", elapsed_time=0.1)
        response = PointResponse(status=status, points=[])
        
        assert response.get_timestamp_range() is None

    def test_filter_by_quality(self, sample_response):
        """Test filtering by quality codes."""
        filtered = sample_response.filter_by_quality(["A"])
        valid_points = filtered.get_valid_points()
        
        assert len(valid_points) == 2  # Two 'A' quality points
        assert all(point.quality_code == "A" for point in valid_points)

    def test_filter_by_quality_no_matches(self, sample_response):
        """Test filtering by quality codes with no matches."""
        filtered = sample_response.filter_by_quality(["X"])
        assert len(filtered.get_valid_points()) == 0

    def test_get_average_value(self, sample_response):
        """Test getting average value."""
        avg = sample_response.get_average_value()
        expected = (10.5 + 12.3 + 15.7 + 18.2) / 4
        assert abs(avg - expected) < 0.01

    def test_get_average_value_empty(self):
        """Test getting average value with no data."""
        from dbhydro_py.models.responses.point import PointResponse
        from dbhydro_py.models.responses.base import Status
        
        status = Status(status_code=200, message="Success", elapsed_time=0.1)
        response = PointResponse(status=status, points=[])
        
        assert response.get_average_value() is None

    def test_get_quality_summary(self, sample_response):
        """Test getting quality code summary."""
        summary = sample_response.get_quality_summary()
        expected = {"A": 2, "P": 1, "M": 1, "E": 1}
        assert summary == expected

    def test_get_data_count(self, sample_response):
        """Test getting data count."""
        count = sample_response.get_data_count()
        assert count == 5  # 6 points - 1 None point

    def test_get_null_count(self, sample_response):
        """Test getting null count."""
        null_count = sample_response.get_null_count()
        assert null_count == 1  # 1 None point

    def test_has_data(self, sample_response):
        """Test checking if response has data."""
        assert sample_response.has_data() is True
        
        # Test empty response
        from dbhydro_py.models.responses.point import PointResponse
        from dbhydro_py.models.responses.base import Status
        
        status = Status(status_code=200, message="Success", elapsed_time=0.1)
        empty_response = PointResponse(status=status, points=[])
        assert empty_response.has_data() is False

    def test_has_null_points(self, sample_response):
        """Test checking if response has null points."""
        assert sample_response.has_null_points() is True
        
        # Test response without null points
        from dbhydro_py.models.responses.point import PointResponse, Point
        from dbhydro_py.models.responses.base import Status
        
        points = [Point(value=10.5, timestamp=1672574400000, quality_code="A")]
        status = Status(status_code=200, message="Success", elapsed_time=0.1)
        no_null_response = PointResponse(status=status, points=points)
        assert no_null_response.has_null_points() is False

    def test_get_latest_value(self, sample_response):
        """Test getting latest value."""
        latest = sample_response.get_latest_value()
        assert latest == 18.2  # Value with latest timestamp (1672574700000)

    def test_get_latest_value_empty(self):
        """Test getting latest value with no data."""
        from dbhydro_py.models.responses.point import PointResponse
        from dbhydro_py.models.responses.base import Status
        
        status = Status(status_code=200, message="Success", elapsed_time=0.1)
        response = PointResponse(status=status, points=[])
        
        assert response.get_latest_value() is None

    def test_get_earliest_value(self, sample_response):
        """Test getting earliest value."""
        earliest = sample_response.get_earliest_value()
        assert earliest == 10.5  # Value with earliest timestamp (1672574400000)

    def test_get_earliest_value_empty(self):
        """Test getting earliest value with no data."""
        from dbhydro_py.models.responses.point import PointResponse
        from dbhydro_py.models.responses.base import Status
        
        status = Status(status_code=200, message="Success", elapsed_time=0.1)
        response = PointResponse(status=status, points=[])
        
        assert response.get_earliest_value() is None

    def test_get_points_by_quality(self, sample_response):
        """Test grouping points by quality code."""
        grouped = sample_response.get_points_by_quality()
        
        assert "A" in grouped
        assert "P" in grouped
        assert "M" in grouped
        assert "E" in grouped
        
        assert len(grouped["A"]) == 2
        assert len(grouped["P"]) == 1
        assert len(grouped["M"]) == 1
        assert len(grouped["E"]) == 1

    def test_empty_response_methods(self):
        """Test convenience methods with empty response."""
        from dbhydro_py.models.responses.point import PointResponse
        from dbhydro_py.models.responses.base import Status
        
        status = Status(status_code=200, message="Success", elapsed_time=0.1)
        empty_response = PointResponse(status=status, points=[])
        
        assert empty_response.get_valid_points() == []
        assert empty_response.get_values() == []
        assert empty_response.get_quality_codes() == []
        assert empty_response.get_timestamps() == []
        assert empty_response.get_value_range() is None
        assert empty_response.get_timestamp_range() is None
        assert empty_response.get_average_value() is None
        assert empty_response.get_quality_summary() == {}
        assert empty_response.get_data_count() == 0
        assert empty_response.get_null_count() == 0
        assert empty_response.has_data() is False
        assert empty_response.has_null_points() is False
        assert empty_response.get_latest_value() is None
        assert empty_response.get_earliest_value() is None
        assert empty_response.get_points_by_quality() == {}