"""Tests for aggregate response models."""

import pytest
from unittest.mock import patch

from dbhydro_py.models.responses.aggregate import AggregateResponse, AggregateInterval, Timespan, Tag


class TestAggregateModels:
    """Test cases for aggregate response models."""
    
    @pytest.fixture
    def sample_aggregate_response(self):
        """Sample aggregate response data."""
        return {
            "intervals": [{
                "endMilliSinceEpoch": 1388059200000,
                "statisticType": "MAX",
                "timespan": {
                    "scalar": 1,
                    "unitOfTime": "DAY"
                },
                "startMilliSinceEpoch": 1387972800000,
                "value": 0.03,
                "key": "S123-R",
                "tag": {
                    "tag": None
                },
                "endDate": "2013-12-26 07:00:00.000",
                "startDate": "2013-12-25 07:00:00.000",
                "keyType": "station_id",
                "qualityCode": "A",
                "percentAvailable": 100.0,
                "origin": "MANIPULATED"
            }]
        }
    
    def test_tag_from_dict(self):
        """Test creating Tag from dictionary."""
        tag_data = {"tag": "test_tag"}
        tag = Tag.from_dict(tag_data)
        assert tag.tag == "test_tag"
        
        # Test with null tag
        null_tag_data = {"tag": None}
        null_tag = Tag.from_dict(null_tag_data)
        assert null_tag.tag is None
    
    def test_timespan_from_dict(self):
        """Test creating Timespan from dictionary."""
        timespan_data = {
            "scalar": 1,
            "unitOfTime": "DAY"
        }
        timespan = Timespan.from_dict(timespan_data)
        assert timespan.scalar == 1
        assert timespan.unit_of_time == "DAY"
    
    def test_aggregate_interval_from_dict(self, sample_aggregate_response):
        """Test creating AggregateInterval from dictionary."""
        interval_data = sample_aggregate_response["intervals"][0]
        interval = AggregateInterval.from_dict(interval_data)
        
        assert interval.end_millis_since_epoch == 1388059200000
        assert interval.statistic_type == "MAX"
        assert interval.value == 0.03
        assert interval.key == "S123-R"
        assert interval.quality_code == "A"
        assert interval.percent_available == 100.0
        assert interval.timespan.scalar == 1
        assert interval.timespan.unit_of_time == "DAY"
        assert interval.tag.tag is None
    
    def test_aggregate_response_from_dict(self, sample_aggregate_response):
        """Test creating AggregateResponse from dictionary."""
        response = AggregateResponse.from_dict(sample_aggregate_response)
        
        assert len(response.intervals) == 1
        assert response.intervals[0].statistic_type == "MAX"
        assert response.intervals[0].value == 0.03
    
    def test_aggregate_response_empty_intervals(self):
        """Test AggregateResponse with empty intervals."""
        empty_response = AggregateResponse.from_dict({"intervals": []})
        assert len(empty_response.intervals) == 0
        
        # Test with null intervals - should handle gracefully
        null_response = AggregateResponse.from_dict({"intervals": None})
        assert null_response.intervals is None or len(null_response.intervals) == 0
    
    @patch('pandas.DataFrame')
    def test_to_dataframe_without_pandas_raises_error(self, mock_df, sample_aggregate_response):
        """Test that missing pandas raises ImportError."""
        with patch('builtins.__import__', side_effect=ImportError):
            response = AggregateResponse.from_dict(sample_aggregate_response)
            
            with pytest.raises(ImportError, match="pandas is required"):
                response.to_dataframe()
    
    def test_to_dataframe_with_data(self, sample_aggregate_response):
        """Test DataFrame conversion with actual data."""
        pytest.importorskip("pandas")  # Skip if pandas not available
        
        response = AggregateResponse.from_dict(sample_aggregate_response)
        df = response.to_dataframe()
        
        assert not df.empty
        assert len(df) == 1
        assert 'value' in df.columns
        assert 'statistic_type' in df.columns
        assert 'key' in df.columns
        assert df['value'].iloc[0] == 0.03
        assert df['statistic_type'].iloc[0] == "MAX"
        assert df['key'].iloc[0] == "S123-R"
    
    def test_to_dataframe_with_metadata(self, sample_aggregate_response):
        """Test DataFrame conversion with metadata."""
        pytest.importorskip("pandas")
        
        response = AggregateResponse.from_dict(sample_aggregate_response)
        df = response.to_dataframe(include_metadata=True)
        
        assert not df.empty
        assert 'timespan_scalar' in df.columns
        assert 'timespan_unit' in df.columns
        assert 'percent_available' in df.columns
        assert df['timespan_scalar'].iloc[0] == 1
        assert df['timespan_unit'].iloc[0] == "DAY"
    
    def test_to_dataframe_empty_data(self):
        """Test DataFrame conversion with empty data."""
        pytest.importorskip("pandas")
        
        empty_response = AggregateResponse.from_dict({"intervals": []})
        df = empty_response.to_dataframe()
        
        assert df.empty
        assert 'start_datetime' in df.columns
        assert 'end_datetime' in df.columns
        assert 'statistic_type' in df.columns
        assert 'value' in df.columns


class TestAggregateResponseConvenienceMethods:
    """Test convenience methods for AggregateResponse."""

    @pytest.fixture
    def sample_response(self):
        """Create a response with multiple intervals for testing."""
        from dbhydro_py.models.responses.aggregate import AggregateResponse, AggregateInterval, Timespan, Tag
        
        intervals = [
            AggregateInterval(
                end_millis_since_epoch=1388059200000,
                statistic_type="MAX",
                timespan=Timespan(scalar=1, unit_of_time="DAY"),
                start_millis_since_epoch=1387972800000,
                value=12.5,
                key="S123-R",
                tag=Tag(tag="flow"),
                end_date="2013-12-26 07:00:00.000",
                start_date="2013-12-25 07:00:00.000",
                key_type="station_id",
                quality_code="A",
                percent_available=100.0,
                origin="MANIPULATED"
            ),
            AggregateInterval(
                end_millis_since_epoch=1388145600000,
                statistic_type="MIN",
                timespan=Timespan(scalar=1, unit_of_time="DAY"),
                start_millis_since_epoch=1388059200000,
                value=8.2,
                key="S123-R",
                tag=Tag(tag=None),
                end_date="2013-12-27 07:00:00.000",
                start_date="2013-12-26 07:00:00.000",
                key_type="station_id",
                quality_code="A",
                percent_available=95.0,
                origin="MANIPULATED"
            ),
            AggregateInterval(
                end_millis_since_epoch=1388059200000,
                statistic_type="MAX",
                timespan=Timespan(scalar=1, unit_of_time="DAY"),
                start_millis_since_epoch=1387972800000,
                value=15.7,
                key="S124-R",
                tag=Tag(tag="level"),
                end_date="2013-12-26 07:00:00.000",
                start_date="2013-12-25 07:00:00.000",
                key_type="station_id",
                quality_code="P",
                percent_available=85.0,
                origin="TELEMETRY"
            ),
            AggregateInterval(
                end_millis_since_epoch=1388232000000,
                statistic_type="MEAN",
                timespan=Timespan(scalar=7, unit_of_time="DAY"),
                start_millis_since_epoch=1387972800000,
                value=10.1,
                key="S124-R",
                tag=Tag(tag=None),
                end_date="2013-12-28 07:00:00.000",
                start_date="2013-12-25 07:00:00.000",
                key_type="station_id",
                quality_code="E",
                percent_available=78.0,
                origin="TELEMETRY"
            )
        ]
        
        return AggregateResponse(intervals=intervals)

    def test_get_keys(self, sample_response):
        """Test getting all unique keys."""
        keys = sample_response.get_keys()
        assert keys == ["S123-R", "S124-R"]

    def test_get_statistic_types(self, sample_response):
        """Test getting all unique statistic types."""
        stat_types = sample_response.get_statistic_types()
        assert stat_types == ["MAX", "MEAN", "MIN"]

    def test_get_values(self, sample_response):
        """Test getting all values."""
        values = sample_response.get_values()
        assert values == [12.5, 8.2, 15.7, 10.1]

    def test_get_quality_codes(self, sample_response):
        """Test getting all unique quality codes."""
        quality_codes = sample_response.get_quality_codes()
        assert quality_codes == ["A", "E", "P"]

    def test_get_origins(self, sample_response):
        """Test getting all unique origins."""
        origins = sample_response.get_origins()
        assert origins == ["MANIPULATED", "TELEMETRY"]

    def test_get_timespans(self, sample_response):
        """Test getting all unique timespan definitions."""
        timespans = sample_response.get_timespans()
        expected = [(1, "DAY"), (7, "DAY")]
        assert timespans == expected

    def test_get_timestamp_range(self, sample_response):
        """Test getting timestamp range."""
        timestamp_range = sample_response.get_timestamp_range()
        assert timestamp_range == (1387972800000, 1388232000000)

    def test_get_timestamp_range_empty(self):
        """Test getting timestamp range with empty response."""
        from dbhydro_py.models.responses.aggregate import AggregateResponse
        
        empty_response = AggregateResponse(intervals=[])
        assert empty_response.get_timestamp_range() is None

    def test_get_value_range(self, sample_response):
        """Test getting value range."""
        value_range = sample_response.get_value_range()
        assert value_range == (8.2, 15.7)

    def test_get_value_range_empty(self):
        """Test getting value range with empty response."""
        from dbhydro_py.models.responses.aggregate import AggregateResponse
        
        empty_response = AggregateResponse(intervals=[])
        assert empty_response.get_value_range() is None

    def test_filter_by_key(self, sample_response):
        """Test filtering by keys."""
        filtered = sample_response.filter_by_key(["S123-R"])
        
        assert len(filtered.intervals) == 2
        assert all(interval.key == "S123-R" for interval in filtered.intervals)

    def test_filter_by_key_no_matches(self, sample_response):
        """Test filtering by keys with no matches."""
        filtered = sample_response.filter_by_key(["S999-R"])
        assert len(filtered.intervals) == 0

    def test_filter_by_statistic(self, sample_response):
        """Test filtering by statistic types."""
        filtered = sample_response.filter_by_statistic(["MAX"])
        
        assert len(filtered.intervals) == 2
        assert all(interval.statistic_type == "MAX" for interval in filtered.intervals)

    def test_filter_by_quality(self, sample_response):
        """Test filtering by quality codes."""
        filtered = sample_response.filter_by_quality(["A"])
        
        assert len(filtered.intervals) == 2
        assert all(interval.quality_code == "A" for interval in filtered.intervals)

    def test_filter_by_origin(self, sample_response):
        """Test filtering by origins."""
        filtered = sample_response.filter_by_origin(["MANIPULATED"])
        
        assert len(filtered.intervals) == 2
        assert all(interval.origin == "MANIPULATED" for interval in filtered.intervals)

    def test_get_intervals_for_key(self, sample_response):
        """Test getting intervals for a specific key."""
        intervals = sample_response.get_intervals_for_key("S124-R")
        
        assert len(intervals) == 2
        assert all(interval.key == "S124-R" for interval in intervals)
        # Should be sorted by start time
        assert intervals[0].start_millis_since_epoch <= intervals[1].start_millis_since_epoch

    def test_get_intervals_by_statistic(self, sample_response):
        """Test getting intervals for a specific statistic type."""
        intervals = sample_response.get_intervals_by_statistic("MAX")
        
        assert len(intervals) == 2
        assert all(interval.statistic_type == "MAX" for interval in intervals)

    def test_get_latest_values_by_key(self, sample_response):
        """Test getting latest values for each key."""
        latest = sample_response.get_latest_values_by_key()
        
        # S123-R latest is at 1388145600000 with value 8.2
        # S124-R latest is at 1388232000000 with value 10.1
        assert latest["S123-R"] == 8.2
        assert latest["S124-R"] == 10.1

    def test_get_earliest_values_by_key(self, sample_response):
        """Test getting earliest values for each key."""
        earliest = sample_response.get_earliest_values_by_key()
        
        # Both keys have earliest at 1387972800000
        assert earliest["S123-R"] == 12.5
        assert earliest["S124-R"] == 15.7

    def test_get_value_ranges_by_key(self, sample_response):
        """Test getting value ranges for each key."""
        ranges = sample_response.get_value_ranges_by_key()
        
        assert ranges["S123-R"] == (8.2, 12.5)
        assert ranges["S124-R"] == (10.1, 15.7)

    def test_get_average_values_by_key(self, sample_response):
        """Test getting average values for each key."""
        averages = sample_response.get_average_values_by_key()
        
        # S123-R: (12.5 + 8.2) / 2 = 10.35
        # S124-R: (15.7 + 10.1) / 2 = 12.9
        assert abs(averages["S123-R"] - 10.35) < 0.01
        assert abs(averages["S124-R"] - 12.9) < 0.01

    def test_get_quality_summary(self, sample_response):
        """Test getting quality code summary."""
        summary = sample_response.get_quality_summary()
        
        expected = {"A": 2, "P": 1, "E": 1}
        assert summary == expected

    def test_get_statistic_summary(self, sample_response):
        """Test getting statistic type summary."""
        summary = sample_response.get_statistic_summary()
        
        expected = {"MAX": 2, "MIN": 1, "MEAN": 1}
        assert summary == expected

    def test_get_data_count(self, sample_response):
        """Test getting data count."""
        count = sample_response.get_data_count()
        assert count == 4

    def test_get_data_counts_by_key(self, sample_response):
        """Test getting data counts by key."""
        counts = sample_response.get_data_counts_by_key()
        
        assert counts["S123-R"] == 2
        assert counts["S124-R"] == 2

    def test_has_data(self, sample_response):
        """Test checking if response has data."""
        assert sample_response.has_data() is True
        
        # Test empty response
        from dbhydro_py.models.responses.aggregate import AggregateResponse
        
        empty_response = AggregateResponse(intervals=[])
        assert empty_response.has_data() is False

    def test_get_intervals_by_key_and_statistic(self, sample_response):
        """Test grouping intervals by key and statistic type."""
        grouped = sample_response.get_intervals_by_key_and_statistic()
        
        assert "S123-R" in grouped
        assert "S124-R" in grouped
        
        # S123-R should have 'MAX' and 'MIN'
        assert "MAX" in grouped["S123-R"]
        assert "MIN" in grouped["S123-R"]
        assert len(grouped["S123-R"]["MAX"]) == 1
        assert len(grouped["S123-R"]["MIN"]) == 1
        
        # S124-R should have 'MAX' and 'MEAN'
        assert "MAX" in grouped["S124-R"]
        assert "MEAN" in grouped["S124-R"]
        assert len(grouped["S124-R"]["MAX"]) == 1
        assert len(grouped["S124-R"]["MEAN"]) == 1

    def test_get_tagged_intervals(self, sample_response):
        """Test getting intervals with non-empty tags."""
        tagged = sample_response.get_tagged_intervals()
        
        assert len(tagged) == 2  # Two intervals have non-null tags: "flow" and "level"
        tag_values = [interval.tag.tag for interval in tagged]
        assert "flow" in tag_values
        assert "level" in tag_values

    def test_get_time_coverage_by_key(self, sample_response):
        """Test getting time coverage for each key."""
        coverage = sample_response.get_time_coverage_by_key()
        
        # S123-R: from 1387972800000 to 1388145600000
        # S124-R: from 1387972800000 to 1388232000000
        assert coverage["S123-R"] == (1387972800000, 1388145600000)
        assert coverage["S124-R"] == (1387972800000, 1388232000000)

    def test_empty_response_methods(self):
        """Test convenience methods with empty response."""
        from dbhydro_py.models.responses.aggregate import AggregateResponse
        
        empty_response = AggregateResponse(intervals=[])
        
        assert empty_response.get_keys() == []
        assert empty_response.get_statistic_types() == []
        assert empty_response.get_values() == []
        assert empty_response.get_quality_codes() == []
        assert empty_response.get_origins() == []
        assert empty_response.get_timespans() == []
        assert empty_response.get_timestamp_range() is None
        assert empty_response.get_value_range() is None
        assert empty_response.get_latest_values_by_key() == {}
        assert empty_response.get_earliest_values_by_key() == {}
        assert empty_response.get_value_ranges_by_key() == {}
        assert empty_response.get_average_values_by_key() == {}
        assert empty_response.get_quality_summary() == {}
        assert empty_response.get_statistic_summary() == {}
        assert empty_response.get_data_count() == 0
        assert empty_response.get_data_counts_by_key() == {}
        assert empty_response.has_data() is False
        assert empty_response.get_intervals_by_key_and_statistic() == {}
        assert empty_response.get_tagged_intervals() == []
        assert empty_response.get_time_coverage_by_key() == {}