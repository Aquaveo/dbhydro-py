"""Tests for interpolate response models."""

import pytest
from datetime import datetime

from dbhydro_py.models.responses.interpolate import (
    InterpolateResponse, 
    InterpolateEntry, 
    InterpolateTag
)


class TestInterpolateModels:
    """Test cases for interpolate response models."""

    @pytest.fixture
    def sample_interpolate_data(self):
        """Sample interpolate response data."""
        return {
            "status": {
                "statusCode": 200,
                "statusMessage": "request successful",
                "elapsedTime": 0
            },
            "list": [
                {
                    "origin": "MANIPULATED",
                    "key": "S123-R",
                    "keyType": "station_id",
                    "msSinceEpoch": 1387972800000,
                    "value": 12.5,
                    "tag": {
                        "tag": None
                    },
                    "qualityCode": "A",
                    "percentAvailable": 75.0,
                    "time": 1387972800000,
                    "date": 1387972800000
                }
            ]
        }

    def test_interpolate_tag_creation(self):
        """Test InterpolateTag creation from dictionary."""
        tag_data = {"tag": "flow"}
        tag = InterpolateTag.from_dict(tag_data)
        
        assert tag.tag == "flow"

    def test_interpolate_tag_null_value(self):
        """Test InterpolateTag with null tag value."""
        tag_data = {"tag": None}
        tag = InterpolateTag.from_dict(tag_data)
        
        assert tag.tag is None

    def test_interpolate_entry_creation(self, sample_interpolate_data):
        """Test InterpolateEntry creation from dictionary."""
        entry_data = sample_interpolate_data["list"][0]
        entry = InterpolateEntry.from_dict(entry_data)
        
        assert entry.origin == "MANIPULATED"
        assert entry.key == "S123-R"
        assert entry.key_type == "station_id"
        assert entry.ms_since_epoch == 1387972800000
        assert entry.value == 12.5
        assert entry.tag.tag is None
        assert entry.quality_code == "A"
        assert entry.percent_available == 75.0
        assert entry.time == 1387972800000
        assert entry.date == 1387972800000

    def test_interpolate_response_creation(self, sample_interpolate_data):
        """Test InterpolateResponse creation from dictionary."""
        response = InterpolateResponse.from_dict(sample_interpolate_data)
        
        assert len(response.entries) == 1
        assert response.entries[0].key == "S123-R"
        assert response.entries[0].value == 12.5

    def test_interpolate_response_empty_list(self):
        """Test InterpolateResponse with empty list."""
        empty_response = InterpolateResponse.from_dict({"list": []})
        assert len(empty_response.entries) == 0
        
        # Test with null list - should handle gracefully
        null_response = InterpolateResponse.from_dict({"list": None})
        assert null_response.entries is None or len(null_response.entries) == 0

    def test_interpolate_response_missing_list(self):
        """Test InterpolateResponse with missing list key."""
        missing_response = InterpolateResponse.from_dict({})
        assert len(missing_response.entries) == 0

    def test_to_dataframe_basic(self, sample_interpolate_data):
        """Test basic DataFrame conversion."""
        response = InterpolateResponse.from_dict(sample_interpolate_data)
        
        try:
            df = response.to_dataframe()
            
            assert len(df) == 1
            assert list(df.columns) == ['key', 'value', 'quality_code', 'origin']
            assert df.iloc[0]['key'] == 'S123-R'
            assert df.iloc[0]['value'] == 12.5
            assert df.iloc[0]['quality_code'] == 'A'
            assert df.iloc[0]['origin'] == 'MANIPULATED'
            
        except ImportError:
            # pandas not installed, skip this test
            pytest.skip("pandas not installed")

    def test_to_dataframe_with_metadata(self, sample_interpolate_data):
        """Test DataFrame conversion with metadata."""
        response = InterpolateResponse.from_dict(sample_interpolate_data)
        
        try:
            df = response.to_dataframe(include_metadata=True)
            
            assert len(df) == 1
            expected_columns = [
                'key', 'value', 'quality_code', 'origin',
                'key_type', 'ms_since_epoch', 'percent_available', 
                'time', 'date', 'tag'
            ]
            assert list(df.columns) == expected_columns
            
            assert df.iloc[0]['key_type'] == 'station_id'
            assert df.iloc[0]['ms_since_epoch'] == 1387972800000
            assert df.iloc[0]['percent_available'] == 75.0
            assert df.iloc[0]['tag'] is None
            
        except ImportError:
            # pandas not installed, skip this test
            pytest.skip("pandas not installed")

    def test_to_dataframe_empty_response(self):
        """Test DataFrame conversion with empty response."""
        response = InterpolateResponse.from_dict({"list": []})
        
        try:
            df = response.to_dataframe()
            
            assert len(df) == 0
            assert list(df.columns) == ['key', 'value', 'quality_code', 'origin']
            
        except ImportError:
            # pandas not installed, skip this test
            pytest.skip("pandas not installed")

    def test_to_dataframe_without_pandas_raises_error(self, sample_interpolate_data, monkeypatch):
        """Test that ImportError is raised when pandas is not available."""
        response = InterpolateResponse.from_dict(sample_interpolate_data)
        
        # Mock the pandas import to fail
        def mock_import(name, *args, **kwargs):
            if name == 'pandas':
                raise ImportError("No module named 'pandas'")
            return __import__(name, *args, **kwargs)
        
        monkeypatch.setattr('builtins.__import__', mock_import)
        
        with pytest.raises(ImportError, match="pandas is required for DataFrame conversion"):
            response.to_dataframe()


class TestInterpolateResponseConvenienceMethods:
    """Test convenience methods for InterpolateResponse."""

    @pytest.fixture
    def sample_response(self):
        """Create a response with multiple entries for testing."""
        from dbhydro_py.models.responses.interpolate import InterpolateResponse, InterpolateEntry, InterpolateTag
        
        entries = [
            InterpolateEntry(
                origin="MANIPULATED",
                key="S123-R",
                key_type="station_id",
                ms_since_epoch=1387972800000,
                value=12.5,
                tag=InterpolateTag(tag="flow"),
                quality_code="A",
                percent_available=100.0,
                time=1387972800000,
                date=1387972800000
            ),
            InterpolateEntry(
                origin="MANIPULATED",
                key="S123-R",
                key_type="station_id",
                ms_since_epoch=1387972860000,
                value=13.2,
                tag=InterpolateTag(tag=None),
                quality_code="A",
                percent_available=100.0,
                time=1387972860000,
                date=1387972860000
            ),
            InterpolateEntry(
                origin="TELEMETRY",
                key="S124-R",
                key_type="station_id",
                ms_since_epoch=1387972800000,
                value=8.7,
                tag=InterpolateTag(tag="level"),
                quality_code="P",
                percent_available=95.0,
                time=1387972800000,
                date=1387972800000
            ),
            InterpolateEntry(
                origin="TELEMETRY",
                key="S124-R",
                key_type="station_id",
                ms_since_epoch=1387972920000,
                value=9.1,
                tag=InterpolateTag(tag=None),
                quality_code="E",
                percent_available=85.0,
                time=1387972920000,
                date=1387972920000
            )
        ]
        
        return InterpolateResponse(entries=entries)

    def test_get_keys(self, sample_response):
        """Test getting all unique keys."""
        keys = sample_response.get_keys()
        assert keys == ["S123-R", "S124-R"]

    def test_get_values(self, sample_response):
        """Test getting all values."""
        values = sample_response.get_values()
        assert values == [12.5, 13.2, 8.7, 9.1]

    def test_get_quality_codes(self, sample_response):
        """Test getting all unique quality codes."""
        quality_codes = sample_response.get_quality_codes()
        assert quality_codes == ["A", "E", "P"]

    def test_get_origins(self, sample_response):
        """Test getting all unique origins."""
        origins = sample_response.get_origins()
        assert origins == ["MANIPULATED", "TELEMETRY"]

    def test_get_key_types(self, sample_response):
        """Test getting all unique key types."""
        key_types = sample_response.get_key_types()
        assert key_types == ["station_id"]

    def test_get_timestamps(self, sample_response):
        """Test getting all timestamps sorted."""
        timestamps = sample_response.get_timestamps()
        expected = [1387972800000, 1387972800000, 1387972860000, 1387972920000]
        assert timestamps == sorted(expected)

    def test_get_value_range(self, sample_response):
        """Test getting value range."""
        value_range = sample_response.get_value_range()
        assert value_range == (8.7, 13.2)

    def test_get_value_range_empty(self):
        """Test getting value range with empty response."""
        from dbhydro_py.models.responses.interpolate import InterpolateResponse
        
        empty_response = InterpolateResponse(entries=[])
        assert empty_response.get_value_range() is None

    def test_get_timestamp_range(self, sample_response):
        """Test getting timestamp range."""
        timestamp_range = sample_response.get_timestamp_range()
        assert timestamp_range == (1387972800000, 1387972920000)

    def test_get_timestamp_range_empty(self):
        """Test getting timestamp range with empty response."""
        from dbhydro_py.models.responses.interpolate import InterpolateResponse
        
        empty_response = InterpolateResponse(entries=[])
        assert empty_response.get_timestamp_range() is None

    def test_filter_by_key(self, sample_response):
        """Test filtering by keys."""
        filtered = sample_response.filter_by_key(["S123-R"])
        
        assert len(filtered.entries) == 2
        assert all(entry.key == "S123-R" for entry in filtered.entries)

    def test_filter_by_key_no_matches(self, sample_response):
        """Test filtering by keys with no matches."""
        filtered = sample_response.filter_by_key(["S999-R"])
        assert len(filtered.entries) == 0

    def test_filter_by_quality(self, sample_response):
        """Test filtering by quality codes."""
        filtered = sample_response.filter_by_quality(["A"])
        
        assert len(filtered.entries) == 2
        assert all(entry.quality_code == "A" for entry in filtered.entries)

    def test_filter_by_origin(self, sample_response):
        """Test filtering by origins."""
        filtered = sample_response.filter_by_origin(["MANIPULATED"])
        
        assert len(filtered.entries) == 2
        assert all(entry.origin == "MANIPULATED" for entry in filtered.entries)

    def test_get_entries_for_key(self, sample_response):
        """Test getting entries for a specific key."""
        entries = sample_response.get_entries_for_key("S124-R")
        
        assert len(entries) == 2
        assert all(entry.key == "S124-R" for entry in entries)

    def test_get_latest_value_by_key(self, sample_response):
        """Test getting latest value for each key."""
        latest = sample_response.get_latest_value_by_key()
        
        # S123-R latest is at 1387972860000 with value 13.2
        # S124-R latest is at 1387972920000 with value 9.1
        assert latest["S123-R"] == 13.2
        assert latest["S124-R"] == 9.1

    def test_get_earliest_value_by_key(self, sample_response):
        """Test getting earliest value for each key."""
        earliest = sample_response.get_earliest_value_by_key()
        
        # Both keys have earliest at 1387972800000
        assert earliest["S123-R"] == 12.5
        assert earliest["S124-R"] == 8.7

    def test_get_average_values_by_key(self, sample_response):
        """Test getting average values for each key."""
        averages = sample_response.get_average_values_by_key()
        
        # S123-R: (12.5 + 13.2) / 2 = 12.85
        # S124-R: (8.7 + 9.1) / 2 = 8.9
        assert abs(averages["S123-R"] - 12.85) < 0.01
        assert abs(averages["S124-R"] - 8.9) < 0.01

    def test_get_value_ranges_by_key(self, sample_response):
        """Test getting value ranges for each key."""
        ranges = sample_response.get_value_ranges_by_key()
        
        assert ranges["S123-R"] == (12.5, 13.2)
        assert ranges["S124-R"] == (8.7, 9.1)

    def test_get_quality_summary(self, sample_response):
        """Test getting quality code summary."""
        summary = sample_response.get_quality_summary()
        
        expected = {"A": 2, "P": 1, "E": 1}
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
        from dbhydro_py.models.responses.interpolate import InterpolateResponse
        
        empty_response = InterpolateResponse(entries=[])
        assert empty_response.has_data() is False

    def test_get_entries_by_key_and_quality(self, sample_response):
        """Test grouping entries by key and quality code."""
        grouped = sample_response.get_entries_by_key_and_quality()
        
        assert "S123-R" in grouped
        assert "S124-R" in grouped
        
        # S123-R should have 'A' quality
        assert "A" in grouped["S123-R"]
        assert len(grouped["S123-R"]["A"]) == 2
        
        # S124-R should have 'P' and 'E' quality
        assert "P" in grouped["S124-R"]
        assert "E" in grouped["S124-R"]
        assert len(grouped["S124-R"]["P"]) == 1
        assert len(grouped["S124-R"]["E"]) == 1

    def test_get_tagged_entries(self, sample_response):
        """Test getting entries with non-empty tags."""
        tagged = sample_response.get_tagged_entries()
        
        assert len(tagged) == 2  # Two entries have non-null tags: "flow" and "level"
        tag_values = [entry.tag.tag for entry in tagged]
        assert "flow" in tag_values
        assert "level" in tag_values

    def test_empty_response_methods(self):
        """Test convenience methods with empty response."""
        from dbhydro_py.models.responses.interpolate import InterpolateResponse
        
        empty_response = InterpolateResponse(entries=[])
        
        assert empty_response.get_keys() == []
        assert empty_response.get_values() == []
        assert empty_response.get_quality_codes() == []
        assert empty_response.get_origins() == []
        assert empty_response.get_key_types() == []
        assert empty_response.get_timestamps() == []
        assert empty_response.get_value_range() is None
        assert empty_response.get_timestamp_range() is None
        assert empty_response.get_latest_value_by_key() == {}
        assert empty_response.get_earliest_value_by_key() == {}
        assert empty_response.get_average_values_by_key() == {}
        assert empty_response.get_value_ranges_by_key() == {}
        assert empty_response.get_quality_summary() == {}
        assert empty_response.get_data_count() == 0
        assert empty_response.get_data_counts_by_key() == {}
        assert empty_response.has_data() is False
        assert empty_response.get_entries_by_key_and_quality() == {}
        assert empty_response.get_tagged_entries() == []