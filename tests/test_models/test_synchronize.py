"""Tests for synchronize response models."""

import pytest
from unittest.mock import Mock

from dbhydro_py.models.responses.synchronize import (
    SynchronizeResponse, 
    SynchronizeEntry, 
    SynchronizeValue
)


class TestSynchronizeModels:
    """Test cases for synchronize response models."""

    @pytest.fixture
    def sample_synchronize_data(self):
        """Sample synchronize response data matching the original JSON structure."""
        return {
            "1449794818000": {
                "S6-H": {
                    "origin": "MANIPULATED",
                    "key": "S6-H",
                    "keyType": "station_id",
                    "msSinceEpoch": 1449794818000,
                    "value": 7.7600594795539,
                    "tag": {},
                    "qualityCode": "A",
                    "percentAvailable": 0
                },
                "S6P-3": {
                    "origin": "MANIPULATED",
                    "key": "S6P-3",
                    "keyType": "station_id",
                    "msSinceEpoch": 1449794818000,
                    "value": 8.1234567890123,
                    "tag": {},
                    "qualityCode": "A",
                    "percentAvailable": 0
                }
            },
            "1449794819000": {
                "S6-H": {
                    "origin": "MANIPULATED",
                    "key": "S6-H",
                    "keyType": "station_id",
                    "msSinceEpoch": 1449794819000,
                    "value": 7.7512345678901,
                    "tag": {},
                    "qualityCode": "A",
                    "percentAvailable": 0
                },
                "S6P-3": {
                    "origin": "MANIPULATED",
                    "key": "S6P-3",
                    "keyType": "station_id",
                    "msSinceEpoch": 1449794819000,
                    "value": 8.1098765432109,
                    "tag": {},
                    "qualityCode": "A",
                    "percentAvailable": 0
                }
            }
        }

    @pytest.fixture
    def sample_single_value_data(self):
        """Sample data for a single synchronize value."""
        return {
            "origin": "MANIPULATED",
            "key": "S6-H",
            "keyType": "station_id",
            "msSinceEpoch": 1449794818000,
            "value": 7.7600594795539,
            "tag": {"custom": "data"},
            "qualityCode": "A",
            "percentAvailable": 0
        }

    def test_synchronize_value_creation(self, sample_single_value_data):
        """Test SynchronizeValue creation from dictionary."""
        value = SynchronizeValue.from_dict(sample_single_value_data)
        
        assert value.origin == "MANIPULATED"
        assert value.key_type == "station_id"
        assert value.ms_since_epoch == 1449794818000
        assert value.value == 7.7600594795539
        assert value.tag == {"custom": "data"}
        assert value.quality_code == "A"
        assert value.percent_available == 0

    def test_synchronize_value_json_key_mapping(self, sample_single_value_data):
        """Test that JSON keys are properly mapped to field names."""
        value = SynchronizeValue.from_dict(sample_single_value_data)
        
        # Test snake_case conversion
        assert value.key_type == "station_id"  # from keyType
        assert value.ms_since_epoch == 1449794818000  # from msSinceEpoch
        assert value.quality_code == "A"  # from qualityCode
        assert value.percent_available == 0  # from percentAvailable

    def test_synchronize_entry_creation(self, sample_synchronize_data):
        """Test SynchronizeEntry creation from grouped station data."""
        # Extract station data for S6-H
        station_data = {}
        for timestamp, stations in sample_synchronize_data.items():
            if "S6-H" in stations:
                station_data[timestamp] = stations["S6-H"]
        
        entry = SynchronizeEntry.from_dict("S6-H", station_data)
        
        assert entry.station_id == "S6-H"
        assert entry.key == "S6-H"
        assert len(entry.values) == 2
        
        # Check values are sorted by timestamp
        assert entry.values[0].ms_since_epoch == 1449794818000
        assert entry.values[1].ms_since_epoch == 1449794819000
        
        # Check value data
        assert entry.values[0].value == 7.7600594795539
        assert entry.values[1].value == 7.7512345678901

    def test_synchronize_entry_empty_data(self):
        """Test SynchronizeEntry with empty data."""
        entry = SynchronizeEntry.from_dict("TEST", {})
        
        assert entry.station_id == "TEST"
        assert entry.key == "TEST"  # defaults to station_id
        assert len(entry.values) == 0

    def test_synchronize_response_creation(self, sample_synchronize_data):
        """Test SynchronizeResponse creation from dictionary."""
        response = SynchronizeResponse.from_dict(sample_synchronize_data)
        
        assert len(response.stations) == 2
        assert "S6-H" in response.stations
        assert "S6P-3" in response.stations
        
        # Check S6-H data
        s6h_entry = response.stations["S6-H"]
        assert s6h_entry.station_id == "S6-H"
        assert len(s6h_entry.values) == 2
        
        # Check S6P-3 data
        s6p3_entry = response.stations["S6P-3"]
        assert s6p3_entry.station_id == "S6P-3"
        assert len(s6p3_entry.values) == 2

    def test_synchronize_response_empty_data(self):
        """Test SynchronizeResponse with empty data."""
        response = SynchronizeResponse.from_dict({})
        
        assert len(response.stations) == 0

    def test_synchronize_response_invalid_data(self):
        """Test SynchronizeResponse handles invalid data gracefully."""
        # Test with non-dict values
        invalid_data = {
            "timestamp1": "not_a_dict",
            "timestamp2": {
                "station1": "not_a_dict"
            }
        }
        
        response = SynchronizeResponse.from_dict(invalid_data)
        assert len(response.stations) == 0

    def test_get_stations(self, sample_synchronize_data):
        """Test getting all station IDs."""
        response = SynchronizeResponse.from_dict(sample_synchronize_data)
        stations = response.get_stations()
        
        assert stations == {"S6-H", "S6P-3"}

    def test_get_timestamps(self, sample_synchronize_data):
        """Test getting all timestamps."""
        response = SynchronizeResponse.from_dict(sample_synchronize_data)
        timestamps = response.get_timestamps()
        
        assert timestamps == [1449794818000, 1449794819000]
        # Verify they're sorted
        assert timestamps == sorted(timestamps)

    def test_get_station_data(self, sample_synchronize_data):
        """Test getting data for a specific station."""
        response = SynchronizeResponse.from_dict(sample_synchronize_data)
        
        # Test existing station
        s6h_data = response.get_station_data("S6-H")
        assert s6h_data is not None
        assert s6h_data.station_id == "S6-H"
        assert len(s6h_data.values) == 2
        
        # Test non-existing station
        missing_data = response.get_station_data("NON-EXISTENT")
        assert missing_data is None

    def test_get_values_at_timestamp(self, sample_synchronize_data):
        """Test getting all station values at a specific timestamp."""
        response = SynchronizeResponse.from_dict(sample_synchronize_data)
        
        # Test existing timestamp
        values_at_time = response.get_values_at_timestamp(1449794818000)
        assert len(values_at_time) == 2
        assert "S6-H" in values_at_time
        assert "S6P-3" in values_at_time
        assert values_at_time["S6-H"].value == 7.7600594795539
        assert values_at_time["S6P-3"].value == 8.1234567890123
        
        # Test non-existing timestamp
        empty_values = response.get_values_at_timestamp(9999999999999)
        assert len(empty_values) == 0

    def test_get_station_values(self, sample_synchronize_data):
        """Test getting all values for a specific station."""
        response = SynchronizeResponse.from_dict(sample_synchronize_data)
        
        # Test existing station
        s6h_values = response.get_station_values("S6-H")
        assert len(s6h_values) == 2
        assert s6h_values[0].ms_since_epoch == 1449794818000
        assert s6h_values[1].ms_since_epoch == 1449794819000
        
        # Test non-existing station
        missing_values = response.get_station_values("NON-EXISTENT")
        assert len(missing_values) == 0

    def test_to_dataframe_basic(self, sample_synchronize_data):
        """Test DataFrame conversion with basic columns."""
        pytest.importorskip("pandas")
        
        response = SynchronizeResponse.from_dict(sample_synchronize_data)
        df = response.to_dataframe()
        
        # Check basic structure
        assert len(df) == 4  # 2 stations × 2 timestamps
        assert list(df.columns) == ['station_id', 'ms_since_epoch', 'value', 'quality_code']
        
        # Check data
        s6h_rows = df[df['station_id'] == 'S6-H']
        assert len(s6h_rows) == 2
        assert s6h_rows.iloc[0]['value'] == 7.7600594795539
        assert s6h_rows.iloc[1]['value'] == 7.7512345678901

    def test_to_dataframe_with_metadata(self, sample_synchronize_data):
        """Test DataFrame conversion with metadata columns."""
        pytest.importorskip("pandas")
        
        response = SynchronizeResponse.from_dict(sample_synchronize_data)
        df = response.to_dataframe(include_metadata=True)
        
        # Check extended structure
        assert len(df) == 4
        expected_columns = [
            'station_id', 'ms_since_epoch', 'value', 'quality_code',
            'key', 'origin', 'key_type', 'tag', 'percent_available'
        ]
        assert list(df.columns) == expected_columns
        
        # Check metadata
        first_row = df.iloc[0]
        assert first_row['origin'] == 'MANIPULATED'
        assert first_row['key_type'] == 'station_id'
        assert first_row['percent_available'] == 0

    def test_to_dataframe_empty(self):
        """Test DataFrame conversion with empty response."""
        pytest.importorskip("pandas")
        
        response = SynchronizeResponse.from_dict({})
        df = response.to_dataframe()
        
        assert len(df) == 0
        assert list(df.columns) == ['station_id', 'ms_since_epoch', 'value', 'quality_code']

    def test_to_dataframe_without_pandas(self, sample_synchronize_data):
        """Test DataFrame conversion raises error when pandas not available."""
        response = SynchronizeResponse.from_dict(sample_synchronize_data)
        
        # Mock pandas import failure
        import sys
        original_import = __builtins__['__import__']
        
        def mock_import(name, *args, **kwargs):
            if name == 'pandas':
                raise ImportError("No module named 'pandas'")
            return original_import(name, *args, **kwargs)
        
        __builtins__['__import__'] = mock_import
        
        try:
            with pytest.raises(ImportError, match="pandas is required"):
                response.to_dataframe()
        finally:
            __builtins__['__import__'] = original_import


class TestSynchronizeResponseConvenienceMethods:
    """Test convenience methods for SynchronizeResponse."""

    @pytest.fixture
    def multi_station_response(self):
        """Create a response with multiple stations and various data for testing."""
        from dbhydro_py.models.responses.synchronize import SynchronizeResponse, SynchronizeEntry, SynchronizeValue
        
        # Create test data with multiple stations, quality codes, and timestamps
        values1 = [
            SynchronizeValue(
                ms_since_epoch=1449794818000,
                value=7.76,
                quality_code="A",
                percent_available=100.0,
                origin="MANIPULATED",
                key_type="station_id",
                tag={}
            ),
            SynchronizeValue(
                ms_since_epoch=1449794819000,
                value=7.75,
                quality_code="A",
                percent_available=100.0,
                origin="MANIPULATED",
                key_type="station_id",
                tag={}
            ),
            SynchronizeValue(
                ms_since_epoch=1449794820000,
                value=7.80,
                quality_code="P",
                percent_available=95.0,
                origin="TELEMETRY",
                key_type="station_id",
                tag={"flag": "estimated"}
            )
        ]
        
        values2 = [
            SynchronizeValue(
                ms_since_epoch=1449794818000,
                value=8.12,
                quality_code="A",
                percent_available=100.0,
                origin="MANIPULATED",
                key_type="station_id",
                tag={}
            ),
            SynchronizeValue(
                ms_since_epoch=1449794819000,
                value=None,  # Missing value
                quality_code="M",
                percent_available=0.0,
                origin="TELEMETRY",
                key_type="station_id",
                tag={"flag": "missing"}
            ),
            SynchronizeValue(
                ms_since_epoch=1449794820000,
                value=8.15,
                quality_code="E",
                percent_available=90.0,
                origin="TELEMETRY",
                key_type="station_id",
                tag={"flag": "estimated"}
            )
        ]

        entry1 = SynchronizeEntry(station_id="S6-H", key="S6-H", values=values1)
        entry2 = SynchronizeEntry(station_id="S6P-3", key="S6P-3", values=values2)
        
        return SynchronizeResponse(stations={"S6-H": entry1, "S6P-3": entry2})

    def test_get_station_ids(self, multi_station_response):
        """Test getting all station IDs."""
        station_ids = multi_station_response.get_station_ids()
        assert sorted(station_ids) == ["S6-H", "S6P-3"]

    def test_get_quality_codes(self, multi_station_response):
        """Test getting all unique quality codes."""
        quality_codes = multi_station_response.get_quality_codes()
        assert quality_codes == ["A", "E", "M", "P"]

    def test_get_origins(self, multi_station_response):
        """Test getting all unique origins."""
        origins = multi_station_response.get_origins()
        assert origins == ["MANIPULATED", "TELEMETRY"]

    def test_get_value_ranges(self, multi_station_response):
        """Test getting value ranges for each station."""
        ranges = multi_station_response.get_value_ranges()
        assert "S6-H" in ranges
        assert "S6P-3" in ranges
        
        # S6-H has values: 7.76, 7.75, 7.80
        assert ranges["S6-H"] == (7.75, 7.80)
        # S6P-3 has values: 8.12, None, 8.15 (None is excluded)
        assert ranges["S6P-3"] == (8.12, 8.15)

    def test_get_latest_values(self, multi_station_response):
        """Test getting latest values for each station."""
        latest = multi_station_response.get_latest_values()
        
        # Latest timestamp is 1449794820000
        assert latest["S6-H"] == 7.80  # Latest value for S6-H
        assert latest["S6P-3"] == 8.15  # Latest value for S6P-3 (not None)

    def test_get_earliest_values(self, multi_station_response):
        """Test getting earliest values for each station."""
        earliest = multi_station_response.get_earliest_values()
        
        # Earliest timestamp is 1449794818000
        assert earliest["S6-H"] == 7.76
        assert earliest["S6P-3"] == 8.12

    def test_filter_by_quality(self, multi_station_response):
        """Test filtering by quality codes."""
        # Filter for only 'A' quality
        filtered = multi_station_response.filter_by_quality(["A"])
        
        assert len(filtered.stations) == 2
        assert len(filtered.get_station_values("S6-H")) == 2  # 2 'A' values
        assert len(filtered.get_station_values("S6P-3")) == 1  # 1 'A' value

    def test_filter_by_quality_no_matches(self, multi_station_response):
        """Test filtering by quality codes with no matches."""
        filtered = multi_station_response.filter_by_quality(["X"])
        assert len(filtered.stations) == 0

    def test_filter_by_origin(self, multi_station_response):
        """Test filtering by origins."""
        # Filter for only 'MANIPULATED' origin
        filtered = multi_station_response.filter_by_origin(["MANIPULATED"])
        
        assert len(filtered.stations) == 2
        assert len(filtered.get_station_values("S6-H")) == 2  # 2 MANIPULATED values
        assert len(filtered.get_station_values("S6P-3")) == 1  # 1 MANIPULATED value

    def test_get_timestamp_range(self, multi_station_response):
        """Test getting timestamp range."""
        range_result = multi_station_response.get_timestamp_range()
        assert range_result == (1449794818000, 1449794820000)

    def test_get_timestamp_range_empty(self):
        """Test getting timestamp range with empty response."""
        empty_response = SynchronizeResponse()
        range_result = empty_response.get_timestamp_range()
        assert range_result is None

    def test_get_quality_summary(self, multi_station_response):
        """Test getting quality code summary."""
        summary = multi_station_response.get_quality_summary()
        
        expected = {"A": 3, "P": 1, "M": 1, "E": 1}
        assert summary == expected

    def test_get_data_count(self, multi_station_response):
        """Test getting total data count."""
        count = multi_station_response.get_data_count()
        assert count == 6  # 3 values per station × 2 stations

    def test_get_data_counts_by_station(self, multi_station_response):
        """Test getting data counts by station."""
        counts = multi_station_response.get_data_counts_by_station()
        
        assert counts["S6-H"] == 3
        assert counts["S6P-3"] == 3

    def test_has_data(self, multi_station_response):
        """Test checking if response has data."""
        assert multi_station_response.has_data() is True
        
        # Test empty response
        empty_response = SynchronizeResponse()
        assert empty_response.has_data() is False

    def test_get_average_values(self, multi_station_response):
        """Test getting average values for each station."""
        averages = multi_station_response.get_average_values()
        
        # S6-H: (7.76 + 7.75 + 7.80) / 3 = 7.77
        assert abs(averages["S6-H"] - 7.77) < 0.01
        
        # S6P-3: (8.12 + 8.15) / 2 = 8.135 (None value excluded)
        assert abs(averages["S6P-3"] - 8.135) < 0.01

    def test_get_values_by_station_and_quality(self, multi_station_response):
        """Test grouping values by station and quality code."""
        grouped = multi_station_response.get_values_by_station_and_quality()
        
        assert "S6-H" in grouped
        assert "S6P-3" in grouped
        
        # S6-H should have 'A' and 'P' quality codes
        assert "A" in grouped["S6-H"]
        assert "P" in grouped["S6-H"]
        assert len(grouped["S6-H"]["A"]) == 2  # Two 'A' values
        assert len(grouped["S6-H"]["P"]) == 1  # One 'P' value
        
        # S6P-3 should have 'A' and 'E' quality codes (None value excluded)
        assert "A" in grouped["S6P-3"]
        assert "E" in grouped["S6P-3"]
        # 'M' quality code should not be present because its value is None
        assert "M" not in grouped["S6P-3"]

    def test_empty_response_methods(self):
        """Test convenience methods with empty response."""
        empty_response = SynchronizeResponse()
        
        assert empty_response.get_station_ids() == []
        assert empty_response.get_quality_codes() == []
        assert empty_response.get_origins() == []
        assert empty_response.get_value_ranges() == {}
        assert empty_response.get_latest_values() == {}
        assert empty_response.get_earliest_values() == {}
        assert empty_response.get_quality_summary() == {}
        assert empty_response.get_data_count() == 0
        assert empty_response.get_data_counts_by_station() == {}
        assert empty_response.has_data() is False
        assert empty_response.get_average_values() == {}
        assert empty_response.get_values_by_station_and_quality() == {}