"""Tests for real-time API endpoint functionality."""

import pytest
from unittest.mock import Mock
from datetime import datetime

from dbhydro_py.api import DbHydroApi
from dbhydro_py.models.responses import TimeSeriesResponse
from dbhydro_py.exceptions import DbHydroException


class TestRealTimeApi:
    """Test cases for real-time API functionality."""

    @pytest.fixture
    def api_client(self):
        """Create API client with mocked REST adapter."""
        rest_adapter = Mock()
        return DbHydroApi(rest_adapter, "test_client", "test_secret")

    @pytest.fixture
    def sample_realtime_response(self):
        """Sample real-time API response data."""
        return {
            "timeSeriesResponse": {
                "timeSeries": [
                    {
                        "sourceInfo": {
                            "siteName": "S-3 PUMP FROM MIAMI CANAL TO LAKE OKEECHOBEE",
                            "siteCode": {
                                "network": "DBHYDRO",
                                "agencyCode": "WMD", 
                                "value": "S3"
                            },
                            "geoLocation": {
                                "geogLocation": {
                                    "type": "LatLonPointType",
                                    "srs": "",
                                    "latitude": 26.6982737187492,
                                    "longitude": -80.8069834711599
                                }
                            },
                            "type": "SiteInfoType"
                        },
                        "periodOfRecord": {
                            "porBeginDate": "1985-05-31T23:04:00:000",
                            "porLastDate": "2025-12-08T12:05:57:000",
                            "provisionalBeginDate": "2025-11-23T23:32:18:000",
                            "provisionalLastDate": "2025-12-08T12:05:57:000",
                            "approvedBeginDate": "1985-05-31T23:04:00:000",
                            "approvedLastDate": "2025-11-23T23:32:17:000"
                        },
                        "name": "S3-H",
                        "description": "S3 Headwater",
                        "applicationName": "SG4",
                        "recorderClass": "MOSCAD",
                        "currentStatus": "A",
                        "timeSeriesId": "2582",
                        "referenceElevation": {
                            "values": [
                                {
                                    "effectiveDate": "2014-05-14T00:00:00:000",
                                    "referenceElevation": 34.99
                                }
                            ]
                        },
                        "parameter": {
                            "parameterCode": {
                                "parameterID": 0,
                                "value": "H"
                            },
                            "parameterName": "HEADWATER ELEVATION",
                            "parameterDescription": "HEADWATER ELEVATION",
                            "unit": {
                                "unitCode": "ft NAVD88"
                            },
                            "noDataValue": -99999
                        },
                        "values": [
                            {
                                "qualifier": None,
                                "qualityCode": "P",
                                "dateTime": "2025-12-08T12:05:57:000",
                                "value": 9.431,
                                "percentAvailable": 0
                            }
                        ],
                        "summary": {
                            "min": 7.2,
                            "max": 10.176,
                            "mean": 9.234,
                            "median": 9.254,
                            "stddev": 0.64,
                            "numOfRecords": 40
                        }
                    }
                ],
                "status": {
                    "statusCode": 200,
                    "statusMessage": "request successful",
                    "elapsedTime": 0.395780975
                }
            }
        }

    def test_get_real_time_validation_errors(self, api_client):
        """Test parameter validation in get_real_time."""
        # Test empty identifiers list
        with pytest.raises(ValueError, match="The 'identifier' list cannot be empty."):
            api_client.get_real_time([], 'sites')
        
        # Test invalid identifier (empty string)
        with pytest.raises(ValueError, match="Invalid identifier: ''. Each identifier must be a non-empty string."):
            api_client.get_real_time([''], 'sites')
        
        # Test invalid identifier (whitespace only)
        with pytest.raises(ValueError, match="Invalid identifier: '   '. Each identifier must be a non-empty string."):
            api_client.get_real_time(['   '], 'sites')
        
        # Test invalid identifier_type
        with pytest.raises(ValueError, match="Invalid identifier_type: 'invalid'"):
            api_client.get_real_time(['S3'], 'invalid')

    def test_get_real_time_sites_success(self, api_client, sample_realtime_response):
        """Test successful real-time data retrieval with sites."""
        # Mock the REST adapter response
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=sample_realtime_response
        )
        
        # Make request
        response = api_client.get_real_time(['S3'], 'sites')
        
        # Verify response
        assert isinstance(response, TimeSeriesResponse)
        assert len(response.time_series) == 1
        assert response.time_series[0].name == "S3-H"
        assert len(response.time_series[0].values) == 1
        assert response.time_series[0].values[0].value == 9.431
        
        # Verify API call parameters
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert call_args[1]['params']['sites'] == 'S3'
        assert call_args[1]['params']['format'] == 'json'

    def test_get_real_time_sites_with_status(self, api_client, sample_realtime_response):
        """Test real-time data retrieval with sites and status filter."""
        # Mock the REST adapter response
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=sample_realtime_response
        )
        
        # Make request with status
        response = api_client.get_real_time(['S3'], 'sites', 'A')
        
        # Verify response
        assert isinstance(response, TimeSeriesResponse)
        assert len(response.time_series) == 1
        
        # Verify API call includes status parameter
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert call_args[1]['params']['status'] == 'A'

    def test_get_real_time_timeseries_success(self, api_client, sample_realtime_response):
        """Test successful real-time data retrieval with timeseries."""
        # Modify sample response for multiple time series
        multi_response = sample_realtime_response.copy()
        multi_response["timeSeriesResponse"]["timeSeries"] = [
            sample_realtime_response["timeSeriesResponse"]["timeSeries"][0],
            {
                **sample_realtime_response["timeSeriesResponse"]["timeSeries"][0],
                "name": "S4-H",
                "timeSeriesId": "4398",
                "values": [
                    {
                        "qualifier": None,
                        "qualityCode": "P",
                        "dateTime": "2025-12-08T12:04:14:000",
                        "value": 10.337,
                        "percentAvailable": 0
                    }
                ]
            }
        ]
        
        # Mock the REST adapter response
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=multi_response
        )
        
        # Make request
        response = api_client.get_real_time(['S3-H', 'S4-H'], 'timeseries')
        
        # Verify response
        assert isinstance(response, TimeSeriesResponse)
        assert len(response.time_series) == 2
        
        # Verify API call parameters
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert call_args[1]['params']['timeseries'] == 'S3-H,S4-H'
        assert 'status' not in call_args[1]['params']

    def test_get_real_time_timeseries_with_status(self, api_client, sample_realtime_response):
        """Test real-time data retrieval with timeseries and status (should be ignored)."""
        # Mock the REST adapter response
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=sample_realtime_response
        )
        
        # Make request with status (should be allowed but ignored by API)
        response = api_client.get_real_time(['S3-H'], 'timeseries', 'A')
        
        # Verify response
        assert isinstance(response, TimeSeriesResponse)
        
        # Verify API call includes status parameter (even though it has no effect)
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert call_args[1]['params']['status'] == 'A'

    def test_get_real_time_multiple_sites(self, api_client, sample_realtime_response):
        """Test real-time data retrieval with multiple sites."""
        # Mock the REST adapter response
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=sample_realtime_response
        )
        
        # Make request with multiple sites
        response = api_client.get_real_time(['S3', 'S4'], 'sites')
        
        # Verify response
        assert isinstance(response, TimeSeriesResponse)
        
        # Verify API call parameters
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert call_args[1]['params']['sites'] == 'S3,S4'

    def test_get_real_time_custom_status_values(self, api_client, sample_realtime_response):
        """Test real-time data retrieval with custom status values."""
        # Mock the REST adapter response
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=sample_realtime_response
        )
        
        # Test various status values (should all be accepted)
        status_values = ['A', 'I', 'D', 'CUSTOM', '123']
        
        for status in status_values:
            response = api_client.get_real_time(['S3'], 'sites', status)
            assert isinstance(response, TimeSeriesResponse)
            
            # Verify status parameter is passed through
            call_args = api_client.rest_adapter.get.call_args
            assert call_args[1]['params']['status'] == status

    def test_get_real_time_no_status(self, api_client, sample_realtime_response):
        """Test real-time data retrieval without status parameter."""
        # Mock the REST adapter response
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=sample_realtime_response
        )
        
        # Make request without status
        response = api_client.get_real_time(['S3'], 'sites')
        
        # Verify response
        assert isinstance(response, TimeSeriesResponse)
        
        # Verify API call does not include status parameter
        api_client.rest_adapter.get.assert_called_once()
        call_args = api_client.rest_adapter.get.call_args
        assert 'status' not in call_args[1]['params']

    def test_get_real_time_api_error_handling(self, api_client):
        """Test API error handling for real-time endpoint."""
        # Mock an API error response
        error_response = {
            "timeSeriesResponse": {
                "status": {
                    "statusCode": 400,
                    "statusMessage": "Invalid status value",
                    "elapsedTime": 0.1
                }
            }
        }
        
        api_client.rest_adapter.get.return_value = Mock(
            status_code=400,
            data=error_response
        )
        
        # Should raise DbHydroException
        with pytest.raises(DbHydroException, match="API request failed: Invalid status value"):
            api_client.get_real_time(['S3'], 'sites', 'INVALID_STATUS')

    def test_get_real_time_empty_response(self, api_client):
        """Test real-time data retrieval with empty time series."""
        # Mock empty response
        empty_response = {
            "timeSeriesResponse": {
                "timeSeries": [],
                "status": {
                    "statusCode": 200,
                    "statusMessage": "request successful",
                    "elapsedTime": 0.1
                }
            }
        }
        
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=empty_response
        )
        
        # Make request
        response = api_client.get_real_time(['NONEXISTENT'], 'sites')
        
        # Verify response
        assert isinstance(response, TimeSeriesResponse)
        assert len(response.time_series) == 0

    def test_get_real_time_inactive_time_series(self, api_client):
        """Test real-time data with inactive time series (empty values)."""
        # Mock response with inactive time series (empty values array)
        inactive_response = {
            "timeSeriesResponse": {
                "timeSeries": [
                    {
                        "sourceInfo": {
                            "siteName": "Inactive Site",
                            "siteCode": {
                                "network": "DBHYDRO",
                                "agencyCode": "WMD",
                                "value": "TEST"
                            },
                            "geoLocation": {
                                "geogLocation": {
                                    "type": "LatLonPointType",
                                    "srs": "",
                                    "latitude": 0.0,
                                    "longitude": 0.0
                                }
                            },
                            "type": "SiteInfoType"
                        },
                        "periodOfRecord": {
                            "porBeginDate": None,
                            "porLastDate": None,
                            "provisionalBeginDate": None,
                            "provisionalLastDate": None,
                            "approvedBeginDate": None,
                            "approvedLastDate": None
                        },
                        "name": "TEST-INACTIVE",
                        "description": "Inactive test station",
                        "applicationName": "TEST",
                        "recorderClass": "MANUAL",
                        "currentStatus": "I",
                        "timeSeriesId": "99999",
                        "referenceElevation": {
                            "values": []
                        },
                        "parameter": {
                            "parameterCode": {
                                "parameterID": 0,
                                "value": "H"
                            },
                            "parameterName": "TEST PARAMETER",
                            "parameterDescription": "TEST PARAMETER",
                            "unit": {
                                "unitCode": "ft"
                            },
                            "noDataValue": -99999
                        },
                        "values": [],  # Empty values array
                        "summary": {
                            "min": None,
                            "max": None,
                            "mean": None,
                            "median": None,
                            "stddev": None,
                            "numOfRecords": None
                        }
                    }
                ],
                "status": {
                    "statusCode": 200,
                    "statusMessage": "request successful",
                    "elapsedTime": 0.1
                }
            }
        }
        
        api_client.rest_adapter.get.return_value = Mock(
            status_code=200,
            data=inactive_response
        )
        
        # Make request
        response = api_client.get_real_time(['TEST'], 'sites', 'I')
        
        # Verify response
        assert isinstance(response, TimeSeriesResponse)
        assert len(response.time_series) == 1
        assert response.time_series[0].current_status == "I"
        assert len(response.time_series[0].values) == 0  # No current values