"""Pytest configuration and shared fixtures."""

import pytest
from unittest.mock import Mock

from dbhydro_py.rest_adapters import RestAdapterRequests
from dbhydro_py.api import DbHydroApi
from dbhydro_py.models.transport import Result


@pytest.fixture
def mock_rest_adapter():
    """Mock REST adapter for testing."""
    return Mock(spec=RestAdapterRequests)


@pytest.fixture
def api_client(mock_rest_adapter):
    """DbHydroApi instance with mocked REST adapter."""
    return DbHydroApi(
        rest_adapter=mock_rest_adapter,
        client_id="test_client_id",
        client_secret="test_client_secret"
    )


@pytest.fixture
def sample_time_series_response():
    """Sample time series API response for testing."""
    return {
        "timeSeriesResponse": {
            "status": {
                "statusCode": 200,
                "statusMessage": "Success",
                "elapsedTime": 0.123
            },
            "timeSeries": [
                {
                    "sourceInfo": {
                        "siteName": "Test Site",
                        "siteCode": {
                            "network": "SFWMD",
                            "agencyCode": "SFWMD",
                            "value": "S123-R"
                        },
                        "geoLocation": {
                            "geogLocation": {
                                "type": "Point",
                                "srs": "EPSG:4326",
                                "latitude": 25.123,
                                "longitude": -80.456
                            }
                        }
                    },
                    "periodOfRecord": {
                        "porBeginDate": "2020-01-01",
                        "porLastDate": "2023-12-31"
                    },
                    "name": "Test Time Series",
                    "description": "Test data",
                    "timeSeriesId": "12345",
                    "referenceElevation": {
                        "values": []
                    },
                    "parameter": {
                        "parameterCode": {
                            "parameterID": "00065",
                            "value": "GAGE_HEIGHT"
                        },
                        "parameterName": "Gage Height",
                        "parameterDescription": "Water level",
                        "unit": {
                            "unitCode": "ft"
                        },
                        "noDataValue": -999.0
                    },
                    "values": [
                        {
                            "qualifier": None,
                            "qualityCode": "A",
                            "dateTime": "2023-01-01T00:00:00:000",
                            "value": 12.5,
                            "percentAvailable": 100.0
                        },
                        {
                            "qualifier": None,
                            "qualityCode": "A", 
                            "dateTime": "2023-01-01T01:00:00:000",
                            "value": 12.3,
                            "percentAvailable": 100.0
                        }
                    ]
                }
            ]
        }
    }