"""Pytest configuration and shared fixtures."""

import pytest
from unittest.mock import Mock

from dbhydro_py.rest_adapters import RestAdapterRequests
from dbhydro_py.api import DbHydroApi
from dbhydro_py.models.responses.water_quality import WaterQualityResponse
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


@pytest.fixture
def sample_water_quality_api_response():
    """Sample water quality API response for testing."""
    return [
        {
            "station": "G211",
            "projectCode": "8SQM",
            "dateCollected": 1001430900000,
            "dateCollectedStr": "2001-09-25 11:15:00.000",
            "sampleType": "SAMP",
            "programType": "MON",
            "matrix": "SW",
            "collectMethod": "G",
            "firstTriggerDate": None,
            "firstTriggerDateStr": None,
            "depth": ".5",
            "depthUnits": None,
            "testNumber": 7,
            "parameter": "Temperature",
            "dataType": "TEMP",
            "value": 27.08,
            "remarkCode": None,
            "flag": None,
            "sigFigValue": "27.1",
            "uncertainty": None,
            "dilution": None,
            "mdl": None,
            "pql": None,
            "rdl": None,
            "units": "Degrees Celsius",
            "nDec": 1,
            "bdl": "N",
            "qualityCode": "A",
            "sampleId": "P9622-3",
            "upDownStream": "UPSTREAM",
            "discharge": "1",
            "weather": "2",
            "dcsMeters": None,
            "totalDepth": None,
            "upperDepth": None,
            "lowerDepth": None,
            "latitude": 253936.111,
            "longitude": -802951.421,
            "collectionAgency": None,
            "workLab": None,
            "source": "WMD",
            "owner": "WMD",
            "validator": None,
            "validationLevel": None,
            "samplingPurpose": None,
            "dataInvestigation": None,
            "receiveDate": 1001476800000,
            "receiveDateStr": "2001-09-26 00:00:00.000",
            "measureDate": None,
            "measureDateStr": None,
            "method": "FIELD",
            "filtrationDate": None,
            "filtrationDateStr": None,
            "sampleComment": None,
            "resultComment": None,
            "collectionSpan": None,
            "limsNumber": "L18133-3",
            "storetCode": "10"
        },
    ]


@pytest.fixture
def sample_water_quality_response(sample_water_quality_api_response):
    """Sample WaterQualityResponse for testing."""
    response = {
        "status_code": 200,
        "message": "OK",
        "data": sample_water_quality_api_response
    }
    
    return WaterQualityResponse.from_dict(response)
