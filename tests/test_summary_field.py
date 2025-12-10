import pytest
from dbhydro_py.models.responses import TimeSeriesResponse
from dbhydro_py.models.responses.time_series import Summary


def test_time_series_response_with_summary_field():
    """Test that TimeSeriesResponse correctly handles the summary field from real-time endpoint."""
    # Sample response data with summary field (from real-time endpoint)
    response_data = {
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
                            "latitude": 26.698273718749174,
                            "longitude": -80.80698347115994
                        }
                    }
                },
                "periodOfRecord": {
                    "porBeginDate": None,
                    "porLastDate": None,
                    "provisionalBeginDate": None,
                    "provisionalLastDate": None,
                    "approvedBeginDate": None,
                    "approvedLastDate": None
                },
                "name": "S3-P-Q2",
                "description": "S3 discharge using fixed sensor combination",
                "applicationName": "SG4",
                "recorderClass": "CALCULATED",
                "currentStatus": "I",
                "timeSeriesId": "11420",
                "referenceElevation": {
                    "values": []
                },
                "parameter": {
                    "parameterCode": {
                        "parameterID": 0,
                        "value": "Q"
                    },
                    "parameterName": "FLOW",
                    "parameterDescription": "FLOW",
                    "unit": {
                        "unitCode": "cfs"
                    },
                    "noDataValue": -99999.0
                },
                "values": [],
                "summary": {
                    "min": 0.0,
                    "max": 0.0,
                    "mean": 0.0,
                    "median": 0.0,
                    "stddev": 0.0,
                    "numOfRecords": 1
                }
            }
        ],
        "status": {
            "statusCode": 200,
            "statusMessage": "request successful",
            "elapsedTime": 3.634721342
        }
    }
    
    # Create TimeSeriesResponse from the data
    response = TimeSeriesResponse.from_dict(response_data)
    
    # Verify the response structure
    assert response is not None
    assert len(response.time_series) == 1
    
    time_series = response.time_series[0]
    
    # Verify summary field is correctly parsed
    assert time_series.summary is not None
    assert isinstance(time_series.summary, Summary)
    assert time_series.summary.min == 0.0
    assert time_series.summary.max == 0.0
    assert time_series.summary.mean == 0.0
    assert time_series.summary.median == 0.0
    assert time_series.summary.stddev == 0.0
    assert time_series.summary.num_of_records == 1


def test_time_series_response_with_null_summary_values():
    """Test that Summary correctly handles null values."""
    response_data = {
        "timeSeries": [
            {
                "sourceInfo": {
                    "siteName": "S-3 SIPHON 1",
                    "siteCode": {
                        "network": "DBHYDRO",
                        "agencyCode": "WMD",
                        "value": "S3"
                    },
                    "geoLocation": {
                        "geogLocation": {
                            "type": "LatLonPointType",
                            "srs": "",
                            "latitude": 26.698273718749174,
                            "longitude": -80.80698347115994
                        }
                    }
                },
                "periodOfRecord": {
                    "porBeginDate": None,
                    "porLastDate": None,
                    "provisionalBeginDate": None,
                    "provisionalLastDate": None,
                    "approvedBeginDate": None,
                    "approvedLastDate": None
                },
                "name": "S3@S1",
                "description": "S3 SIPHON 1",
                "timeSeriesId": "9020",
                "referenceElevation": {
                    "values": []
                },
                "parameter": {
                    "parameterCode": {
                        "parameterID": 0,
                        "value": "S"
                    },
                    "parameterName": "SIPHONING",
                    "parameterDescription": "SIPHONING",
                    "unit": {
                        "unitCode": "0 (OFF) OR 1 (ON)"
                    },
                    "noDataValue": -99999.0
                },
                "values": [],
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
            "elapsedTime": 2.5
        }
    }
    
    # Create TimeSeriesResponse from the data
    response = TimeSeriesResponse.from_dict(response_data)
    
    # Verify the response structure
    assert response is not None
    assert len(response.time_series) == 1
    
    time_series = response.time_series[0]
    
    # Verify summary field handles null values correctly
    assert time_series.summary is not None
    assert isinstance(time_series.summary, Summary)
    assert time_series.summary.min is None
    assert time_series.summary.max is None
    assert time_series.summary.mean is None
    assert time_series.summary.median is None
    assert time_series.summary.stddev is None
    assert time_series.summary.num_of_records is None


def test_time_series_response_without_summary_field():
    """Test that TimeSeriesResponse works correctly when summary field is absent (regular endpoints)."""
    response_data = {
        "timeSeries": [
            {
                "sourceInfo": {
                    "siteName": "Test Site",
                    "siteCode": {
                        "network": "DBHYDRO",
                        "agencyCode": "WMD",
                        "value": "TEST"
                    },
                    "geoLocation": {
                        "geogLocation": {
                            "type": "LatLonPointType",
                            "srs": "",
                            "latitude": 26.0,
                            "longitude": -80.0
                        }
                    }
                },
                "periodOfRecord": {
                    "porBeginDate": None,
                    "porLastDate": None,
                    "provisionalBeginDate": None,
                    "provisionalLastDate": None,
                    "approvedBeginDate": None,
                    "approvedLastDate": None
                },
                "name": "TEST-H",
                "description": "Test Water Level",
                "timeSeriesId": "12345",
                "referenceElevation": {
                    "values": []
                },
                "parameter": {
                    "parameterCode": {
                        "parameterID": 0,
                        "value": "H"
                    },
                    "parameterName": "WATER LEVEL",
                    "parameterDescription": "WATER LEVEL",
                    "unit": {
                        "unitCode": "ft"
                    },
                    "noDataValue": -99999.0
                },
                "values": []
                # Note: No summary field here - typical for non-real-time endpoints
            }
        ],
        "status": {
            "statusCode": 200,
            "statusMessage": "request successful",
            "elapsedTime": 1.5
        }
    }
    
    # Create TimeSeriesResponse from the data
    response = TimeSeriesResponse.from_dict(response_data)
    
    # Verify the response structure
    assert response is not None
    assert len(response.time_series) == 1
    
    time_series = response.time_series[0]
    
    # Verify summary field is None when not present in the response
    assert time_series.summary is None


def test_summary_dataclass_creation():
    """Test direct creation of Summary dataclass."""
    summary = Summary(
        min=1.5,
        max=10.2,
        mean=5.8,
        median=6.0,
        stddev=2.3,
        num_of_records=100
    )
    
    assert summary.min == 1.5
    assert summary.max == 10.2
    assert summary.mean == 5.8
    assert summary.median == 6.0
    assert summary.stddev == 2.3
    assert summary.num_of_records == 100