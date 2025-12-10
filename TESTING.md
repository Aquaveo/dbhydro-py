# DBHydro-py Testing

This document explains how to run and work with the test suite for dbhydro-py.

## Running Tests

### Run All Tests
```bash
python -m pytest tests/ -v
```

### Run Specific Test Categories
```bash
# Run API tests only
python -m pytest tests/test_api.py -v

# Run specific endpoint tests
python -m pytest tests/test_api_time_series.py -v
python -m pytest tests/test_api_daily_data.py -v
python -m pytest tests/test_api_aggregate.py -v
python -m pytest tests/test_api_interpolate.py -v
python -m pytest tests/test_api_synchronize.py -v
python -m pytest tests/test_api_realtime.py -v
python -m pytest tests/test_api_nexrad.py -v
python -m pytest tests/test_api_tsarithmetic.py -v
python -m pytest tests/test_api_water_quality.py -v
python -m pytest tests/test_api_por.py -v

# Run model tests only  
python -m pytest tests/test_models/ -v

# Run REST adapter tests only
python -m pytest tests/test_rest_adapters/ -v

# Run utility tests only
python -m pytest tests/test_utils.py -v
```

### Run Tests with Coverage
```bash
python -m pytest tests/ --cov=dbhydro_py --cov-report=html
```

## Test Structure

### API Tests
- `tests/test_api.py` - Tests for the main DbHydroApi class and general functionality
- `tests/test_api_time_series.py` - Tests for get_time_series endpoint
- `tests/test_api_daily_data.py` - Tests for get_daily_data endpoint  
- `tests/test_api_aggregate.py` - Tests for get_aggregate endpoint
- `tests/test_api_interpolate.py` - Tests for get_interpolate endpoint
- `tests/test_api_synchronize.py` - Tests for get_synchronize endpoint
- `tests/test_api_realtime.py` - Tests for get_realtime endpoint
- `tests/test_api_nexrad.py` - Tests for get_nexrad endpoint
- `tests/test_api_tsarithmetic.py` - Tests for get_tsarithmetic endpoint
- `tests/test_api_water_quality.py` - Tests for get_water_quality endpoint
- `tests/test_api_por.py` - Tests for get_period_of_record endpoint

### Model Tests
- `tests/test_models/test_time_series.py` - Tests for TimeSeriesResponse and related models
- `tests/test_models/test_aggregate.py` - Tests for AggregateResponse and related models
- `tests/test_models/test_interpolate.py` - Tests for InterpolateResponse and related models
- `tests/test_models/test_synchronize.py` - Tests for SynchronizeResponse and related models
- `tests/test_models/test_point.py` - Tests for PointResponse and related models

### Infrastructure Tests
- `tests/test_rest_adapters/test_requests_rest_adapter.py` - Tests for HTTP transport layer
- `tests/test_utils.py` - Tests for utility functions
- `tests/test_summary_field.py` - Tests for summary field handling

### Configuration
- `tests/conftest.py` - Shared pytest fixtures and test data

## Key Test Fixtures

- `mock_rest_adapter` - Mock REST adapter for testing API calls
- `api_client` - Configured API client instance for testing
- `sample_time_series_response` - Realistic TimeSeriesResponse data
- `sample_aggregate_response` - Realistic AggregateResponse data
- `sample_interpolate_response` - Realistic InterpolateResponse data
- `sample_synchronize_response` - Realistic SynchronizeResponse data
- `sample_point_response` - Realistic PointResponse data
- Various endpoint-specific response fixtures for comprehensive testing

## Response Model Features

All response models include convenience methods for data analysis:
- `.to_dataframe()` - Convert to pandas DataFrame for analysis
- `.has_data()` - Check if response contains data
- `.get_*()` methods - Extract specific data subsets (keys, values, quality codes, etc.)
- `.filter_*()` methods - Filter data by various criteria
- Statistical and summary methods for data analysis

## Adding New Tests

When adding tests for new endpoints or features, follow this pattern:

### For New API Endpoints
1. Create a dedicated test file: `tests/test_api_[endpoint_name].py`
2. Add sample response data to `conftest.py` following the naming pattern `sample_[endpoint]_response`
3. Test parameter validation, success cases, and error handling
4. Include tests for any endpoint-specific features

### For New Response Models
1. Create model test file: `tests/test_models/test_[model_name].py`
2. Test dataclass conversion with `from_dict()` method
3. Test all convenience methods (`.get_*()`, `.filter_*()`, etc.)
4. Test DataFrame conversion with `.to_dataframe()`
5. Test data validation and edge cases

### Test Organization
- Keep endpoint tests focused on API parameter validation and response handling
- Keep model tests focused on data structure and convenience method functionality
- Use existing fixtures where possible to maintain consistency
- Follow the established naming conventions

## Common Test Patterns

### API Endpoint Test Pattern
```python
def test_api_method(self, api_client, mock_response_data):
    """Test a specific API method."""
    # Setup mock
    api_client.rest_adapter.get.return_value = Result(
        status_code=200, message="OK", data=mock_response_data
    )
    
    # Call method
    result = api_client.some_method('param1', 'param2')
    
    # Verify results
    assert isinstance(result, ExpectedResponseType)
    assert result.has_data()
    api_client.rest_adapter.get.assert_called_once()
```

### Parameter Validation Test Pattern
```python
def test_invalid_parameter(self, api_client):
    """Test parameter validation."""
    with pytest.raises(ValueError, match="Expected error message"):
        api_client.some_method(invalid_param="value")
```

### Model Convenience Method Test Pattern
```python
def test_convenience_method(self, sample_response):
    """Test response model convenience methods."""
    # Test data extraction methods
    keys = sample_response.get_keys()
    assert isinstance(keys, list)
    assert len(keys) > 0
    
    # Test filtering methods
    filtered = sample_response.filter_by_key(['key1'])
    assert isinstance(filtered, type(sample_response))
    
    # Test DataFrame conversion
    df = sample_response.to_dataframe()
    assert not df.empty
```
