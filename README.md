# DBHydro-py

A Python client library for accessing the South Florida Water Management District's DBHydro API. Retrieve water level, flow, rainfall, and other environmental data with a simple, intuitive interface.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Type Checking: mypy](https://img.shields.io/badge/type%20checking-mypy-blue.svg)](https://mypy.readthedocs.io/)

## Features

- **Simple API**: Intuitive methods for common data retrieval tasks
- **Multiple Endpoints**: Support for time series, daily data, real-time data, aggregates, interpolation, synchronization, NEXRAD, and more
- **Flexible Date Handling**: Multiple date format support with automatic parsing
- **Type Safe**: Full type hints for excellent IDE support and error prevention
- **Pandas Integration**: Optional DataFrame conversion for data analysis
- **Robust Error Handling**: Comprehensive error reporting and validation
- **Production Ready**: Extensive test coverage and strict type checking

## Installation

```bash
pip install dbhydro-py
```

### Optional Dependencies

For DataFrame functionality (recommended for data analysis):

```bash
pip install dbhydro-py[pandas]
```

For development:

```bash
pip install dbhydro-py[dev]
```

## Quick Start

### Simple Setup (Recommended)

```python
from dbhydro_py import DbHydroApi

# Initialize the API client
client = DbHydroApi.with_default_adapter(
    client_id="your_client_id",
    client_secret="your_client_secret"
)

# Get time series data
response = client.get_time_series(
    site_ids=['S79-E'],
    date_start='2023-01-01',
    date_end='2023-01-31'
)

# Access the data
for ts in response.time_series:
    print(f"Site: {ts.source_info.site_name}")
    for value in ts.values:
        print(f"  {value.date_time}: {value.value}")

# Convert to DataFrame (requires pandas)
df = response.to_dataframe()
print(df.head())
```

### Custom REST Adapter

If you need to customize the HTTP client behavior:

```python
from dbhydro_py import DbHydroApi, RestAdapterRequests

# Initialize with custom adapter
client = DbHydroApi(
    rest_adapter=RestAdapterRequests(),
    client_id="your_client_id",
    client_secret="your_client_secret"
)

# Usage is identical to the simple setup
response = client.get_time_series(site_ids=['S79-E'], ...)
```

## API Credentials

To use the DBHydro API, you need credentials from the South Florida Water Management District. Contact SFWMD to request API access and obtain your `client_id` and `client_secret`.

## Supported Endpoints

### Time Series Data

```python
# Basic time series
response = client.get_time_series(
    site_ids=['S79-E', 'S308-C'],
    date_start='2023-01-01',
    date_end='2023-01-31'
)

# With aggregation
response = client.get_time_series(
    site_ids=['S79-E'],
    date_start='2023-01-01',
    date_end='2023-12-31',
    calculation='MEAN',
    timespan_unit='MONTH'
)
```

### Daily Data

```python
response = client.get_daily_data(
    identifiers=['S79-E'],
    identifier_type='station',
    date_start='2023-01-01',
    date_end='2023-01-31',
    requested_datum='NAVD88'
)
```

### Real-Time Data

```python
response = client.get_real_time(
    identifiers=['S79-E'],
    identifier_type='sites',
    status='A'  # Active only
)
```

### Aggregate Statistics

```python
response = client.get_aggregate(
    station_id='S79-E',
    date_start='2023-01-01',
    date_end='2023-12-31',
    calculation='MAX',
    timespan_unit='DAY'
)
```

### Interpolated Values

```python
response = client.get_interpolate(
    station_id='S79-E',
    date_time='2023-06-15 12:00:00'
)
```

### NEXRAD Precipitation Data

```python
# Pixel data
response = client.get_nexrad_pixel_data(
    pixel_ids=['12345'],
    date_start='2023-01-01',
    date_end='2023-01-31',
    frequency='D'
)

# Polygon data
response = client.get_nexrad_polygon_data(
    identifiers=['WCA1'],
    identifier_type='polygonName',
    polygon_type=1,
    date_start='2023-01-01',
    date_end='2023-01-31',
    frequency='D'
)
```

### Synchronized Data

```python
response = client.get_synchronize(
    time_series_names=['S79-E', 'S308-C'],
    date_start='2023-01-01',
    date_end='2023-01-31',
    requested_datum='NAVD88'
)
```

## Date Format Support

The library accepts multiple date formats:

```python
# String formats
'2023-01-01'                    # Date only
'2023-01-01 12:30'             # Date and time
'2023-01-01T12:30'             # ISO format
'2023-01-0112:30:45:123'       # Full precision

# Python datetime objects
from datetime import datetime
dt = datetime(2023, 1, 1, 12, 30)
client.get_time_series(site_ids=['S79-E'], date_start=dt, date_end='2023-01-31')
```

## Data Analysis with Pandas

```python
# Convert any response to DataFrame
df = response.to_dataframe()

# Include metadata
df = response.to_dataframe(include_metadata=True)

# Response-specific methods
site_codes = response.get_site_codes()
latest_values = response.get_latest_values()
date_range = response.get_date_range()
```

## Error Handling

```python
from dbhydro_py import DbHydroException

try:
    response = client.get_time_series(
        site_ids=['INVALID-SITE'],
        date_start='2023-01-01',
        date_end='2023-01-31'
    )
except DbHydroException as e:
    print(f"API Error: {e.message}")
    print(f"HTTP Status: {e.http_status_code}")
    if e.api_status_code:
        print(f"API Status: {e.api_status_code}")
```

## Configuration

### Custom REST Adapter

```python
from dbhydro_py.rest_adapters import RestAdapterBase

class CustomRestAdapter(RestAdapterBase):
    def get(self, endpoint: str, params: dict):
        # Your custom implementation
        pass

client = DbHydroApi(
    rest_adapter=CustomRestAdapter(),
    client_id="your_client_id", 
    client_secret="your_client_secret"
)
```

### API Versioning

```python
# Use a specific API version
client = DbHydroApi(
    rest_adapter=RestAdapterRequests(),
    client_id="your_client_id",
    client_secret="your_client_secret",
    api_version=2  # Default is 1
)
```

## Testing

The library includes comprehensive tests. See [TESTING.md](TESTING.md) for details on running tests and contributing.

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=dbhydro_py

# Type checking
mypy dbhydro_py/
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass (`pytest`)
6. Ensure type checking passes (`mypy dbhydro_py/`)
7. Commit your changes (`git commit -m 'Add amazing feature'`)
8. Push to the branch (`git push origin feature/amazing-feature`)
9. Open a Pull Request

## Requirements

- Python 3.10+
- requests >= 2.25.0
- pandas >= 1.3.0 (optional, for DataFrame functionality)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Note**: This is an unofficial client library. For official API documentation and support, please contact the South Florida Water Management District.