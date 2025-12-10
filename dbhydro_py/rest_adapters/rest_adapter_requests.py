# Third-party imports
import requests
from requests.exceptions import RequestException

# Local imports
from dbhydro_py.models.transport import Result
from dbhydro_py.rest_adapters.rest_adapter_base import RestAdapterBase

# Get package version dynamically
try:
    from importlib.metadata import version
    __version__ = version('dbhydro-py')
except ImportError:
    # Fallback for Python < 3.8 or if package not installed
    __version__ = 'unknown'

class RestAdapterRequests(RestAdapterBase):
    """REST adapter implementation using the requests library."""
    
    def get(self, endpoint: str, params: dict | None = None, headers: dict | None = None) -> Result:
        """Perform a GET request to the specified endpoint.

        Args:
            endpoint (str): The API endpoint to send the GET request to.
            params (dict, optional): Query parameters for the request.
            headers (dict, optional): Headers for the request.

        Returns:
            Result: The raw response data from the GET request.
        """
        return self._perform_request('GET', endpoint, params, headers=headers)


    def post(self, endpoint: str, headers: dict | None = None, params: dict | None = None, data: dict | None = None) -> Result:
        """Perform a POST request to the specified endpoint.

        Args:
            endpoint (str): The API endpoint to send the POST request to.
            headers (dict, optional): Headers for the request.
            params (dict, optional): Query parameters for the request.
            data (dict, optional): The data to include in the POST request body.

        Returns:
            Result: The raw response data from the POST request.
        """
        return self._perform_request('POST', endpoint, params, headers=headers, data=data)


    def delete(self, endpoint: str, headers: dict | None = None, params: dict | None = None) -> Result:
        """Perform a DELETE request to the specified endpoint.
        Args:
            endpoint (str): The API endpoint to send the DELETE request to.
            headers (dict, optional): Headers for the request.
            params (dict, optional): Query parameters for the request.
            
        Returns:
            Result: The raw response data from the DELETE request.
        """
        return self._perform_request('DELETE', endpoint, params, headers=headers, data=None)

    def _perform_request(self, http_method: str, endpoint: str, endpoint_params: dict | None, headers: dict | None = None, data: dict | None = None) -> Result:
        """Helper method to perform HTTP requests.

        Args:
            http_method (str): The HTTP method to use ('GET', 'POST', 'DELETE').
            endpoint (str): The API endpoint to send the request to.
            endpoint_params (dict): Parameters or data for the request.
            headers (dict, optional): Headers for the request.
            data (dict, optional): The data to include in the request body.

        Returns:
            Result: The raw response data from the request.
        """
        # Perform the HTTP request using the requests library
        try:
            # Set default headers if none provided
            if headers is None:
                headers = {}
            
            # Add User-Agent if not already set
            if 'User-Agent' not in headers:
                headers['User-Agent'] = f'dbhydro-py/{__version__}'
            
            response = requests.request(method=http_method, url=endpoint, verify=True, headers=headers, params=endpoint_params, json=data)
            
        except RequestException as e:
            # Return a Result with error information instead of raising exception
            # This keeps the adapter generic and lets the API layer handle errors
            return Result(status_code=0, message=f'Request failed: {e}', data={})
        
        # Get the response data
        try:
            response_data_json = response.json()
        except ValueError:
            # If JSON parsing fails, fall back to basic HTTP error
            response_data_json = {}
        
        # Return the Result for all responses - let the API layer decide what constitutes an error
        return Result(status_code=response.status_code, message=response.reason, data=response_data_json)
