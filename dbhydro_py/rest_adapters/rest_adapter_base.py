# Standard library imports
from abc import ABC, abstractmethod

# Local imports
from dbhydro_py.models.transport import Result

class RestAdapterBase(ABC):
    """Abstract base class for REST adapters."""

    @abstractmethod
    def get(self, endpoint: str, params: dict | None = None, headers: dict | None = None) -> Result:
        """Perform a GET request to the specified endpoint.

        Args:
            endpoint (str): The API endpoint to send the GET request to.
            params (dict, optional): Query parameters for the request.
            headers (dict, optional): Headers for the request.

        Returns:
            Result: The raw response data from the GET request.
        """
        pass

    @abstractmethod
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
        pass
    
    @abstractmethod
    def delete(self, endpoint: str, headers: dict | None = None, params: dict | None = None) -> Result:
        """Perform a DELETE request to the specified endpoint.

        Args:
            endpoint (str): The API endpoint to send the DELETE request to.
            headers (dict, optional): Headers for the request.
            params (dict, optional): Query parameters for the request.
            
        Returns:
            Result: The raw response data from the DELETE request.
        """
        pass