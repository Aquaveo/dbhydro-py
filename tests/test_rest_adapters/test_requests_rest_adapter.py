"""Tests for RestAdapterRequests class."""

import pytest
from unittest.mock import Mock, patch
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError

from dbhydro_py.rest_adapters.rest_adapter_requests import RestAdapterRequests
from dbhydro_py.exceptions import DbHydroException


class TestRestAdapterRequests:
    """Test cases for RestAdapterRequests."""
    
    def test_init(self):
        """Test adapter initialization."""
        adapter = RestAdapterRequests()
        assert adapter is not None
    
    @patch('dbhydro_py.rest_adapters.rest_adapter_requests.requests')
    def test_get_success(self, mock_requests):
        """Test successful GET request."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.reason = "OK"
        mock_response.json.return_value = {"status": "success", "data": []}
        mock_requests.request.return_value = mock_response
        
        adapter = RestAdapterRequests()
        result = adapter.get(endpoint="https://api.test.com/test", params={"param1": "value1"})
        
        assert result.status_code == 200
        assert result.message == "OK"
        assert result.data == {"status": "success", "data": []}
        
        # Verify requests.request was called correctly
        mock_requests.request.assert_called_once()
        call_args = mock_requests.request.call_args
        assert call_args[1]['method'] == 'GET'
        assert call_args[1]['url'] == "https://api.test.com/test"
        assert call_args[1]['verify'] is True
        assert call_args[1]['params'] == {"param1": "value1"}
        assert call_args[1]['json'] is None
        assert 'User-Agent' in call_args[1]['headers']
        assert call_args[1]['headers']['User-Agent'].startswith('dbhydro-py/')
    
    @patch('dbhydro_py.rest_adapters.rest_adapter_requests.requests')
    def test_get_http_error(self, mock_requests):
        """Test GET request with HTTP error."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.reason = "Not Found"
        mock_response.json.return_value = {"error": "Not found"}
        mock_requests.request.return_value = mock_response
        
        adapter = RestAdapterRequests()
        result = adapter.get(endpoint="https://api.test.com/not-found")
        
        assert result.status_code == 404
        assert result.message == "Not Found"
        assert result.data == {"error": "Not found"}
    
    @patch('dbhydro_py.rest_adapters.rest_adapter_requests.requests')
    def test_get_connection_error(self, mock_requests):
        """Test GET request with connection error."""
        mock_requests.request.side_effect = ConnectionError("Connection failed")
        
        adapter = RestAdapterRequests()
        
        result = adapter.get(endpoint="https://api.test.com/test")
        
        # Should return Result with status_code=0 for network errors
        assert result.status_code == 0
        assert "Connection failed" in result.message
        assert result.data == {}
    
    @patch('dbhydro_py.rest_adapters.rest_adapter_requests.requests')
    def test_get_timeout_error(self, mock_requests):
        """Test GET request with timeout error."""
        mock_requests.request.side_effect = Timeout("Request timed out")
        
        adapter = RestAdapterRequests()
        
        result = adapter.get(endpoint="https://api.test.com/test")
        
        # Should return Result with status_code=0 for network errors
        assert result.status_code == 0
        assert "Request timed out" in result.message
        assert result.data == {}
    
    @patch('dbhydro_py.rest_adapters.rest_adapter_requests.requests')
    def test_get_json_decode_error(self, mock_requests):
        """Test GET request with JSON decode error."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.reason = "OK"
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_requests.request.return_value = mock_response
        
        adapter = RestAdapterRequests()
        result = adapter.get(endpoint="https://api.test.com/test")
        
        assert result.status_code == 200
        assert result.data == {}  # Falls back to empty dict
    
    def test_post_method_exists(self):
        """Test that POST method exists (implementation not required yet)."""
        adapter = RestAdapterRequests()
        
        # The method should exist but may raise DbHydroException due to URL format
        # This just tests the method is callable
        assert hasattr(adapter, 'post')
        assert callable(adapter.post)
    
    def test_delete_method_exists(self):
        """Test that DELETE method exists (implementation not required yet)."""
        adapter = RestAdapterRequests()
        
        # The method should exist but may raise DbHydroException due to URL format
        # This just tests the method is callable
        assert hasattr(adapter, 'delete')
        assert callable(adapter.delete)