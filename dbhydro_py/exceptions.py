"""Custom exception classes for DBHydro API."""


class DbHydroException(Exception):
    """Base exception class for DbHydro-related errors.
    
    Attributes:
        message (str): Human-readable error description
        http_status_code (int, optional): HTTP response status code
        api_status_code (int, optional): API-level status code from response
        api_status_message (str, optional): Detailed API error message
        elapsed_time (float, optional): Request elapsed time from API
    """
    
    def __init__(self, message: str, http_status_code: int | None = None, api_status_code: int | None = None, api_status_message: str | None = None, elapsed_time: float | None = None):
        super().__init__(message)
        self.message = message
        self.http_status_code = http_status_code
        self.api_status_code = api_status_code
        self.api_status_message = api_status_message
        self.elapsed_time = elapsed_time
    
    def __str__(self) -> str:
        """Return a comprehensive error description."""
        parts = [self.message]
        
        if self.api_status_message:
            parts.append(f'API Error: {self.api_status_message}')
        
        if self.http_status_code:
            parts.append(f'HTTP Status: {self.http_status_code}')
            
        if self.api_status_code and self.api_status_code != self.http_status_code:
            parts.append(f'API Status: {self.api_status_code}')
            
        return ' | '.join(parts)
