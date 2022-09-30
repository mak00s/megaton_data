"""Common error types used across the library."""


class Error(Exception):
    """Base class for other exceptions"""
    pass


class BadRequest(Error):
    """"Request given has errors"""

    def __init__(self, message=None):
        self.message = message or "There is an error in the request."


class PardotAPIError(Error):
    """Basic exception class for errors encountered in API get requests.
    Takes the json response and parses out the error code and message.
    """

    def __init__(self, json_response):
        self.response = json_response
        self.err_code = json_response.get('code')
        self.message = json_response.get('message')
        if self.err_code is None:
            self.err_code = 0
            self.message = 'Unknown API error occurred'

    def __str__(self):
        return 'Error #{err_code}: {message}'.format(err_code=self.err_code, message=self.message)
