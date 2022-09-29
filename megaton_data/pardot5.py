from __future__ import annotations

import json
from logging import DEBUG

import pandas as pd
import requests
from megaton_data import log

BASE_URI = 'https://pi.pardot.com'

logger = log.Logger(__name__)
logger.setLevel(DEBUG)


class Prospects(object):
    """A class to query and use Pardot prospects.
    """

    def __init__(self, client):
        self.client = client

    def query(self, **kwargs):
        return self._get(params=kwargs)

    def _get(self, params=None):
        """GET requests for the Prospect object."""
        if params is None:
            params = {}
        return self.client.get(object_name='prospects', params=params)


class VisitorActivities(object):
    """A class to query and use Pardot visitor activities.
    """

    def __init__(self, client):
        self.client = client

    def query(self, **kwargs):
        return self._get(params=kwargs)

    def _get(self, params=None):
        """GET requests for the Visitor Activity object."""
        if params is None:
            params = {}
        return self.client.get(object_name='visitor-activities', params=params)


class Visits(object):
    """A class to query and use Pardot visits.
    """

    def __init__(self, client):
        self.client = client

    def query(self, **kwargs):
        return self._get(params=kwargs)

    def _get(self, params=None):
        """GET requests for the Visit object."""
        if params is None:
            params = {}
        return self.client.get(object_name='visits', params=params)


class Pardot(object):
    """Class to manage Salesforce Pardot API
    """

    def __init__(self,
                 business_unit_id: str,
                 client_id: str,
                 client_secret: str,
                 token: str | None = None,
                 refresh_token: str | None = None,
                 login_url: str = 'https://login.salesforce.com'
                 ):
        self.business_unit_id = business_unit_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = token
        self.login_url = login_url
        # リフレッシュトークンから新しいトークンを得る
        if refresh_token:
            self.token = self.get_token_from_refresh_token(refresh_token)
        # APIリクエスト用の認証ヘッダを作っておく
        self.headers = self._build_auth_header()
        self.date_from = None
        self.date_to = None
        self.prospects = Prospects(self)
        self.visits = Visits(self)
        self.visitoractivities = VisitorActivities(self)

    def _build_auth_header(self):
        """
        Builds Pardot Authorization Header to be used with GET requests
        """
        if self.token and self.business_unit_id:
            return {"Authorization": "Bearer " + self.token, "Pardot-Business-Unit-Id": self.business_unit_id}
        else:
            raise Exception('Cannot build Authorization header. token or refresh_token is empty')

    def get_token_from_refresh_token(self, refresh_token: str) -> str:
        """refresh tokenからaccess tokenを得る
        """
        response = requests.post(
            self.login_url + '/services/oauth2/token',
            data={
                'grant_type': 'refresh_token',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'refresh_token': refresh_token
            })
        if response.status_code == 200:
            # success
            return json.loads(response.text)['access_token']
        else:
            # error
            # https://help.salesforce.com/s/articleView?id=sf.remoteaccess_oauth_flow_errors.htm&type=5
            error_type = json.loads(response.text)['error']
            error_description = json.loads(response.text)['error_description']
            message = f"Refresh Token Error (Salesforce API): {error_type} - {error_description}"
            raise errors.BadRequest(message)

    def set_dates(self, date1: str, date2: str):
        """Sets start date and end date"""
        self.date_from = date1
        self.date_to = date2

    def get(self, object_name, params=None):
        """
        Makes a GET request to the API. Checks for invalid requests that raise PardotAPIErrors.
        If no errors are raised, returns either the JSON response, or if no JSON was returned,
        returns the HTTP response status code.
        """
        if params is None:
            params = {}
        request = requests.get(
            self._full_path(object_name),
            headers=self.headers,
            params=params,
        )
        response = self._check_response(request).json()
        values = response.get('values')
        print(f"{len(values)} records were retrieved", end='')

        if response['nextPageUrl']:
            print(", but more", end='')
            while response['nextPageUrl'] is not None:
                print(".", end='')
                request = requests.get(response['nextPageUrl'], headers=self.headers)
                response = self._check_response(request).json()
                values += response.get('values')
            print()
        logger.debug(f"Total {len(values)} records were retrieved.")
        if len(values) == 100000:
            logger.warning("DATA LOSS: The limit of 100,000 records is reached.")

        return pd.json_normalize(values)

    @staticmethod
    def _full_path(object_name, version=5):
        """Builds the full path for the API request"""
        return f'{BASE_URI}/api/v{version}/objects/{object_name}'

    @staticmethod
    def _check_response(response):
        """
        Checks the HTTP response to see if it contains JSON. If it does, checks the JSON for error codes and messages.
        Raises PardotAPIError if an error was found. If no error was found, returns the JSON. If JSON was not found,
        returns the response status code.
        """
        if response.status_code != 200:
            json_data = response.json()
            if json_data.get('code'):
                raise errors.PardotAPIError(json_response=json_data)
            return json_data
        else:
            return response


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
