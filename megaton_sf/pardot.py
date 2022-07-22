"""Functions for Pardot
"""

import logging

from pypardot.client import PardotAPI
import pandas as pd

from . import utils

LOGGER = logging.getLogger(__name__)


class Pardot(object):
    """Class for Salesforce Pardot client
    """

    def __init__(self):
        self.consumer_key = None
        self.consumer_secret = None
        self.refresh_token = None
        self.business_unit_id = None
        self.date_from = None
        self.date_to = None
        self.prospect_ids = None
        self._client = None

    def authorize(self, key: str, secret: str, refresh_token: str, bu: str):
        self.consumer_key = key
        self.consumer_secret = secret
        self.refresh_token = refresh_token
        self.business_unit_id = bu
        return self

    @property
    def client(self):
        """get or create a api client"""
        if self._client is None:
            self._client = PardotAPI(
                sf_consumer_key=self.consumer_key,
                sf_consumer_secret=self.consumer_secret,
                sf_refresh_token=self.refresh_token,
                business_unit_id=self.business_unit_id,
                version=4
            )
        return self._client

    def set_dates(self, date1: str, date2: str):
        self.date_from = date1
        self.date_to = date2

    def _retry(self, method: str, limit: int = 200, **kwargs):
        """Automatic Paging"""
        all_rows = []
        offset = 0

        while True:
            total, rows = getattr(self, method)(offset=offset, **kwargs)
            retrieved = len(rows)

            if offset == 0:
                print(f"{total} rows found: ", end='')

            num1 = offset + 1
            num2 = offset + retrieved
            print(f"{num1}-{num2}", end='')

            all_rows.extend(rows)

            if offset + limit < total:
                # continue the loop
                offset = offset + limit
                print(", ", end='')
            else:
                print()
                break

        return all_rows

    def _query_prospects(self,
                         offset: int,
                         limit: int = 200,
                         fields='id,crm_lead_fid,email,company,campaign,created_at,updated_at'):
        date1 = self.date_from
        date2 = self.date_to

        response = self.client.prospects.query(
            updated_after=date1,
            updated_before=date2,
            fields=fields,
            sort_by='updated_at',
            limit=limit,
            offset=offset
        )
        rows = response['prospect']

        total = response['total_results']
        return total, rows

    def _query_visits_by_prospect_ids(self,
                                      prospect_ids: str,
                                      offset: int,
                                      limit: int = 200):
        response = self.client.visits.query_by_prospect_ids(
            prospect_ids=prospect_ids,
            limit=limit,
            offset=offset
        )
        rows = response['visit']

        total = response['total_results']
        return total, rows

    def _query_activities(self,
                          offset: int,
                          limit: int = 200,
                          type_: list = "1,2,4,6,11,21"):
        date1 = self.date_from
        date2 = self.date_to

        response = self.client.visitoractivities.query(
            updated_after=date1,
            updated_before=date2,
            prospect_only="true",
            type=type_,
            limit=limit,
            offset=offset
        )
        rows = response['visitor_activity']

        total = response['total_results']
        return total, rows

    def _query_activities_by_prospect_ids(self,
                                          prospect_ids: str,
                                          offset: int,
                                          limit: int = 200,
                                          type_: list = "1,2,4,6,11,21"):
        date1 = self.date_from
        date2 = self.date_to

        response = self.client.visitoractivities.query(
            updated_after=date1,
            updated_before=date2,
            prospect_id=prospect_ids,
            type=type_,
            limit=limit,
            offset=offset
        )
        rows = response['visitor_activity']

        total = response['total_results']
        return total, rows

    def _loop_by_ids(self, method: str, **kwargs):
        chunked_list = utils.get_chunked_list(self.prospect_ids, chunk_size=500)
        small_dfs = []
        for items in chunked_list:
            # 対象のprospect_idをカンマ区切りに整形
            prospect_ids = ",".join(items)
            data = self._retry(method, prospect_ids=prospect_ids, **kwargs)
            _df = pd.json_normalize(data)
            small_dfs.append(_df)
        df = pd.concat(small_dfs, ignore_index=True)
        return df

    def get_active_prospects(self, fields: str):
        """Gets active Prospects """
        data = self._retry(
            method='_query_prospects',
            fields=fields)
        df = pd.json_normalize(data)

        ids = [str(e) for e in df['id'].unique()]
        self.prospect_ids = sorted(ids)

        logging.debug(f"{len(ids)} Prospect IDs were retrieved.")

        return df

    def get_visits(self):
        """Gets Visits for prospects specified
        """
        df = self._loop_by_ids(method='_query_visits_by_prospect_ids')

        logging.debug(f"{len(df)} records were retrieved.")
        return df

    def get_activities(self, by: str = 'updated'):
        """Gets Visitor Activities
        """
        if by == 'id':
            # Get Visitor Activities for specific Prospect ID
            df = self._loop_by_ids(method='_query_activities_by_prospect_ids',
                                   type_="1,2,4,6,11,21")
        else:
            # Get all Visitor Activities updated after the date
            data = self._retry(method='_query_activities')
            df = pd.json_normalize(data)

        logging.debug(f"{len(df)} activities were retrieved.")
        return df
