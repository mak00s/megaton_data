"""Functions for Pardot API
"""

import logging

from pypardot.client import PardotAPI
import pandas as pd

from . import utils

logger = logging.getLogger(__name__)


class Pardot(object):
    """Class to manage Salesforce Pardot API
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

    @property
    def client(self) -> PardotAPI:
        """Gets or creates an api client"""
        if self._client is None:
            self._client = PardotAPI(
                sf_consumer_key=self.consumer_key,
                sf_consumer_secret=self.consumer_secret,
                sf_refresh_token=self.refresh_token,
                business_unit_id=self.business_unit_id,
                version=4
            )
        return self._client

    def authorize(self, key: str, secret: str, refresh_token: str, bu: str):
        self.consumer_key = key
        self.consumer_secret = secret
        self.refresh_token = refresh_token
        self.business_unit_id = bu
        return self

    def set_dates(self, date1: str, date2: str):
        """Sets start date and end date"""
        self.date_from = date1
        self.date_to = date2

    def retry(self, method: str, limit: int = 200, **kwargs) -> pd.DataFrame:
        """Automatic Paging

        Args:
            method (str): method name to run
            limit (int): max number of items to retrieve in a single request
        Returns:
            pd.DataFrame
        """
        all_rows = []
        offset = 0

        while True:
            total, rows = getattr(self, method)(offset=offset, **kwargs)
            retrieved = len(rows)

            if offset == 0:
                logger.info(f"Found total {total} rows.")

            if retrieved:
                num1 = offset + 1
                num2 = offset + retrieved
                logger.debug(f"Retrieved rows #{num1} - {num2}.")

            all_rows.extend(rows)

            if offset + limit < total:
                # continue the loop
                offset = offset + limit
            else:
                break

        if not len(all_rows):
            logger.warning("No data found.")
            return pd.DataFrame()
        else:
            return pd.json_normalize(all_rows)

    def loop_by_ids(self, method: str, **kwargs) -> pd.DataFrame:
        """Loop to execute a method

        Args:
            method (str): method name to run
        Returns:
            pd.DataFrame
        """
        df = pd.DataFrame()
        if self.prospect_ids:
            chunked_list = utils.get_chunked_list(self.prospect_ids, chunk_size=300)
            small_dfs = []
            for items in chunked_list:
                prospect_ids = ",".join(items)
                _df = self.retry(method, prospect_ids=prospect_ids, **kwargs)
                small_dfs.append(_df)
            df = pd.concat(small_dfs, ignore_index=True)
        else:
            logger.warning("No prospect_ids found.")

        return df

    def _query_prospects(self,
                         offset: int,
                         limit: int = 200,
                         fields: str = 'id,crm_lead_fid,email,company,campaign,created_at,updated_at',
                         ) -> tuple:
        """Gets Prospects updated during the period
        """
        response = self.client.prospects.query(
            created_after=self.date_from,
            created_before=self.date_to,
            fields=fields,
            sort_by='created_at',
            limit=limit,
            offset=offset
        )
        rows = response['prospect']

        total = response['total_results']
        return total, rows

    def _query_visits_by_prospect_ids(self,
                                      prospect_ids: str,
                                      offset: int,
                                      limit: int = 200,
                                      ) -> tuple:
        """Gets visits by specified Prospect IDs
        """
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
                          type_: list = "1,2,4,6,11,21"
                          ) -> tuple:
        """Gets Visitor Activities updated during the period
        """
        response = self.client.visitoractivities.query(
            updated_after=self.date_from,
            updated_before=self.date_to,
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
                                          type_: list = "1,2,4,6,11,21"
                                          ) -> tuple:
        """Gets Visitor Activities updated during the period
        """
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

    def get_new_prospects(self, fields: str) -> pd.DataFrame:
        """Gets active Prospects
        """
        df = self.retry(method='_query_prospects', fields=fields)

        # store prospect ids
        if 'id' in df.columns:
            ids = [str(e) for e in df['id'].unique()]
            self.prospect_ids = sorted(ids)
        else:
            self.prospect_ids = []

        return df

    def get_visits(self) -> pd.DataFrame:
        """Gets Visits for prospects specified
        """
        df = self.loop_by_ids(method='_query_visits_by_prospect_ids')

        return df

    def get_activities(self, by: str = 'updated', type_: str = "1,2,4,6,11,21") -> pd.DataFrame:
        """Gets Visitor Activities
        """
        if by == 'id':
            # Get Visitor Activities for specific Prospect ID
            df = self.loop_by_ids(method='_query_activities_by_prospect_ids', type_=type_)
        else:
            # Get all Visitor Activities updated after the date time specified
            df = self.retry(method='_query_activities', type_=type_)

            # store prospect ids
            if 'prospect_id' in df.columns:
                ids = [str(e) for e in df['prospect_id'].unique()]
                self.prospect_ids = sorted(ids)
            else:
                self.prospect_ids = []

        return df
