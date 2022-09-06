"""Common helper BigQuery functions"""

import logging
import os

from google.api_core import retry
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT")
LOGGER = logging.getLogger(__name__)

# Reuse GCP Clients across function invocations using globbals
# https://cloud.google.com/functions/docs/bestpractices/tips#use_global_variables_to_reuse_objects_in_future_invocations
BQ_CLIENT = None


def lazy_client() -> bigquery.Client:
    """
    Return a BigQuery Client that may be shared between cloud function
    invocations.
    """
    global BQ_CLIENT
    if not BQ_CLIENT:
        BQ_CLIENT = bigquery.Client(
            project=PROJECT_ID)
    return BQ_CLIENT


def get_df_from_query(query: str):
    rows_iterable = run_query(query)
    df = rows_iterable.to_dataframe()
    LOGGER.info(f"{len(df)} rows were retrieved.")

    return df


def run_query(query: str):
    bq_client = lazy_client()
    # Make a API request
    LOGGER.debug(f"Querying BQ: {query}")
    query_job = bq_client.query(query)

    return query_job.result()  # Waits for query to finish
