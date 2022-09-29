"""Common helper FTP functions"""
import logging
import os

from google.cloud import secretmanager

PROJECT_ID = os.getenv("GCP_PROJECT")
LOGGER = logging.getLogger(__name__)

# Reuse GCP Clients across function invocations using globbals
# https://cloud.google.com/functions/docs/bestpractices/tips#use_global_variables_to_reuse_objects_in_future_invocations
SM_CLIENT = None


def lazy_client() -> secretmanager.SecretManagerServiceClient:
    """
    Return a BigQuery Client that may be shared between cloud function
    invocations.
    """
    global SM_CLIENT
    if not SM_CLIENT:
        LOGGER.debug("Creating Storage Client")
        SM_CLIENT = secretmanager.SecretManagerServiceClient()
    return SM_CLIENT


def get_secret_text(project_id: str, secret_num: str):
    """
    Get information about the given secret
    """
    client = lazy_client()
    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_num}/versions/latest"

    # Get the secret
    response = client.access_secret_version(request={"name": name})

    return response.payload.data.decode('UTF-8')
