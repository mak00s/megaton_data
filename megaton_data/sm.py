import io
import logging
import os

from google.cloud import secretmanager
from paramiko import RSAKey

LOGGER = logging.getLogger(__name__)


class Secret:
    def __init__(self, project_id=None):
        if project_id:
            self.project_id = project_id
        else:
            self.project_id = os.getenv("GCP_PROJECT")

    def text(self, secret_id: str, version_id='latest'):
        """
        Access the payload for the given secret version if one exists. The version
        can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
        """

        # Create the Secret Manager client
        client = secretmanager.SecretManagerServiceClient()

        # Build the resource name of the secret version
        name = client.secret_version_path(self.project_id, secret_id, version_id)

        # Get the secret version
        response = client.access_secret_version(name=name)

        # Return the decoded payload.
        return response.payload.data.decode("UTF-8")

    def private_key(self, secret_id: str):
        if secret_id:
            sec = self.text(secret_id).rstrip('\n')
            LOGGER.debug(f"key found: {sec[:25]}")
            p_key = io.StringIO(sec)
            return RSAKey.from_private_key(p_key)
        logging.warn(f"key NOT found for {secret_id}")
