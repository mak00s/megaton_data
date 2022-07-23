"""Common helper GCS functions"""

import logging
import os

from google.cloud import storage

PROJECT_ID = os.getenv("GCP_PROJECT")
LOGGER = logging.getLogger(__name__)

# Reuse GCP Clients across function invocations using globbals
# https://cloud.google.com/functions/docs/bestpractices/tips#use_global_variables_to_reuse_objects_in_future_invocations
CS_CLIENT = None


def lazy_client() -> storage.Client:
    """Returns a Storage Client that may be shared between cloud function invocations.
    """
    global CS_CLIENT
    if not CS_CLIENT:
        logging.debug("Creating Storage Client")
        CS_CLIENT = storage.Client()
    return CS_CLIENT


def upload_object(bucket_name: str, local_path: str, remote_path: str = None) -> str:
    """GCSにファイルを転送する
    """
    if not remote_path:
        remote_path = os.path.basename(local_path)
    client = lazy_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(remote_path)

    # https://github.com/googleapis/python-storage/issues/74
    blob._MAX_MULTIPART_SIZE = 1024 * 1024 * 5  # 5MB
    blob.chunk_size = 1024 * 1024 * 5  # 5MB

    try:
        logging.debug(f"Uploading {local_path} to gs://{bucket_name}/{remote_path}")
        blob.upload_from_filename(local_path, timeout=3600)
    except Exception:
        raise
    else:
        # returns a public url
        return blob.public_url


def delete_object(bucket_name: str, blob_name: str) -> None:
    """GCSからファイルを削除する
    """
    client = lazy_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    logging.debug(f"Deleting {blob_name} from GCS bucket {bucket_name}.")
    blob.delete()


def copy_file(source_bucket_name: str, destination_bucket_name: str, source_blob_name: str) -> None:
    """GCSのファイルを別のBucketへコピーする
    """
    client = lazy_client()
    source_bucket = client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    destination_bucket = client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, source_blob_name
    )
