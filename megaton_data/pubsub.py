"""Common helper Pub/Sub functions"""
import base64
import logging
import os

from google.cloud import pubsub

PROJECT_ID = os.getenv("GCP_PROJECT")
LOGGER = logging.getLogger(__name__)

# Reuse GCP Clients across function invocations using globals
# https://cloud.google.com/functions/docs/bestpractices/tips#use_global_variables_to_reuse_objects_in_future_invocations
PS_CLIENT = None


def lazy_client() -> pubsub.PublisherClient:
    """Returns a Pub/Sub Client that may be shared between cloud function invocations.
    """
    global PS_CLIENT
    if not PS_CLIENT:
        logging.debug("Creating Pub/Sub Client")
        PS_CLIENT = pubsub.PublisherClient()
    return PS_CLIENT


def publish(topic: str, message: str):
    """Publishes a message to a Pub/Sub topic.

    Args:
        topic (str): Pub/Sub topic path
        message (str): message to send to Pub/Sub
    """
    client = lazy_client()
    if topic:
        topic_path = client.topic_path(PROJECT_ID, topic)
        data = message.encode('utf-8')
        future = client.publish(topic_path, data)
        future.result()


def parse_topic_path(path: str) -> tuple:
    """Parses a topic path into its component segments.

    Args:
        path (str): Pub/Sub topic path
    Returns:
        tuple of project (str) and topic (str)
    """
    d = lazy_client().parse_topic_path(path)
    return d['project'], d['topic']


def get_message(event: dict) -> str:
    """base64-decode the data field of Pub/Sub message

    Args:
        event (dict): Pub/Sub message
    Returns:
        data field of Pub/Sub message (str)
    """
    try:
        return base64.b64decode(event['data']).decode('utf-8')
    except Exception:
        return ""
