import logging
import os

LOGGER = logging.getLogger(__name__)


def cd(destination_dir: str = None):
    """Changes current directory
        default destination is /tmp or TMP_DIR
    """
    if not destination_dir:
        destination_dir = os.getenv('TMP_DIR', '/tmp')

    os.makedirs(destination_dir, exist_ok=True)
    os.chdir(destination_dir)
    logging.debug(f"changed current directory to {destination_dir}")
