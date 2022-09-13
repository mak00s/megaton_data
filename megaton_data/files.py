import logging
import os

LOGGER = logging.getLogger(__name__)


def cd(destination_dir: str = None):
    """Changes current directory
        default destination is /tmp or TMP_DIR
    """
    if not destination_dir:
        destination_dir = os.getenv('TMP_DIR', '/tmp')

    LOGGER.debug(f"Changing current directory to {destination_dir}")
    os.makedirs(destination_dir, exist_ok=True)
    os.chdir(destination_dir)


def save_df_to_file(df, file_path: str):
    # Save df to a file
    df.to_csv(file_path,
              header=False,
              mode='a',
              index=False,
              sep='\t')
    LOGGER.info(f"Data saved to {file_path}.")
