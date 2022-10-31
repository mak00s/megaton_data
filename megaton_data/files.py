import logging
import os
import re
from zipfile import ZipFile


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


def unzip(file):
    """ 指定ファイルをZIP解凍
    """

    if file.endswith('.zip'):
        with ZipFile(file, 'r') as zipObj:
            # Get a list of all archived file names from the zip
            filenames = zipObj.namelist()
            # Extract all the contents of zip file in different directory
            zipObj.extractall()
            # shutil.unpack_archive(file)
            logging.info("Unzipped.")
        return filenames
    else:
        return [file]


def filter_files(files, pattern_str):
    """ 文字列の配列からパターンに合致する項目だけを残す
    """
    pat = re.compile(pattern_str)
    matched_files = [x for x in files if re.search(pat, x)]
    return matched_files
