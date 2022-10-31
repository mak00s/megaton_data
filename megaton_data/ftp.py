"""Common helper sftp functions"""
import io
import logging
import os
from urllib.parse import urlparse

import paramiko

from . import files


LOGGER = logging.getLogger(__name__)


class Connection:
    def __init__(self, url: str, key=None):
        conf = urlparse(url)
        self.host = conf.hostname
        self.port = conf.port
        self.username = conf.username
        self.password = conf.password
        self.path = conf.path
        self.key = key
        LOGGER.debug(f"FTP host={self.host}, port={self.port}, user={self.username}, path={self.path}")
        self.client = None
        self.sftp = None
        self.is_open = False

    def open(self):
        if not self.is_open:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.client.connect(
                self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                pkey=self.key,
                timeout=30.0,
                look_for_keys=False,
                allow_agent=False,
                # disabled_algorithms=dict(pubkeys=["rsa-sha2-512", "rsa-sha2-256"])
            )
            self.sftp = self.client.open_sftp()
            self.is_open = True
            LOGGER.info("Connected.")

    def list(self, target_dir=None, filter_pattern=None):
        if target_dir is None:
            target_dir = self.path
        remote_files = [_files for _files in self.sftp.listdir(path=target_dir)]
        if filter_pattern:
            return files.filter_files(remote_files, filter_pattern)
        else:
            return remote_files
        # return remote_files

    def download(self, filename, local_path=None):
        if local_path is None:
            local_path = filename
        remote_path = f"{self.path}/{filename}"
        self.open()
        self.sftp.get(remote_path, local_path)
        LOGGER.info(f"Fetched {remote_path} to {os.getcwd()}.")

    def upload(self, local_path, remote_path=None):
        self.open()
        self.sftp.put(local_path, remote_path)
        LOGGER.info(f"Uploaded {local_path} to {remote_path}.")

    def delete(self, filename):
        remote_path = f"{self.path}/{filename}"
        self.open()
        self.sftp.remove(remote_path)
        LOGGER.info(f"Deleted {remote_path} from FTP server.")

    def close(self):
        if self.is_open:
            self.client.close()
            self.is_open = False


def parse_url(url):
    return urlparse(url)
