import json
from logging import getLogger, INFO, WARNING, ERROR
import os

# executed in Google Cloud Functions?
IN_GCF = False
if os.getenv('FUNCTION_REGION') or os.getenv('FUNCTION_TARGET'):
    IN_GCF = True


class Logger:
    def __init__(self, logger_name: str):
        self.level = WARNING
        # if not IN_GCF:
        self.logger = getLogger(logger_name)

    def setLevel(self, level):
        self.level = level
        # if not IN_GCF:
        self.logger.setLevel(level)

    def debug(self, msg: str, *args, **kwargs):
        if IN_GCF:
            print(json.dumps(dict(severity='DEBUG', message=msg)))
        else:
            self.logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):
        if IN_GCF:
            print(json.dumps(dict(severity='INFO', message=msg)))
        else:
            self.logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):
        if IN_GCF:
            print(json.dumps(dict(severity='WARNING', message=msg)))
        else:
            self.logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        if IN_GCF:
            print(json.dumps(dict(severity='ERROR', message=msg)))
        else:
            self.logger.error(msg, *args, **kwargs)
