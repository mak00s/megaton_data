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

    def output(self, msg: str, *args, **kwargs):
        if not IN_GCF:
            self.logger.debug(msg, *args, **kwargs)
        else:
            if self.level not in (INFO, WARNING, ERROR):
                print(json.dumps(dict(severity='DEBUG', message=msg)))
            elif self.level not in (WARNING, ERROR):
                print(json.dumps(dict(severity='INFO', message=msg)))
            elif self.level is not ERROR:
                print(json.dumps(dict(severity='WARNING', message=msg)))
            else:
                print(json.dumps(dict(severity='ERROR', message=msg)))

    def debug(self, msg: str, *args, **kwargs):
        self.output(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):
        self.output(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):
        self.output(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        self.output(msg, *args, **kwargs)
