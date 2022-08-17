##
##

import logging
import os


class cb_debug(object):

    def __init__(self, name, filename=None, level=None, overwrite=False):
        if filename:
            self.default_debug_file = filename
        else:
            self.default_debug_file = 'cb_debug.log'
        self.debug_file = os.environ.get("CB_PERF_DEBUG_FILE", self.default_debug_file)
        self._logger = logging.getLogger(name)
        self.handler = logging.FileHandler(self.debug_file)
        self.formatter = logging.Formatter(logging.BASIC_FORMAT)
        self.handler.setFormatter(self.formatter)
        self.debug = False
        default_level = 3

        if overwrite:
            try:
                open(self.debug_file, 'w').close()
            except Exception as err:
                print(f"warning: can not clear log file {self.debug_file}: {err}")

        try:
            default_level = int(os.environ['CB_PERF_DEBUG_LEVEL']) if 'CB_PERF_DEBUG_LEVEL' in os.environ else 2
        except ValueError:
            print(f"warning: ignoring debug: environment variable CB_PERF_DEBUG_LEVEL should be a number")

        self.debug_level = level if level else default_level

        try:
            if self.debug_level == 0:
                self._logger.setLevel(logging.DEBUG)
            elif self.debug_level == 1:
                self._logger.setLevel(logging.INFO)
            elif self.debug_level == 2:
                self._logger.setLevel(logging.ERROR)
            else:
                self._logger.setLevel(logging.CRITICAL)

            self._logger.addHandler(self.handler)
            self.debug = True
        except Exception as err:
            print(f"warning: can not initialize logging: {err}")

    @property
    def do_debug(self):
        return self.debug

    @property
    def logger(self):
        return self._logger

    def close(self):
        handlers = self._logger.handlers[:]
        for handler in handlers:
            self._logger.removeHandler(handler)
            handler.close()
