##
##

import time
from typing import Callable
from functools import wraps
from .cbdebug import cb_debug

retry_debugger = cb_debug('retry')
retry_logger = retry_debugger.logger


def retry(retry_count=5,
          factor=0.01,
          allow_list=None,
          always_raise_list=None
          ) -> Callable:
    def retry_handler(func):
        @wraps(func)
        def f_wrapper(*args, **kwargs):
            for retry_number in range(retry_count + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as err:
                    if always_raise_list and isinstance(err, always_raise_list):
                        raise

                    if allow_list and not isinstance(err, allow_list):
                        raise

                    if retry_number == retry_count:
                        raise

                    retry_logger.debug(f"retry {func.__name__}")
                    wait = factor
                    wait *= (2**(retry_number+1))
                    time.sleep(wait)
        return f_wrapper
    return retry_handler


def retry_a(retry_count=5,
            factor=0.01,
            allow_list=None,
            always_raise_list=None
            ) -> Callable:
    def retry_handler(func):
        @wraps(func)
        async def f_wrapper(*args, **kwargs):
            for retry_number in range(retry_count + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as err:
                    if always_raise_list and isinstance(err, always_raise_list):
                        raise

                    if allow_list and not isinstance(err, allow_list):
                        raise

                    if retry_number == retry_count:
                        raise

                    retry_logger.debug(f"retry_a {func.__name__}")
                    wait = factor
                    wait *= (2**(retry_number+1))
                    time.sleep(wait)
        return f_wrapper
    return retry_handler
