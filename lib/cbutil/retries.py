##
##

import time
from typing import Callable
from functools import wraps
from lib.cbutil.cbdebug import cb_debug


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
                        debug = cb_debug(retry.__name__)
                        logger = debug.logger
                        logger.error(f"{func.__name__} retry exceeded")
                        debug.close()
                        raise

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
                        debug = cb_debug(retry_a.__name__)
                        logger = debug.logger
                        logger.error(f"{func.__name__} retry exceeded")
                        debug.close()
                        raise

                    wait = factor
                    wait *= (2**(retry_number+1))
                    time.sleep(wait)
        return f_wrapper
    return retry_handler
