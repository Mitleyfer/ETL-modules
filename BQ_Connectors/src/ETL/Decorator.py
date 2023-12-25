"""Parent error handling class

Parent class for decorating ETL classes with methods for handling errors

Author: Anton Popkov

"""

import sys
import yaml
import logging

with open('/<path>/src/config.yaml', 'r') as file:
    credentials = yaml.load(file, Loader=yaml.Loader)

logging.basicConfig(
    filename=credentials['logs_path'],
    level=logging.INFO,
    format='%(asctime)s :: %(name)s - %(levelname)s - %(message)s')


class ErrorDecorator:

    def sys_error_decorator(func):
        """Method for decorating inner class methods with try-except block.
        Tries to execute the function, raises error if fails.

        """

        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                logging.error(f'Unexpected error in {func.__name__} {sys.exc_info()}')
                raise

        return wrapper

    def pass_error_decorator(func):
        """Method for decorating inner class methods with try-except block.
        Tries to execute the function, continue code execution if fails.

        """

        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                logging.error(f'Passing error in {func.__name__} {sys.exc_info()}')
                pass

        return wrapper
