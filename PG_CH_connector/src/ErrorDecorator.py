import sys
import yaml
import logging

with open('/<path>/config.yaml', 'r') as file:
    credentials = yaml.safe_load(file)

logging.basicConfig(
    filename=credentials['logs_path'],
    level=logging.INFO,
    # filemode = 'w',
    format='%(asctime)s :: %(name)s - %(levelname)s - %(message)s')


class ErrDecorator(object):

    def __init__(self):
        super().__init__()

    def sys_error_decorator(func):
        """Method for decorating inner class methods with try-except block.
        Tries to execute the function, raises error if fails.

        """

        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except:
                logging.error(f'Unexpected error in {func.__name__} {sys.exc_info()}')
                raise

        return wrapper

    def pass_error_decorator(func):
        """Method for decorating inner class methods with try-except block.
        Tries to execute the function, continue code execution if fails.

        """

        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except:
                logging.error(f'Passing error in {func.__name__} {sys.exc_info()}')
                pass

        return wrapper
