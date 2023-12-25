"""Classes for HTTP requests to API

Classes for data extraction from API

Author: Anton Popkov

"""

import logging
from time import sleep
from src.ETL.Interfaces import ExtractorHTTP


class GeneralRequest(ExtractorHTTP):
    """Class for http request to API

    Parameters
    ----------

    request_func : func
        method from requests module
    params : dict
        request parameters

    """

    def __init__(self, request_func, params):
        super().__init__()
        self.request_func = request_func
        self.params = params

    @ExtractorHTTP.sys_error_decorator
    def http_request(self, request_params, tries_num=3):
        """Method for http request. Take requests parameters as argument and applies it to class requests method. Tries
        to get response several times to avoid 500 error

        Parameters
        ----------
        request_params : dict
            parameters of request
        tries_num : int
            number of tries to avoid 500 error

        Returns
        -------
        Response object
            request response

        """
        result = self.request_func(**request_params)
        if result.status_code != 200:
            if result.status_code == 500:
                logging.error("Internal server error")
                sleep(5)
                tries_num -= 1
                if not tries_num:
                    logging.error("Unable to get response from server")
                    return 0
                return self.http_request(request_params, tries_num)
            logging.error(f"Invalid response with code {result.status_code}")
            return result
        logging.info("Successful response")
        return result

    @ExtractorHTTP.sys_error_decorator
    def run_requests(self):
        """Method for running request to API

        Returns
        -------
        Response object
            request response

        """
        result = self.http_request(self.params)
        return result
