"""ETL abstract classes

Blueprint for ETL classes

Author: Anton Popkov

"""

from abc import ABC, abstractmethod
from src.ETL.Decorator import ErrorDecorator


class ExtractorHTTP(ABC, ErrorDecorator):
    """Template class for extracting data from web API

    """

    def __init__(self):
        super().__init__()

    @abstractmethod
    def http_request(self, *args, **kwargs):
        """Method for http request

        Returns
        -------
        response : Response object

        """
        pass

    @abstractmethod
    def run_requests(self):
        """Method for running http requests pipeline

        """
        pass


class ExtractorDB(ABC, ErrorDecorator):
    """Template class for extracting data from data vaults

    """

    def __init__(self):
        super().__init__()

    @abstractmethod
    def create_connection(self):
        """Method for establishing a connection to a database

        """
        pass

    @abstractmethod
    def extract_data(self):
        """Method for extracting data from data vaults

        """
        pass


class Transformation(ABC, ErrorDecorator):
    """Template class for transformations applied to Zendesk API responses

    """

    @abstractmethod
    def run_transformation(self):
        pass


class LoaderCloud(ABC, ErrorDecorator):
    """Template class for data ingestion into Managed Cloud clusters

    """

    def __init__(self):
        super().__init__()

    @abstractmethod
    def create_client(self):
        """Method for creating service client

        """
        pass

    @abstractmethod
    def check_if_exists(self, *args, **kwargs):
        """Method for checking existence of object in BigQuery

        """
        pass

    @abstractmethod
    def create_schema(self):
        """Method for creating data schema

        """
        pass

    @abstractmethod
    def create_table_if_not_exists(self, *args, **kwargs):
        """Method for creating table in dataset if not exists

        """
        pass

    @abstractmethod
    def execute_loading(self):
        """Method for launching data loading process

        """
        pass
