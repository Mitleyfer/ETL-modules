"""Classes for transformations

Classes for data transformations into loadable format

Author: Anton Popkov

"""

import logging
import pandas as pd
import dask.dataframe as dd
from src.ETL.Interfaces import Transformation


class TransformationDask(Transformation):
    """Class for flattening and creating dask dataframe from requests API responses

    Parameters
    ----------

    raw_data : response object
        API response object
    creds : dict
        params for transformation

    """

    def __init__(self, raw_data, creds):
        self.raw_data = raw_data
        self.creds = creds

    @Transformation.sys_error_decorator
    def format_response(self):
        """Method for converting response object into json. Returns data from json
        if key with data specified and actual json otherwise

        Returns
        -------

        dict
            response json

        """
        if self.creds['json_key']:
            json_file = self.raw_data.json()[self.creds['json_key']]
        else:
            json_file = self.raw_data.json()
        return json_file

    @Transformation.sys_error_decorator
    def json_normalize(self, file):
        """Method for flattening nested response dict

        Parameters
        ----------
        file : dict
            API response dict

        Returns
        -------

        pandas Dataframe
            pandas dataframe with response data

        """
        json_norm = pd.json_normalize(file, sep=self.creds['separator']).to_dict(orient='records')
        return json_norm

    @Transformation.sys_error_decorator
    def to_dd(self, data):
        """Method for converting pandas dataframe into dask dataframe

        Parameters
        ----------
        data : pandas Dataframe
            normalized pandas Dataframe

        Returns
        -------

        dask dataframe
            dataframe with response data

        """
        dd_df = dd.from_pandas(pd.DataFrame(data, dtype=str), npartitions=1)
        return dd_df

    @Transformation.sys_error_decorator
    def run_transformation(self):
        """Method for sequential running of class transformation methods

        Returns
        -------

        dask dataframe
            dataframe with response data

        """
        json_file = self.format_response()
        normalized_file = self.json_normalize(json_file)
        dd_df = self.to_dd(normalized_file)
        dd_df[self.creds['created_on_col']] = pd.Timestamp.today()
        logging.info("Transformation has been successful")
        return dd_df
