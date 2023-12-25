"""Classes for creation of DDL

Integrate PostgreSQL and Clickhouse via PostgreSQL ENGINE

Author: Anton Popkov

"""

import pandas as pd
from functools import partial
from src.ErrorDecorator import ErrDecorator


class Connector(ErrDecorator):
    """Class for connection to Database

    Parameters
    ----------
    func : connection driver function
        driver that provides connection (psycopg2.connect/clickhouse_driver.connect/etc.)
    sql_query : str
        sql query to execute wit connection driver
    params: dict
        connection parameters to parse into driver

    """

    def __init__(self, func, sql_query, params):
        super().__init__()
        self.func = func
        self.params = params
        self.sql_query = sql_query

    @ErrDecorator.sys_error_decorator
    def create_connection(self):
        """Method for establishing a connection to Database

        Returns
        -------
        connection object
            conn

        """
        conn = self.func(**self.params)
        return conn

    @ErrDecorator.sys_error_decorator
    def execute_query(self, conn):
        """Method for executing sql query in Database

        Parameters
        ----------
        conn : connection object
            established connection

        Returns
        -------
        pandas DataFrame
            data

        """
        data = pd.read_sql(sql=self.sql_query, con=conn)
        return data

    @ErrDecorator.sys_error_decorator
    def execute_pipeline(self):
        """Method for executing connection pipeline

        Returns
        -------
        pandas DataFrame
            data

        """
        conn = self.create_connection()
        data = self.execute_query(conn)
        return data


class TypesMapper(ErrDecorator):
    """Class for connection to Database

    Parameters
    ----------
    connector : Connector class
         provides connection to DataBase
    creds : dict
        parameters from config.yaml
    from_db_params: dict
        parameters for Connector to establish connection to source DB
    to_db_params: dict
        parameters for Connector to establish connection to target DB

    """

    def __init__(self, connector, creds, from_db_params, to_db_params):
        super().__init__()
        self.connector = connector
        self.creds = creds
        self.from_db_params = from_db_params
        self.to_db_params = to_db_params

    @ErrDecorator.sys_error_decorator
    def create_connector(self, params):
        """Method for creating a connection

        Parameters
        ----------
        params : dict
            parameters for Connector to establish connection to target DB

        Returns
        -------
        Connector class instance
            conn

        """
        conn = self.connector(**params)
        return conn

    @ErrDecorator.sys_error_decorator
    def get_types(self):
        """Method for creating a dataframe of target db types

        Returns
        -------
        pandas DataFrame
            to_types_df

        """
        to_types_df = pd.DataFrame.from_dict(self.creds['types_mappings'], orient='index',
                                             columns=['to_data_type']).reset_index()
        return to_types_df

    @ErrDecorator.sys_error_decorator
    def get_unmapped_tables(self):
        """Method for getting a set of difference between table names in source and target databases

        Returns
        -------
        set
            tables_diff

        """
        from_connection = self.create_connector(self.from_db_params)
        from_df = from_connection.execute_pipeline()
        to_connection = self.create_connector(self.to_db_params)
        to_df = to_connection.execute_pipeline()
        from_tables_set = set(from_df['table_name'].values)
        to_tables_set = set(to_df['name'].values)
        tables_diff = from_tables_set - to_tables_set
        return tables_diff

    @ErrDecorator.sys_error_decorator
    def execute_mapping(self, table_name, to_types_df):
        """Method for creating a DDL query for target database

        Parameters
        ----------
        table_name : str
            table name to process
        to_types_df : pandas.DataFrame
            dataframe with desired data types

        Returns
        -------
        None

        """
        self.from_db_params['sql_query'] = self.creds['pg_queries']['table_schema_query'].format(name=table_name)
        connection = self.create_connector(self.from_db_params)
        from_df = connection.execute_pipeline()
        converted_types_df = pd.merge(from_df, to_types_df, left_on='data_type',
                                      right_on='index', how='left')[['column_name', 'to_data_type']]
        converted_types_list = converted_types_df.values.tolist()
        body_str = ',\n'.join([' '.join(i) for i in converted_types_list])
        query = self.creds['ch_queries']['create_table_query'].format(name=table_name, body=body_str)
        self.to_db_params['sql_query'] = query
        connection = self.create_connector(self.to_db_params)
        connection.execute_pipeline()

    @ErrDecorator.sys_error_decorator
    def execute_full_mapping(self):
        """Method for mapping types for all tables in difference set

        Returns
        -------
        None

        """
        tables_diff = self.get_unmapped_tables()
        to_types_df = self.get_types()
        args = partial(self.execute_mapping, to_types_df=to_types_df)
        _ = [*map(args, tables_diff)]
