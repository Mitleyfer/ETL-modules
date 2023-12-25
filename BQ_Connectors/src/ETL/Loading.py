"""Classes for data ingestion

Classes for loading data into managed cloud storages(Google Cloud BigQuery, AWS etc.)

Author: Anton Popkov

"""

import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from src.ETL.Interfaces import LoaderCloud


class LoaderBQ(LoaderCloud):
    """Class for loading data into BigQuery

    Parameters
    ----------

    creds : dict
        creds for authorization in BigQuery
    data : dask.DataFrame
        data to load into BigQuery

    """

    def __init__(self, data, creds):
        super().__init__()
        self.creds = creds
        self.data = data

    @LoaderCloud.sys_error_decorator
    def create_client(self):
        """Method for creating BigQuery client

        Returns
        -------
        BigQuery client
            client object

        """
        client = bigquery.Client()
        logging.info("BQ Client created")
        return client

    @LoaderCloud.sys_error_decorator
    def create_schema(self):
        """Method for creating schema of a BigQuery table

        Returns
        -------
        list
            list of BigQuery types

        """
        columns = list(self.data.columns)
        schema = [bigquery.SchemaField(col, "STRING") if col != self.creds['partition_col']
                  else bigquery.SchemaField(col, "DATETIME") for col in columns]
        return schema

    def check_if_exists(self, func, obj_name):
        """Method for checking if specific object exists in BQ project

        Parameters
        ----------
        func : google client function
            BQ client method
        obj_name : str
            full name of the object in BQ

        Returns
        -------
        bool
            flag indicating whether object exists

        """
        try:
            func(obj_name)
            logging.info(f"{obj_name} already exists")
            return True
        except NotFound:
            logging.info(f"{obj_name} does not yet exist")
            return False

    @LoaderCloud.sys_error_decorator
    def create_dataset_if_not_exists(self, client):
        """Method for creating BigQuery dataset if not exists. Extracts full dataset name from provided credentials

        Parameters
        ----------
        client : BigQuery client
            authorized BigQuery client

        Returns
        -------
        None

        """
        project_id = self.creds['bq_table'].split('.')[0]
        dataset_id = self.creds['bq_table'].split('.')[1]
        dataset_name = f"{project_id}.{dataset_id}"
        if not self.check_if_exists(client.get_dataset, dataset_name):
            dataset = bigquery.Dataset(dataset_name)
            dataset.location = self.creds['bq_region']
            client.create_dataset(dataset, timeout=60)
            logging.info(f"{dataset_name} successfully created")

    @LoaderCloud.sys_error_decorator
    def create_table_if_not_exists(self, client, schema):
        """Method for creating BigQuery table if not exists

        Parameters
        ----------
        client : BigQuery client
            authorized BigQuery client
        schema : list
            schema for BigQuery table

        Returns
        -------
        None

        """
        table_id = self.creds['bq_table']
        if self.creds['sharding_date']:
            table_id = f"{self.creds['bq_table']}_{self.creds['sharding_date']}"
        if not self.check_if_exists(client.get_table, table_id):
            table = bigquery.Table(table_id, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                                          type_=bigquery.TimePartitioningType.DAY,
                                          field=self.creds['partition_col']
                                          )
            client.create_table(table)
            logging.info(f"{table_id} successfully created")

    @LoaderCloud.sys_error_decorator
    def load_data(self, client):
        """Method for loading data into BigQuery table

        Parameters
        ----------
        client : BigQuery client
            authorized BigQuery client

        Returns
        -------
        None

        """
        dataset_id = self.creds['bq_table'].split('.')[1]
        table_id = self.creds['bq_table'].split('.')[2]
        if self.creds['sharding_date']:
            table_id = f"{table_id}_{self.creds['sharding_date']}"
        table_ref = client.dataset(dataset_id).table(table_id)
        data = self.data.compute()
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.write_disposition = self.creds['bq_writing_mode']
        load_job = client.load_table_from_dataframe(data, table_ref, job_config=job_config)
        logging.info(f'Data loaded at {load_job}')

    @LoaderCloud.sys_error_decorator
    def bq_query(self, sql_query, client):
        """Method for executing sql queries in BQ project

        Parameters
        ----------
        client : BigQuery client
            authorized BigQuery client
        sql_query : str
            query to be executed

        Returns
        -------
        Result of sql query

        """
        result = client.query(sql_query)
        logging.info(f"{sql_query.split(' ')[0]} executed")
        return result

    @LoaderCloud.sys_error_decorator
    def execute_loading(self):
        """Method for running loading pipeline. Sequentially executes Loader class methods

        Returns
        -------
        None

        """
        client = self.create_client()
        self.create_dataset_if_not_exists(client)
        schema = self.create_schema()
        self.create_table_if_not_exists(client, schema)
        table_id = self.creds['bq_table']
        if self.creds['sharding_date']:
            table_id = f"{table_id}_{self.creds['sharding_date']}"
        table = client.get_table(table_id)
        current_schema = table.schema
        new_columns = list(set(schema).difference(set(current_schema)))
        if new_columns:
            current_schema.extend(new_columns)
            table.schema = current_schema
            client.update_table(table, ["schema"])
        if self.creds['by_date_del'] == 'Y':
            date = self.data[self.creds['date_col']].min().compute()
            del_query = self.creds['delete_by_date'].format(start_date=date)
            self.bq_query(del_query, client)
        if self.creds['in_clause_del'] == 'Y':
            arr = tuple(self.data[self.creds['primary_key']].unique().compute())
            del_query = self.creds['delete_by_condition'].format(arr=arr)
            self.bq_query(del_query, client)
        self.load_data(client)
