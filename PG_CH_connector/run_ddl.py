#!/usr/bin/env python3
"""DDL pipeline

Map types from PostgreSQL to Clickhouse, creates DDL query and executes it

Author: Anton Popkov

"""

import psycopg2
from clickhouse_driver import connect
from src.ErrorDecorator import credentials
from src.ETL import Connector, TypesMapper


pg_conn_dict = {
    'func': psycopg2.connect,
    'params': credentials['pg_params'],
    'sql_query': credentials['pg_queries']['dbtables_query']
}

ch_conn_dict = {
    'func': connect,
    'params': credentials['ch_params'],
    'sql_query': credentials['ch_queries']['dbtables_query']
}


def run_ddl():
    integrator = TypesMapper(connector=Connector, creds=credentials, from_db_params=pg_conn_dict,
                             to_db_params=ch_conn_dict)
    integrator.execute_full_mapping()


if __name__ == '__main__':
    run_ddl()
