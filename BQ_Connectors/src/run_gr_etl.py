#!/usr/bin/env python3
"""API ETL pipelines

Pipelines for GetResponse ETL job

Author: Anton Popkov

"""

import os
from src.ETL.Decorator import credentials
from src.utils.request_funtions import pagination_getresponse

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials['bq_creds']['json_creds_path']


def run_etl():
    pagination_getresponse(creds=credentials, key_ind='gr_contacts_creds')
    pagination_getresponse(creds=credentials, key_ind='gr_unsubscription_creds')


if __name__ == '__main__':
    run_etl()
