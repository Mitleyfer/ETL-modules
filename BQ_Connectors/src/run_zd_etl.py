#!/usr/bin/env python3
"""API ETL pipelines

Pipelines for Zendesk ETL job

Author: Anton Popkov

"""

import os
from src.ETL.Decorator import credentials
from src.utils.request_funtions import pagination_zendesk

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials['bq_creds']['json_creds_path']


def run_etl():
    pagination_zendesk(creds=credentials, key_ind='zd_tickets_creds')


if __name__ == '__main__':
    run_etl()
