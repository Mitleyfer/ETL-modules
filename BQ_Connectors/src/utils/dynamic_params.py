"""Dynamic parameters

Dynamic parameters for config.yaml file

Author: Anton Popkov

"""

import pandas as pd


def create_date(dt_timedelta, dt_format, add=True):
    """Function for date creation in a specified format

    Parameters
    ----------
    dt_timedelta : int
        number of days to subtract from today
    dt_format : str
        date format ('%Y-%m-%d %H:%M:%S' etc.)
    add : bool
        flag for plus sign

    Returns
    -------
    dt : str
        formatted date

    """
    dt = (pd.Timestamp.today() - pd.Timedelta(days=dt_timedelta),
          pd.Timestamp.today() + pd.Timedelta(days=dt_timedelta))[add]
    dt = dt.strftime(dt_format)
    return dt


def create_zd_query(str_template, sddt_timedelta, sddt_format, sdadd, fddt_timedelta, fddt_format, fdadd):
    start_date = create_date(sddt_timedelta, sddt_format, sdadd)
    finish_date = create_date(fddt_timedelta, fddt_format, fdadd)
    params = {'start_date': start_date, 'finish_date': finish_date}
    return str_template.format(**params)
