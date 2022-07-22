"""Common Functions
"""

import re

import pandas as pd


def is_integer(n):
    """Determines the provided string is an integer number"""
    try:
        float(n)
    except ValueError:
        return False
    else:
        return float(n).is_integer()


def extract_integer_from_string(s):
    """Extracts integer from string provided."""
    m = re.search(r'(\d+)', s)
    if m:
        return int(m.group(1))


def change_column_type(df: pd.DataFrame, to_date=None, to_datetime=None):
    """Changes column type in dataframe from str to date or datetime"""
    if not to_date:
        to_date = ['date', 'firstSessionDate']
    if not to_datetime:
        to_datetime = ['dateHour', 'dateHourMinute']

    for col in df.columns:
        if col in to_date:
            df[col] = pd.to_datetime(df[col], infer_datetime_format=True, errors='coerce').dt.date
        if col in to_datetime:
            df[col] = pd.to_datetime(df[col], infer_datetime_format=True, errors='coerce')

    return df


def replace_columns(df: pd.DataFrame, rules: list):
    """Converts dataframe columns using regex

    Args
        df: dataframe to be converted
        rules: list of tuple (column name, regex, to)
    """
    for r in rules:
        col, rule, to = r
        try:
            _ = df[col].replace(rule, to, inplace=True, regex=True)
        except KeyError as e:
            print(e)
            pass


def prep_df(df, rename_columns=None, delete_columns=None, type_columns=None):
    """Processes dataframe

    Args
        delete_columns:
            list of column name to be deleted
        type_columns:
            dict of column name -> data type
            ex. {'pageviews': 'int32'}
        rename_columns:
            dict of column name -> new column name
    Returns
        processed dataframe
    """
    if len(df) > 0:
        if type_columns:
            try:
                df = df.astype(type_columns, errors='ignore')
            except KeyError:
                pass
        if delete_columns:
            try:
                df.drop(delete_columns, axis=1, inplace=True)
            except KeyError:
                pass
        if rename_columns:
            df.columns = df.columns.to_series().replace(rename_columns, regex=True)
        return df


def get_date_range(start_date: str, end_date: str, format_: str = '%Y-%m-%d'):
    """Converts date range to a list of each date in the range"""
    date_range = pd.date_range(start_date, end_date)
    return [d.strftime(format_) for d in date_range]


def get_chunked_list(original_list: list, chunk_size: int = 500):
    """Splits a list into chunks"""
    chunked_list = []
    for i in range(0, len(original_list), chunk_size):
        chunked_list.append(original_list[i:i + chunk_size])
    return chunked_list
