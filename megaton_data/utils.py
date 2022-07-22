"""Common Functions
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from dateutil import parser, tz
import re

import pandas as pd

JST = timezone(timedelta(hours=+9), 'JST')


def is_integer(n) -> bool:
    """Determines the provided string is an integer number"""
    try:
        float(n)
    except ValueError:
        return False
    else:
        return float(n).is_integer()


def extract_integer_from_string(s: str) -> int:
    """Extracts integer from string provided."""
    m = re.search(r'(\d+)', s)
    if m:
        return int(m.group(1))


def get_chunked_list(original_list: list, chunk_size: int = 500) -> list:
    """Splits a list into chunks

    Args:
        original_list (list): list to be split into chunks
        chunk_size (int): Chunk site
    Returns:
        list of list
    """
    chunked_list = []
    for i in range(0, len(original_list), chunk_size):
        chunked_list.append(original_list[i:i + chunk_size])
    return chunked_list


def format_datetime(string: str, timezone: str = 'Asia/Tokyo', format_: str = '%Y-%m-%d %H:%M:%S') -> str:
    """ 日付の文字列 2021-12-20T13:16:58.130Z
        を日本時間のYYYY-MM-DD h:m:s形式に変換
    """
    converted_timezone = tz.gettz(timezone)
    original_timezone = tz.gettz("UTC")
    local_datetime = parser.parse(string).replace(tzinfo=original_timezone).astimezone(converted_timezone)
    return local_datetime.strftime(format_)


def today(string: bool = False, format_: str = '%Y-%m-%d') -> datetime | str:
    """Gets a string of today

    Args:
        string (bool): if True, string is returned
        format_ (str): format. defaults to YYYY-MM-DD
    Returns:
        datetime.datetime or string representation of datetime.datetime in a format specified
    """
    date = datetime.now(JST)
    if string:
        return date.strftime(format_)
    return date


def yesterday(string: bool = False, format_: str = '%Y-%m-%d') -> datetime | str:
    """Gets a string of yesterday

    Args:
        string (bool): if True, string is returned
        format_ (str): format. defaults to YYYY-MM-DD
    Returns:
        datetime.datetime or string representation of datetime.datetime in a format specified
    """
    date = datetime.now(JST) - timedelta(1)
    if string:
        return date.strftime(format_)
    return date


"""Dataframe"""


def change_column_type(df: pd.DataFrame,
                       to_date: list | None = None,
                       to_datetime: list | None = None
                       ) -> pd.DataFrame:
    """Changes column type in dataframe from str to date or datetime
    """
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
        df (DataFrame): dataframe to be converted
        rules (list): list of tuple (column name, regex, to)
    Returns:
        none
    """
    for r in rules:
        col, rule, to = r
        try:
            _ = df[col].replace(rule, to, inplace=True, regex=True)
        except KeyError as e:
            print(e)
            pass


def prep_df(df: pd.DataFrame,
            rename_columns: dict = None,
            delete_columns: list = None,
            type_columns: dict = None
            ) -> pd.DataFrame:
    """Processes dataframe

    Args:
        df (DataFrame):
            DataFrame to be processed
        delete_columns (list):
            list of column name to be deleted
        type_columns (dict):
            dict of column name -> data type
            ex. {'pageviews': 'int32'}
        rename_columns (dict):
            dict of column name -> new column name
    Returns:
        processed dataframe
    """
    if isinstance(df, pd.DataFrame) and len(df) > 0:
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


def get_date_range(start_date: str, end_date: str, format_: str = '%Y-%m-%d') -> list:
    """Converts date range to a list of each date in the range
    """
    date_range = pd.date_range(start_date, end_date)
    return [d.strftime(format_) for d in date_range]
