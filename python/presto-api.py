import logging
from pathlib import Path
import json
import sqlite3

from datetime import datetime
from typing import List
from typing import Dict
from collections import namedtuple

from urllib .parse import urlparse, urlunparse

import requests

import pandas as pd

from IPython import embed

SCRIPT_DIR = Path(__file__).parent.resolve()
log = logging.getLogger(__name__)


def retrieve_query_list(netloc=None):
    if netloc is None:
        netloc = '10.0.0.2:8080'

    scheme = 'http'
    path = '/v1/query'

    presto_url = urlunparse((scheme, netloc, path, '', '', ''))
    response = requests.get(presto_url)
    response.raise_for_status()
    return response.json()


def retrieve_query(query_url):
    response = requests.get(query_url)
    response.raise_for_status()
    return response.json()


def get_query_url(query_self_url, netloc):
    uo = urlparse(query_self_url)
    query_url = urlunparse(
        (uo.scheme, netloc, uo.path, '', '', '')
    )
    return query_url


query_extract_fields = [
    'queryId', 'state', 'url', 'createTime', 'endTime', 'errorType', 'query']

QueryExtractBase = namedtuple(
    'QueryExtractBase', query_extract_fields)


def get_query_extract(query, netloc):
    url = get_query_url(query['self'], netloc)
    errorType = query.get('errorType', '')
    createTime = query['queryStats']['createTime']
    endTime = query['queryStats']['endTime']
    query_str = query['query']
    return QueryExtractBase(
        query['queryId'], query['state'], url, createTime, endTime,
        errorType, query_str)


def get_query_detail_extract(query_detail):
    inputs = query_detail['inputs']
    return inputs


def get_query_detail_table(table):
    columns = [col['name'] for col in table['columns']]
    return {
        'schema': table['schema'],
        'table': table['table'],
        'columns': columns
    }


def get_query_inputs(query_detail_url):
    '''
    retrieves query detail and extracts the inputs
    '''
    query_detail = retrieve_query(query_detail_url)
    inputs = get_query_detail_extract(query_detail)
    return inputs


def query_tables_to_dataframe(tables):
    records = []
    for idx, table in enumerate(tables):
        if idx == 0:
            schema = table['schema']
            table_name = table['table']

        column_list = table['columns']
        for column in column_list:
            record = [schema, table_name]
            record.append(column)
            records.append(record)
    df = pd.DataFrame(records, columns=['schema', 'table', 'column'])
    return df


class QueryList(list):
    @classmethod
    def retrieve(cls):
        netloc = '10.0.0.2:8080'

        def get_query_extract_url(query):
            return get_query_extract(query, netloc)

        query_list = retrieve_query_list(netloc)
        query_extract_list = list(map(get_query_extract_url, query_list))
        for query_extract in query_extract_list:
            # print(query_extract)
            inputs = get_query_inputs(query_extract.url)
            tables = map(get_query_detail_table, inputs)
            # print(list(tables))
            df = query_tables_to_dataframe(tables)
            print(df)

        return QueryList(query_extract_list)

    def to_frame(self):
        return pd.DataFrame.from_records(
            (qs for qs in self), columns=query_extract_fields)


def main():
    ql = QueryList.retrieve()
    df = ql.to_frame()
    print(df)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
