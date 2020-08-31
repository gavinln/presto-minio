import logging
from pathlib import Path
import json
import sqlite3

from datetime import datetime
from typing import List
from typing import Dict
from collections import namedtuple

import sqlite3

from urllib .parse import urlparse, urlunparse

import requests

import pandas as pd

from IPython import embed

SCRIPT_DIR = Path(__file__).parent.resolve()
log = logging.getLogger(__name__)


def retrieve_query_list(netloc=None):
    if netloc is None:
        # netloc = '10.0.0.2:8080'
        netloc = 'presto.liftoff.io'

    scheme = 'https'
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
    endTime = query['queryStats'].get('endTime', '')
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
    try:
        query_detail = retrieve_query(query_detail_url)
        inputs = get_query_detail_extract(query_detail)
    except requests.exceptions.HTTPError as err:
        if err.response.status_code == 410:  # ignore 410 Gone errors
            return []
        raise err
    return inputs


def query_tables_to_dataframe(tables, query_id):
    records = []
    for idx, table in enumerate(tables):
        if idx == 0:
            schema = table['schema']
            table_name = table['table']

        column_list = table['columns']
        for column in column_list:
            record = [query_id, schema, table_name]
            record.append(column)
            records.append(record)
    df = pd.DataFrame(records,
                      columns=['queryId', 'schema', 'table', 'column'])
    return df


class QueryList(list):

    def __init__(self, items, columns_df):
        self.columns_df = columns_df
        super().__init__(items)

    @classmethod
    def retrieve(cls):
        # netloc = '10.0.0.2:8080'
        netloc = 'presto.liftoff.io'

        def get_query_extract_url(query):
            return get_query_extract(query, netloc)

        query_list = retrieve_query_list(netloc)
        query_extract_list = list(map(get_query_extract_url, query_list))
        df_list = []
        for idx, query_extract in enumerate(query_extract_list):
            # print(query_extract)
            inputs = get_query_inputs(query_extract.url)
            tables = list(map(get_query_detail_table, inputs))
            if len(tables) > 0:
                df = query_tables_to_dataframe(tables, query_extract.queryId)
                df_list.append(df)

        return QueryList(query_extract_list, pd.concat(df_list))

    def to_frame(self):
        return pd.DataFrame.from_records(
            (qs for qs in self), columns=query_extract_fields)

    def columns_frame(self):
        return self.columns_df


def is_table_present(table_name, conn):
    ' returns True if table is present '
    table_names = conn.execute('''
        select name
        from sqlite_master
        where type = 'table' and name=?
    ''', (table_name,)).fetchall()
    return True if len(table_names) > 0 else False


def delete_query_extract_query_ids(query_ids, conn):
    '''
    deletes query_ids that exist in the query_extract table
    '''
    table_name = 'query_extract'
    if is_table_present(table_name, conn):
        queryId_str = ", ".join("'{}'".format(id) for id in query_ids)
        delete_sql = 'delete from {} where queryId in ({})'.format(
            table_name, queryId_str)
        delete_count = conn.execute(delete_sql).rowcount
        logging.info('delete {} rows from table {}'.format(
            delete_count, table_name))
    else:
        logging.info('table {} not present'.format(table_name))


def delete_query_column_extract_query_ids(query_ids, conn):
    '''
    deletes query_ids that exist in the query_columns_extract table
    '''
    table_name = 'query_column_extract'
    if is_table_present(table_name, conn):
        queryId_str = ", ".join("'{}'".format(id) for id in query_ids)
        delete_sql = 'delete from {} where queryId in ({})'.format(
            table_name, queryId_str)
        delete_count = conn.execute(delete_sql).rowcount
        logging.info('delete {} rows from table {}'.format(
            delete_count, table_name))
    else:
        logging.info('table {} not present'.format(table_name))


def main():
    QUERY_DB = (SCRIPT_DIR / '..' / 'query_db.sqlite3').resolve()

    ql = QueryList.retrieve()
    df = ql.to_frame()
    log.info('query_extract new data: {}'.format(df.shape))
    col_df = ql.columns_frame()
    log.info('query_column_extract new data: {}'.format(col_df.shape))
    conn = sqlite3.connect(QUERY_DB)

    delete_query_extract_query_ids(df.queryId, conn)
    df.to_sql(
        'query_extract', conn, if_exists='append', index=False)

    delete_query_column_extract_query_ids(df.queryId, conn)
    col_df.to_sql(
        'query_column_extract', conn, if_exists='append', index=False)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
