#!/usr/bin/env python3

'''
Display Presto and Hive metadata

DBM_PRESTO=localhost:8080
DBM_HIVE=10.0.0.2:10000
DBM_THEME=light
'''

import logging
import pathlib
import types
import datetime
import textwrap
import difflib
import itertools
import os
import sys
import warnings
from collections import namedtuple
from collections import defaultdict
from typing import NamedTuple

from operator import itemgetter

import pandas as pd

import yaml

import sqlalchemy as sa
import sqlalchemy.sql.schema as sa_schema

from rich.console import Console
from rich.syntax import Syntax

import fire

import pyarrow.parquet as pq
# from pyarrow import fs
import s3fs

from clickhouse_sqlalchemy import types as ch_types
from clickhouse_sqlalchemy import engines

from query_yes_no import query_yes_no

from presto_hive_lib import print_all
from presto_hive_lib import get_presto_records
from presto_hive_lib import get_presto_catalogs

from presto_hive_lib import get_hive_databases
from presto_hive_lib import check_hive_database
from presto_hive_lib import get_hive_records
from presto_hive_lib import get_hive_records_database_like_table
from presto_hive_lib import get_hive_records_database_dot_table
from presto_hive_lib import get_hive_table_extended

from presto_hive_lib import get_sa_table
from presto_hive_lib import get_sa_new_table
from presto_hive_lib import get_hive_sa_metadata
from presto_hive_lib import get_presto_sa_metadata
from presto_hive_lib import create_sa_table_from_table
from presto_hive_lib import print_sa_table

from presto_hive_lib import get_clickhouse_engine
from presto_hive_lib import get_clickhouse_sa_metadata

from presto_hive_lib import timed

# from IPython import embed

# Print all syntax highlighting styles
# from pygments.styles import get_all_styles
# print(list(get_all_styles()))

SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
log = logging.getLogger(__name__)


def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def get_hive_host_port():
    host = '10.0.0.2'
    # host = 'hive.liftoff.io'
    port = 10000
    return host, port


def get_presto_host_port():
    host = '10.0.0.2'
    host = 'presto.liftoff.io'
    port = 8080
    port = 8088
    return host, port


def get_clickhouse_host_port():
    host = '10.0.0.2'
    port = 8123
    return host, port


def get_s3_client_kwargs():
    client_kwargs = {'endpoint_url': 'http://10.0.0.2:9001'}
    client_kwargs = {}
    return client_kwargs


def get_presto_catalog_schema():
    return 'hive', 'temp'


def to_string_ljustify(df):
    ''' pandas dataframe to a string with left justified text
    '''
    col_formatters = []
    for col_name in df.columns:
        col = df[col_name]
        if col.dtype == 'object':
            col_len_max = col.apply(len).max()
            col_format = '{{:<{}s}}'.format(col_len_max)
            col_formatters.append(col_format.format)
        else:
            col_formatters.append(None)

    # left justify strings
    str_df = df.to_string(index=False, formatters=col_formatters)
    # remove trailing whitespaces
    return '\n'.join(line.rstrip() for line in str_df.split('\n'))

    # default printing is right justified
    # return df.to_string(index=False)


def test_to_string_ljustify():
    d = {'c1': ['1', '222', '3'], 'c2': [4, 25, 6], 'c3': ['111', '2', '3']}
    df = pd.DataFrame(d)

    out = '''

  c1  c2   c3
 1     4  111
 222  25  2
 3     6  3'''

    def compare_line_by_line(item1, item2):
        lines1 = item1.split('\n')
        lines2 = item2.split('\n')
        assert len(lines1) == len(lines2), 'Does not match'

        for l1, l2 in zip(lines1, lines2):
            assert l1.strip() == l2.strip(), f'mismatch ({l1}), ({l2})'

    out_str = out.replace('\n\n', '')
    out_df = to_string_ljustify(df)
    compare_line_by_line(out_str, out_df)


def print_tty_redir(df):
    ''' print data frame to a tty (partial) or redirected output (full)
    '''
    if df is not None:
        if sys.stdout.isatty():
            print(df)
            # print(df.to_string(index=False))
        else:
            with pd.option_context("display.max_rows", None,
                                   "display.max_columns", None):
                print(to_string_ljustify(df))


def get_formatted_value(formatter, value):
    ''' formatter is a function or a string value
    '''
    if isinstance(formatter, types.FunctionType):
        formatted_value = formatter(value)
    else:
        formatted_value = formatter.format(value)
    return formatted_value


def print_name_value_dict(name_value, formatter=None):
    if len(name_value) == 0:
        return
    max_name_len = max(len(name) for name in name_value)
    if max_name_len > 40:
        max_name_len = 40
    value_format = {} if formatter is None else formatter
    for raw_name, value in name_value.items():
        name = raw_name.strip()
        if name in value_format:
            value_formatter = formatter[name]
            formatted_value = get_formatted_value(value_formatter, value)
        else:
            formatted_value = value
        print('{name:{width}s} : {value}'.format(name=name,
                                                 width=max_name_len,
                                                 value=formatted_value))


def dataframe_to_dict(df):
    if not df.empty and df.columns.size >= 2:
        df_dict = dict(df.iloc[:, [0, 1]].to_records(index=False))
    else:
        df_dict = {}
    return df_dict


class HeaderDataFrame:
    ''' holds a header (text string) and data frame '''

    def __init__(self, header, dataframe):
        self.header = header
        self.dataframe = dataframe

    def __str__(self):
        out = ''
        out += self.header
        out += str(self.dataframe)
        return out


def _srs_match_index(srs, match_str):
    match_list = srs.index[srs == match_str].values
    if len(match_list) > 0:
        return match_list[0]
    return -1


def _remove_empty_rows(df):
    df2 = df.replace(to_replace=[None], value=[''])
    empty_rows = [
        idx for idx, row in df2.iterrows() if row.unique().shape[0] == 1
    ]
    return df2.drop(axis='index', index=empty_rows)


def _remove_empty_cols(df):
    df2 = df.replace(to_replace=[None], value=[''])
    empty_cols = [
        col for col, row in df2.iteritems() if row.unique().shape[0] == 1
    ]
    return df2.drop(axis='columns', columns=empty_cols)


def _get_header_row_df(df, headers):
    ' returns header row if exists '
    assert df.shape[0] > 0
    header = df.iloc[0, 0].strip()
    if header in headers:
        return header
    return None


def _remove_empty_rows_cols(df):
    return _remove_empty_rows(_remove_empty_cols(df))


def _print_clean_name_value_df(df, formatter=None):
    name_value = dataframe_to_dict(df)
    print_name_value_dict(name_value, formatter)


def _get_header_dataframes(df):
    col_names = df.col_name.str.strip()
    matches = [
        '# col_name', '# Partition Information',
        '# Detailed Table Information', 'Table Parameters:',
        '# Storage Information', 'Storage Desc Params:'
    ]
    match_idx_list = [_srs_match_index(col_names, match) for match in matches]
    valid_match_idx_list = [
        match_idx for match_idx in match_idx_list if match_idx >= 0
    ]
    all_match_idx_list = valid_match_idx_list + [col_names.size]

    header_dataframes = []

    for idx, (start, stop) in enumerate(pairwise(all_match_idx_list)):
        df_part = df.iloc[start:stop]
        header = _get_header_row_df(df_part, matches)
        if header:
            df_output = df_part.iloc[1:, ]
            header_dataframes.append(HeaderDataFrame(header, df_output))
        else:
            df_output = df_part
            header_dataframes.append(HeaderDataFrame(None, df_output))
    return header_dataframes


def _desc_formatted(host, port, table, database):
    sql = 'desc formatted'
    df = get_hive_records_database_dot_table(host, port, sql, database, table)
    return _get_header_dataframes(df)


def get_hive_table_location(host, port, table, database):
    ' returns the location of a hive table if it exists or None '
    header_dataframes = _desc_formatted(host, port, table, database)

    locations = []
    for hdf in header_dataframes:
        header, dataframe = hdf.header, hdf.dataframe
        if header == '# Detailed Table Information':
            df_clean = _remove_empty_rows_cols(dataframe)
            name_value = dataframe_to_dict(df_clean)
            locations = [
                value for name, value in name_value.items()
                if name.startswith('Location:')
            ]
    if len(locations) > 0:
        return locations[0]
    return None


def get_s3_parquet_file(s3_location, parq_file_path, client_kwargs=None):
    ' copy parquet file to local machine '
    file_system = s3fs.S3FileSystem(client_kwargs=client_kwargs)

    # new_files = file_system.ls(s3_location)
    # remove zero sized files if exist
    # if len(files) > 1:
    #     new_files = files[:-1]
    # else:
    #     new_files = files

    # dataset = pq.ParquetDataset(new_files, filesystem=file_system)
    dataset = pq.ParquetDataset(s3_location, filesystem=file_system)
    parq_table = dataset.read()
    pq.write_table(parq_table, parq_file_path)
    parq_path = pathlib.Path(parq_file_path)
    print('Saved to file {}, size {:,d} bytes'.format(
        parq_path.name,
        parq_path.stat().st_size))


class HiveDatabase:
    ''' display meta data from a hive database

        presto-hive.py hive show-databases
        presto-hive.py hive show-tables rtb
        presto-hive.py hive show-table rtb bids
        presto-hive.py hive show-columns rtb bids
        presto-hive.py hive show-columns proto2parquet bids
        presto-hive.py hive show-create-table proto2parquet bids
        # VERY SLOW: presto-hive.py hive show-table-extended proto2parquet bids
        presto-hive.py hive show-tblproperties proto2parquet bids
        presto-hive.py hive desc-formatted proto2parquet bids
    '''

    def show_databases(self):
        ' list all databases '
        host, port = get_hive_host_port()
        databases = get_hive_databases(host, port)
        print(databases)

    def show_tables(self, database):
        ''' list all tables
        '''
        host, port = get_hive_host_port()
        database_valid = check_hive_database(host, port, database)
        sql = 'show tables'
        tables = get_hive_records_database_like_table(host, port, sql,
                                                      database_valid)
        print_tty_redir(tables)

    def show_table(self, database, table):
        ''' show table
        '''
        host, port = get_hive_host_port()
        database_valid = check_hive_database(host, port, database)
        sql = 'show tables'
        tables = get_hive_records_database_like_table(host, port, sql,
                                                      database_valid, table)
        print(tables)

    def show_table_extended(self, database, table):
        ''' show more table info
        '''
        host, port = get_hive_host_port()
        database_valid = check_hive_database(host, port, database)
        df = get_hive_table_extended(host, port, table, database_valid)
        if df.size == 0:
            print('table {} not found'.format(table))
        else:
            name_value = {}
            for items in df.tab_name.str.split(':').values:
                if len(items[0]) > 0:
                    name_value[items[0]] = ':'.join(items[1:])
            print(pd.Series(name_value))

    def show_create_table(self, database, table):
        ''' show sql create table
        '''
        host, port = get_hive_host_port()
        database_valid = check_hive_database(host, port, database)
        sql = 'show create table'
        table = get_hive_records_database_dot_table(host, port, sql,
                                                    database_valid, table)
        lines = table.createtab_stmt.values
        console = Console()
        # print('\n'.join(lines))
        syntax = Syntax('\n'.join(lines), 'sql', theme='default')
        console.print(syntax)

    @staticmethod
    def _get_create_stmt(database, table):
        sql = 'show create table'
        host, port = get_hive_host_port()
        table = get_hive_records_database_dot_table(host, port, sql, database,
                                                    table)
        lines = table.createtab_stmt.values
        create_stmt = '\n'.join(lines)
        return create_stmt

    @staticmethod
    def _show_create_tables(database):
        def print_indent(text, indent_level: int, indent_str='\t'):
            assert indent_level > 0, 'indent_level should be greater than 0'
            print(textwrap.indent(text, indent_str * indent_level))

        sql = 'show tables'
        host, port = get_hive_host_port()
        tables = get_hive_records_database_like_table(host, port, sql,
                                                      database)

        table_count_str = 'Database {} has {} tables'.format(
            database, tables.shape[0])
        print_indent(table_count_str, 1)

        create_stmt_list = []
        for idx, table in enumerate(tables.tab_name.values):
            table_number_str = '{}: {} of {}'.format(table, idx,
                                                     tables.shape[0])
            print_indent(table_number_str, 2)

            create_stmt = HiveDatabase._get_create_stmt(database, table)
            print_indent(create_stmt, 3)

            create_stmt_list.append(create_stmt)

        # print('\n'.join(create_stmt_list))

    def show_create_tables(self):
        '''
            show create table statements for all tables in all databases

        database ben has 292 tables
        database temp has 111 tables
        '''
        host, port = get_hive_host_port()
        databases = get_hive_databases(host, port)
        for idx, database in enumerate(databases.database_name.values):
            print('\nDATABASE {}: {} of {} databases'.format(
                database, idx, databases.shape[0]))
            HiveDatabase._show_create_tables(database)

    def show_partitions(self, database, table):
        ''' show partitions for a hive table
        '''
        host, port = get_hive_host_port()
        database_valid = check_hive_database(host, port, database)
        sql = 'show partitions'
        partitions = get_hive_records_database_dot_table(
            host, port, sql, database_valid, table)
        print(partitions)

    def show_tblproperties(self, database, table):
        ''' display table properties
        '''
        host, port = get_hive_host_port()
        database_valid = check_hive_database(host, port, database)
        sql = 'show tblproperties'
        table = get_hive_records_database_dot_table(host, port, sql,
                                                    database_valid, table)
        name_value = dataframe_to_dict(table)
        formatter = {
            'transient_lastDdlTime':
            lambda ts: datetime.datetime.fromtimestamp(int(ts))
        }
        print_name_value_dict(name_value, formatter)

    def show_columns(self, database, table):
        ''' display table columns
        '''
        host, port = get_hive_host_port()
        database_valid = check_hive_database(host, port, database)
        sql = 'show columns in '
        tables = get_hive_records_database_dot_table(host, port, sql,
                                                     database_valid, table)
        print_tty_redir(tables)

    def show_functions(self):
        ''' list all functions
        '''
        sql = 'show functions'
        host, port = get_hive_host_port()
        functions = get_hive_records(host, port, sql)
        for idx, function in enumerate(functions.tab_name.values):
            print(idx, function)

    def desc(self, database, table):
        ''' shows list of columns including partition
        '''
        host, port = get_hive_host_port()
        database_valid = check_hive_database(host, port, database)
        sql = 'desc'
        info = get_hive_records_database_dot_table(host, port, sql,
                                                   database_valid, table)
        print_tty_redir(info)

    def desc_formatted(self, database, table):
        ''' show table metadata in tablular format
        '''
        host, port = get_hive_host_port()
        database_valid = check_hive_database(host, port, database)

        header_dataframes = _desc_formatted(host, port, table, database_valid)

        formatter = {
            'numRows': lambda x: '{:,d}'.format(int(x)),
            'rawDataSize': lambda x: '{:,d}'.format(int(x)),
            'totalSize': lambda x: '{:,d}'.format(int(x))
        }
        for idx, hdf in enumerate(header_dataframes):
            # print(idx); print(hdf)
            header, dataframe = hdf.header, hdf.dataframe
            print()
            print(header)
            df_clean = _remove_empty_rows_cols(dataframe)
            _print_clean_name_value_df(df_clean, formatter)

    def show_table_location(self, database, table):
        ''' show table file location if exist
        '''
        host, port = get_hive_host_port()
        database_valid = check_hive_database(host, port, database)

        table_location = get_hive_table_location(host, port, table,
                                                 database_valid)
        if table_location:
            print(table_location)
        else:
            print('Cannot find file for table {}'.format(table))

    def get_table_s3(self, database, table):
        ''' get hive table stored as a parquet file
        '''
        host, port = get_hive_host_port()
        database_valid = check_hive_database(host, port, database)
        client_kwargs = get_s3_client_kwargs()
        table_location = get_hive_table_location(host, port, table,
                                                 database_valid)
        print('hive table {}, location {}'.format(table, table_location))
        if table_location:
            if table_location.startswith('s3'):
                with timed():
                    parq_file_path = '{}.parq'.format(table)
                    get_s3_parquet_file(table_location, parq_file_path,
                                        client_kwargs)
            else:
                sys.exit('Can only read s3 files. Unknown location: {}'.format(
                    table_location))
        else:
            print('Cannot find file for table {}'.format(table))

    def export_metadata(self, database, table):
        ''' export table metadata
        '''
        host, port = get_hive_host_port()
        database_valid = check_hive_database(host, port, database)
        metadata = get_hive_sa_metadata(host, port, database_valid)

        with warnings.catch_warnings():
            # ignore sqlalchemy warnings SAWarning: Did not recognize type
            warnings.simplefilter('ignore')
            metadata.reflect(only=[table])

        col_info_list = []
        table_info = {'table': table, 'columns': col_info_list}

        for table_name in metadata.tables:
            tbl = metadata.tables[table_name]
            for idx, column in enumerate(tbl.columns):
                if not isinstance(column.type, sa.types.NullType):
                    col_type = column.type
                else:
                    col_type = 'UNKNOWN'
                col_info = TableColumnInfo(str(column.name), str(col_type),
                                           column.nullable)
                col_info_list.append(col_info_as_dict(col_info))

        tbl_yaml = yaml.dump(table_info, sort_keys=False)
        print(tbl_yaml)


def display_df_all(df):
    max_rows = 1000
    max_cols = 1000
    with pd.option_context("display.max_rows", max_rows, "display.max_columns",
                           max_cols):
        print(df)


def check_presto_catalog(catalog):
    ' returns valid catalog or raises error if not valid '
    host, port = get_presto_host_port()
    catalogs = get_presto_catalogs(host, port)
    names = catalogs.Catalog.values
    # TODO: should this be case-insensitive?
    if catalog in names:
        return catalog
    close_matches = difflib.get_close_matches(catalog, names)
    if len(close_matches) > 0:
        message = 'Did you mean {}?'.format(close_matches[0])
        response = query_yes_no(message)
        if response:
            return close_matches[0]
    raise ValueError('Invalid catalog name {}'.format(catalog))


def check_presto_env():
    presto_env = 'DBM_PRESTO'
    if presto_env not in os.environ:
        raise ValueError('missing environment variable {}'.format(presto_env))
    return os.environ[presto_env]


def check_hive_env():
    hive_env = 'DBM_HIVE'
    if hive_env not in os.environ:
        raise ValueError('missing environment variable {}'.format(hive_env))
    return os.environ[hive_env]


TableColumnInfo = namedtuple('TableColumnInfo', 'name col_type nullable')


def col_info_as_dict(col_info):
    return {
        name.replace('col_type', 'type'): val
        for name, val in col_info._asdict().items()
    }


def is_basic_type(db_type: str) -> bool:
    ' returns True if db_type is basic type '
    return '(' not in db_type


def get_presto_boolean_info_sql(catalog, schema, table, column):
    sql = '''

    with param as (
        select
        '2021-07-01T00:00:00.000Z' as start_dt,
        '2021-07-02T00:00:00.000Z' as end_dt
    )
    select
        count_if({column} = true) as true_count,
        count_if({column} is null) as null_count,
        count(*) as total_count,
        '{column}' as col_name
    from param p, {catalog}.{schema}.{table}
    where dt >= p.start_dt and
        dt < p.end_dt

    '''.format(catalog=catalog, schema=schema, table=table, column=column)
    return sql


def get_presto_bigint_info_sql(catalog, schema, table, column):
    sql = '''

    with param as (
        select
        '2021-07-01T00:00:00.000Z' as start_dt,
        '2021-07-02T00:00:00.000Z' as end_dt
    )
    select
        min({column}) as min_val,
        approx_percentile({column}, 0.5) as median_val,
        avg({column}) as avg_val,
        max({column}) as max_val,
        approx_distinct({column}) as approx_dist,
        count_if({column} is null) as null_count,
        count(*) as total_count,
        '{column}' as col_name
    from param p, {catalog}.{schema}.{table}
    where dt >= p.start_dt and
        dt < p.end_dt

    '''.format(catalog=catalog, schema=schema, table=table, column=column)
    return sql


def get_presto_varchar_info_sql(catalog, schema, table, column):
    sql = '''

    with param as (
        select
        '2021-07-01T00:00:00.000Z' as start_dt,
        '2021-07-02T00:00:00.000Z' as end_dt
    )
    select
        min(length({column})) as min_len,
        approx_percentile(length({column}), 0.5) as median_len,
        avg(length({column})) as avg_len,
        max(length({column})) as max_len,
        approx_distinct({column}) as approx_dist,
        count_if({column} is null) as null_count,
        count(*) as total_count,
        '{column}' as col_name
    from param p, {catalog}.{schema}.{table}
    where dt >= p.start_dt and
        dt < p.end_dt

    '''.format(catalog=catalog, schema=schema, table=table, column=column)
    return sql


def get_presto_varchar_distinct_sql(catalog, schema, table, column):
    sql = '''

    with param as (
        select
        '2021-07-01T00:00:00.000Z' as start_dt,
        '2021-07-02T00:00:00.000Z' as end_dt
    )
    select
        distinct {column}
    from param p, {catalog}.{schema}.{table}
    where dt >= p.start_dt and
        dt < p.end_dt

    '''.format(catalog=catalog, schema=schema, table=table, column=column)
    return sql


def print_presto_column_distinct_values(
        catalog, schema, table, column, column_type):
    ' displays unique values for a Presto column '
    if not is_basic_type(column_type):
        assert False, 'Column {} is not a basic column type'.format(
            column_type)

    sql = None
    if column_type in ['varchar']:
        sql = get_presto_varchar_distinct_sql(catalog, schema, table, column)
    elif column_type in ('bigint', 'integer', 'double', 'boolean'):
        print('Not printing column {} of type {}'.format(
            column, column_type))
    else:
        msg = 'Cannot print column info for column {} type {}'.format(
            column, column_type)
        assert False, msg

    if sql:
        host, port = get_presto_host_port()
        column_info = get_presto_records(host, port, sql)
        print_tty_redir(column_info)


def print_presto_column_info(catalog, schema, table, column, column_type):
    ' displays info about a Presto column '
    if not is_basic_type(column_type):
        assert False, 'Column {} is not a basic column type'.format(
            column_type)

    sql = None
    if column_type in ['bigint', 'integer']:
        sql = get_presto_bigint_info_sql(catalog, schema, table, column)
    elif column_type == 'varchar':
        sql = get_presto_varchar_info_sql(catalog, schema, table, column)
    elif column_type == 'boolean':
        # sql = get_presto_boolean_info_sql(catalog, schema, table, column)
        pass
    elif column_type == 'double':
        print('Not printing column {} of type {}'.format(
            column, column_type))
    else:
        msg = 'Cannot print column info for column {} type {}'.format(
            column, column_type)
        assert False, msg

    if sql:
        host, port = get_presto_host_port()
        column_info = get_presto_records(host, port, sql)
        select_column_info = column_info[['col_name', 'approx_dist']]
        print_tty_redir(select_column_info)


def is_analytics_hourly_group_column(col_name):
    if col_name in [
    ]:
        return True
    return False


def _get_presto_columns(catalog, schema, table):
    sql = "show columns from {}.{}.{}".format(catalog, schema, table)
    host, port = get_presto_host_port()
    columns = get_presto_records(host, port, sql)
    return columns


class PrestoDatabase:
    ''' display meta data from a presto database

    presto-hive.py presto show-catalogs
    presto-hive.py presto show-schemas minio
    presto-hive.py presto show-tables default minio
    presto-hive.py presto show-table customer_text default minio
    presto-hive.py presto show-columns customer_text default minio
    presto-hive.py presto show-create-table customer_text default minio
    presto-hive.py presto show-stats customer_text default minio

    show create function
    show create view
    show functions
    show grants
    show role grants
    show roles
    show session

    describe database.table;
    '''

    def prepare_describe(self, schema, catalog):
        table = 'million_rows'
        sql = '''
            prepare my_select
            from
            select * from {}.{}.{};
            describe input myselect;
        '''.format(catalog, schema, table)
        host, port = get_presto_host_port()
        get_presto_records(host, port, sql)
        # sql = '''
        #     describe input myselect
        # '''.format(catalog, schema, table)
        # results = get_presto_records(host, port, sql)
        # print_tty_redir(results)

    def show_catalogs(self):
        '''
        '''
        host, port = get_presto_host_port()
        catalogs = get_presto_catalogs(host, port)
        print(catalogs)

    def show_schemas(self, catalog):
        ''' show schemas
        '''
        valid_catalog = check_presto_catalog(catalog)
        sql = 'show schemas from {}'.format(valid_catalog)
        host, port = get_presto_host_port()
        catalogs = get_presto_records(host, port, sql)
        print(catalogs)

    def show_tables(self, catalog, schema):
        ''' show tables
        '''
        valid_catalog = check_presto_catalog(catalog)
        sql = 'show tables from {}.{}'.format(catalog, schema)
        host, port = get_presto_host_port()
        tables = get_presto_records(host, port, sql)
        print_tty_redir(tables)

    def show_table(self, catalog, schema, table):
        '''
        '''
        sql = "show tables from {}.{} like '{}'".format(catalog, schema, table)
        host, port = get_presto_host_port()
        tables = get_presto_records(host, port, sql)
        print(tables)

    def show_columns(self, catalog, schema, table):
        '''
        '''
        columns = _get_presto_columns(catalog, schema, table)
        print_tty_redir(columns)

    def show_column_distinct(self, catalog, schema, table, column):
        '''
        '''
        columns = _get_presto_columns(catalog, schema, table)
        if column not in columns.Column.values:
            print('Column {} does not exist in {} {} {}'.format(
                column, catalog, schema, table))
        else:
            column_type = columns[columns.Column == column]['Type'].values
            if column_type.size == 1:
                print_presto_column_distinct_values(
                    catalog, schema, table, column, column_type)
            else:
                assert False, 'Cannot get column_type for column {}'.format(
                    column)

    def show_column_info(self, catalog, schema, table, column):
        '''
        '''
        columns = _get_presto_columns(catalog, schema, table)
        if column not in columns.Column.values:
            print('Column {} does not exist in {} {} {}'.format(
                column, catalog, schema, table))
        else:
            column_type = columns[columns.Column == column]['Type'].values
            if column_type.size == 1:
                print_presto_column_info(
                    catalog, schema, table, column, column_type)
            else:
                assert False, 'Cannot get column_type for column {}'.format(
                    column)

    def show_column_infos(self, catalog, schema, table):
        '''
        '''
        columns = _get_presto_columns(catalog, schema, table)
        for column in columns.Column.values:
            if is_analytics_hourly_group_column(column):
                column_type = columns[columns.Column == column]['Type'].values
                if column_type.size == 1:
                    # print(column, column_type)
                    print_presto_column_info(
                        catalog, schema, table, column, column_type)
                else:
                    msg = 'Cannot get column_type for column {}'.format(
                        column)
                    assert False, msg

    def show_create_table(self, catalog, schema, table):
        '''
            show create table
        '''
        sql = 'show create table {}.{}.{}'.format(catalog, schema, table)
        host, port = get_presto_host_port()
        df = get_presto_records(host, port, sql)
        create_stmt = df[['Create Table']].values[0][0]
        print(create_stmt)

    def show_create_view(self, catalog, schema, table):
        ''' show create view
        '''
        sql = 'show create view {}.{}.{}'.format(catalog, schema, table)
        host, port = get_presto_host_port()
        df = get_presto_records(host, port, sql)
        print(df)
        create_stmt = df[['Create View']].values[0][0]
        print(create_stmt)

    def show_stats(self, catalog, schema, table):
        ''' show table statistics
        '''
        sql = 'show stats for {}.{}.{}'.format(catalog, schema, table)
        host, port = get_presto_host_port()
        stats = get_presto_records(host, port, sql)
        # print(stats)
        print_tty_redir(stats)

    def export_metadata(self, catalog, schema, table):
        ''' export table metadata
        '''
        host, port = get_presto_host_port()
        metadata = get_presto_sa_metadata(host, port, catalog, schema)

        with warnings.catch_warnings():
            # ignore sqlalchemy warnings SAWarning: Did not recognize type
            warnings.simplefilter('ignore')
            metadata.reflect(only=[table])

        col_info_list = []
        table_info = {'table': table, 'columns': col_info_list}

        for table_name in metadata.tables:
            tbl = metadata.tables[table_name]
            for idx, column in enumerate(tbl.columns):
                if not isinstance(column.type, sa.types.NullType):
                    col_type = column.type
                else:
                    col_type = 'UNKNOWN'
                col_info = TableColumnInfo(str(column.name), str(col_type),
                                           column.nullable)
                col_info_list.append(col_info_as_dict(col_info))

        tbl_yaml = yaml.dump(table_info, sort_keys=False)
        print(tbl_yaml)

    def _desc_table(self, table, schema):
        '''describe table from hive
        '''
        assert False, 'May not be used'

        sql = 'describe hive.{}.{}'.format(schema, table)
        host, port = get_presto_host_port()
        df = get_presto_records(host, port, sql)
        print(df)

    def _desc_tables(self):
        '''describe all tables from hive.flat_rtb
        '''
        assert False, 'May not be used'

        sql = 'show tables from hive.flat_rtb'
        host, port = get_presto_host_port()
        tables = get_presto_records(host, port, sql)
        if tables is None:
            return

        df_list = []
        column_count = 0
        for idx, table in enumerate(tables.Table.values):
            print(table)
            sql = 'describe hive.flat_rtb.{}'.format(table)
            host, port = get_presto_host_port()
            df = get_presto_records(host, port, sql)
            column_count += df.shape[0]
            df_list.append(df)
            if idx > 100:
                break

        message = 'Showing details for {} tables, {} columns'.format(
            len(df_list), column_count)
        print(message)
        all_tables = pd.concat(df_list, axis='index')
        print('max comment length {}'.format(
            all_tables.Comment.str.len().max()))


def check_copy_presto_table(host, port, catalog, schema, old_table, new_table):
    metadata = get_presto_sa_metadata(host, port, catalog, schema)
    table_names = metadata.bind.table_names()
    if old_table not in table_names:
        sys.exit('Old table {} does not exist'.format(old_table))

    if new_table in table_names:
        sys.exit('New table {} should not exist'.format(new_table))

    return metadata


class OpDatabase:
    ''' miscellaneous database operations
    '''

    def copy_presto_table(self, old_table, new_table):
        """ copy a Presto table into a new table with the same types

            old_table should exist
            new_table should not exist
        """
        host, port = get_presto_host_port()
        catalog, schema = get_presto_catalog_schema()
        metadata = check_copy_presto_table(host, port, catalog, schema,
                                           old_table, new_table)

        old_sa_table = get_sa_table(metadata, old_table)
        print_sa_table(old_sa_table)

        new_sa_table = get_sa_new_table(metadata, old_table, new_table)
        create_sa_table_from_table(new_sa_table, old_sa_table)
        print_sa_table(new_sa_table)

    def copy_presto_table_compact(self, old_table, new_table):
        """ copy a Presto table into a new table with compact types

            old_table should exist
            new_table should not exist
        """
        host, port = get_presto_host_port()
        catalog, schema = get_presto_catalog_schema()
        metadata = check_copy_presto_table(host, port, catalog, schema,
                                           old_table, new_table)

        old_sa_table = get_sa_table(metadata, old_table)
        print_sa_table(old_sa_table)

        new_sa_table = get_sa_new_table(metadata,
                                        old_table,
                                        new_table,
                                        smallest_int_types=True)
        create_sa_table_from_table(new_sa_table, old_sa_table)
        print_sa_table(new_sa_table)


def print_parq_file_info(parq_file):
    parq_file = pq.ParquetFile(parq_file)
    print(textwrap.indent(str(parq_file.metadata), prefix='\t'))
    print(textwrap.indent(str(parq_file.schema), prefix='\t'))


class ParquetAction:
    ''' parquet actions
    '''

    def desc(self, file_name):
        ' describe the pieces and schema of a parquet file '
        if file_name.startswith('s3://'):
            # client_kwargs = {'endpoint_url': 'http://10.0.0.2:9000'}
            client_kwargs = get_s3_client_kwargs()
            file_system = s3fs.S3FileSystem(client_kwargs=client_kwargs)
            files = file_system.ls(file_name)
            print(file_name)
            print('There are {} files'.format(len(files)))
        else:
            path = pathlib.Path(file_name)
            if path.exists():
                print('Parquet file {}'.format(file_name))
                dataset = pq.ParquetDataset(file_name)
                print('\t{} pieces'.format(len(dataset.pieces)))
                print(textwrap.indent(str(dataset.schema), prefix='\t'))


def execute_clickhouse_sql(host, port, database, sql):
    engine = get_clickhouse_engine(host, port, database)
    with engine.connect() as conn:
        conn.execute(sql)


class TblCol:
    def __init__(self, table, column):
        self.table = table
        self.column = column

    def __str__(self):
        return self.table + '.' + self.column

    def __repr__(self):
        return 'TblCol:' + self.table + '.' + self.column

    def __eq__(self, other):
        # if last two parts match then equal even if tables are not the same
        col1 = self.column.split('__')
        col2 = other.column.split('__')
        if len(col1) >= 2 and len(col2) >= 2:
            return col1[-1] == col2[-1] and col1[-2] == col2[-2]
        elif self.table == other.table and self.column == other.column:
            return True
        return False

    def __hash__(self):
        col = self.column.split('__')
        return hash(str(col[-1]))


def get_clickhouse_records(host, port, database, sql):
    ' runs a clickhouse sql statement and returns dataframe result '
    engine = get_clickhouse_engine(host, port, database)
    try:
        df = pd.read_sql(sql, engine)
        return df
    except Exception:
        Console().print_exception(theme='solarized-light')
    return None


class ClickhouseDatabase:
    ''' clickhouse operations
    '''

    def show_databases(self):
        ' show a list of all databases '
        host, port = get_clickhouse_host_port()
        sql = 'show databases'
        database = 'system'
        df = get_clickhouse_records(host, port, database, sql)
        print(df)

    def show_tables(self, database):
        ' show a list of all databases '
        host, port = get_clickhouse_host_port()
        sql = 'show tables from {}'.format(database)
        df = get_clickhouse_records(host, port, database, sql)
        print(df)

    def show_version(self):
        ' DOES NOT WORK: show clickhouse server version '
        host, port = get_clickhouse_host_port()
        sql = 'select version() as ver'
        database = 'system'
        df = get_clickhouse_records(host, port, database, sql)
        print(df)

    def temp_metadata(self, table, database):
        ' display table metadata '
        host, port = get_clickhouse_host_port()
        metadata = get_clickhouse_sa_metadata(host, port, database)
        metadata.reflect(only=[table])
        for table_name in metadata.tables:
            print(table_name)
            tbl = metadata.tables[table_name]
            for column in tbl.columns:
                print('\t', column.name, column.nullable)
                # print('\t', column.get_children)
                if hasattr(column.type, 'nested_type'):
                    print('\t\t', column.type.__class__)
                    print('\t\t', column.type.nested_type)
                else:
                    print('\t\t', column.type.__class__)

    def temp_create_table_from_metadata(self, metadata_file, database):
        ' create a table '
        meta_path = pathlib.Path(metadata_file)
        if not meta_path.exists() or not meta_path.is_file():
            sys.exit('{} is not a valid file'.format(meta_path))

        metadata = yaml.load(meta_path.open(), Loader=yaml.SafeLoader)

        def get_clickhouse_type(sa_type):
            clickhouse_types = {
                'BOOLEAN': ch_types.UInt8,
                'TINYINT': ch_types.Int8,
                'SMALLINT': ch_types.Int16,
                'INTEGER': ch_types.Int32,
                'BIGINT': ch_types.Int64,
                'FLOAT': ch_types.Float64,
                'VARCHAR': ch_types.String
            }
            return clickhouse_types.get(sa_type, None)

        def get_clickhouse_sa_columns(metadata):
            columns = metadata['columns']
            ch_columns = []
            for idx, col in enumerate(columns):
                name = col['name']
                col_type = col['type'],
                ch_type = get_clickhouse_type(col['type'])
                nullable = col['nullable']
                # make the first column non-nullable (needed for Clickhouse)
                if idx == 0:
                    tbl_col = sa_schema.Column(name, ch_type)
                else:
                    tbl_col = sa_schema.Column(name,
                                               ch_types.Nullable(ch_type))
                ch_columns.append(tbl_col)
            return ch_columns

        host, port = get_clickhouse_host_port()
        engine = get_clickhouse_engine(host, port, database)
        ch_columns = get_clickhouse_sa_columns(metadata)

        first_col_name = ch_columns[0].name

        meta = sa.sql.schema.MetaData()
        # temp = sa_schema.Table(metadata['table'], meta)
        new_table = sa_schema.Table(metadata['table'], meta)
        for idx, col in enumerate(ch_columns):
            new_table.append_column(col)
        new_table.append_column(engines.MergeTree(order_by=(first_col_name, )))
        new_table.create(engine)

        sql = '''
        create table temp4
        (
            `id` Int64,
            `grp_code` Int64
        )
        ENGINE = MergeTree()
        ORDER BY id
        '''
        host, port = get_clickhouse_host_port()
        database = 'default'
        execute_clickhouse_sql(host, port, database, sql)

    def temp_create_table2(self, database):
        ' create a table using sqlalchmey '
        host, port = get_clickhouse_host_port()
        engine = get_clickhouse_engine(host, port, database)

        meta = sa.sql.schema.MetaData()
        temp = sa_schema.Table('temp5', meta)
        temp.append_column(sa_schema.Column('id', ch_types.Int64))
        temp.append_column(
            sa_schema.Column('grp_code', ch_types.Nullable(ch_types.Int64)))
        temp.append_column(engines.MergeTree(order_by=('id', )))
        temp.create(engine)


class Databases:
    def __init__(self):
        self.hive = HiveDatabase()
        self.presto = PrestoDatabase()
        self.op = OpDatabase()
        self.parquet = ParquetAction()
        self.clickhouse = ClickhouseDatabase()


if __name__ == '__main__':
    # https://github.com/willmcgugan/rich
    logging.basicConfig(level=logging.WARN)
    fire.Fire(Databases)
