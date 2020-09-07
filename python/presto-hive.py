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

import pandas as pd

from rich.console import Console
from rich.syntax import Syntax

from IPython import embed

import fire

import sqlalchemy as sa
from sqlalchemy import MetaData
from sqlalchemy.schema import Table

from query_yes_no import query_yes_no
from presto_hive_lib import get_presto_records
from presto_hive_lib import get_presto_catalogs

from presto_hive_lib import get_hive_records
from presto_hive_lib import get_hive_records_database_like_table
from presto_hive_lib import get_hive_records_database_dot_table
from presto_hive_lib import get_hive_table_extended

from presto_hive_lib import timed


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
    # host = 'presto.liftoff.io'
    port = 8080
    return host, port


def print_all(df):
    with pd.option_context(
            "display.max_rows", None,
            "display.max_columns", None):
        print(df)


# need a function get_hive_tbl_database_dot_table
# for show tblproperties


def get_hive_databases(host, port):
    sql = 'show databases'
    return get_hive_records(host, port, sql)


def check_hive_database(database):
    ' returns valid database or raises error if not valid '
    host, port = get_hive_host_port()
    databases = get_hive_databases(host, port)
    names = databases.database_name.values
    # TODO: should this be case-insensitive?
    if database in names:
        return database
    close_matches = difflib.get_close_matches(database, names)
    if len(close_matches) > 0:
        message = 'Did you mean {}?'.format(close_matches[0])
        response = query_yes_no(message)
        if response:
            return close_matches[0]
    raise ValueError('Invalid database name {}'.format(database))


def get_formatted_value(formatter, value):
    ''' formatter is a function or a string value
    '''
    if isinstance(formatter, types.FunctionType):
        formatted_value = formatter(value)
    else:
        formatted_value = formatter.format(value)
    return formatted_value


def print_name_value_dict(name_value, formatter=None):
    max_name_len = max(len(name) for name in name_value)
    if max_name_len > 40:
        max_name_len = 40
    value_format = {} if formatter is None else formatter
    for name, value in name_value.items():
        if name in value_format:
            value_formatter = formatter[name]
            formatted_value = get_formatted_value(value_formatter, value)
        else:
            formatted_value = value
        print('{name:{width}s}: {value}'.format(
            name=name, width=max_name_len, value=formatted_value))


def dataframe_to_dict(df):
    return dict(df.iloc[:, [0, 1]].to_records(index=False))


class HiveDatabase:
    ''' display meta data from a hive database

        presto-hive.py hive show-databases
        presto-hive.py hive show-tables --database default
        presto-hive.py hive show-table customer_text --database default
        presto-hive.py hive show-columns customer_text --database default
        presto-hive.py hive show-create-table customer_text default
        presto-hive.py hive show-table-extended customer_text default
        presto-hive.py hive show-tblproperties customer_text --database default

        presto-hive.py hive desc-formatted customer_text --database default
    '''

    def show_databases(self):
        ' list all databases '
        databases = get_hive_databases()
        print(databases)

    def show_tables(self, database=None):
        ''' list all tables
        '''
        if database is not None:
            database = check_hive_database(database)
        sql = 'show tables'
        host, port = get_hive_host_port()
        tables = get_hive_records_database_like_table(
            host, port, sql, database)
        print(tables)

    def show_table(self, table, database=None):
        ''' show table
        '''
        if database is not None:
            database = check_hive_database(database)
        sql = 'show tables'
        host, port = get_hive_host_port()
        tables = get_hive_records_database_like_table(
            host, port, sql, database, table)
        print(tables)

    def show_table_extended(self, table, database=None):
        '''
            validate table
        '''
        if database is not None:
            database = check_hive_database(database)
        host, port = get_hive_host_port()
        df = get_hive_table_extended(host, port, table, database)
        if df.size == 0:
            print('table {} not found'.format(table))
        else:
            name_value = {}
            for items in df.tab_name.str.split(':').values:
                if len(items[0]) > 0:
                    name_value[items[0]] = ':'.join(items[1:])
            print(pd.Series(name_value))

    def show_create_table(self, table, database=None):
        '''
            validate table
        '''
        if database is not None:
            database = check_hive_database(database)
        sql = 'show create table'
        host, port = get_hive_host_port()
        table = get_hive_records_database_dot_table(
            host, port, sql, database, table)
        lines = table.createtab_stmt.values
        console = Console()
        # print('\n'.join(lines))
        syntax = Syntax('\n'.join(lines), 'sql', theme='default')
        console.print(syntax)

    @staticmethod
    def _get_create_stmt(database, table):
        sql = 'show create table'
        host, port = get_hive_host_port()
        table = get_hive_records_database_dot_table(
            host, port, sql, database, table)
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
        tables = get_hive_records_database_like_table(
            host, port, sql, database)

        table_count_str = 'Database {} has {} tables'.format(
            database, tables.shape[0])
        print_indent(table_count_str, 1)

        create_stmt_list = []
        for idx, table in enumerate(tables.tab_name.values):
            table_number_str = '{}: {} of {}'.format(
                table, idx, tables.shape[0])
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
        databases = get_hive_databases()
        for idx, database in enumerate(databases.database_name.values):
            print('\nDATABASE {}: {} of {} databases'.format(
                database, idx, databases.shape[0]))
            HiveDatabase._show_create_tables(database)

    def show_partitions(self, table, database=None):
        '''
            validate table
        '''
        if database is not None:
            database = check_hive_database(database)
        sql = 'show partitions'
        partitions = get_hive_records_database_dot_table(sql, database, table)
        print(partitions)

    def show_tblproperties(self, table, database=None):
        '''
            validate table
        '''
        if database is not None:
            database = check_hive_database(database)
        sql = 'show tblproperties'
        table = get_hive_records_database_dot_table(sql, database, table)
        name_value = dataframe_to_dict(table)
        formatter = {
            'transient_lastDdlTime':
                lambda ts: datetime.datetime.fromtimestamp(int(ts))
        }
        print_name_value_dict(name_value, formatter)

    def show_columns(self, table, database=None):
        '''
            validate table
        '''
        if database is not None:
            database = check_hive_database(database)
        sql = 'show columns in '
        tables = get_hive_records_database_dot_table(sql, database, table)
        print(tables)

    def show_functions(self):
        ''' list all functions
        '''
        sql = 'show functions'
        host, port = get_hive_host_port()
        functions = get_hive_records(host, port, sql)
        for idx, function in enumerate(functions.tab_name.values):
            print(idx, function)

    def desc(self, table, database=None):
        '''
        '''
        if database is not None:
            database = check_hive_database(database)
        sql = 'desc'
        info = get_hive_records_database_dot_table(sql, database, table)
        print_all(info)

    def desc_formatted(self, table, database=None):
        '''
        '''
        def srs_match_index(srs, match_str):
            match_list = srs.index[srs == match_str].values
            if len(match_list) > 0:
                return match_list[0]
            return -1

        def remove_empty_rows(df):
            df2 = df.replace(to_replace=[None], value=[''])
            empty_rows = [
                idx for idx, row in df2.iterrows(
                    ) if row.unique().shape[0] == 1]
            return df2.drop(axis='index', index=empty_rows)

        def remove_empty_cols(df):
            df2 = df.replace(to_replace=[None], value=[''])
            empty_cols = [
                col for col, row in df2.iteritems(
                    ) if row.unique().shape[0] == 1]
            return df2.drop(axis='columns', columns=empty_cols)

        if database is not None:
            database = check_hive_database(database)
        sql = 'desc formatted'
        info = get_hive_records_database_dot_table(sql, database, table)
        col_names = info.col_name.str.strip()

        matches = [
            '# Partition Information', '# Detailed Table Information',
            'Table Parameters:', '# Storage Information',
            'Storage Desc Params:'
        ]
        match_idx_list = [
            srs_match_index(col_names, match) for match in matches]
        valid_match_idx_list = [
            match_idx for match_idx in match_idx_list if match_idx > 0]
        all_match_idx_list = [0] + valid_match_idx_list + [col_names.size]
        for idx, (start, stop) in enumerate(pairwise(all_match_idx_list)):
            print('Part {}: [{}, {}]'.format(idx, start, stop))
            # TODO: replace None by '' and remove all rows/cols with only ''
            df = info.iloc[start:stop]
            print_all(remove_empty_rows(remove_empty_cols(df)))


def display_df_all(df):
    max_rows = 1000
    max_cols = 1000
    with pd.option_context(
            "display.max_rows", max_rows, "display.max_columns", max_cols):
        print(df)


def check_presto_catalogs(catalog):
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

    def show_catalogs(self):
        '''
        '''
        host, port = get_presto_host_port()
        catalogs = get_presto_catalogs(host, port)
        print(catalogs)

    def show_schemas(self, catalog):
        '''
        '''
        valid_catalog = check_presto_catalogs(catalog)
        sql = 'show schemas from {}'.format(valid_catalog)
        host, port = get_presto_host_port()
        catalogs = get_presto_records(host, port, sql)
        print(catalogs)

    def show_tables(self, schema, catalog):
        '''
        '''
        sql = 'show tables from {}.{}'.format(catalog, schema)
        host, port = get_presto_host_port()
        catalogs = get_presto_records(host, port, sql)
        print(catalogs)

    def show_table(self, table, schema, catalog):
        '''
        '''
        sql = "show tables from {}.{} like '{}'".format(
            catalog, schema, table)
        host, port = get_presto_host_port()
        tables = get_presto_records(host, port, sql)
        print(tables)

    def show_columns(self, table, schema, catalog):
        '''
        '''
        sql = "show columns from {}.{}.{}".format(
            catalog, schema, table)
        host, port = get_presto_host_port()
        columns = get_presto_records(host, port, sql)
        print(columns)

    def show_create_table(self, table, schema, catalog):
        '''
            show create table
        '''
        sql = 'show create table {}.{}.{}'.format(catalog, schema, table)
        host, port = get_presto_host_port()
        df = get_presto_records(host, port, sql)
        create_stmt = df[['Create Table']].values[0][0]
        print(create_stmt)

    def show_create_view(self, table, schema, catalog):
        '''
            show create view
        '''
        sql = 'show create view {}.{}.{}'.format(catalog, schema, table)
        host, port = get_presto_host_port()
        df = get_presto_records(host, port, sql)
        print(df)
        create_stmt = df[['Create View']].values[0][0]
        print(create_stmt)

    def show_stats(self, table, schema, catalog):
        '''
        '''
        sql = 'show stats for {}.{}.{}'.format(catalog, schema, table)
        host, port = get_presto_host_port()
        catalogs = get_presto_records(host, port, sql)
        print(catalogs)

    def _desc_table(self, table, schema):
        '''
            describe table from hive
        '''
        sql = 'describe hive.{}.{}'.format(schema, table)
        host, port = get_presto_host_port()
        df = get_presto_records(host, port, sql)
        print(df)

    def _desc_tables(self):
        '''
            describe all tables from hive.flat_rtb
        '''
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
        print(
            'max comment length {}'.format(
                all_tables.Comment.str.len().max()))
        # embed()


def print_sa_table_rows(table, nrows):
    ' prints nrows rows from table '
    stmt = table.select().limit(nrows)
    results = stmt.execute().fetchall()
    for result in results:
        print(result)


def create_new_table(metadata, table, new_table):
    ' create new table with data from table '
    assert metadata.is_bound(), 'Metadata is not bound'
    table = Table(table, metadata, autoload=True)
    new_table = Table(new_table, metadata)
    for column in table.columns:
        print(column.name, column.type)
        new_table.append_column(sa.Column(column.name, column.type))

    # create a new table
    new_table.create()

    # insert data from old table
    stmt = new_table.insert().from_select(table.columns, select=table.select())
    stmt.execute()


def print_sa_table_names(host, port, catalog, schema):
    conn_str = f'presto://{host}:{port}/{catalog}/{schema}'
    engine = sa.create_engine(conn_str)
    metadata = MetaData(bind=engine)
    tables = metadata.bind.table_names()
    print(textwrap.indent('\n'.join(tables), prefix='\t'))


def get_sa_table(host, port, catalog, schema, table):
    ' get sqlalchemy presto table '
    conn_str = f'presto://{host}:{port}/{catalog}/{schema}'
    engine = sa.create_engine(conn_str)
    metadata = MetaData(bind=engine)
    table = Table(table, metadata, autoload=True)
    return table


def get_sa_table_int_min_max(table):
    ' get min, max values for int columns for a sqlalchemy table '

    col_types = (sa.types.SmallInteger, sa.types.Integer, sa.types.BigInteger)

    queries = []
    for idx, column in enumerate(table.columns):
        query = sa.select(
            [sa.literal(column.name),
             sa.func.min(column), sa.func.max(column)])
        if isinstance(column.type, col_types):
            queries.append(query)
    if len(queries) > 0:
        return list(sa.union(*queries).execute())
    return []


class TempOperations:
    ''' temp operations
    '''
    def min_max_type(self):
        ''' sqlalchemy to min and max of Presto columns by type
        '''
        host, port = get_presto_host_port()
        table_name = 'multi_types'
        table = get_sa_table(host, port, 'minio', 'default', table_name)
        print(f'Table {table_name}')
        print(get_sa_table_int_min_max(table))

    def sqlalchemy(self):
        ''' sqlalchemy with Presto example
        '''
        host, port = get_presto_host_port()
        catalog = 'minio'
        schema = 'default'
        print_sa_table_names(host, port, catalog, schema)

        nrows = 3

        with timed():
            table_name = 'million_rows'
            table = get_sa_table(host, port, catalog, schema, table_name)
            print_sa_table_rows(table, nrows)

        # create_new_table(metadata, 'million_rows', 'million_rows_v3')

        with timed():
            table_name = 'million_rows_v3'


class Databases:
    def __init__(self):
        self.hive = HiveDatabase()
        self.presto = PrestoDatabase()
        self.temp = TempOperations()


if __name__ == '__main__':
    # https://github.com/willmcgugan/rich
    logging.basicConfig(level=logging.WARN)
    fire.Fire(Databases)
