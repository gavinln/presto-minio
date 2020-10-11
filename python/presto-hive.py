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

import pandas as pd

import sqlalchemy as sa

from rich.console import Console
from rich.syntax import Syntax

import fire

import pyarrow.parquet as pq
from pyarrow import fs
import s3fs

from query_yes_no import query_yes_no
from presto_hive_lib import get_presto_records
from presto_hive_lib import get_presto_catalogs

from presto_hive_lib import get_hive_records
from presto_hive_lib import get_hive_records_database_like_table
from presto_hive_lib import get_hive_records_database_dot_table
from presto_hive_lib import get_hive_table_extended

from presto_hive_lib import get_sa_table
from presto_hive_lib import get_sa_new_table
from presto_hive_lib import get_sa_metadata
from presto_hive_lib import create_sa_table_from_table
from presto_hive_lib import print_sa_table

from presto_hive_lib import timed

from IPython import embed


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
    # port = 8889
    return host, port


def get_clickhouse_host_port():
    host = '10.0.0.2'
    port = 8123
    return host, port


def get_s3_client_kwargs():
    client_kwargs = {'endpoint_url': 'http://10.0.0.2:9001'}
    return client_kwargs


def get_presto_catalog_schema():
    return 'hive', 'temp'


def print_all(df):
    if df is not None:
        with pd.option_context(
                "display.max_rows", None,
                "display.max_columns", None):
            print(df.to_string(index=False))


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
        if name in value_format and formatter is not None:
            value_formatter = formatter[name]
            formatted_value = get_formatted_value(value_formatter, value)
        else:
            formatted_value = value
        print('{name:{width}s} : {value}'.format(
            name=name, width=max_name_len, value=formatted_value))


def dataframe_to_dict(df):
    return dict(df.iloc[:, [0, 1]].to_records(index=False))


class HeaderDataFrame:
    ''' holds a header (text string) and data frame '''
    def __init__(self, header, dataframe):
        self.header = header
        self.dataframe = dataframe


def _srs_match_index(srs, match_str):
    match_list = srs.index[srs == match_str].values
    if len(match_list) > 0:
        return match_list[0]
    return -1


def _remove_empty_rows(df):
    df2 = df.replace(to_replace=[None], value=[''])
    empty_rows = [
        idx for idx, row in df2.iterrows(
            ) if row.unique().shape[0] == 1]
    return df2.drop(axis='index', index=empty_rows)


def _remove_empty_cols(df):
    df2 = df.replace(to_replace=[None], value=[''])
    empty_cols = [
        col for col, row in df2.iteritems(
            ) if row.unique().shape[0] == 1]
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


def _print_clean_name_value_df(df):
    name_value = dataframe_to_dict(df)
    print_name_value_dict(name_value)


def _get_header_dataframes(df):
    col_names = df.col_name.str.strip()
    matches = [
        '# col_name', '# Partition Information',
        '# Detailed Table Information',
        'Table Parameters:', '# Storage Information',
        'Storage Desc Params:'
    ]
    match_idx_list = [
        _srs_match_index(col_names, match) for match in matches]
    valid_match_idx_list = [
        match_idx for match_idx in match_idx_list if match_idx >= 0]
    all_match_idx_list = valid_match_idx_list + [col_names.size]

    header_dataframes = []

    for idx, (start, stop) in enumerate(pairwise(all_match_idx_list)):
        df_part = df.iloc[start:stop]
        header = _get_header_row_df(df_part, matches)
        if header:
            df_output = df_part.iloc[1:, ]
            header_dataframes.append(
                HeaderDataFrame(header, df_output))
        else:
            df_output = df_part
            header_dataframes.append(
                HeaderDataFrame(None, df_output))
    return header_dataframes


def _desc_formatted(host, port, table, database):
    sql = 'desc formatted'
    df = get_hive_records_database_dot_table(
        host, port, sql, database, table)
    return _get_header_dataframes(df)


def get_hive_table_location(host, port, table, database):
    ' returns the location of a hive table if it exists or None '
    header_dataframes = _desc_formatted(
        host, port, table, database)

    locations = []
    for hdf in header_dataframes:
        header, dataframe = hdf.header, hdf.dataframe
        if header == '# Detailed Table Information':
            df_clean = _remove_empty_rows_cols(dataframe)
            name_value = dataframe_to_dict(df_clean)
            locations = [value for name, value in name_value.items(
                ) if name.startswith('Location:')]
    if len(locations) > 0:
        return locations[0]
    return None


def get_s3_parquet_file(
        s3_location, parq_file_path, client_kwargs=None):
    ' copy parquet file to local machine '
    file_system = s3fs.S3FileSystem(
        client_kwargs=client_kwargs)

    dataset = pq.ParquetDataset(s3_location, filesystem=file_system)
    parq_table = dataset.read()
    pq.write_table(parq_table, parq_file_path)
    parq_path = pathlib.Path(parq_file_path)
    print('Saved to file {}, size {:,d} bytes'.format(
        parq_path.name, parq_path.stat().st_size))


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
        host, port = get_hive_host_port()
        databases = get_hive_databases(host, port)
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
        print_all(tables)

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
        host, port = get_hive_host_port()
        databases = get_hive_databases(host, port)
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
        host, port = get_hive_host_port()
        partitions = get_hive_records_database_dot_table(
            host, port, sql, database, table)
        print(partitions)

    def show_tblproperties(self, table, database=None):
        '''
            validate table
        '''
        if database is not None:
            database = check_hive_database(database)
        sql = 'show tblproperties'
        host, port = get_hive_host_port()
        table = get_hive_records_database_dot_table(
            host, port, sql, database, table)
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
        host, port = get_hive_host_port()
        tables = get_hive_records_database_dot_table(
            host, port, sql, database, table)
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
        ''' shows list of columns including partition
        '''
        if database is not None:
            database = check_hive_database(database)
        sql = 'desc'
        host, port = get_hive_host_port()
        info = get_hive_records_database_dot_table(
            host, port, sql, database, table)
        print_all(info)

    def desc_formatted(self, table, database=None):
        ''' show table metadata in tablular format
        '''
        if database is not None:
            database = check_hive_database(database)
        host, port = get_hive_host_port()

        header_dataframes = _desc_formatted(
            host, port, table, database)

        for hdf in header_dataframes:
            header, dataframe = hdf.header, hdf.dataframe
            print()
            print(header)
            df_clean = _remove_empty_rows_cols(dataframe)
            _print_clean_name_value_df(df_clean)

    def show_table_location(self, table, database=None):
        ''' show table file location if exist
        '''
        if database is not None:
            database = check_hive_database(database)
        host, port = get_hive_host_port()

        table_location = get_hive_table_location(host, port, table, database)
        if table_location:
            print(table_location)
        else:
            print('Cannot find file for table {}'.format(table))

    def get_table_s3(self, table, database=None):
        ''' get hive table stored as a parquet file
        '''
        if database is not None:
            database = check_hive_database(database)
        host, port = get_hive_host_port()
        # client_kwargs = {'endpoint_url': 'http://10.0.0.2:9000'}
        client_kwargs = get_s3_client_kwargs()
        # client_kwargs = {}
        table_location = get_hive_table_location(host, port, table, database)
        print('hive table {}, location {}'.format(table, table_location))
        if table_location:
            if table_location.startswith('s3'):
                with timed():
                    parq_file_path = '{}.parq'.format(table)
                    get_s3_parquet_file(
                        table_location, parq_file_path, client_kwargs)
            else:
                sys.exit(
                    'Can only read s3 files. Unknown location: {}'.format(
                        table_location))
        else:
            print('Cannot find file for table {}'.format(table))


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
        # print_all(results)

    def show_catalogs(self):
        '''
        '''
        host, port = get_presto_host_port()
        catalogs = get_presto_catalogs(host, port)
        print(catalogs)

    def show_schemas(self, catalog):
        ''' show schemas
        '''
        valid_catalog = check_presto_catalogs(catalog)
        sql = 'show schemas from {}'.format(valid_catalog)
        host, port = get_presto_host_port()
        catalogs = get_presto_records(host, port, sql)
        print(catalogs)

    def show_tables(self, schema, catalog):
        ''' show tables
        '''
        sql = 'show tables from {}.{}'.format(catalog, schema)
        host, port = get_presto_host_port()
        tables = get_presto_records(host, port, sql)
        print_all(tables)

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
        stats = get_presto_records(host, port, sql)
        # print(stats)
        print_all(stats)

    def _desc_table(self, table, schema):
        '''
            describe table from hive
        '''
        assert False, 'May not be used'

        sql = 'describe hive.{}.{}'.format(schema, table)
        host, port = get_presto_host_port()
        df = get_presto_records(host, port, sql)
        print(df)

    def _desc_tables(self):
        '''
            describe all tables from hive.flat_rtb
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
        print(
            'max comment length {}'.format(
                all_tables.Comment.str.len().max()))


def check_copy_presto_table(host, port, catalog, schema, old_table, new_table):
    metadata = get_sa_metadata(host, port, catalog, schema)
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
        metadata = check_copy_presto_table(
            host, port, catalog, schema, old_table, new_table)

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
        metadata = check_copy_presto_table(
            host, port, catalog, schema, old_table, new_table)

        old_sa_table = get_sa_table(metadata, old_table)
        print_sa_table(old_sa_table)

        new_sa_table = get_sa_new_table(
            metadata, old_table, new_table, smallest_int_types=True)
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
        if  file_name.startswith('s3://'):
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


def get_clickhouse_connection(host, port):
    engine = sa.create_engine(
        'clickhouse://default@{}:{}/default'.format(host, port))
    return engine.connect()


def get_clickhouse_records(host, port, sql):
    ' runs a clickhouse sql statement and returns dataframe result '
    conn = get_clickhouse_connection(host, port)
    try:
        df = pd.read_sql(sql, conn)
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
        df = get_clickhouse_records(host, port, sql)
        print(df)

    def show_tables(self, database):
        ' show a list of all databases '
        host, port = get_clickhouse_host_port()
        sql = 'show tables from {}'.format(database)
        df = get_clickhouse_records(host, port, sql)
        print(df)


class Databases:
    def __init__(self):
        self.hive = HiveDatabase()
        self.presto = PrestoDatabase()
        self.clickhouse = ClickhouseDatabase()
        self.op = OpDatabase()
        self.parquet = ParquetAction()


if __name__ == '__main__':
    # https://github.com/willmcgugan/rich
    logging.basicConfig(level=logging.WARN)
    fire.Fire(Databases)
