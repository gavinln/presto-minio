'''
Display Presto and Hive metadata
'''

import logging
import pathlib
import types
import datetime
import textwrap
import difflib
import itertools

from dataclasses import dataclass

import pandas as pd

from pyhive import presto
from pyhive import hive

from rich.console import Console
from rich.syntax import Syntax

from IPython import embed

import fire

from query_yes_no import query_yes_no


SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
log = logging.getLogger(__name__)


def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def presto_execute_fetchall(server, sql):
    cursor = presto.connect(server, port=8889).cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    return result


@dataclass
class PrestoTable:
    server: str
    name: str
    schema_name: str
    catalog_name: str

    def _get_full_name(self):
        full_name = '{}.{}.{}'.format(
            self.catalog_name, self.schema_name, self.name)
        return full_name

    def desc(self):
        conn = presto.connect(host=self.server, port=8889)
        sql = 'desc {}'.format(self._get_full_name())
        df = pd.read_sql_query(sql, conn)
        return df

    def stats(self):
        conn = presto.connect(host=self.server, port=8889)
        sql = 'show stats for {}'.format(self._get_full_name())
        df = pd.read_sql_query(sql, conn)
        return df

    @property
    def full_name(self):
        return self._get_full_name()


@dataclass
class PrestoSchema:
    server: str
    name: str
    catalog_name: str

    def tables(self, name=None):
        sql = 'show tables from {}.{}'.format(
            self.catalog_name, self.name)
        tables = [
            PrestoTable(
                self.server, table_name, self.name, self.catalog_name) for (
                table_name,) in presto_execute_fetchall(
                    self.server, sql)]
        table_names = [table.name for table in tables]
        if name is not None:
            if name not in table_names:
                raise ValueError(
                    'table {} not present. Should be one of {}'.format(
                        name, ', '.join(table_names)))
            return [pc for pc in tables if pc.name == name][0]
        return tables


@dataclass
class PrestoCatalog:
    server: str
    name: str

    def schemas(self, name=None):
        sql = 'show schemas from {}'.format(self.name)
        schemas = [
            PrestoSchema(self.server, schema_name, self.name) for (
                schema_name,) in presto_execute_fetchall(
                    self.server, sql)]
        schema_names = [schema.name for schema in schemas]
        if name is not None:
            if name not in schema_names:
                raise ValueError(
                    'schema {} not present. Should be one of {}'.format(
                        name, ', '.join(schema_names)))
            return [pc for pc in schemas if pc.name == name][0]
        return schemas


@dataclass
class PrestoMeta:
    server: str

    def catalogs(self, name=None):
        sql = 'show catalogs'
        catalogs = [
            PrestoCatalog(self.server, name) for (
                name,) in presto_execute_fetchall(self.server, sql)]
        catalog_names = [catalog.name for catalog in catalogs]
        if name is not None:
            if name not in catalog_names:
                raise ValueError(
                    'Catalog {} not present. Should be one of {}'.format(
                        name, ', '.join(catalog_names)))
            return [pc for pc in catalogs if pc.name == name][0]
        return catalogs


def get_hive_host():
    host = '10.0.0.2'
    host = 'hive.liftoff.io'
    return host


def get_presto_host():
    host = '10.0.0.2'
    host = 'presto.liftoff.io'
    return host


def get_presto_records(sql):
    ' runs a presto sql statement and returns result '
    host = get_presto_host()
    # conn = presto.Connection(host=host)
    # conn = presto.Connection(host=host, port=8889)
    conn = presto.Connection(host=host, port=8080)
    df = pd.read_sql(sql, conn)
    return df


def main():
    log_pyhive = logging.getLogger('pyhive.presto')
    log_pyhive.setLevel(logging.WARN)

    log.info('in main')

    host = get_hive_host()

    conn = hive.Connection(host=host)
    cursor = conn.cursor()
    cursor.execute("select * from example")
    for result in cursor.fetchall():
        print(result)

    df = pd.read_sql("select * from example", conn)
    print(df)
    conn.close()
    return

    pm = PrestoMeta(server)
    print(pm.catalogs())
    print(pm.catalogs('minio'))
    print(pm.catalogs('minio').schemas())
    print(pm.catalogs('minio').schemas('default'))
    print(pm.catalogs('minio').schemas('default').tables())
    print(pm.catalogs('minio').schemas('default').tables('example'))


def print_all(df):
    with pd.option_context(
            "display.max_rows", None,
            "display.max_columns", None):
        print(df)


def get_schemas(catalog: str):
    ' get schemas for a specified catalog '

    server = 'presto.liftoff.io'
    pm = PrestoMeta(server)
    for c in pm.catalogs():
        if c.name == catalog:
            schemas = c.schemas()
            for s in schemas:
                print(s.name)
            return
    print('Unknown catalog {}'.format(catalog))


def get_catalogs():
    ' get catalogs '
    server = 'presto.liftoff.io'
    pm = PrestoMeta(server)
    for catalog in pm.catalogs():
        print(catalog.name)


def get_hive_list(sql):
    host = get_hive_host()
    cursor = hive.connect(host=host).cursor()
    cursor.execute(sql)
    items = [items for items in cursor.fetchall()]
    cursor.close()
    return items


def get_hive_records(sql):
    ' runs a hive sql statement and returns result '
    host = get_hive_host()
    conn = hive.Connection(host=host)
    df = pd.read_sql(sql, conn)
    return df


def get_hive_records_database_like_table(
        sql, database=None, table=None):
    if database is not None:
        sql += ' in {}'.format(database)
    if table is not None:
        sql += " like '{}'".format(table)
    return get_hive_records(sql)


def get_hive_records_database_dot_table(
        sql, database=None, table=None):
    if database is not None:
        sql += ' {}.'.format(database)
    if table is not None:
        sql += ' ' + table
    return get_hive_records(sql)

# need a function get_hive_tbl_database_dot_table
# for show tblproperties

# fire.Fire({
#     'catalogs': get_catalogs,
#     'schemas': get_schemas,
#     'hive-tables': get_hive_tables,
#     'hive-databases': get_hive_databases
# })


def get_hive_databases():
    sql = 'show databases'
    return get_hive_records(sql)


def check_hive_database(database):
    ' returns valid database or None if not valid '
    databases = get_hive_databases()
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
        tables = get_hive_records_database_like_table(sql, database)
        print(tables)

    def show_table(self, table, database=None):
        ''' show table
        '''
        if database is not None:
            database = check_hive_database(database)
        sql = 'show tables'
        tables = get_hive_records_database_like_table(sql, database, table)
        print(tables)

    def show_table_extended(self, table, database=None):
        '''
            validate table
        '''
        if database is not None:
            database = check_hive_database(database)
        sql = 'show table extended'
        table = get_hive_records_database_like_table(sql, database, table)
        # print(table)
        name_value = {}
        for items in table.tab_name.str.split(':').values:
            if len(items[0]) > 0:
                name_value[items[0]] = ':'.join(items[1:])
        print_name_value_dict(name_value)

    def show_create_table(self, table, database=None):
        '''
            validate table
        '''
        if database is not None:
            database = check_hive_database(database)
        sql = 'show create table'
        table = get_hive_records_database_dot_table(sql, database, table)
        lines = table.createtab_stmt.values
        console = Console()
        # print('\n'.join(lines))
        syntax = Syntax('\n'.join(lines), 'sql', theme='default')
        console.print(syntax)

    @staticmethod
    def _get_create_stmt(database, table):
        sql = 'show create table'
        table = get_hive_records_database_dot_table(sql, database, table)
        lines = table.createtab_stmt.values
        create_stmt = '\n'.join(lines)
        return create_stmt

    @staticmethod
    def _show_create_tables(database):

        def print_indent(text, indent_level: int, indent_str='\t'):
            assert indent_level > 0, 'indent_level should be greater than 0'
            print(textwrap.indent(text, indent_str * indent_level))

        sql = 'show tables'
        tables = get_hive_records_database_like_table(sql, database)

        table_count_str = 'Database {} has {} tables'.format(
            database, tables.shape[0])
        print_indent(table_count_str, 1)

        create_stmt_list = []
        for idx, table in enumerate(tables.tab_name.values):
            table_number_str = '{}: {} of {}'.format(table, idx, tables.shape[0])
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
        functions = get_hive_records(sql)
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
            print_all(info.iloc[start:stop])


def display_df_all(df):
    max_rows = 1000
    max_cols = 1000
    with pd.option_context(
            "display.max_rows", max_rows, "display.max_columns", max_cols):
        print(df)


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
        sql = 'show catalogs'
        catalogs = get_presto_records(sql)
        print(catalogs)

    def show_schemas(self, catalog):
        '''
        '''
        sql = 'show schemas from {}'.format(catalog)
        catalogs = get_presto_records(sql)
        print(catalogs)

    def show_tables(self, schema, catalog):
        '''
        '''
        sql = 'show tables from {}.{}'.format(catalog, schema)
        catalogs = get_presto_records(sql)
        print(catalogs)

    def show_table(self, table, schema, catalog):
        '''
        '''
        sql = "show tables from {}.{} like '{}'".format(
            catalog, schema, table)
        tables = get_presto_records(sql)
        print(tables)

    def show_columns(self, table, schema, catalog):
        '''
        '''
        sql = "show columns from {}.{}.{}".format(
            catalog, schema, table)
        columns = get_presto_records(sql)
        print(columns)

    def show_create_table(self, table, schema, catalog):
        '''
            show create table
        '''
        sql = 'show create table {}.{}.{}'.format(catalog, schema, table)
        df = get_presto_records(sql)
        create_stmt = df[['Create Table']].values[0][0]
        print(create_stmt)

    def show_stats(self, table, schema, catalog):
        '''
        '''
        sql = 'show stats for {}.{}.{}'.format(catalog, schema, table)
        catalogs = get_presto_records(sql)
        print(catalogs)

    def _desc_table(self, table, schema):
        '''
            describe table from hive
        '''
        sql = 'describe hive.{}.{}'.format(schema, table)
        df = get_presto_records(sql)
        print(df)

    def _desc_tables(self):
        '''
            describe all tables from hive.flat_rtb
        '''
        sql = 'show tables from hive.flat_rtb'
        tables = get_presto_records(sql)
        df_list = []
        column_count = 0
        for idx, table in enumerate(tables.Table.values):
            print(table)
            sql = 'describe hive.flat_rtb.{}'.format(table)
            df = get_presto_records(sql)
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


class Databases:
    def __init__(self):
        self.hive = HiveDatabase()
        self.presto = PrestoDatabase()


if __name__ == '__main__':
    # https://github.com/willmcgugan/rich
    # main()
    logging.basicConfig(level=logging.WARN)
    fire.Fire(Databases)
