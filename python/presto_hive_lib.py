'''
library of functions to get data from Presto and Hive
'''
from contextlib import contextmanager
from time import time
from datetime import datetime as dt
from collections import namedtuple
import textwrap

from typing import Dict, Optional, Union, List

from more_itertools import first

from pyhive import presto
from pyhive import hive

from dataclasses import dataclass
import pandas as pd

import sqlalchemy as sa
from sqlalchemy import MetaData
from sqlalchemy.schema import Table

from rich.console import Console


def get_presto_connection(host, port):
    return presto.Connection(host=host, port=port)


def get_presto_records(host, port, sql):
    ' runs a presto sql statement and returns result '
    conn = get_presto_connection(host, port)
    try:
        df = pd.read_sql(sql, conn)
        return df
    except Exception:
        Console().print_exception(theme='solarized-light')
    return None


def get_presto_catalogs(host, port):
    sql = 'show catalogs'
    return get_presto_records(host, port, sql)


def get_hive_connection(host, port):
    return hive.Connection(host=host, port=port)


def get_hive_records(host, port, sql):
    ' runs a hive sql statement and returns result '
    conn = get_hive_connection(host, port)
    df = pd.read_sql(sql, conn)
    return df


def get_hive_records_database_like_table(host,
                                         port,
                                         sql,
                                         database=None,
                                         table=None):
    if database is not None:
        sql += ' in {}'.format(database)
    if table is not None:
        sql += " like '{}'".format(table)
    return get_hive_records(host, port, sql)


def get_hive_records_database_dot_table(host,
                                        port,
                                        sql,
                                        database=None,
                                        table=None):
    if database is not None:
        sql += ' {}.'.format(database)
    if table is not None:
        sql += ' ' + table
    return get_hive_records(host, port, sql)


def get_hive_list(host, port, sql):
    conn = get_hive_connection(host, port)
    cursor = conn.cursor()
    cursor.execute(sql)
    items = [items for items in cursor.fetchall()]
    cursor.close()
    return items


def get_hive_table_extended(host, port, table, database=None):
    sql = 'show table extended'
    df = get_hive_records_database_like_table(host, port, sql, database, table)
    return df


def presto_execute_fetchall(server, sql):
    cursor = presto.connect(server, port=8889).cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    return result


@contextmanager
def timed():
    'Simple timer context manager, implemented using a generator function'
    start = time()
    print("===== Start {:%H:%M:%S}".format(dt.fromtimestamp(start)))

    yield

    end = time()
    print("===== End   {:%H:%M:%S} (total: {:.2f} seconds)".format(
        dt.fromtimestamp(end), end - start))


def print_sa_table_rows(table, nrows):
    ' prints nrows rows from table '
    stmt = table.select().limit(nrows)
    results = stmt.execute().fetchall()
    for result in results:
        print(result)


def print_sa_table(table):
    ' print sqlalchemy table definition '
    print('table: {}, url: {}'.format(table.name, table.bind.url))
    for column in table.columns:
        print('\t{} {}'.format(column.name, repr(column.type)))


def get_presto_sa_metadata(host, port, catalog, schema):
    ' get sqlalchemy presto table '
    conn_str = f'presto://{host}:{port}/{catalog}/{schema}'
    engine = sa.create_engine(conn_str)
    metadata = MetaData(bind=engine)
    return metadata


def get_clickhouse_engine(host, port, database):
    engine = sa.create_engine(
        # 'clickhouse://default:@{}:{}/{}'.format(host, port, database))
        'clickhouse://default:@{}:{}/'.format(host, port))
    return engine


def get_clickhouse_sa_metadata(host, port, database):
    ' get sqlalchemy clickhouse metadata'
    engine = get_clickhouse_engine(host, port, database)
    metadata = MetaData(bind=engine)
    return metadata



def print_sa_table_names(metadata):
    ' print all tables in the metadata '
    tables = metadata.bind.table_names()
    print(textwrap.indent('\n'.join(tables), prefix='\t'))


def get_sa_table(metadata, table_name):
    ' get sqlalchemy presto table '
    table = Table(table_name, metadata, autoload=True)
    return table


def get_sa_table_int_min_max(table) -> Optional[Dict]:
    ' get min, max values for int columns for a sqlalchemy table '

    col_types = (sa.types.SmallInteger, sa.types.Integer, sa.types.BigInteger)

    queries = []
    for idx, column in enumerate(table.columns):
        query = sa.select([
            sa.literal(column.name),
            sa.func.min(column),
            sa.func.max(column)
        ])
        if isinstance(column.type, col_types):
            queries.append(query)
    if len(queries) > 0:
        name_min_max_list = list(sa.union(*queries).execute())
        return {
            name: (min_val, max_val)
            for name, min_val, max_val in name_min_max_list
        }
    return None


PrestoIntType = namedtuple('PrestoIntType', 'type power min max')

presto_int_types = (
    PrestoIntType(sa.types.SmallInteger, 15, -(1 << 15), 1 << 15),
    PrestoIntType(sa.types.Integer, 31, -(1 << 31), 1 << 31),
    PrestoIntType(sa.types.BigInteger, 63, -(1 << 63), 1 << 63),
)


def get_presto_smallest_int_type_min_max(min_val, max_val):
    for int_stats in presto_int_types:
        if min_val >= int_stats.min and max_val <= int_stats.max:
            return int_stats.type
    return None


def get_presto_smallest_int_type(int_val):
    return get_presto_smallest_int_type_min_max(int_val, int_val)


def get_sa_new_table(metadata,
                     table_name,
                     new_table_name,
                     smallest_int_types=False):
    ' create new table with data from table '
    assert metadata.is_bound(), 'Metadata is not bound'
    table = Table(table_name, metadata, autoload=True)

    # get smallest int types for all data in the table
    if smallest_int_types:
        min_max_types = get_sa_table_int_min_max(table)
        if min_max_types:
            smallest_int_types = {}
            for presto_col, (min_val, max_val) in min_max_types.items():
                smallest_int_type = get_presto_smallest_int_type_min_max(
                    min_val, max_val)
                smallest_int_types.update({presto_col: smallest_int_type})

    new_table = Table(new_table_name, metadata)
    for column in table.columns:
        if smallest_int_types:
            smallest_int_type = smallest_int_types.get(column.name, None)
            if smallest_int_type:
                new_table.append_column(
                    sa.Column(column.name, smallest_int_type()))
                continue
        new_table.append_column(sa.Column(column.name, column.type))
    return new_table


def create_sa_table_from_table(new_table: sa.Table, table: sa.Table):
    ''' create a new sqlalchemy table from table

        Create a new sqlalchemy with data from an existing table
    '''
    # create a new table
    new_table.create()

    # insert data from old table
    stmt = new_table.insert().from_select(table.columns, select=table.select())
    stmt.execute()


def get_hive_table_location(host, port, table, database):
    ' returns the location of a hive table if it exists or None '
    df = get_hive_table_extended(host, port, table, database)
    if df.size > 0:
        location_idx = df.tab_name.str.startswith('location:')
        location_srs = df.tab_name[location_idx]
        if location_srs.size > 0:
            location_name_val = location_srs.values[0]
            idx = location_name_val.find(':')
            if idx >= 0:
                _ = location_name_val[:idx]
                loc_val = location_name_val[idx + 1:]
                return loc_val
    return None


@dataclass
class PrestoTable:
    server: str
    name: str
    schema_name: str
    catalog_name: str

    def _get_full_name(self):
        full_name = '{}.{}.{}'.format(self.catalog_name, self.schema_name,
                                      self.name)
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
        sql = 'show tables from {}.{}'.format(self.catalog_name, self.name)
        tables = [
            PrestoTable(self.server, table_name, self.name, self.catalog_name)
            for (table_name, ) in presto_execute_fetchall(self.server, sql)
        ]
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

    def schema(self, name) -> Optional[PrestoSchema]:
        schemas = self.schemas()
        schema_names = [schema.name for schema in schemas]
        if name not in schema_names:
            raise ValueError(
                'schema {} not present. Should be one of {}'.format(
                    name, ', '.join(schema_names)))
        matched_schemas = [pc for pc in schemas if pc.name == name]
        return first(matched_schemas, None)

    def schemas(self) -> List[PrestoSchema]:
        sql = 'show schemas from {}'.format(self.name)
        schemas = [
            PrestoSchema(self.server, schema_name, self.name)
            for (schema_name, ) in presto_execute_fetchall(self.server, sql)
        ]
        return schemas


@dataclass
class PrestoMeta:
    server: str

    def catalog(self, name) -> Optional[PrestoCatalog]:
        catalogs = self.catalogs()
        catalog_names = [catalog.name for catalog in catalogs]
        if name not in catalog_names:
            raise ValueError(
                'Catalog {} not present. Should be one of {}'.format(
                    name, ', '.join(catalog_names)))
        matched_catalogs = [pc for pc in catalogs if pc.name == name]
        return first(matched_catalogs, None)


    def catalogs(self) -> List[PrestoCatalog]:
        sql = 'show catalogs'
        catalogs = [
            PrestoCatalog(self.server, name)
            for (name, ) in presto_execute_fetchall(self.server, sql)
        ]
        return catalogs


def main():
    server = ''
    pm = PrestoMeta(server)
    print(pm.catalogs())
    print(pm.catalog('minio'))
    print(pm.catalog('minio').schemas())
    print(pm.catalog('minio').schema('default'))
    print(pm.catalog('minio').schema('default').tables())
    print(pm.catalog('minio').schema('default').tables('example'))
