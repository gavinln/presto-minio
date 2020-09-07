'''
library of functions to get data from Presto and Hive
'''
from contextlib import contextmanager
from time import time
from datetime import datetime as dt

from pyhive import presto
from pyhive import hive

from dataclasses import dataclass
import pandas as pd

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


def get_hive_records_database_like_table(
        host, port, sql,
        database=None, table=None):
    if database is not None:
        sql += ' in {}'.format(database)
    if table is not None:
        sql += " like '{}'".format(table)
    return get_hive_records(host, port, sql)


def get_hive_records_database_dot_table(
        host, port, sql,
        database=None, table=None):
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
    df = get_hive_records_database_like_table(
        host, port, sql, database, table)
    sql = 'show table extended'
    df = get_hive_records_database_like_table(
        host, port, sql, database, table)
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


def main():
    server = None
    pm = PrestoMeta(server)
    print(pm.catalogs())
    print(pm.catalogs('minio'))
    print(pm.catalogs('minio').schemas())
    print(pm.catalogs('minio').schemas('default'))
    print(pm.catalogs('minio').schemas('default').tables())
    print(pm.catalogs('minio').schemas('default').tables('example'))
