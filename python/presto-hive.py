'''
Display Presto and Hive metadata
'''

import logging
import pathlib

from dataclasses import dataclass

import pandas as pd

from pyhive import presto
from pyhive import hive

import fire


SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
log = logging.getLogger(__name__)


def presto_execute_fetchall(server, sql):
    cursor = presto.connect(server).cursor()
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
        conn = presto.connect(host=self.server)
        sql = 'desc {}'.format(self._get_full_name())
        df = pd.read_sql_query(sql, conn)
        return df

    def stats(self):
        conn = presto.connect(host=self.server)
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
    log_pyhive = logging.getLogger('pyhive.presto')
    log_pyhive.setLevel(logging.WARN)

    log.info('in main')

    pm = PrestoMeta('10.0.0.2')
    print(pm.catalogs())
    print(pm.catalogs('minio'))
    print(pm.catalogs('minio').schemas())
    print(pm.catalogs('minio').schemas('default'))
    print(pm.catalogs('minio').schemas('default').tables())
    print(pm.catalogs('minio').schemas('default').tables('example'))


def get_catalogs():
    ' get catalogs '
    pm = PrestoMeta('10.0.0.2')
    for catalog in pm.catalogs():
        print(catalog.name)


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARN)
    fire.Fire({
        'catalogs': get_catalogs
    })
