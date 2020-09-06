'''

Compare reading data using a Presto SQL database driver with reading directly
from a Parquet file on S3.

Also compare using int64 with int32 column types

'''
import logging
import pathlib
import sys
from contextlib import contextmanager
from time import time
from datetime import datetime as dt

from pyhive import hive

import pyarrow.parquet as pq

import s3fs

from presto_hive_lib import get_presto_records
from presto_hive_lib import get_hive_table_extended


SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
log = logging.getLogger(__name__)


@contextmanager
def timed():
    'Simple timer context manager, implemented using a generator function'
    start = time()
    print("Starting at {:%H:%M:%S}".format(dt.fromtimestamp(start)))

    yield

    end = time()
    print("Ending at {:%H:%M:%S} (total: {:.2f} seconds)".format(
        dt.fromtimestamp(end), end - start))


def get_hive_host_port():
    host = '10.0.0.2'
    # host = 'hive.liftoff.io'
    port = 10000
    return host, port


def get_hive_connection():
    host, port = get_hive_host_port()
    return hive.Connection(host=host, port=port)


def get_presto_host_port():
    host = '10.0.0.2'
    # host = 'presto.liftoff.io'
    port = 8080
    return host, port


# fire.Fire({
#     'catalogs': get_catalogs,
#     'schemas': get_schemas,
#     'hive-tables': get_hive_tables,
#     'hive-databases': get_hive_databases
# })


'''
create table minio.default.million_rows_v as
select cast(id as int) id, cast(grp_code as int) grp_code
from minio.default.million_rows
;
'''


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


def print_parquet_pandas_shape(bucket_uri, file_system):
    dataset = pq.ParquetDataset(bucket_uri, filesystem=file_system)
    table = dataset.read()
    df = table.to_pandas()
    print(df.shape)


def get_parquet_pandas_file_system(location, file_system):
    dataset = pq.ParquetDataset(location, filesystem=file_system)
    table = dataset.read()
    return table.to_pandas()


def get_parquet_pandas_S3(location, client_kwargs):
    file_system = s3fs.S3FileSystem(client_kwargs=client_kwargs)
    return get_parquet_pandas_file_system(location, file_system)


def get_hive_table_pandas_S3(host, port, table, database, client_kwargs):
    table_location = get_hive_table_location(
        host, port, table, database)
    if table_location is not None:
        if table_location.startswith('s3'):
            df = get_parquet_pandas_S3(table_location, client_kwargs)
            return df
        else:
            sys.exit('Unknown location: {}'.format(table_location))
    else:
        sys.exit('Unknown table: {}'.format(table))
    return None


def main():
    log.info('in file %s', __file__)

    client_kwargs = {
        'endpoint_url': 'http://10.0.0.2:9000'
    }

    presto_host, presto_port = get_presto_host_port()
    hive_host, hive_port = get_hive_host_port()

    print('Reading from S3 vs using SQL for internal/external tables')

    with timed():
        table = 'million_external'
        database = 'default'
        df = get_hive_table_pandas_S3(
            hive_host, hive_port, table, database, client_kwargs)
        if df is not None:
            print('parquet table', df.shape)
        else:
            msg = 'Cannot get dataframe for table {}, database {}'.format(
                table, database)
            print(msg)

    with timed():
        df1 = get_presto_records(
            presto_host, presto_port,
            'select * from minio.default.million_rows')
        print('internal table', df1.shape)

    with timed():
        df2 = get_presto_records(
            presto_host, presto_port,
            'select * from minio.default.million_external')
        print('external table', df2.shape)

    del df, df1, df2

    print('Compare reading from S3 with bigint vs int')
    with timed():
        table = 'ft_million_external'
        database = 'default'
        df3 = get_hive_table_pandas_S3(
            hive_host, hive_port, table, database, client_kwargs)
        if df3 is not None:
            print('parquet table', df3.shape)
        else:
            msg = 'Cannot get dataframe for table {}, database {}'.format(
                table, database)
            print(msg)

    print(df3.dtypes)
    print('memory usage: {:,.0f}'.format(df3.memory_usage(deep=True).sum()))

    with timed():
        table = 'ft_million_external_typed'
        database = 'default'
        df4 = get_hive_table_pandas_S3(
            hive_host, hive_port, table, database, client_kwargs)
        if df4 is not None:
            print('parquet table', df4.shape)
        else:
            msg = 'Cannot get dataframe for table {}, database {}'.format(
                table, database)
            print(msg)

    print(df4.dtypes)
    print('memory usage: {:,.0f}'.format(df4.memory_usage(deep=True).sum()))


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARN)
    main()
