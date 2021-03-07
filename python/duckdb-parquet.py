''' Example of querying parquet files using pandas and duckdb
'''
from pathlib import Path

from contextlib import contextmanager

import numpy as np
import pyarrow.parquet as pq
import duckdb
import time

from IPython import embed


SCRIPT_DIR = Path(__file__).parent.resolve()


@contextmanager
def elapsed_time(message):
    start_time = time.time()
    yield
    elapsed = time.time() - start_time
    print(f'{message}: {elapsed:.0f} seconds')


@contextmanager
def parquet_to_pandas(parquet_file):
    with elapsed_time('Elasped'):
        tbl = pq.read_table(parquet_file)
        df = tbl.to_pandas()
        yield df
        del df


@contextmanager
def parquet_to_duckdb(parquet_file):
    with elapsed_time('Elasped'):
        db_file = ':memory:'
        con = duckdb.connect(database=db_file, read_only=False)
        yield con
        con.close()


def execute_print_sql(con, sql):
    con.execute(sql)
    for row in con.fetchall():
        print(row)


def process_ft_million_clustered():
    parq_file_name = 'ft_million_clustered.parq'
    parq_file = SCRIPT_DIR / parq_file_name
    # breakpoint()

    with parquet_to_pandas(parq_file) as df:
        print(df.agg({'id': np.mean, 'grp_code': np.mean}))
        print(df.agg({'id': np.std, 'grp_code': np.std}))


    with parquet_to_duckdb(parq_file) as con:
        # create view temp_parq is slower than create table
        sql = '''
            create table temp_parq as select id, grp_code
            from parquet_scan('{}')'''.format(parq_file)
        execute_print_sql(con, sql)

        sql = '''
            select avg(id), avg(grp_code)
            from temp_parq'''
        execute_print_sql(con, sql)

        sql = '''
            select stddev_pop(id), stddev_pop(grp_code)
            from temp_parq'''
        execute_print_sql(con, sql)


def process_airline_parquet():
    db_file = ':memory:'
    con = duckdb.connect(database=db_file, read_only=False)
    parq_file_str = 'airline-data/*.parq'
    print(parq_file_str)
    sql = "select avg(Year), avg(Month) from parquet_scan('{}')".format(
        parq_file_str)
    sql = """
        create table flights as
        select * from parquet_scan('{}')
        limit 20000000
        """.format(parq_file_str)
    con.execute(sql)
    con.execute('select avg(Year), avg(Month) from flights')
    for row in con.fetchall():
        print(row)
    con.close()


def main():
    process_ft_million_clustered()
    # process_airline_parquet()


if __name__ == '__main__':
    main()
