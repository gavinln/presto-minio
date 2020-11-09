from pathlib import Path

import pyarrow.parquet as pq
import duckdb

from IPython import embed


SCRIPT_DIR = Path(__file__).parent.resolve()


def process_ft_million_clustered():
    parq_file_name = 'ft_million_clustered.parq'
    parq_file = SCRIPT_DIR / parq_file_name
    tbl = pq.read_table(parq_file)
    df = tbl.to_pandas()
    print(df.agg({'id': 'mean', 'grp_code': 'mean'}))

    db_file = ':memory:'
    con = duckdb.connect(database=db_file, read_only=False)
    sql = "select avg(id), avg(grp_code) from parquet_scan('{}')".format(
        parq_file)
    con.execute(sql)
    for row in con.fetchall():
        print(row)
    con.close()


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
    # process_ft_million_clustered()
    process_airline_parquet()


if __name__ == '__main__':
    main()
