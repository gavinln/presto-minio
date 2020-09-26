'''
Time the execution of dask dataframes
'''
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import numpy as np

import dask
import dask.dataframe as dd

import pandas as pd


SCRIPT_DIR = Path(__file__).parent.resolve()


def get_files(data_dir):
    files = list(data_dir.glob('*.parq'))
    return files


def get_data_dir():
    SCRIPT_DIR = Path('.')
    data_dir = SCRIPT_DIR / '..' / 'data'
    return data_dir.resolve()


def get_pandas():
    '''
    %timeit df.groupby('Year').agg(np.mean)  # 2 seconds
    '''
    files = get_files(get_data_dir())
    dfs = [pd.read_parquet(
        file_name, columns=['Year', 'ArrDelay']) for file_name in files]
    df = pd.concat(dfs)
    return df


@dask.delayed
def read_parquet_delayed(file_name):
    return pd.read_parquet(file_name, columns=['Year', 'ArrDelay'])


def get_pandas_parallel():
    files = get_files(get_data_dir())
    dfs = dask.compute(
        read_parquet_delayed(file_name) for file_name in files)
    df = pd.concat(dfs[0])
    return df


def get_dask():
    '''
     %timeit df.groupby('Year').agg(np.mean).compute()  # 7.5 seconds
    '''
    files = get_files(get_data_dir())
    df = dd.read_parquet(files, columns=['Year', 'ArrDelay'])
    return df


df = get_pandas_parallel()


def time_dask():
    # df = get_dask()
    print(df.groupby('Year').ArrDelay.mean().compute())


def time_pandas():
    # df = get_pandas()
    print(df.groupby('Year').agg(np.mean))


def time_pandas_parallel():
    # df = get_pandas_parallel()
    print(df.groupby('Year').agg(np.mean))


def main():
    print('timing dataframe using airline data')
    # time_dask()
    # time_pandas()
    time_pandas_parallel()


if __name__ == '__main__':
    import timeit
    print(timeit.timeit("main()", setup="from __main__ import main", number=1))
    # main()
