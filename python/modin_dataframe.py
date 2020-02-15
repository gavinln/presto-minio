'''
%load_ext autoreload
%autoreload 2
%autoindent
'''

from pathlib import Path

import ray
import os
os.environ["MODIN_ENGINE"] = "ray"
import modin.pandas as pdm

import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import numpy as np



SCRIPT_DIR = Path(__file__).parent.resolve()


def get_files(data_dir):
    files = list(data_dir.glob('*.parq'))
    return files


def get_data_dir():
    SCRIPT_DIR = Path('.')
    data_dir = SCRIPT_DIR / '..' / 'data'
    return data_dir.resolve()


def get_pandas():
    ' 9 seconds '
    files = get_files(get_data_dir())
    dfs = [pd.read_parquet(
        file_name, columns=['Year', 'ArrDelay']) for file_name in files]
    df = pd.concat(dfs)
    return df


@ray.remote
def pandas_read_parquet(file_name):
    return pd.read_parquet(
        file_name, columns=['Year', 'ArrDelay'])


def get_pandas_parallel():
    ' 8.5 seconds '
    files = get_files(get_data_dir())
    dfs = [pandas_read_parquet.remote(file_name) for file_name in files]
    df = pd.concat(ray.get(dfs))
    return df


def get_pa_table():
    table = pq.read_table(get_data_dir(), columns=['Year', 'ArrDelay'])
    return table


@ray.remote
def read_parquet_parallel(file_name):
    return pd.read_parquet(
        file_name, columns=['Year', 'ArrDelay'])


def get_modin():
    files = get_files(get_data_dir())
    dfs = [pdm.read_parquet(
        file_name, columns=['Year', 'ArrDelay']) for file_name in files]
    df = pdm.concat(dfs)
    return df


def get_modin_parallel():
    files = get_files(get_data_dir())
    dfs = [read_parquet_parallel.remote(file_name) for file_name in files]
    df = pdm.concat(ray.get(dfs))
    return df


def time_modin():
    ' 40 seconds '
    df = get_modin()
    print(df.groupby('Year').agg(np.mean))


def time_modin_parallel():
    ' 125 seconds '
    df = get_modin_parallel()
    print(df.groupby('Year').agg(np.mean))


def main():
    print('timing dataframe using airline data')
    # df = get_pandas()
    # df = get_pandas_parallel()
    # time_modin()
    time_modin_parallel()


if __name__ == '__main__':
    import timeit
    print(timeit.timeit("main()", setup="from __main__ import main", number=1))
    # main()
