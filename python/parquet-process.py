from pathlib import Path

import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import numpy as np


SCRIPT_DIR = Path(__file__).parent.resolve()


def get_dataframe():
    return pd.DataFrame({'one': [-1, np.nan, 2.5],
        'two': ['foo', 'bar', 'baz'],
        'three': [True, False, True]},
        index=list('abc'))

'''
create external table example_parq(one double, two string, three boolean) STORED AS PARQUET location 's3a://example-parquet/'
'''

def main():
    print('in parquet-process')
    df = get_dataframe()
    table = pa.Table.from_pandas(df)
    minio_data_dir = SCRIPT_DIR / '..' / 'presto-minio' / 'minio' / 'data'
    # create a directory
    parq_dir = minio_data_dir / 'example-parquet'
    parq_dir.mkdir(exist_ok=True)

    # snappy vs gzip
    # https://stackoverflow.com/questions/35789412/spark-sql-difference-between-gzip-vs-snappy-vs-lzo-compression-formats
    pq.write_table(table, parq_dir / 'example.parq', compression='snappy')


if __name__ == '__main__':
    main()
