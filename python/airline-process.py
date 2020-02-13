from pathlib import Path

import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import numpy as np

from IPython import embed


SCRIPT_DIR = Path(__file__).parent.resolve()


'''
create external table example_parq(one double, two string, three boolean) STORED AS PARQUET location 's3a://example-parquet/'
'''


def main():
    print('in airline-process')
    airline_data_file = SCRIPT_DIR / '..' / 'presto-minio' / 'minio' / 'data' / 'airline-parq' / '1987_cleaned.gzip.parq'

    table = pq.read_table(airline_data_file)
    embed()


if __name__ == '__main__':
    main()
