from pathlib import Path

import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import numpy as np

from sqlalchemy import create_engine
from sqlalchemy import MetaData

from pyhive import hive  # or import hive

from IPython import embed


SCRIPT_DIR = Path(__file__).parent.resolve()


'''
create external table example_parq(one double, two string, three boolean) STORED AS PARQUET location 's3a://example-parquet/'
'''

def read_data():
    airline_data_file = SCRIPT_DIR / '..' / 'presto-minio' / 'minio' / 'data' / 'airline-parq' / '1987_cleaned.gzip.parq'

    table = pq.read_table(airline_data_file)
    df = table.to_pandas()
    return df


def reflect_db():
    # engine = create_engine('hive://localhost:10000/default')
    engine = create_engine('presto://localhost:8080/minio/default')

    meta = MetaData(bind=engine)
    meta.reflect()
    print(meta.tables.keys())
    # df = read_data()
    # df.to_sql('airline_data', con=engine, if_exists='replace')


def create_airline_table():
    cursor = hive.connect('localhost').cursor()

    sql = '''
        create external table airline_data (
            Year int,
            Month int,
            DayofMonth int,
            DayOfWeek int,
            DepTime int,
            CRSDepTime int,
            ArrTime int,
            CRSArrTime int,
            UniqueCarrier string,
            FlightNum int,
            TailNum string,
            ActualElapsedTime int,
            CRSElapsedTime int,
            AirTime int,
            ArrDelay int,
            DepDelay int,
            Origin string,
            Dest string,
            Distance int,
            TaxiIn int,
            TaxiOut int,
            Cancelled int,
            CancellationCode string,
            Diverted int,
            CarrierDelay int,
            WeatherDelay int,
            NASDelay int,
            SecurityDelay int,
            LateAircraftDelay int
        )
        STORED AS PARQUET location 's3a://airline-parq/'
    '''
    cursor.execute(sql)


def main():
    print('in airline-process')
    df = read_data()
    print(df.dtypes)
    create_airline_table()


if __name__ == '__main__':
    main()
