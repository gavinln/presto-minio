import random
import sqlite3
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from faker import Faker

from pyhive import presto  # or import hive
from IPython import embed


SQLITE_FILE = '/home/vagrant/sqlite-data.db'
fake = Faker()

SCRIPT_DIR = Path(__file__).parent.resolve()
log = logging.getLogger(__name__)


def create_rows(num=1):
    output = [{
        "name": fake.name(),
        "email": fake.email(),
        "city": fake.city(),
        "state": fake.state(),
        "date_time": fake.date_time(),
        "randomdata": random.randint(1000, 2000),
        "ipv4": fake.ipv4_public(network=False, address_class=None),
        "ipv6": fake.ipv6(network=False)
    } for x in range(num)]
    return output


def create_sqlite_connection():
    try:
        conn = sqlite3.connect(SQLITE_FILE)
        return conn
    except sqlite3.OperationalError as err:
        log.exception(err)

    return None


def write_to_sqlite():
    df = pd.DataFrame(create_rows(50))
    print(df.columns)
    print(df.head())

    engine = create_engine('sqlite:///' + SQLITE_FILE)
    with engine.connect() as conn:
        df.to_sql('ip_data', conn, if_exists='replace', index=False)


def get_presto():
    cursor = presto.connect('localhost').cursor()
    cursor.execute('SELECT * from pokes limit 10')
    for row in cursor.fetchall():
        print(row)


def create_presto():
    sql = '''
        CREATE TABLE ip_data (
            name varchar,
            email varchar,
            city varchar,
            state varchar,
            date_time TIMESTAMP,
            randomdata BIGINT,
            ipv4 VARBINARY,
            ipv6 VARBINARY
        )
        with (format='parquet', external_location='s3a://customer-data-orc/');
    '''
    print(sql)
    conn = presto.connect('localhost')
    cursor = conn.cursor()
    try:
        result = cursor.execute(sql)
        print(result)
    except Exception as err:
        print(err)
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        cursor.close()


def main():
    # write_to_sqlite()
    # get_presto()
    # create_presto()
    df = pd.DataFrame(create_rows(50))
    engine = create_engine('presto://localhost:8080/hive/default')
    embed()
    with engine.connect() as conn:
        df.to_sql('ip_data', conn, if_exists='append', index=False)
    # embed()


if __name__ == '__main__':
    # logging.basicConfig(level=logging.INFO)
    main()
