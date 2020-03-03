import logging
from pathlib import Path
import sqlite3

from typing import List
from typing import Dict

import requests

import pandas as pd

from IPython import embed

SCRIPT_DIR = Path(__file__).parent.resolve()
log = logging.getLogger(__name__)


def query_list():
    presto_url = 'http://10.0.0.2:8080/v1/query'
    response = requests.get(presto_url)
    response.raise_for_status()
    return response.json()


class QueryList(list):

    @classmethod
    def retrieve(cls):
        ql = QueryList()
        for query in query_list():
            ql.append(query)
        return ql


def main():
    print('in main')
    ql = QueryList.retrieve()
    print(ql)
    # embed()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
