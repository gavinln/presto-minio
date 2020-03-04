import logging
from pathlib import Path
import json
import sqlite3

from datetime import datetime
from typing import List
from typing import Dict

from urllib .parse import urlparse, urlunparse

from pydantic import BaseModel, parse_obj_as

import requests

import pandas as pd

from IPython import embed

SCRIPT_DIR = Path(__file__).parent.resolve()
log = logging.getLogger(__name__)


def retrieve_query_list(netloc=None):
    if netloc is None:
        netloc = '10.0.0.2:8080'

    scheme = 'http'
    path = '/v1/query'

    presto_url = urlunparse((scheme, netloc, path, '', '', ''))
    response = requests.get(presto_url)
    response.raise_for_status()
    return response.json()


def retrieve_query(query_url):
    response = requests.get(query_url)
    response.raise_for_status()
    return response.json()


def get_query_url(query_obj, netloc):
    uo = urlparse(query_obj.self)
    query_url = urlunparse(
        (uo.scheme, netloc, uo.path, '', '', '')
    )
    return query_url


class QuerySummary(BaseModel):
    queryId: str
    state: str
    self: str
    errorType: str = None


class QueryList(list):
    @classmethod
    def retrieve(cls):
        netloc = '10.0.0.2:8080'
        ql = retrieve_query_list(netloc)

        qs_list = parse_obj_as(List[QuerySummary], ql)
        qs = qs_list[2]
        query_url = get_query_url(qs, netloc)
        query = retrieve_query(query_url)
        print(json.dumps(query['inputs']))
        return QueryList(qs_list)

    def to_frame(self):
        return pd.DataFrame.from_records(qs.dict() for qs in self)


def main():
    ql = QueryList.retrieve()
    df = ql.to_frame()
    print(df)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
