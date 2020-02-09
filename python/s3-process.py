import dask.dataframe as dd
from IPython import embed

df = dd.read_csv(
    's3://customer-data-text/customer.csv',
    storage_options={
        "client_kwargs": {
            "endpoint_url": "http://192.168.33.10:9000"
        }
    })

embed()
