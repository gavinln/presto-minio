'''
Example of reading from S3

1. Using Dask
2. Using pyarrow

export AWS_SHARED_CREDENTIALS_FILE=./do_not_checkin/credentials-minio
aws --endpoint=http://10.0.0.2:9000/ s3 ls
python python/s3-process.py

'''

import textwrap

from IPython import embed

# import dask.dataframe as dd
import pyarrow.parquet as pq

from pyarrow import fs

store_opt = {
    "client_kwargs": {
        "endpoint_url": "http://10.0.0.2:9000"
    }
}


def read_s3_dask():
    file_csv = 's3://customer-data-text/customer.csv'

    print(f'reading {file_csv}')
    # df = dd.read_csv(file_csv, storage_options=store_opt)
    # print(df.compute())


def print_parq_file_info(parq_file):
    parq_file = pq.ParquetFile(parq_file)
    print(textwrap.indent(str(parq_file.metadata), prefix='\t'))
    print(textwrap.indent(str(parq_file.schema), prefix='\t'))


def read_pafs_input_file(pafs, file_name):
    f = pafs.open_input_file(file_name)
    print(f.readall())
    f.close()


def read_pafs_input_stream(pafs, file_name):
    f = pafs.open_input_stream(file_name)
    print(f.readall())
    f.close()


def main():
    read_s3_dask()
    file_parquet = 'airline-parq/'
    print(file_parquet)

    # By default, MinIO will listen for unencrypted HTTP traffic.
    minio = fs.S3FileSystem(scheme="http", endpoint_override="10.0.0.2:9000")

    # List all contents in a bucket, recursively
    t1 = fs.FileSelector('customer-data-text', recursive=True)
    # t2 = minio.get_file_info(t1)
    read_pafs_input_file(minio, 'customer-data-text/customer.csv')
    read_pafs_input_stream(minio, 'customer-data-text/customer.csv')

    pq_file = pq.ParquetFile(minio.open_input_file(
        'airline-parq/airline-flights-1987.parq'))

    embed()

    return

    dataset = pq.ParquetDataset(file_parquet, filesystem=minio)
    table = dataset.read()
    print(table)
    # print_parq_file_info(file_parquet)


if __name__ == '__main__':
    main()
