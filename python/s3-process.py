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
import pyarrow as pa

import s3fs

from pyarrow import fs

import boto3

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


def read_pafs_file(pafs, file_name):
    f = pafs.open_input_file(file_name)
    data = f.readall()
    f.close()
    return data


def read_pafs_stream(pafs, file_name):
    f = pafs.open_input_stream(file_name)
    data = f.readall()
    f.close()
    return data


def print_file_info(file_system, file_selector):
    file_info_list = file_system.get_file_info(file_selector)
    print('file info for base dir {}'.format(file_selector.base_dir))
    for file_info in file_info_list:
        print('\t' + file_info.path)


def print_boto3_buckets(endpoint_url):
    session = boto3.session.Session()

    s3_client = session.client(
        service_name='s3',
        endpoint_url=endpoint_url
    )
    buckets_dict = s3_client.list_buckets()
    buckets_list = [bucket['Name'] for bucket in buckets_dict['Buckets']]
    print(buckets_list)


def print_parquet_pandas_shape(bucket_uri, file_system):
    dataset = pq.ParquetDataset(bucket_uri, filesystem=file_system)
    table = dataset.read()
    df = table.to_pandas()
    print(df.shape)


def print_parquet_dataset_info(bucket_uri, file_system, verbose=False):
    dataset = pq.ParquetDataset(
        bucket_uri, filesystem=file_system, use_legacy_dataset=False)
    print('{} pieces'.format(len(dataset.pieces)))
    row_count = 0
    for idx, piece in enumerate(dataset.pieces):
        piece.ensure_complete_metadata()
        if verbose:
            print('\t{} {}'.format(idx, piece.path))
            print('\tThere are {} row groups'.format(len(piece.row_groups)))
        for idx2, row_group in enumerate(piece.row_groups):
            if verbose:
                print('\t\t{} rows={}'.format(idx2, row_group.num_rows))
            row_count += row_group.num_rows
    print('Total rows {}'.format(row_count))


def main2():
    read_s3_dask()
    file_parquet = 'airline-parq/'
    print(file_parquet)

    # By default, MinIO will listen for unencrypted HTTP traffic.
    minio = fs.S3FileSystem(scheme="http", endpoint_override="10.0.0.2:9000")

    # List all contents in a bucket, recursively
    file_selector = fs.FileSelector('customer-data-text', recursive=True)
    print_file_info(minio, file_selector)

    print(read_pafs_file(minio, 'customer-data-text/customer.csv'))
    print(read_pafs_stream(minio, 'customer-data-text/customer.csv'))

    endpoint_url = 'http://10.0.0.2:9000'
    print_boto3_buckets(endpoint_url)

    # TODO: read multiple files using dataset

    # https://stackoverflow.com/questions/45082832/how-to-read-partitioned-parquet-files-from-s3-using-pyarrow-in-python
    client_kwargs = {
        'endpoint_url': 'http://10.0.0.2:9000'
    }

    file_system = s3fs.S3FileSystem(client_kwargs=client_kwargs)
    print(file_system.ls('example-data'))

    bucket_uri = 's3://example-data/external-data'
    print_parquet_pandas_shape(bucket_uri, file_system)
    print_parquet_dataset_info(bucket_uri, file_system, verbose=False)

    bucket_uri = 's3://example-data/external-clustered'
    print_parquet_pandas_shape(bucket_uri, file_system)
    print_parquet_dataset_info(bucket_uri, file_system, verbose=False)


def main():
    client_kwargs = {
        'endpoint_url': 'http://10.0.0.2:9000'
    }

    bucket_uri = 's3://example-data/external-clustered'
    file_sys = s3fs.S3FileSystem(client_kwargs=client_kwargs)
    files = file_sys.ls(bucket_uri)
    row_count = 0
    for file_name in files:
        f = file_sys.open(file_name)
        pq_file = pq.ParquetFile(f)
        row_count += pq_file.metadata.num_rows
        f.close()
    print('{:,}'.format(row_count))


if __name__ == '__main__':
    main()
    import timeit
    # print(timeit.timeit("main()", setup="from __main__ import main",
    #       number=20))
