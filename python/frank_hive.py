import typer

from presto_hive_lib import print_all
from presto_hive_lib import get_hive_databases
from presto_hive_lib import check_hive_database
from presto_hive_lib import get_hive_records_database_like_table

app = typer.Typer()


def get_hive_host_port():
    host = '10.0.0.2'
    host = 'hive.liftoff.io'
    port = 10000
    return host, port


def get_presto_host_port():
    host = '10.0.0.2'
    host = 'presto.liftoff.io'
    port = 8080
    # port = 8889
    return host, port


@app.command()
def show_databases():
    ''' List all databases
    '''
    host, port = get_hive_host_port()
    databases = get_hive_databases(host, port)
    print(databases)


@app.command()
def show_tables(database: str):
    host, port = get_hive_host_port()
    database_valid = check_hive_database(host, port, database)
    sql = 'show tables'
    tables = get_hive_records_database_like_table(host, port, sql,
                                                  database_valid)
    print_all(tables)


@app.command()
def show_table(database: str, table: str):
    typer.echo(f'show table {table}')


@app.command()
def show_columns(database: str, table: str):
    typer.echo(f'show columns {table}')


if __name__ == '__main__':
    app()
