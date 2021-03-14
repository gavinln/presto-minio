import typer


app = typer.Typer()


@app.command()
def show_catalogs():
    typer.echo('show catalogs')


@app.command()
def show_schemas(database: str):
    typer.echo(f'show schemas {database}')


@app.command()
def show_tables(database: str):
    typer.echo(f'show tables {database}')


@app.command()
def show_table(database: str, table: str):
    typer.echo(f'show table {table}')


@app.command()
def show_columns(database: str, table: str):
    typer.echo(f'show columns {table}')


if __name__ == '__main__':
    app()
