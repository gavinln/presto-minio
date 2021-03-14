import typer

import frank_hive
import frank_presto


app = typer.Typer()

app.add_typer(frank_hive.app, name="hive")
app.add_typer(frank_presto.app, name="presto")

if __name__ == '__main__':
    app()
