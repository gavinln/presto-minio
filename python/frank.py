import click

# python frank.py presto hello


@click.group()
def cli():
    click.echo('in group')


@cli.group()
def presto():
    ' presto database commands '
    click.echo('presto')


@cli.group()
def hive():
    ' hive database commands '
    click.echo('hive')


@presto.command()
@click.option('--count', default=1, help='Number of greetings.')
@click.option('--name', prompt='Your name', help='The person to greet.')
def hello(count, name):
    'Simple program that greets NAME for a total of COUNT times.'
    for x in range(count):
        click.echo('Hello %s!' % name)


if __name__ == '__main__':
    cli()
