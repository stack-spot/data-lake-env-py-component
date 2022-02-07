import click
from plugin.entrypoint.cli import create


@click.group()
def cli():
    pass # We just need a click.group to create our command


cli.add_command(create)


if __name__ == '__main__':
    cli()
