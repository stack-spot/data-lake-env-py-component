import click
from plugin.usecase.datalake.main import DataLakeUseCase
from plugin.infrastructure.resource.aws.cdk.main import AwsCdk
from plugin.domain.manifest import Manifest



@click.group()
def create():
    pass # We just need a click.group to create our command


@create.command('datalake')
@click.option('-f', '--file', 'path')
def create_datalake(path: str):
    manifest = Manifest(manifest=path)
    DataLakeUseCase(AwsCdk()).create(manifest.datalake)
