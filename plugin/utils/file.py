import logging

import os
import subprocess
import sys
from tempfile import mkstemp
import yaml


def remove_from_os(path: str):
    if os.path.exists(path):
        subprocess.call(f'rm -rf {path}', shell=True)


def get_temporaryfile(data: str) -> str:
    """
    Returns the path of a temporary file containing the data passed.

        Note: after use, remove this file from operating system. Example::
            os.remove(temporaryfile_path)
    """
    fd, temporaryfile_path = mkstemp()

    with open(temporaryfile_path, 'w', encoding='utf-8') as temporaryfile:
        temporaryfile.write(data)
        os.close(fd)
        return temporaryfile_path


def no_duplicates_constructor(loader, node, deep=False):
    """Check for duplicate keys."""

    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        value = loader.construct_object(value_node, deep=deep)
        if key in mapping:
            msg = f"Duplicate key '{key}' found while constructing a mapping in YAML file. \n" + \
                "Please check its structure:\n" + \
                f"{node.start_mark}\n" + f"{key_node.start_mark}"
            raise yaml.constructor.ConstructorError(msg)
        mapping[key] = value

    return loader.construct_mapping(node, deep)


class DuplicateKeysCheckLoader(yaml.SafeLoader):
    """YAML SafeLoader with duplicate keys checking."""


def read_yaml(file: str) -> dict:
    DuplicateKeysCheckLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        no_duplicates_constructor)

    try:
        if os.path.isfile(os.path.abspath(file)):
            with open(file, 'r', encoding='utf-8') as yml:
                return yaml.load(yml, Loader=DuplicateKeysCheckLoader)
        else:
            temp_file_path = get_temporaryfile(file)
            with open(temp_file_path, 'r', encoding='utf-8') as yml:
                output_dict = yaml.load(yml, Loader=DuplicateKeysCheckLoader)
            remove_from_os(temp_file_path)
            return output_dict
    except FileNotFoundError as error:
        logging.error(error)
        sys.exit(1)
