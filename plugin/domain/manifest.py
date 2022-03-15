from dataclasses import dataclass
from plugin.utils.file import read_yaml


@dataclass
class DataLake:
    """
    TODO
    """
    name: str
    region: str

    def __post_init__(self):
        self._is_valid_name()

    def _is_valid_name(self):
        if self.name is None or len(self.name) > 25:
            raise ValueError(
                "'name' must be non empty string less than 25 characters.")
        
        special_characters = '"!@#$%^&*()-+?=,<>/'

        if any(c in special_characters for c in self.name):
            raise ValueError(
                'Datalake name must not contains any of these characters: "!@#$%^&*()-+?=,<>/')


@dataclass
class Manifest:
    """
    TODO
    """
    datalake: DataLake

    def __init__(self, manifest) -> None:
        if isinstance(manifest, str):
            file = read_yaml(manifest)
            datalake_config = file['datalake']
            self.datalake = DataLake(**datalake_config)
        elif isinstance(manifest, dict):
            datalake_config = manifest['datalake']
            self.datalake = DataLake(**datalake_config)
        else:
            raise TypeError
