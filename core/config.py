"""
Tools for validating and parsing a pipeline configuration and/or manifest file based on the corresponding jsonschema
"""
import json
import jsonschema
import os

from .errors import BaseTapisPipelinesError, ManifestFormatError, PipelineConfigFormatError


HERE = os.path.dirname(os.path.abspath(__file__))

# load the pipeline config schema
pipeline_schema = json.load(open(os.path.join(HERE, 'configschema.json'), 'r'))

# load the manifest file schema
manifest_schema = json.load(open(os.path.join(HERE, 'manifest_schema.json'), 'r'))


class Config(dict):
    """
    A class containing an pipeline's config or manifest file, as a Python dictionary, with getattr and setattr defined
    to make attribute access work like a "normal" object. One should use the parse_manifest_file() or the
    parse_pipeline_config() helper functions to generate Config objects.

    Example usage:
    ~~~~~~~~~~~~~~
    from config import parse_pipeline_config
    manifest = parse_pipeline_config('/path/to/manifest.json')  # <- some manifest file
    manifest.some_key <-- AttributeError raised if some_key (optional) not defined
    """

    def __getattr__(self, key):
        # returning an AttributeError is important for making deepcopy work. cf.,
        # http://stackoverflow.com/questions/25977996/supporting-the-deep-copy-operation-on-a-custom-class
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e)

    def __setattr__(self, key, value):
        self[key] = value

    @classmethod
    def get_config_from_bytes(cls, b, schema):
        """
        Return a config object from a bytes object, b.
        :param d:
        :return:
        """
        try:
            data = json.loads(b.decode('utf-8').replace('\n', ''))
        except Exception as e:
            msg = f'Could not load JSON from bytes: {b}; expcetion: {e}'
            print(msg)
            raise BaseTapisPipelinesError(msg)
        try:
            jsonschema.validate(instance=data, schema=schema)
        except jsonschema.SchemaError as e:
            msg = f'Config not valid; exception: {e}'
            raise BaseTapisPipelinesError(msg)
        return data

    @classmethod
    def get_config_from_file(cls, path, schema):
        """
        Reads config object from a JSON file at path, `path`
        :return:
        """
        if os.path.exists(path):
            try:
                file_config = json.load(open(path, 'r'))
            except Exception as e:
                msg = f'Could not load configs from JSON file at: {path}. exception: {e}'
                print(msg)
                raise BaseTapisPipelinesError(msg)
        else:
            raise BaseTapisPipelinesError(f"Invalid configuration: could not load config file at path: {path}; "
                                          f"file not found.")
        try:
            jsonschema.validate(instance=file_config, schema=schema)
        except jsonschema.SchemaError as e:
            msg = f'Config from file {path} not valid; json: {file_config}; exception: {e}'
            raise BaseTapisPipelinesError(msg)
        return file_config


# ----------
# Utilities
# ----------

def parse_manifest_file(path_to_manifest):
    """
    Parses the manifest.json file and returns a python object representing the manifest contents, if successful, and
    raises a ManifestFormatError if validation fails.
    :param path_to_manifest: string file path to manifest file.
    :return:
    """
    try:
        return Config(Config.get_config_from_file(path_to_manifest, manifest_schema))
    except BaseTapisPipelinesError as e:
        raise ManifestFormatError(e.msg)


def parse_manifest_bytes(manifest_bytes):
    """
    Parses a manifest bytestream and returns a python object representing the manifest contents, if successful, and
    aises a ManifestFormatError if validation fails.
    :param manifest_bytes:
    :return:
    """
    try:
        return Config(Config.get_config_from_bytes(manifest_bytes, manifest_schema))
    except BaseTapisPipelinesError as e:
        raise ManifestFormatError(e.msg)


def parse_pipeline_config(path_to_pipeline_config):
    """
    Parses a pipeline_config.json file and returns a python object representing the manifest contents, if successful, and
    raises a ManifestFormatError if validation fails.
    :param path_to_pipeline_config:
    :return:
    """
    try:
        return Config(Config.get_config_from_file(path_to_pipeline_config, pipeline_schema))
    except BaseTapisPipelinesError as e:
        raise PipelineConfigFormatError(e.msg)
