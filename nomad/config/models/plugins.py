#
# Copyright The NOMAD Authors.
#
# This file is part of NOMAD. See https://nomad-lab.eu for further info.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import sys
import shutil
from tempfile import TemporaryDirectory
from abc import ABCMeta, abstractmethod
import importlib
from typing import Optional, Dict, Union, List, Literal, TYPE_CHECKING
import requests
from pydantic import BaseModel, Field, root_validator

from nomad.common import get_package_path

from .common import Options
from .ui import App

example_upload_path_prefix = '__example_uploads__'

if TYPE_CHECKING:
    from nomad.metainfo import SchemaPackage
    from nomad.normalizing import Normalizer as NormalizerBaseClass
    from nomad.parsing import Parser as ParserBaseClass


class EntryPoint(BaseModel):
    """Base model for a NOMAD plugin entry points."""

    id: Optional[str] = Field(
        description='Unique identifier corresponding to the entry point name. Automatically set to the plugin entry point name in pyproject.toml.'
    )
    entry_point_type: str = Field(description='Determines the entry point type.')
    name: Optional[str] = Field(description='Name of the plugin entry point.')
    description: Optional[str] = Field(
        description='A human readable description of the plugin entry point.'
    )
    plugin_package: Optional[str] = Field(
        description='The plugin package from which this entry points comes from.'
    )

    def dict_safe(self):
        """Used to serialize the non-confidential parts of a plugin model. This
        function can be overridden in subclasses to expose more information.
        """
        return self.dict(include=EntryPoint.__fields__.keys(), exclude_none=True)


class AppEntryPoint(EntryPoint):
    """Base model for app plugin entry points."""

    entry_point_type: Literal['app'] = Field(
        'app', description='Determines the entry point type.'
    )
    app: App = Field(description='The app configuration.')

    def dict_safe(self):
        return self.dict(include=AppEntryPoint.__fields__.keys(), exclude_none=True)


class SchemaPackageEntryPoint(EntryPoint, metaclass=ABCMeta):
    """Base model for schema package plugin entry points."""

    entry_point_type: Literal['schema_package'] = Field(
        'schema_package', description='Specifies the entry point type.'
    )

    @abstractmethod
    def load(self) -> 'SchemaPackage':
        """Used to lazy-load a schema package instance. You should override this
        method in your subclass. Note that any Python module imports required
        for the schema package should be done within this function as well."""
        pass


class NormalizerEntryPoint(EntryPoint, metaclass=ABCMeta):
    """Base model for normalizer plugin entry points."""

    entry_point_type: Literal['normalizer'] = Field(
        'normalizer', description='Determines the entry point type.'
    )
    level: int = Field(
        0,
        description="""
        Integer that determines the execution order of this normalizer. Normalizers are
        run in order from lowest level to highest level.
        """,
    )

    @abstractmethod
    def load(self) -> 'NormalizerBaseClass':
        """Used to lazy-load a normalizer instance. You should override this
        method in your subclass. Note that any Python module imports required
        for the normalizer class should be done within this function as well."""
        pass


class ParserEntryPoint(EntryPoint, metaclass=ABCMeta):
    """Base model for parser plugin entry points."""

    entry_point_type: Literal['parser'] = Field(
        'parser', description='Determines the entry point type.'
    )
    level: int = Field(
        0,
        description="""
        Integer that determines the execution order of this parser. Parser with lowest
        level will attempt to match raw files first.
    """,
    )
    aliases: List[str] = Field([], description="""List of alternative parser names.""")
    mainfile_contents_re: Optional[str] = Field(
        description="""
        A regular expression that is applied the content of a potential mainfile.
        If this expression is given, the parser is only considered for a file, if the
        expression matches.
    """
    )
    mainfile_name_re: str = Field(
        r'.*',
        description="""
        A regular expression that is applied the name of a potential mainfile.
        If this expression is given, the parser is only considered for a file, if the
        expression matches.
    """,
    )
    mainfile_mime_re: str = Field(
        r'.*',
        description="""
        A regular expression that is applied the mime type of a potential
        mainfile. If this expression is given, the parser is only considered
        for a file, if the expression matches.
    """,
    )
    mainfile_binary_header: Optional[bytes] = Field(
        description="""
        Matches a binary file if the given bytes are included in the file.
    """,
    )
    mainfile_binary_header_re: Optional[bytes] = Field(
        description="""
        Matches a binary file if the given binary regular expression bytes matches the
        file contents.
    """,
    )
    mainfile_alternative: bool = Field(
        False,
        description="""
        If True, the parser only matches a file, if no other file in the same directory
        matches a parser.
    """,
    )
    mainfile_contents_dict: Optional[dict] = Field(
        description="""
        Is used to match structured data files like JSON or HDF5.
    """
    )
    supported_compressions: List[str] = Field(
        [],
        description="""
        Files compressed with the given formats (e.g. xz, gz) are uncompressed and
        matched like normal files.
    """,
    )

    @abstractmethod
    def load(self) -> 'ParserBaseClass':
        """Used to lazy-load a parser instance. You should override this method
        in your subclass. Note that any Python module imports required for the
        parser class should be done within this function as well."""
        pass

    def dict_safe(self):
        # The binary data types are removed from the safe serialization: binary
        # data is not JSON serializable.
        keys = set(list(ParserEntryPoint.__fields__.keys()))
        keys.remove('mainfile_binary_header_re')
        keys.remove('mainfile_binary_header')

        return self.dict(include=keys, exclude_none=True)


class ExampleUploadEntryPoint(EntryPoint):
    """Base model for example upload plugin entry points."""

    entry_point_type: Literal['example_upload'] = Field(
        'example_upload', description='Determines the entry point type.'
    )
    category: str = Field(description='Category for the example upload.')
    title: str = Field(description='Title of the example upload.')
    description: str = Field(description='Longer description of the example upload.')
    path: Optional[str] = Field(
        description="""
        Path to the example upload contents folder. Should be a path that starts
        from the package root folder, e.g. 'example_uploads/my_upload'.
        """,
    )
    url: Optional[str] = Field(
        description="""
        URL that points to an online file. If you use this instead of 'path',
        the file will be downloaded once upon app startup.
        """,
    )
    local_path: Optional[str] = Field(
        description="""
        The final path to use when creating the upload. This field will be
        automatically generated by the 'load' function and is typically not set
        manually.
        """
    )

    @root_validator(pre=True)
    def _validate(cls, values):
        """Checks that only either path or url is given."""
        path = values.get('path')
        url = values.get('url')
        local_path = values.get('local_path')
        if path and url:
            raise ValueError('Provide only "path" or "url", not both.')
        if not path and not url and not local_path:
            raise ValueError('Provide one of "path", "url" or "local_path".')

        return values

    def load(self) -> None:
        """Used to initialize the example upload data and set the final upload
        path.

        By default this function will simply populate the 'upload_path' variable
        based on 'path' or 'url', but you may also overload this function in
        your plugin to perform completely custom data loading, after which you
        set the 'upload_path'. Note that you should store any data you fetch
        directly into your plugin installation folder (you can use
        'get_package_path' to fetch the location), because only that folder is
        accessible by the upload API. This function is called once upon app
        startup.
        """
        path = self.path
        # If local path is already set, use it
        if self.local_path:
            return
        # Create local path from given path or url
        if not path and self.url:
            final_folder = os.path.join(
                get_package_path(self.plugin_package), 'example_uploads'
            )
            filename = self.url.rsplit('/')[-1]
            final_filepath = os.path.join(final_folder, filename)

            if not os.path.exists(final_filepath):
                try:
                    with requests.get(self.url, stream=True) as response:
                        response.raise_for_status()
                        # Download into a temporary directory to ensure the integrity of
                        # the download
                        with TemporaryDirectory() as tmp_folder:
                            tmp_filepath = os.path.join(tmp_folder, filename)
                            with open(tmp_filepath, mode='wb') as file:
                                for chunk in response.iter_content(
                                    chunk_size=10 * 1024
                                ):
                                    file.write(chunk)
                            # If download has succeeeded, copy the files over to
                            # final location
                            os.makedirs(final_folder, exist_ok=True)
                            shutil.copy(tmp_filepath, final_filepath)
                except requests.RequestException as e:
                    raise ValueError(
                        f'Could not fetch the example upload from URL: {self.url}'
                    ) from e

            path = os.path.join('example_uploads', filename)

        if not path:
            raise ValueError('No valid path or URL provided for example upload.')

        prefix = os.path.join(example_upload_path_prefix, self.plugin_package)
        self.local_path = os.path.join(prefix, path)

    def dict_safe(self):
        return self.dict(
            include=ExampleUploadEntryPoint.__fields__.keys(), exclude_none=True
        )


class PluginBase(BaseModel):
    """
    Base model for a NOMAD plugin.

    This should not be used. Plugins should instantiate concrete Plugin models like
    Parser or Schema.
    """

    plugin_type: str = Field(
        description='The type of the plugin.',
    )
    id: Optional[str] = Field(description='The unique identifier for this plugin.')
    name: str = Field(
        description='A short descriptive human readable name for the plugin.'
    )
    description: Optional[str] = Field(
        description='A human readable description of the plugin.'
    )
    plugin_documentation_url: Optional[str] = Field(
        description='The URL to the plugins main documentation page.'
    )
    plugin_source_code_url: Optional[str] = Field(
        description='The URL of the plugins main source code repository.'
    )

    def dict_safe(self):
        """Used to serialize the non-confidential parts of a plugin model. This
        function can be overridden in subclasses to expose more information.
        """
        return self.dict(include=PluginBase.__fields__.keys(), exclude_none=True)


class PythonPluginBase(PluginBase):
    """
    A base model for NOMAD plugins that are implemented in Python.
    """

    python_package: str = Field(
        description="""
        Name of the python package that contains the plugin code and a
        plugin metadata file called `nomad_plugin.yaml`.
    """
    )

    def import_python_package(self):
        if not self.python_package:
            raise ValueError('Python plugins must provide a python_package.')
        importlib.import_module(self.python_package)


class Schema(PythonPluginBase):
    """
    A Schema describes a NOMAD Python schema that can be loaded as a plugin.
    """

    package_path: Optional[str] = Field(
        description='Path of the plugin package. Will be determined using python_package if not explicitly defined.'
    )
    key: Optional[str] = Field(description='Key used to identify this plugin.')
    plugin_type: Literal['schema'] = Field(
        'schema',
        description="""
        The type of the plugin. This has to be the string `schema` for schema plugins.
    """,
    )


class Normalizer(PythonPluginBase):
    """
    A Normalizer describes a NOMAD normalizer that can be loaded as a plugin.
    """

    normalizer_class_name: str = Field(
        description="""
        The fully qualified name of the Python class that implements the normalizer.
        This class must have a function `def normalize(self, logger)`.
    """
    )
    plugin_type: Literal['normalizer'] = Field(
        'normalizer',
        description="""
        The type of the plugin. This has to be the string `normalizer` for normalizer plugins.
    """,
    )


class Parser(PythonPluginBase):
    """
    A Parser describes a NOMAD parser that can be loaded as a plugin.

    The parser itself is referenced via `python_name`. For Parser instances `python_name`
    must refer to a Python class that has a `parse` function. The other properties are
    used to create a `MatchingParserInterface`. This comprises general metadata that
    allows users to understand what the parser is, and metadata used to decide if a
    given file "matches" the parser.
    """

    # TODO the nomad_plugin.yaml for each parser needs some cleanup. The way parser metadata
    #      is presented in the UIs should be rewritten
    # TODO ideally we can somehow load parser plugin models lazily. Right now importing
    #      config will open all `nomad_plugin.yaml` files. But at least there is no python import
    #      happening.
    # TODO this should fully replace MatchingParserInterface
    # TODO most actual parser do not implement any abstract class. The Parser class has an
    #      abstract is_mainfile, which does not allow to separate parser implementation and plugin
    #      definition.

    plugin_type: Literal['parser'] = Field(
        'parser',
        description="""
        The type of the plugin. This has to be the string `parser` for parser plugins.
        """,
    )
    parser_class_name: str = Field(
        description="""
        The fully qualified name of the Python class that implements the parser.
        This class must have a function `def parse(self, mainfile, archive, logger)`.
        """
    )
    parser_as_interface: bool = Field(
        False,
        description="""
        By default the parser metadata from this config (and the loaded nomad_plugin.yaml)
        is used to instantiate a parser interface that is lazy loading the actual parser
        and performs the mainfile matching. If the parser interface matching
        based on parser metadata is not sufficient and you implemented your own
        is_mainfile parser method, this setting can be used to use the given
        parser class directly for parsing and matching.
        """,
    )
    mainfile_contents_re: Optional[str] = Field(
        description="""
        A regular expression that is applied the content of a potential mainfile.
        If this expression is given, the parser is only considered for a file, if the
        expression matches.
        """
    )
    mainfile_name_re: str = Field(
        r'.*',
        description="""
        A regular expression that is applied the name of a potential mainfile.
        If this expression is given, the parser is only considered for a file, if the
        expression matches.
        """,
    )
    mainfile_mime_re: str = Field(
        r'text/.*',
        description="""
        A regular expression that is applied the mime type of a potential mainfile.
        If this expression is given, the parser is only considered for a file, if the
        expression matches.
        """,
    )
    mainfile_binary_header: Optional[bytes] = Field(
        description="""
        Matches a binary file if the given bytes are included in the file.
        """
    )
    mainfile_binary_header_re: Optional[bytes] = Field(
        description="""
        Matches a binary file if the given binary regular expression bytes matches the
        file contents.
        """
    )
    mainfile_alternative: bool = Field(
        False,
        description="""
        If True, the parser only matches a file, if no other file in the same directory
        matches a parser.
        """,
    )
    mainfile_contents_dict: Optional[dict] = Field(
        description="""
        Is used to match structured data files like JSON, HDF5 or csv/excel files. In case of a csv/excel file
        for example, in order to check if certain columns exist in a given sheet, one can set this attribute to
        `{'<sheet name>': {'__has_all_keys': [<column names>]}}`. In case the csv/excel file contains comments that
        are supposed to be ignored, use this reserved key-value pair
        `'__comment_symbol': '<symbol>'` at the top level of the dict right next to the <sheet name>.
        """
    )
    supported_compressions: List[str] = Field(
        [],
        description="""
        Files compressed with the given formats (e.g. xz, gz) are uncompressed and
        matched like normal files.
        """,
    )
    domain: str = Field(
        'dft',
        description="""
        The domain value `dft` will apply all normalizers for atomistic codes. Deprecated.
        """,
    )
    level: int = Field(
        0,
        description="""
        The order by which the parser is executed with respect to other parsers.
        """,
    )
    code_name: Optional[str]
    code_homepage: Optional[str]
    code_category: Optional[str]
    metadata: Optional[dict] = Field(
        description="""
        Metadata passed to the UI. Deprecated."""
    )

    def create_matching_parser_interface(self):
        if self.parser_as_interface:
            from nomad.parsing.parser import import_class

            Parser = import_class(self.parser_class_name)
            return Parser()

        from nomad.parsing.parser import MatchingParserInterface

        data = self.dict()
        del data['id']
        del data['description']
        del data['python_package']
        del data['plugin_type']
        del data['parser_as_interface']
        del data['plugin_source_code_url']
        del data['plugin_documentation_url']

        return MatchingParserInterface(**data)


EntryPointType = Union[
    Schema,
    Normalizer,
    Parser,
    SchemaPackageEntryPoint,
    ParserEntryPoint,
    NormalizerEntryPoint,
    AppEntryPoint,
    ExampleUploadEntryPoint,
]


class EntryPoints(Options):
    options: Dict[str, EntryPointType] = Field(
        dict(), description='The available plugin entry points.'
    )


class PluginPackage(BaseModel):
    name: str = Field(
        description='Name of the plugin Python package, read from pyproject.toml.'
    )
    description: Optional[str] = Field(
        description='Package description, read from pyproject.toml.'
    )
    version: Optional[str] = Field(
        description='Plugin package version, read from pyproject.toml.'
    )
    homepage: Optional[str] = Field(
        description='Link to the plugin package homepage, read from pyproject.toml.'
    )
    documentation: Optional[str] = Field(
        description='Link to the plugin package documentation page, read from pyproject.toml.'
    )
    repository: Optional[str] = Field(
        description='Link to the plugin package source code repository, read from pyproject.toml.'
    )
    entry_points: List[str] = Field(
        description='List of entry point ids contained in this package, read form pyproject.toml'
    )


class Plugins(BaseModel):
    entry_points: EntryPoints = Field(
        description='Used to control plugin entry points.'
    )
    plugin_packages: Dict[str, PluginPackage] = Field(
        description="""
        Contains the installed installed plugin packages with the package name
        used as a key. This is autogenerated and should not be modified.
        """
    )


def add_plugin(plugin: Schema) -> None:
    """Function for dynamically adding a plugin."""
    from nomad.config import config
    from nomad.metainfo.elasticsearch_extension import entry_type

    if plugin.package_path not in sys.path:
        sys.path.insert(0, plugin.package_path)

    # Add plugin to config
    config.plugins.entry_points.options[plugin.key] = plugin

    # Add plugin to Package registry
    package = importlib.import_module(plugin.python_package)
    package.m_package.__init_metainfo__()

    # Reload the dynamic quantities so that API is aware of the plugin
    # quantities.
    entry_type.reload_quantities_dynamic()


def remove_plugin(plugin) -> None:
    """Function for removing a plugin."""
    from nomad.config import config
    from nomad.metainfo.elasticsearch_extension import entry_type
    from nomad.metainfo import Package

    # Remove from path
    try:
        sys.path.remove(plugin.package_path)
    except Exception:
        pass

    # Remove package as plugin
    del config.plugins.entry_points.options[plugin.key]

    # Remove plugin from Package registry
    package = importlib.import_module(plugin.python_package).m_package
    for key, i_package in Package.registry.items():
        if i_package is package:
            del Package.registry[key]
            break

    # Reload the dynamic quantities so that API is aware of the plugin
    # quantities.
    entry_type.reload_quantities_dynamic()
