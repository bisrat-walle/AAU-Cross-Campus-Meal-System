# copyright 2004-2014 LOGILAB S.A. (Paris, FRANCE), all rights reserved.
# contact http://www.logilab.fr/ -- mailto:contact@logilab.fr
#
# This file is part of yams.
#
# yams is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 2.1 of the License, or (at your option)
# any later version.
#
# yams is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License along
# with yams. If not, see <http://www.gnu.org/licenses/>.
"""ER schema loader.

Use either a sql derivated language for entities and relation definitions
files or a direct python definition file.
"""

import sys
import os
import types
import pkgutil
import _frozen_importlib_external
from os import listdir
from os.path import dirname, exists, join, splitext, basename, abspath, realpath
from warnings import warn
from typing import Any, List, Tuple, Dict, Callable, Sequence, Optional, Type, cast

from logilab.common import tempattr, deprecation
from logilab.common.modutils import modpath_from_file, cleanup_sys_modules, clean_sys_modules

from yams import BadSchemaDefinition
from yams import constraints, schema as schemamod
from yams import buildobjs

import yams.types as yams_types

__docformat__: str = "restructuredtext en"

CONSTRAINTS: Dict[str, Callable[..., constraints.BaseConstraint]] = {}

# add constraint classes to the context
for object_name in dir(constraints):
    if object_name[0] == "_":
        continue

    object_ = getattr(constraints, object_name)

    try:
        if issubclass(object_, constraints.BaseConstraint) and (
            object_ is not constraints.BaseConstraint
        ):
            CONSTRAINTS[object_name] = object_
    except TypeError:
        continue


@deprecation.argument_renamed(old_name="erdefs", new_name="entity_relation_definitions")
@deprecation.argument_renamed(
    old_name="remove_unused_rtypes", new_name="remove_unused_relation_types"
)
def fill_schema(
    schema: yams_types.Schema,
    entity_relation_definitions: Dict,
    register_base_types: bool = True,
    remove_unused_relation_types: bool = False,
    post_build_callbacks: List[Callable[[Any], Any]] = [],
) -> yams_types.Schema:

    if register_base_types:
        buildobjs.register_base_types(schema)

    # relation definitions may appear multiple times
    entity_relation_definitions_values: set = set(entity_relation_definitions.values())

    # register relation types and non final entity types
    for definition in entity_relation_definitions_values:
        if isinstance(definition, type):
            definition = definition()

        if isinstance(definition, buildobjs.RelationType):
            schema.add_relation_type(definition)

        elif isinstance(definition, buildobjs.EntityType):
            schema.add_entity_type(definition)

    # register relation definitions
    for definition in entity_relation_definitions_values:
        if isinstance(definition, type):
            definition = definition()

        definition.expand_relation_definitions(entity_relation_definitions, schema)

    # call 'post_build_callback' functions found in schema modules
    for callback in post_build_callbacks:
        callback(schema)

    # finalize schema
    schema.finalize()

    # check permissions are valid on entities and relations
    for entities_and_relations_schema in schema.entities() + schema.relations():  # type: ignore
        entities_and_relations_schema.check_permission_definitions()

    # check unique together consistency
    for entity_schema in schema.entities():
        entity_schema.check_unique_together()

    # optionaly remove relation types without definitions
    if remove_unused_relation_types:
        for relation_schema in schema.relations():
            if not relation_schema.relation_definitions:
                schema.del_relation_type(relation_schema)

    return schema


@deprecation.attribute_renamed(old_name="schemacls", new_name="schema_class")
@deprecation.attribute_renamed(old_name="extrapath", new_name="extra_path")
class SchemaLoader:
    """the schema loader is responsible to build a schema object from a
    set of files
    """

    schema_class: Type[schemamod.Schema] = schemamod.Schema
    extra_path: Optional[str] = None
    context: Dict[str, Callable] = dict(
        [(attr, getattr(buildobjs, attr)) for attr in buildobjs.__all__]
    )
    context.update(CONSTRAINTS)

    @deprecation.argument_renamed(
        old_name="remove_unused_rtypes", new_name="remove_unused_relation_types"
    )
    @deprecation.argument_renamed(old_name="modnames", new_name="module_names")
    def load(
        self,
        module_names: Sequence[Tuple[Any, str]],
        name: Optional[str] = None,
        register_base_types: bool = True,
        construction_mode: str = "strict",
        remove_unused_relation_types: bool = True,
    ) -> yams_types.Schema:
        """return a schema from the schema definition read from <module_names> (a
        list of (PACKAGE, module_name))
        """

        self.defined: Dict = {}
        self.loaded_files: List = []
        self.post_build_callbacks: List = []
        # mypy: Module has no attribute "context"
        sys.modules[__name__].context = self  # type: ignore

        # ensure we don't have an iterator
        module_names = tuple(module_names)

        # legacy usage using a directory list
        is_directories = module_names and not isinstance(module_names[0], (list, tuple))
        try:
            if is_directories:
                warn("provide a list of modules names instead of directories", DeprecationWarning)
                self._load_definition_files(module_names)  # type: ignore # retrocompat situation

            else:
                self._load_module_names(module_names)

            schema = self.schema_class(name or "NoName", construction_mode=construction_mode)
            # if construction_mode != "strict" handle errors

            try:
                fill_schema(
                    schema,
                    self.defined,
                    register_base_types,
                    remove_unused_relation_types=remove_unused_relation_types,
                    post_build_callbacks=self.post_build_callbacks,
                )
            except Exception as exception:
                if not hasattr(exception, "schema_files"):
                    # mypy: "Exception" has no attribute "schema_files"
                    # XXX looks like a hack to transport information
                    exception.schema_files = self.loaded_files  # type: ignore

                raise
        finally:
            # cleanup sys.modules from schema modules
            # ensure we're only cleaning schema [sub]modules
            if is_directories:
                directories = [
                    (
                        not directory.endswith(  # type: ignore
                            os.sep + self.main_schema_directory  # type: ignore # retrocompat
                        )
                        and join(directory, self.main_schema_directory)  # type: ignore
                        or directory
                    )
                    for directory in module_names
                ]

                cleanup_sys_modules(directories)

            else:
                clean_sys_modules([mname for _, mname in module_names])

        # mypy: "Schema" has no attribute "loaded_files"
        # another dynamic attribute
        schema.loaded_files = self.loaded_files  # type: ignore

        return schema

    def _load_definition_files(self, directories: Sequence[str]) -> None:
        for directory in directories:
            package = basename(directory)

            for file_path in self.get_schema_files(directory):
                with tempattr(buildobjs, "PACKAGE", package):
                    self.handle_file(file_path, None)

    @deprecation.argument_renamed(old_name="modnames", new_name="module_names")
    def _load_module_names(self, module_names: Sequence[Tuple[Any, str]]) -> None:
        for package, module_name in module_names:
            loader = pkgutil.find_loader(module_name)

            if loader is None:
                continue

            assert isinstance(loader, _frozen_importlib_external.FileLoader)
            file_path = loader.get_filename()

            if file_path.endswith(".pyc"):
                # check that related source file exists and ensure passing a
                # .py file to exec_file()
                file_path = file_path[:-1]

                if not exists(file_path):
                    continue

            with tempattr(buildobjs, "PACKAGE", package):
                self.handle_file(file_path, module_name=module_name)

    _load_modnames = deprecation.callable_renamed(
        old_name="_load_modnames", new_function=_load_module_names
    )

    # has to be overridable sometimes (usually for test purpose)
    main_schema_directory: str = "schema"

    def get_schema_files(self, directory: str) -> List[str]:
        """return an ordered list of files defining a schema

        look for a schema.py file and or a schema sub-directory in the given
        directory
        """
        result = []

        if exists(join(directory, self.main_schema_directory + ".py")):
            result = [join(directory, self.main_schema_directory + ".py")]

        if exists(join(directory, self.main_schema_directory)):
            directory = join(directory, self.main_schema_directory)

            for filename in listdir(directory):
                if filename[0] == "_":
                    if filename == "__init__.py":
                        result.insert(0, join(directory, filename))

                    continue

                extension = splitext(filename)[1]

                if extension == ".py":
                    result.append(join(directory, filename))
                else:
                    self.unhandled_file(join(directory, filename))

        return result

    @deprecation.argument_renamed(old_name="filepath", new_name="file_path")
    @deprecation.argument_renamed(old_name="modname", new_name="module_name")
    def handle_file(self, file_path: str, module_name: Optional[str] = None) -> None:
        """handle a partial schema definition file according to its extension"""
        assert file_path.endswith(".py"), "not a python file"

        if file_path not in self.loaded_files:
            module_name, module = self.exec_file(file_path, module_name)
            objects_to_add = set()

            for name, object_ in vars(module).items():
                if (
                    isinstance(object_, type)
                    and issubclass(object_, buildobjs.Definition)
                    and object_.__module__ == module_name
                    and not name.startswith("_")
                ):
                    objects_to_add.add(object_)

            for object_ in objects_to_add:
                self.add_definition(object_, file_path)

            if hasattr(module, "post_build_callback"):
                # mypy: Module has no attribute "post_build_callback"
                # it is tested just before in the if
                self.post_build_callbacks.append(module.post_build_callback)  # type: ignore

            self.loaded_files.append(file_path)

    @deprecation.argument_renamed(old_name="filepath", new_name="file_path")
    def unhandled_file(self, file_path: str) -> None:
        """called when a file without handler associated has been found,
        does nothing by default.
        """

    @deprecation.argument_renamed(old_name="filepath", new_name="file_path")
    @deprecation.argument_renamed(old_name="defobject", new_name="definition_object")
    def add_definition(
        self, definition_object: Type[buildobjs.Definition], file_path: Optional[str] = None
    ) -> None:
        """file handler callback to add a definition object

        wildcard capability force to load schema in two steps : first register
        all definition objects (here), then create actual schema objects (done in
        `_build_schema`)
        """
        if not issubclass(definition_object, buildobjs.Definition):
            raise BadSchemaDefinition(file_path, "invalid definition object")

        definition_object.expand_type_definitions(self.defined)

    @deprecation.argument_renamed(old_name="filepath", new_name="file_path")
    @deprecation.argument_renamed(old_name="modname", new_name="module_name")
    def exec_file(self, file_path: str, module_name: Optional[str]) -> Tuple[str, types.ModuleType]:
        if module_name is None:
            try:
                module_name = ".".join(modpath_from_file(file_path, self.extra_path))
            except ImportError:
                warn(
                    "module for %s can't be found, add necessary __init__.py "
                    "files to make it importable" % file_path,
                    DeprecationWarning,
                )

                module_name = splitext(basename(file_path))[0]

        cast(str, module_name)

        if module_name in sys.modules:
            module: types.ModuleType = sys.modules[module_name]

            # NOTE: don't test raw equality to avoid .pyc / .py comparisons
            mpath: str = realpath(abspath(module.__file__))
            fpath: str = realpath(abspath(file_path))

            assert mpath.startswith(fpath), (module_name, file_path, module.__file__)

        else:
            file_globals: Dict[str, str] = {}  # self.context.copy()
            file_globals["__file__"] = file_path
            file_globals["__name__"] = module_name

            package: str = ".".join(module_name.split(".")[:-1])

            if package and package not in sys.modules:
                __import__(package)

            with open(file_path) as f:
                try:
                    code = compile(f.read(), file_path, "exec")
                    exec(code, file_globals)
                except Exception:
                    print("exception while reading %s" % file_path, file=sys.stderr)
                    raise

            file_globals["__file__"] = file_path

            module = types.ModuleType(str(module_name))
            module.__dict__.update(file_globals)

            sys.modules[module_name] = module

            if package:
                setattr(sys.modules[package], module_name.split(".")[-1], module)

            if basename(file_path) == "__init__.py":
                # add __path__ to make dynamic loading work as defined in PEP 302
                # https://www.python.org/dev/peps/pep-0302/#packages-and-the-role-of-path
                module.__path__ = [dirname(file_path)]  # type: ignore # dynamic attribute

        return (module_name, module)


def fill_schema_from_namespace(
    schema: yams_types.Schema, items: Sequence[Tuple[Any, Any]], **kwargs
) -> None:
    entity_relation_definitions: Dict = {}

    for _, object_ in items:
        if (
            isinstance(object_, type)
            and issubclass(object_, buildobjs.Definition)
            and object_
            not in (buildobjs.Definition, buildobjs.RelationDefinition, buildobjs.EntityType)
        ):
            object_.expand_type_definitions(entity_relation_definitions)

    fill_schema(schema, entity_relation_definitions, **kwargs)


def build_schema_from_namespace(items: Sequence[Tuple[Any, Any]]) -> schemamod.Schema:
    schema = schemamod.Schema("noname")

    fill_schema_from_namespace(schema, items)

    return schema


class _Context:
    def __init__(self) -> None:
        self.defined: Dict = {}


context: _Context = _Context()
