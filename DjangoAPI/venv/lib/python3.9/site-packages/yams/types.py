# copyright 2019 LOGILAB S.A. (Paris, FRANCE), all rights reserved.
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
# You should have received a copy of the GNU Lesser General Public License
# along with yams. If not, see <https://www.gnu.org/licenses/>.

"""Types declarations for types annotations"""

import decimal
from typing import Union, TYPE_CHECKING, TypeVar, Dict, Tuple, Callable, Any, NewType


jsonSerializable = TypeVar("jsonSerializable")
Permission = Tuple[str, ...]
Permissions = Dict[str, Permission]
Checkers = Dict[str, Callable[[Any, Any], bool]]
Converters = Dict[str, Callable[[Any], Union[str, bytes, int, float, bool, decimal.Decimal]]]
DefinitionName = NewType("DefinitionName", str)

# to avoid circular imports
if TYPE_CHECKING:
    from yams import schema, buildobjs

    RdefRdefSchema = Union[schema.RelationDefinitionSchema, buildobjs.RelationDefinition]
    RelationDefinitionSchema = schema.RelationDefinitionSchema
    RelationDefinition = buildobjs.RelationDefinition
    Schema = schema.Schema
    EntitySchema = schema.EntitySchema
    EntityType = buildobjs.EntityType
    ERSchema = schema.ERSchema
    RelationSchema = schema.RelationSchema
    RelationType = buildobjs.RelationType

else:
    RdefRdefSchema = TypeVar("rdef_rdefschema")
    RelationDefinitionSchema = TypeVar("RelationDefinitionSchema")
    RelationDefinition = TypeVar("RelationDefinition")
    Schema = TypeVar("Schema")
    EntitySchema = TypeVar("EntitySchema")
    EntityType = TypeVar("EntityType")
    ERSchema = TypeVar("ERSchema")
    RelationSchema = TypeVar("RelationSchema")
    RelationType = TypeVar("RelationType")
    RelationType = TypeVar("RelationType")
