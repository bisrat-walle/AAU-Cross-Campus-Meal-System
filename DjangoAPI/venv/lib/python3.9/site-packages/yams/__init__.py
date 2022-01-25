# copyright 2004-2013 LOGILAB S.A. (Paris, FRANCE), all rights reserved.
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
"""Object model and utilities to define generic Entities/Relations schemas.
"""
import warnings
from datetime import datetime, date

from typing import Set, Type, Dict, Union, Callable, Any, Iterable, TypeVar

import pkg_resources

from logilab.common.date import strptime_time
from logilab.common import nullobject

from yams._exceptions import SchemaError, UnknownType, BadSchemaDefinition, ValidationError  # noqa
import yams.types as yams_types


__docformat__: str = "restructuredtext en"

__version__: str = pkg_resources.get_distribution("yams").version

_: Type[str] = str

MARKER: nullobject = nullobject()

BASE_TYPES: Set[str] = set(
    (
        "String",
        "Password",
        "Bytes",
        "Int",
        "BigInt",
        "Float",
        "Boolean",
        "Decimal",
        "Date",
        "Time",
        "Datetime",
        "TZTime",
        "TZDatetime",
        "Interval",
    )
)

# base groups used in permissions
BASE_GROUPS: Set[str] = set((_("managers"), _("users"), _("guests"), _("owners")))

# default permissions for entity types, relations and attributes
DEFAULT_ETYPEPERMS: yams_types.Permissions = {
    "read": (
        "managers",
        "users",
        "guests",
    ),
    "update": (
        "managers",
        "owners",
    ),
    "delete": ("managers", "owners"),
    "add": (
        "managers",
        "users",
    ),
}
DEFAULT_RELPERMS: yams_types.Permissions = {
    "read": (
        "managers",
        "users",
        "guests",
    ),
    "delete": ("managers", "users"),
    "add": (
        "managers",
        "users",
    ),
}
DEFAULT_ATTRPERMS: yams_types.Permissions = {
    "read": (
        "managers",
        "users",
        "guests",
    ),
    "add": ("managers", "users"),
    "update": ("managers", "owners"),
}
DEFAULT_COMPUTED_RELPERMS: yams_types.Permissions = {
    "read": (
        "managers",
        "users",
        "guests",
    ),
    "delete": (),
    "add": (),
}
DEFAULT_COMPUTED_ATTRPERMS: yams_types.Permissions = {
    "read": (
        "managers",
        "users",
        "guests",
    ),
    "add": (),
    "update": (),
}

# This provides a way to specify callable objects as default values
# First level is the final type, second is the keyword to callable map
_current_date_or_datetime_constructor_type = Callable[[], Union[date, datetime]]
KEYWORD_MAP: Dict[str, Dict[str, _current_date_or_datetime_constructor_type]] = {
    "Datetime": {"NOW": datetime.now, "TODAY": datetime.today},
    "TZDatetime": {"NOW": datetime.utcnow, "TODAY": datetime.today},
    "Date": {"TODAY": date.today},
}

# bw compat for literal date/time values stored as strings in schemas
DATE_FACTORY_MAP: Dict[str, Callable[[str], Union[datetime, float]]] = {
    "Datetime": lambda x: ":" in x
    and datetime.strptime(x, "%Y/%m/%d %H:%M")
    or datetime.strptime(x, "%Y/%m/%d"),
    "Date": lambda x: datetime.strptime(x, "%Y/%m/%d"),
    "Time": strptime_time,  # returns a float (from time())
}

KNOWN_METAATTRIBUTES: Set[str] = set(("format", "encoding", "name"))


_type_of_default = TypeVar("_type_of_default")


def convert_default_value(
    rdef: yams_types.RdefRdefSchema, default: _type_of_default
) -> Union[_type_of_default, datetime, date, float, str, int, bool]:
    # rdef can be either a .schema.RelationDefinitionSchema
    # or a .buildobjs.RelationDefinition

    # mypy ouput: Item "RelationDefinition" of "Union[RelationDefinitionSchema,
    # mypy output: RelationDefinition]" has no attribute "relation_type"
    # RelationDefinition will succed on having a name so it won't jump to ".relation_type"
    relation_type = getattr(rdef, "name", None) or rdef.relation_type.type  # type: ignore

    if isinstance(default, str) and rdef.object != "String":
        # real Strings can be anything,
        # including things that look like keywords for other base types
        if str(rdef.object) in KEYWORD_MAP and default.upper() in KEYWORD_MAP[str(rdef.object)]:
            return KEYWORD_MAP[str(rdef.object)][default.upper()]()
        # else:
        # the default was likely not a special constant
        # like TODAY but some literal

        # bw compat for old schemas
        if rdef.object in DATE_FACTORY_MAP:
            warnings.warn(
                "using strings as default values "
                "for attribute %s of type %s "
                "is deprecated; you should use "
                "the plain python objects instead" % (relation_type, rdef.object),
                DeprecationWarning,
            )

            try:
                return DATE_FACTORY_MAP[str(rdef.object)](default)
            except ValueError as verr:
                raise ValueError(
                    "creating a default value for "
                    "attribute %s of type %s from the string %r"
                    " is not supported (cause %s)" % (relation_type, rdef.object, default, verr)
                )

    if rdef.object == "String":
        return str(default)

    return default  # general case: untouched default


def register_base_type(
    name: str,
    parameters: Union[Dict[str, Any], Iterable[str]] = (),
    check_function: Callable = None,
) -> None:
    """register a yams base (final) type. You'll have to call
    base_type_class to generate the class.
    """
    from yams.schema import RelationDefinitionSchema
    from yams.constraints import BASE_CHECKERS, yes

    # Add the datatype to yams base types
    assert name not in BASE_TYPES, "%s already in BASE_TYPES %s" % (name, BASE_TYPES)

    BASE_TYPES.add(name)

    # Add the new datatype to the authorized types of RelationDefinitionSchema
    if not isinstance(parameters, dict):
        # turn tuple/list into dict with None values
        parameters = dict((p, None) for p in parameters)

    RelationDefinitionSchema.BASE_TYPE_PROPERTIES[name] = parameters

    # Add a yams checker or yes is not specified
    BASE_CHECKERS[name] = check_function or yes


def unregister_base_type(name: str) -> None:
    """Unregister a yams base (final) type"""
    from yams.schema import RelationDefinitionSchema
    from yams.constraints import BASE_CHECKERS

    assert name in BASE_TYPES, "%s not in BASE_TYPES %s" % (name, BASE_TYPES)

    BASE_TYPES.remove(name)
    RelationDefinitionSchema.BASE_TYPE_PROPERTIES.pop(name)
    BASE_CHECKERS.pop(name)
