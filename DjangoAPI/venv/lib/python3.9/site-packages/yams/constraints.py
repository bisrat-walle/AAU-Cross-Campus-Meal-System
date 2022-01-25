# copyright 2004-2016 LOGILAB S.A. (Paris, FRANCE), all rights reserved.
# contact https://www.logilab.fr/ -- mailto:contact@logilab.fr
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
"""Some common constraint classes."""

import re
import decimal
import operator
import json
import datetime
import warnings

from typing import Any, Dict, Match, Tuple, Type, Union, Optional, Callable, Sequence, List, cast

from logilab.common.deprecation import class_renamed

import yams
from yams import BadSchemaDefinition
from yams.interfaces import IConstraint, IVocabularyConstraint
import yams.types as yams_types

__docformat__: str = "restructuredtext en"

_: Type[str] = str


class ConstraintJSONEncoder(json.JSONEncoder):
    def default(self, obj: Union[Any, "NOW", "TODAY"]) -> Union[Any, dict]:
        if isinstance(obj, Attribute):
            return {"__attribute__": obj.attr}

        if isinstance(obj, NOW):
            # it is not a timedelta
            if obj.offset is None:
                return {"__now__": True, "offset": obj.offset}

            d = {
                "days": obj.offset.days,
                "seconds": obj.offset.seconds,
                "microseconds": obj.offset.microseconds,
            }

            return {"__now__": True, "offset": d}

        if isinstance(obj, TODAY):
            # it is not a timedelta
            if obj.offset is None:
                return {"__today__": True, "offset": obj.offset, "type": obj.type}

            d = {
                "days": obj.offset.days,
                "seconds": obj.offset.seconds,
                "microseconds": obj.offset.microseconds,
            }

            return {"__today__": True, "offset": d, "type": obj.type}

        return super(ConstraintJSONEncoder, self).default(obj)


def _json_object_hook(dct: Dict) -> Union[Dict, "NOW", "TODAY", "Attribute"]:
    offset: Optional[datetime.timedelta]

    if "__attribute__" in dct:
        return Attribute(dct["__attribute__"])

    if "__now__" in dct:
        if dct["offset"] is not None:
            offset = datetime.timedelta(**dct["offset"])
        else:
            offset = None

        return NOW(offset)

    if "__today__" in dct:
        if dct["offset"] is not None:
            offset = datetime.timedelta(**dct["offset"])
        else:
            offset = None

        return TODAY(offset=offset, type=dct["type"])

    return dct


def cstr_json_dumps(obj: yams_types.jsonSerializable) -> str:
    return str(ConstraintJSONEncoder(sort_keys=True).encode(obj))


cstr_json_loads: Callable[[str], Dict] = json.JSONDecoder(object_hook=_json_object_hook).decode


def _message_value(boundary) -> Any:
    if isinstance(boundary, Attribute):
        return boundary.attr
    return boundary


class BaseConstraint:
    """base class for constraints"""

    __implements__ = IConstraint

    def __init__(self, msg: Optional[str] = None) -> None:
        self.msg: Optional[str] = msg

    def check_consistency(
        self,
        subjschema: yams_types.EntitySchema,
        objschema: yams_types.EntitySchema,
        rdef: yams_types.RelationDefinition,
    ) -> None:
        pass

    def type(self) -> str:
        return self.__class__.__name__

    def serialize(self) -> str:
        """called to make persistent valuable data of a constraint"""
        return cstr_json_dumps({"msg": self.msg})

    @classmethod
    def deserialize(cls: Type["BaseConstraint"], value: str) -> Any:
        """called to restore serialized data of a constraint. Should return
        a `cls` instance
        """
        value = value.strip()

        if value and value != "None":
            d = cstr_json_loads(value)
        else:
            d = {}

        return cls(**d)

    def failed_message(self, key: str, value, entity=None) -> Tuple[Optional[str], Dict[str, Any]]:
        if entity is None:
            warnings.warn(
                "[yams 0.44] failed message " "should now be given entity has argument.",
                DeprecationWarning,
                stacklevel=2,
            )

        if self.msg:
            return self.msg, {}

        return self._failed_message(entity, key, value)

    def _failed_message(self, entity, key: str, value) -> Tuple[str, Dict[str, Any]]:
        return (
            _("%(KEY-cstr)s constraint failed for value %(KEY-value)r"),
            {key + "-cstr": self, key + "-value": value},
        )

    def __eq__(self, other: Any) -> bool:
        return (self.type(), self.serialize()) == (other.type(), other.serialize())

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __lt__(self, other: Any) -> bool:
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self.type(), self.serialize()))


# possible constraints ########################################################


class UniqueConstraint(BaseConstraint):
    """object of relation must be unique"""

    def __str__(self) -> str:
        return "unique"

    def check_consistency(
        self,
        subjschema: yams_types.EntitySchema,
        objschema: yams_types.EntitySchema,
        rdef: yams_types.RelationDefinition,
    ) -> None:

        if not objschema.final:
            raise BadSchemaDefinition("unique constraint doesn't apply to non " "final entity type")

    def check(self, entity, rtype: yams_types.RelationType, values) -> bool:
        """return true if the value satisfy the constraint, else false"""
        return True


class SizeConstraint(BaseConstraint):
    """the string size constraint :

    if max is not None the string length must not be greater than max
    if min is not None the string length must not be shorter than min
    """

    def __init__(
        self, max: Optional[int] = None, min: Optional[int] = None, msg: Optional[str] = None
    ) -> None:

        super(SizeConstraint, self).__init__(msg)

        assert max is not None or min is not None, "No max or min"

        if min is not None:
            assert isinstance(min, int), "min must be an int, not %r" % min

        if max is not None:
            assert isinstance(max, int), "max must be an int, not %r" % max

        self.max: Optional[int] = max
        self.min: Optional[int] = min

    def __str__(self) -> str:
        res = "size"

        if self.max is not None:
            res = "%s <= %s" % (res, self.max)

        if self.min is not None:
            res = "%s <= %s" % (self.min, res)

        return res

    def check_consistency(
        self,
        subjschema: yams_types.EntitySchema,
        objschema: yams_types.EntitySchema,
        rdef: yams_types.RelationDefinition,
    ) -> None:

        if not objschema.final:
            raise BadSchemaDefinition("size constraint doesn't apply to non " "final entity type")

        if objschema not in ("String", "Bytes", "Password"):
            raise BadSchemaDefinition(
                "size constraint doesn't apply to %s " "entity type" % objschema
            )

        if self.max:
            for cstr in rdef.constraints:
                if cstr.__class__ is StaticVocabularyConstraint:
                    for value in cstr.values:
                        if len(value) > self.max:
                            raise BadSchemaDefinition(
                                "size constraint set to %s but vocabulary "
                                "contains string of greater size" % self.max
                            )

    def check(self, entity, rtype: yams_types.RelationType, value: Sequence) -> bool:
        """return true if the value is in the interval specified by
        self.min and self.max
        """
        if self.max is not None and len(value) > self.max:
            return False

        if self.min is not None and len(value) < self.min:
            return False

        return True

    def _failed_message(self, entity, key: str, value: Sequence) -> Tuple[str, Dict[str, Any]]:
        if self.max is not None and len(value) > self.max:
            return (
                _("value should have maximum size of %(KEY-max)s" " but found %(KEY-size)s"),
                {key + "-max": self.max, key + "-size": len(value)},
            )

        if self.min is not None and len(value) < self.min:
            return (
                _("value should have minimum size of %(KEY-min)s" " but found %(KEY-size)s"),
                {key + "-min": self.min, key + "-size": len(value)},
            )

        assert False, "shouldnt be there"

    def serialize(self) -> str:
        """simple text serialization"""
        return cstr_json_dumps({"min": self.min, "max": self.max, "msg": self.msg})

    @classmethod
    def deserialize(cls: Type["SizeConstraint"], value: str) -> "SizeConstraint":
        """simple text deserialization"""
        try:
            d = cstr_json_loads(value)

            return cls(**d)
        except ValueError:
            kwargs = {}

            for adef in value.split(","):
                key, val = [w.strip() for w in adef.split("=")]

                assert key in ("min", "max")

                kwargs[str(key)] = int(val)

            # mypy: Argument 1 to "SizeConstraint" has incompatible type "**Dict[str, int]";
            # mypy: expected "Optional[str]"
            # mypy seems really broken with **kwargs
            return cls(**kwargs)  # type: ignore


class RegexpConstraint(BaseConstraint):
    """specifies a set of allowed patterns for a string value"""

    __implements__ = IConstraint

    def __init__(self, regexp: str, flags: int = 0, msg: Optional[str] = None) -> None:
        """
        Construct a new RegexpConstraint.

        :Parameters:
         - `regexp`: (str) regular expression that strings must match
         - `flags`: (int) flags that are passed to re.compile()
        """
        super(RegexpConstraint, self).__init__(msg)
        self.regexp: str = regexp
        self.flags: int = flags
        self._rgx = re.compile(regexp, flags)

    def __str__(self) -> str:
        return "regexp %s" % self.regexp

    def check_consistency(
        self,
        subjschema: yams_types.EntitySchema,
        objschema: yams_types.EntitySchema,
        rdef: yams_types.RelationDefinition,
    ) -> None:
        if not objschema.final:
            raise BadSchemaDefinition("regexp constraint doesn't apply to non " "final entity type")

        if objschema not in ("String", "Password"):
            raise BadSchemaDefinition(
                "regexp constraint doesn't apply to %s " "entity type" % objschema
            )

    def check(self, entity, rtype: yams_types.RelationType, value: str) -> Optional[Match[str]]:
        """return true if the value maches the regular expression"""
        return self._rgx.match(value, self.flags)

    def _failed_message(self, entity, key: str, value) -> Tuple[str, Dict[str, Any]]:
        return (
            _("%(KEY-value)r doesn't match " "the %(KEY-regexp)r regular expression"),
            {key + "-value": value, key + "-regexp": self.regexp},
        )

    def serialize(self) -> str:
        """simple text serialization"""
        return cstr_json_dumps({"regexp": self.regexp, "flags": self.flags, "msg": self.msg})

    @classmethod
    def deserialize(cls, value: str) -> "RegexpConstraint":
        """simple text deserialization"""
        try:
            d = cstr_json_loads(value)
            return cls(**d)
        except ValueError:
            regexp, flags = value.rsplit(",", 1)
            return cls(regexp, int(flags))

    def __deepcopy__(self, memo) -> "RegexpConstraint":
        return RegexpConstraint(self.regexp, self.flags)


OPERATORS: Dict[str, Callable[[Any, Any], bool]] = {
    "<=": operator.le,
    "<": operator.lt,
    ">": operator.gt,
    ">=": operator.ge,
}


class BoundaryConstraint(BaseConstraint):
    """the int/float bound constraint :

    set a minimal or maximal value to a numerical value
    """

    __implements__ = IConstraint

    def __init__(
        self, op: str, boundary: Optional[Union["Attribute", "NOW", "TODAY"]] = None, msg=None
    ) -> None:

        super(BoundaryConstraint, self).__init__(msg)

        assert op in OPERATORS, op

        self.operator: str = op
        self.boundary: Optional[Union["Attribute", "NOW", "TODAY"]] = boundary

    def __str__(self) -> str:
        return "value %s" % self.serialize()

    def check_consistency(
        self,
        subjschema: yams_types.EntitySchema,
        objschema: yams_types.EntitySchema,
        rdef: yams_types.RelationDefinition,
    ) -> None:

        if not objschema.final:
            raise BadSchemaDefinition("bound constraint doesn't apply to non " "final entity type")

    def check(self, entity, rtype: yams_types.RelationType, value) -> bool:
        """return true if the value satisfies the constraint, else false"""
        boundary = actual_value(self.boundary, entity)

        if boundary is None:
            return True

        return OPERATORS[self.operator](value, boundary)

    def _failed_message(self, entity, key: str, value) -> Tuple[str, Dict[str, Any]]:
        return (
            "value %%(KEY-value)s must be %s %%(KEY-boundary)s" % self.operator,
            {
                key + "-value": value,
                key + "-boundary": _message_value(actual_value(self.boundary, entity)),
            },
        )

    def serialize(self) -> str:
        """simple text serialization"""
        return cstr_json_dumps({"op": self.operator, "boundary": self.boundary, "msg": self.msg})

    @classmethod
    def deserialize(cls: Type["BoundaryConstraint"], value: str) -> "BoundaryConstraint":
        """simple text deserialization"""
        try:
            d = cstr_json_loads(value)

            return cls(**d)
        except ValueError:
            op, boundary = value.split(" ", 1)

            return cls(op, eval(boundary))

    def type(self) -> str:
        return "BoundaryConstraint"


BoundConstraint = class_renamed("BoundConstraint", BoundaryConstraint)

_("value %(KEY-value)s must be < %(KEY-boundary)s")
_("value %(KEY-value)s must be > %(KEY-boundary)s")
_("value %(KEY-value)s must be <= %(KEY-boundary)s")
_("value %(KEY-value)s must be >= %(KEY-boundary)s")


class IntervalBoundConstraint(BaseConstraint):
    """an int/float bound constraint :

    sets a minimal and / or a maximal value to a numerical value
    This class replaces the BoundConstraint class
    """

    __implements__ = IConstraint

    def __init__(
        self,
        minvalue: Optional[Union[int, float]] = None,
        maxvalue: Optional[Union[int, float]] = None,
        msg: Optional[str] = None,
    ) -> None:
        """
        :param minvalue: the minimal value that can be used
        :param maxvalue: the maxvalue value that can be used
        """
        assert not (minvalue is None and maxvalue is None)

        super(IntervalBoundConstraint, self).__init__(msg)

        self.minvalue: Optional[Union[int, float]] = minvalue
        self.maxvalue: Optional[Union[int, float]] = maxvalue

    def __str__(self) -> str:
        return "value [%s]" % self.serialize()

    def check_consistency(
        self,
        subjschema: yams_types.EntitySchema,
        objschema: yams_types.EntitySchema,
        rdef: yams_types.RelationDefinition,
    ) -> None:

        if not objschema.final:
            raise BadSchemaDefinition(
                "interval bound constraint doesn't apply" " to non final entity type"
            )

    def check(self, entity, rtype: yams_types.RelationType, value: Union[int, float]) -> bool:
        minvalue = actual_value(self.minvalue, entity)

        if minvalue is not None and value < minvalue:
            return False

        maxvalue = actual_value(self.maxvalue, entity)

        if maxvalue is not None and value > maxvalue:
            return False

        return True

    def _failed_message(self, entity, key: str, value) -> Tuple[str, Dict[str, Any]]:
        if self.minvalue is not None and value < actual_value(self.minvalue, entity):
            return (
                _("value %(KEY-value)s must be >= %(KEY-boundary)s"),
                {key + "-value": value, key + "-boundary": _message_value(self.minvalue)},
            )

        if self.maxvalue is not None and value > actual_value(self.maxvalue, entity):
            return (
                _("value %(KEY-value)s must be <= %(KEY-boundary)s"),
                {key + "-value": value, key + "-boundary": _message_value(self.maxvalue)},
            )

        assert False, "shouldnt be there"

    def serialize(self) -> str:
        """simple text serialization"""
        return cstr_json_dumps(
            {"minvalue": self.minvalue, "maxvalue": self.maxvalue, "msg": self.msg}
        )

    @classmethod
    def deserialize(cls: Type["IntervalBoundConstraint"], value: str) -> "IntervalBoundConstraint":
        """simple text deserialization"""
        try:
            d = cstr_json_loads(value)

            return cls(**d)
        except ValueError:
            minvalue, maxvalue = value.split(";")

            return cls(eval(minvalue), eval(maxvalue))


class StaticVocabularyConstraint(BaseConstraint):
    """Enforces a predefined vocabulary set for the value."""

    __implements__ = IVocabularyConstraint

    def __init__(self, values: Sequence[str], msg: Optional[str] = None) -> None:
        super(StaticVocabularyConstraint, self).__init__(msg)
        self.values: Tuple[str, ...] = tuple(values)

    def __str__(self) -> str:
        return "value in (%s)" % ", ".join(repr(str(word)) for word in self.vocabulary())

    def check(self, entity, rtype: yams_types.RelationType, value: str) -> bool:
        """return true if the value is in the specific vocabulary"""
        return value in self.vocabulary(entity=entity)

    def _failed_message(self, entity, key: str, value) -> Tuple[str, Dict[str, Any]]:
        if isinstance(value, str):
            value = '"%s"' % str(value)
            choices = ", ".join('"%s"' % val for val in self.values)
        else:
            choices = ", ".join(str(val) for val in self.values)

        return (
            _("invalid value %(KEY-value)s, " "it must be one of %(KEY-choices)s"),
            {key + "-value": value, key + "-choices": choices},
        )

    def vocabulary(self, **kwargs) -> Tuple[str, ...]:
        """return a list of possible values for the attribute"""
        return self.values

    def serialize(self) -> str:
        """serialize possible values as a json object"""
        return cstr_json_dumps({"values": self.values, "msg": self.msg})

    @classmethod
    def deserialize(
        cls: Type["StaticVocabularyConstraint"], value: str
    ) -> "StaticVocabularyConstraint":
        """deserialize possible values from a csv list of evaluable strings"""
        try:
            values = cstr_json_loads(value)

            return cls(**values)
        except ValueError:
            interpreted_values = [eval(w) for w in re.split("(?<!,), ", value)]

            if interpreted_values and isinstance(interpreted_values[0], str):
                cast(List[str], interpreted_values)

                interpreted_values = [v.replace(",,", ",") for v in interpreted_values]

            return cls(interpreted_values)


class FormatConstraint(StaticVocabularyConstraint):

    regular_formats: Tuple[str, ...] = (
        _("text/rest"),
        _("text/markdown"),
        _("text/html"),
        _("text/plain"),
    )

    # **kwargs to have a common interface between all Constraint initializers
    def __init__(self, msg: Optional[str] = None, **kwargs) -> None:
        values: Tuple[str, ...] = self.regular_formats
        super(FormatConstraint, self).__init__(values, msg=msg)

    def check_consistency(
        self,
        subjschema: yams_types.EntitySchema,
        objschema: yams_types.EntitySchema,
        rdef: yams_types.RelationDefinition,
    ) -> None:

        if not objschema.final:
            raise BadSchemaDefinition("format constraint doesn't apply to non " "final entity type")

        if not objschema == "String":
            raise BadSchemaDefinition("format constraint only apply to String")


FORMAT_CONSTRAINT: FormatConstraint = FormatConstraint()


class MultipleStaticVocabularyConstraint(StaticVocabularyConstraint):
    """Enforce a list of values to be in a predefined set vocabulary."""

    # XXX never used

    def check(self, entity, rtype: yams_types.RelationType, values: Sequence[str]) -> bool:
        """return true if the values satisfy the constraint, else false"""
        vocab = self.vocabulary(entity=entity)

        for value in values:
            if value not in vocab:
                return False

        return True


# special classes to be used w/ constraints accepting values as argument(s):
# IntervalBoundConstraint


def actual_value(value, entity) -> Any:
    if hasattr(value, "value"):
        return value.value(entity)

    return value


class Attribute:
    def __init__(self, attr) -> None:
        self.attr = attr

    def __str__(self) -> str:
        return "%s(%r)" % (self.__class__.__name__, self.attr)

    def value(self, entity) -> Any:
        return getattr(entity, self.attr)


class NOW:
    def __init__(self, offset: Optional[datetime.timedelta] = None) -> None:
        self.offset: Optional[datetime.timedelta] = offset

    def __str__(self) -> str:
        return "%s(%r)" % (self.__class__.__name__, self.offset)

    def value(self, entity) -> datetime.date:
        now = yams.KEYWORD_MAP["Datetime"]["NOW"]()

        if self.offset:
            now += self.offset

        return now


class TODAY:
    def __init__(self, offset: Optional[datetime.timedelta] = None, type: str = "Date") -> None:
        self.offset: Optional[datetime.timedelta] = offset
        # XXX no check that self.type is in KEYWORD_MAP?
        self.type: str = type

    def __str__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.offset, self.type)

    def value(self, entity) -> datetime.date:
        now = yams.KEYWORD_MAP[self.type]["TODAY"]()

        if self.offset:
            now += self.offset

        return now


# base types checking functions ###############################################


def check_string(eschema, value) -> bool:
    """check value is an unicode string"""
    return isinstance(value, str)


def check_password(eschema, value) -> bool:
    """check value is an encoded string"""
    return isinstance(value, bytes)


def check_int(eschema, value) -> bool:
    """check value is an integer"""
    try:
        int(value)
    except ValueError:
        return False

    return True


def check_float(eschema, value) -> bool:
    """check value is a float"""
    try:
        float(value)
    except ValueError:
        return False

    return True


def check_decimal(eschema, value) -> bool:
    """check value is a Decimal"""
    try:
        decimal.Decimal(value)
    except (TypeError, decimal.InvalidOperation):
        return False

    return True


def check_boolean(eschema, value) -> bool:
    """check value is a boolean"""
    return isinstance(value, int)


def check_file(eschema, value) -> bool:
    """check value has a getvalue() method (e.g. StringIO or cStringIO)"""
    return hasattr(value, "getvalue")


def yes(*args, **kwargs) -> bool:
    """dunno how to check"""
    return True


BASE_CHECKERS: yams_types.Checkers = {
    "Date": yes,
    "Time": yes,
    "Datetime": yes,
    "TZTime": yes,
    "TZDatetime": yes,
    "Interval": yes,
    "String": check_string,
    "Int": check_int,
    "BigInt": check_int,
    "Float": check_float,
    "Decimal": check_decimal,
    "Boolean": check_boolean,
    "Password": check_password,
    "Bytes": check_file,
}

BASE_CONVERTERS: yams_types.Converters = {
    "String": str,
    "Password": bytes,
    "Int": int,
    "BigInt": int,
    "Float": float,
    "Boolean": bool,
    "Decimal": decimal.Decimal,
}


def patch_sqlite_decimal() -> None:
    """patch Decimal checker and converter to bypass SQLITE Bug
    (SUM of Decimal return float in SQLITE)"""

    def convert_decimal(value) -> decimal.Decimal:
        # XXX issue a warning
        if isinstance(value, float):
            value = str(value)

        return decimal.Decimal(value)

    def check_decimal(eschema, value) -> bool:
        """check value is a Decimal"""
        try:
            if isinstance(value, float):
                return True

            decimal.Decimal(value)
        except (TypeError, decimal.InvalidOperation):
            return False

        return True

    global BASE_CONVERTERS
    BASE_CONVERTERS["Decimal"] = convert_decimal

    global BASE_CHECKERS
    BASE_CHECKERS["Decimal"] = check_decimal
