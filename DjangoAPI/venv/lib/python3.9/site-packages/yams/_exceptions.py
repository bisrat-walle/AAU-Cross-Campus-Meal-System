# copyright 2004-2012 LOGILAB S.A. (Paris, FRANCE), all rights reserved.
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
"""YAMS exception classes"""
from typing import Generator, Tuple, Optional, Dict, Callable, List

__docformat__ = "restructuredtext en"


class SchemaError(Exception):
    """base class for schema exceptions"""

    def __unicode__(self) -> str:
        raise NotImplementedError()

    def __str__(self) -> str:
        return self.__unicode__()


class UnknownType(SchemaError):
    """using an unknown entity type"""

    msg: str = "Unknown type %s"

    def __unicode__(self) -> str:
        return self.msg % self.args


class BadSchemaDefinition(SchemaError):
    """error in the schema definition

    instance attributes:
    * filename is the source file where the exception was raised
    * lineno is the line number where the exception was raised
    * line is the actual line in text form
    """

    msg: str = "%s line %s: %s"

    @property
    def filename(self) -> Optional[str]:
        if len(self.args) > 1:
            return self.args[0]
        else:
            return None

    def __unicode__(self) -> str:
        msgs = []
        args_offset = 0
        if self.filename is not None:
            msgs.append(self.filename)
            args_offset += 1
            msgs.append(": ")
        msgs.append(" ".join(self.args[args_offset:]))
        return "".join(msgs)


class ValidationError(SchemaError):
    """Validation error details the reason(s) why the validation failed.

    Arguments are:

    * `entity`: the entity that could not be validated; actual type depends on
      the client library

    * `errors`: errors dictionary, None key used for global error, other keys
      should be attribute/relation of the entity, qualified as subject/object
      using :func:`yams.role_name`.  Values are the message associated to the
      keys, and may include interpolation string starting with '%(KEY-' where
      'KEY' will be replaced by the associated key once the message has been
      translated. This allows predictable/translatable message and avoid args
      conflict if used for several keys.

    * `msgargs`: dictionary of substitutions to be inserted in error
      messages once translated (only if msgargs is given)

    * `i18nvalues`: list of keys in msgargs whose value should be translated

    Translation will be done **in-place** by calling :meth:`translate`.
    """

    def __init__(
        self,
        entity,
        errors: Dict,
        msgargs: Optional[Dict] = None,
        i18nvalues: Optional[List] = None,
    ) -> None:
        # set args so ValidationError are serializable through pyro
        SchemaError.__init__(self, entity, errors)
        self.entity = entity
        assert isinstance(errors, dict), "validation errors must be a dict"
        self.errors: Dict = errors
        self.msgargs: Optional[Dict] = msgargs
        self.i18nvalues: Optional[List] = i18nvalues
        self._translated: bool = False

    def __unicode__(self) -> str:
        if self._translated:
            errors_dict = self.errors
        else:
            errors_dict = dict(self._translated_errors(str))

        if len(errors_dict) == 1:
            attr, error = next(iter(errors_dict.items()))

            return "%s (%s): %s" % (self.entity, attr, error)

        errors = "\n".join("* %s: %s" % (k, v) for k, v in errors_dict.items())

        return "%s:\n%s" % (self.entity, errors)

    def translate(self, _: Callable[[str], str]) -> None:
        """Translate and interpolate messsages in the errors dictionary, using
        the given translation function.

        If no substitution has been given, suppose msg is already translated for
        bw compat, so no translation occurs.

        This method may only be called once.
        """
        assert not self._translated
        self._translated = True
        if self.msgargs is not None:
            if self.i18nvalues:
                for key in self.i18nvalues:
                    self.msgargs[key] = _(self.msgargs[key])
        self.errors = dict(self._translated_errors(_))

    def _translated_errors(self, _: Callable[[str], str]) -> Generator[Tuple[str, str], None, None]:
        for key, msg in self.errors.items():
            msg = _(msg)
            if key is not None:
                msg = msg.replace("%(KEY-", "%(" + key + "-")
            if self.msgargs:
                msg = msg % self.msgargs
            yield key, msg
