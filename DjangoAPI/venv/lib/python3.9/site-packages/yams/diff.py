# copyright 2013 LOGILAB S.A. (Paris, FRANCE), all rights reserved.
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
"""Compare two yams schemas

Textual representation of schema are created and standard diff algorithm are
applied.
"""

import subprocess
import tempfile
import os

from typing import (
    Tuple,
    TypeVar,
    Dict,
    Sequence,
    Iterable,
    Any,
    Callable,
    Union,
    Set,
    Type,
    List,
    Optional,
)

import yams.types as yams_types

from yams.interfaces import IConstraint
from yams.constraints import SizeConstraint, UniqueConstraint, StaticVocabularyConstraint
from yams.reader import SchemaLoader
from yams.schema import RelationDefinitionSchema


def quoted(obj: Union[str, Any]) -> str:
    if isinstance(obj, str):
        # NOT repr, because .....
        # strings here turn out as unicode strings (from the repo side)
        # but are (mostly) really str in the schema.py source
        # and we don't want a bogus diff about '...' vs u'...'
        if "'" in obj:
            return '"%s"' % obj
    return "'%s'" % obj


class attr_scope:
    indentation: int = 2
    spacer: str = ""
    nextline: str = ","


class class_scope:
    indentation: int = 1
    spacer: str = " "
    nextline: str = ""


_x_type = TypeVar("_x_type")


def identity(x: _x_type) -> _x_type:
    return x


def format_props(
    props: Dict, scope: Union[Type[attr_scope], Type[class_scope]], ignore: Sequence = ()
) -> str:
    out = []

    for prop, val in sorted(props.items()):
        if prop in ignore:
            continue

        out.append("\t" * scope.indentation + "%s%s=%s%s" % (prop, scope.spacer, scope.spacer, val))

    return ("%s\n" % scope.nextline).join(out)


def format_expression(expr) -> str:
    mainvars = expr.mainvars - set("SUXO")

    return "%s(%s%s)" % (
        expr.__class__.__name__,
        quoted(expr.expression),
        ",".join(mainvars) if mainvars else "",
    )


def format_tuple(iterator: Iterable) -> str:
    out = []
    for thing in iterator:
        out.append(thing)

    return "(%s)" % ",".join(out)


def format_perms(
    perms: Dict[str, Iterable], scope: Union[Type[attr_scope], Type[class_scope]], isdefault: bool
) -> str:
    out = []
    for p in ("read", "add", "update", "delete"):
        rule = perms.get(p)
        if not rule:
            continue

        out.append(
            "%s%s: %s"
            % (
                "\t" + "\t" * scope.indentation,
                quoted(p),
                format_tuple(
                    format_expression(r) if not isinstance(r, str) else quoted(r)
                    for r in sorted(rule)
                ),
            )
        )

    return " {%s\n%s\n%s}" % (
        " # default perms" if isdefault else "",
        ",\n".join(out),
        "\t" * scope.indentation,
    )


def format_constraint(cstr: IConstraint) -> str:
    cclass = cstr.__class__.__name__

    if "RQL" in cclass:
        out = ["%s(%s" % (cclass, quoted(cstr.expression))]  # type:ignore[attr-defined]
        mainvars = getattr(cstr, "mainvars", None)

        if mainvars:
            out.append(", mainvars=%s" % quoted(",".join(mainvars)))

        out.append(")")
    elif "Boundary" in cclass:
        return "%s(%s %s)" % (
            cclass,
            quoted(cstr.operator),  # type:ignore[attr-defined]
            str(cstr.boundary),  # type:ignore[attr-defined]
        )

    else:
        print("formatter: unhandled constraint type", cstr.__class__)

        return str(cstr)

    return "".join(out)


perms_type = TypeVar("perms_type")


def nullhandler(relation, perms: perms_type) -> Tuple[perms_type, bool]:
    return perms, False


_permissionshandlerType = Callable[[Any, perms_type], Tuple[perms_type, bool]]
_ReturnType = Dict[str, Union[str, int, bool, Any, List[Union[str, Any]]]]


def properties_from(
    relation: RelationDefinitionSchema, permissionshandler: _permissionshandlerType = nullhandler
) -> _ReturnType:
    """return a dictionary containing properties of an attribute
    or a relation (if specified with is_relation option)"""
    ret: Dict[str, Any] = {}

    for prop in relation.rproperties():
        if prop == "infered":
            continue

        val = getattr(relation, prop)

        if prop == "permissions":
            val, default = permissionshandler(relation, val)

            if val:
                ret["__permissions__"] = format_perms(val, attr_scope, default)

            continue

        elif prop == "cardinality" and relation.final:
            # attributes: re-sugar cardinality to 'required' prop
            prop = "required"
            val = val[0] == "1"

            if not val:
                continue  # don't emit 'required = False' props

        elif prop in ("indexed", "fulltextindexed", "uid", "internationalizable"):
            if not val:
                continue  # don't emit 'prop = False' props

        elif prop == "constraints":
            other = []
            for constraint in val:
                # first, resugar some classic constraints
                if isinstance(constraint, SizeConstraint):
                    ret["maxsize"] = constraint.max

                elif isinstance(constraint, UniqueConstraint):
                    ret["unique"] = True

                elif isinstance(constraint, StaticVocabularyConstraint):
                    # _x_type is for identity function
                    transform: Callable[[Any], Union[str, _x_type]]

                    if relation.object.type == "String":
                        transform = str
                    else:
                        transform = identity

                    ret["vocabulary"] = [transform(x) for x in constraint.vocabulary()]
                else:
                    other.append(constraint)

            if other:
                ret["constraints"] = "[%s]" % ",".join(format_constraint(cstr) for cstr in other)

            continue

        if val is None:
            continue

        if prop == "default":
            if not isinstance(val, bool):
                if not val:
                    continue  # default = ''

        elif prop in ("description", "cardinality", "composite"):
            if val:
                val = quoted(val)
            else:  # let's skip empty strings
                continue

        ret[prop] = val

    return ret


def schema2descr(
    schema: yams_types.Schema, permissionshandler: _permissionshandlerType, ignore: Sequence = ()
) -> str:
    """convert a yams schema into a text description"""
    output = []
    # mypy: Name 'ignore' already defined on line 232
    # but we redefine it here as a set (for optimisation?)
    ignore: Set = set(ignore)  # type: ignore
    multirtypes = []

    for entity in sorted(schema.entities()):
        if entity.final:
            continue

        output.append("class %s(EntityType):\n" % entity.type)
        perms, isdefault = permissionshandler(entity, entity.permissions)
        output.append("\t__permissions__ = %s\n\n" % format_perms(perms, class_scope, isdefault))

        attributes = [
            (
                attr[0].type,
                attr[1].type,
                properties_from(entity.relation_definition(attr[0].type), permissionshandler),
            )
            for attr in sorted(entity.attribute_definitions())
        ]

        for attr_name, attr_type, attr_props in attributes:
            if attr_name in ignore:
                continue

            output.append(
                "\t%s = %s(\n%s)\n\n"
                % (attr_name, attr_type, format_props(attr_props, attr_scope, ignore))
            )

        subject_relations = [
            (
                rel[0].type,  # rtype
                [r.type for r in rel[1]],  # etype targets
                properties_from(entity.relation_definition(rel[0].type), permissionshandler),
            )  # properties
            for rel in sorted(entity.relation_definitions())
            if rel[2] == "subject"
        ]

        for rtype, targettypes, props in subject_relations:
            targettypes = [etype for etype in targettypes if etype not in ignore]

            if not targettypes:
                continue

            if len(targettypes) == 1:
                ttypes_out = quoted(targettypes[0])
            else:
                multirtypes.append(rtype)
                continue

            output.append(
                "\t%s = SubjectRelation(%s\n%s)\n\n"
                % (rtype, ttypes_out, format_props(props, attr_scope, ignore))
            )

        output.append("\n\n")

    # Handle multi-rdef relation types
    for rtype in sorted(multirtypes):
        items = schema[rtype].rdefs.items()  # type: ignore
        for (subjectschema, objectschema), relation in sorted(items):
            subjetype = subjectschema.type
            objetype = objectschema.type

            output.append(
                "class %s_%s_%s(RelationDefinition):\n"
                % (rtype, subjetype.lower(), objetype.lower())
            )
            output.append("\tname = %s\n" % quoted(rtype))
            output.append("\tsubject = %s\n" % quoted(subjetype))
            output.append("\tobject = %s\n" % quoted(objetype))
            output.append(
                format_props(properties_from(relation, permissionshandler), class_scope, ignore)
            )
            output.append("\n\n")

    return "".join(output)


def schema2file(
    schema: yams_types.Schema,
    output: str,
    permissionshandler: _permissionshandlerType,
    ignore: Sequence = (),
) -> None:
    """Save schema description of schema find
    in directory schema_dir into output file"""
    with open(output, "w") as description_file:
        description_file.write(schema2descr(schema, permissionshandler, ignore))


def schema_diff(
    schema1: yams_types.Schema,
    schema2: yams_types.Schema,
    permissionshandler: _permissionshandlerType = nullhandler,
    diff_tool: Optional[str] = None,
    ignore: Sequence = (),
) -> Tuple[str, str]:
    """schema 1 and 2 are CubicwebSchema
    diff_tool is the name of tool use for file comparison
    """
    tmpdir = tempfile.mkdtemp()
    output1 = os.path.join(tmpdir, "schema1.txt")
    output2 = os.path.join(tmpdir, "schema2.txt")

    schema2file(schema1, output1, permissionshandler, ignore)
    schema2file(schema2, output2, permissionshandler, ignore)

    if diff_tool:
        cmd = "%s %s %s" % (diff_tool, output1, output2)
        subprocess.Popen(cmd, shell=True)
    else:
        print("description files save in %s and %s" % (output1, output2))

    print(output1, output2)

    return output1, output2


if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option(
        "-f", "--first-schema", dest="schema1", help="Specify the directory of the first schema"
    )
    parser.add_option(
        "-s", "--second-schema", dest="schema2", help="Specify the directory of the second schema"
    )
    parser.add_option(
        "-d", "--diff-tool", dest="diff_tool", help="Specify the name of the diff tool"
    )
    (options, args) = parser.parse_args()
    if options.schema1 and options.schema2:
        schema1 = SchemaLoader().load([options.schema1])
        schema2 = SchemaLoader().load([options.schema2])
        output1, output2 = schema_diff(schema1, schema2)
        if options.diff_tool:
            cmd = "%s %s %s" % (options.diff_tool, output1, output2)
            process = subprocess.Popen(cmd, shell=True)
        else:
            print("description files save in %s and %s" % (output1, output2))
    else:
        parser.error("An input file name must be specified")
