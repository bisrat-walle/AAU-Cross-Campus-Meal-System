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
"""Write a schema as a dot file.

"""

import sys
import os.path as osp
from itertools import cycle

from typing import Any, Dict, Generator, Tuple, Set, Sequence, Optional, Union

from logilab.common.graph import DotBackend, GraphGenerator

import yams.types as yams_types


__docformat__: str = "restructuredtext en"

CARD_MAP: Dict[str, str] = {"?": "0..1", "1": "1", "*": "0..n", "+": "1..n"}


class SchemaDotPropsHandler:
    def __init__(self, visitor: "SchemaVisitor") -> None:
        self.visitor = visitor
        # FIXME: colors are arbitrary
        self._colors = cycle(("#aa0000", "#00aa00", "#0000aa", "#000000", "#888888"))
        self.nextcolor = lambda: next(self._colors)

    def node_properties(self, eschema: yams_types.EntitySchema) -> Dict[str, str]:
        """return default DOT drawing options for an entity schema"""
        label = ["{", eschema.type, "|"]

        label.append(
            r"\l".join(
                rel.type
                for rel in eschema.ordered_relations()
                if rel.final and self.visitor.should_display_attr(eschema, rel)
            )
        )

        label.append(r"\l}")  # trailing \l ensure alignement of the last one

        return {
            "label": "".join(label),
            "shape": "record",
            "fontname": "Courier",
            "style": "filled",
        }

    def edge_properties(
        self,
        rschema: Union[yams_types.RelationSchema, None],
        subjnode: yams_types.DefinitionName,
        objnode: yams_types.DefinitionName,
    ) -> Dict[str, str]:
        """return default DOT drawing options for a relation schema"""
        # rschema can be none if the subject is a specialization of the object
        # we get there because we want to draw a specialization arrow anyway
        if rschema is None:
            kwargs = {
                "label": "Parent class",
                "color": "grey",
                "style": "filled",
                "arrowhead": "empty",
            }
        elif rschema.symmetric:
            # symmetric rels are handled differently, let yams decide what's best
            kwargs = {
                "label": rschema.type,
                "color": "#887788",
                "style": "dashed",
                "dir": "both",
                "arrowhead": "normal",
                "arrowtail": "normal",
            }
        else:
            kwargs = {"label": rschema.type, "color": "black", "style": "filled"}
            rdef = rschema.relation_definition(subjnode, objnode)

            # mypy: "RelationDefinitionSchema" has no attribute "composite"
            # this is a dynamically setted attribue using self.__dict__.update(some_dict)
            if rdef.composite == "subject":  # type: ignore
                kwargs["arrowhead"] = "none"
                kwargs["arrowtail"] = "diamond"
            elif rdef.composite == "object":  # type: ignore
                kwargs["arrowhead"] = "diamond"
                kwargs["arrowtail"] = "none"
            else:
                kwargs["arrowhead"] = "normal"
                kwargs["arrowtail"] = "none"

            # UML like cardinalities notation, omitting 1..1

            # mypy: "RelationDefinitionSchema" has no attribute "cardinality"
            # this is a dynamically setted attribue using self.__dict__.update(some_dict)
            if rdef.cardinality[1] != "1":  # type: ignore
                kwargs["taillabel"] = CARD_MAP[rdef.cardinality[1]]  # type: ignore

            if rdef.cardinality[0] != "1":  # type: ignore
                kwargs["headlabel"] = CARD_MAP[rdef.cardinality[0]]  # type: ignore

            kwargs["color"] = self.nextcolor()

        kwargs["fontcolor"] = kwargs["color"]

        # dot label decoration is just awful (1 line underlining the label
        # + 1 line going to the closest edge spline point)
        kwargs["decorate"] = "false"
        # kwargs['labelfloat'] = 'true'

        return kwargs


class SchemaVisitor:
    def __init__(self, skiptypes: Sequence = ()) -> None:
        self._done: Set[Tuple[Any, Any, Any]] = set()
        self.skiptypes: Sequence = skiptypes
        self._nodes: Optional[Set[Tuple[Any, Any]]] = None
        self._edges: Optional[Set[Tuple[Any, Any, Any]]] = None

    def should_display_schema(self, erschema: yams_types.ERSchema) -> bool:
        return not (getattr(erschema, "final", False) or erschema in self.skiptypes)

    def should_display_attr(
        self, eschema: yams_types.EntitySchema, rschema: yams_types.RelationSchema
    ) -> bool:
        return rschema not in self.skiptypes

    def display_rel(
        self,
        rschema: yams_types.RelationSchema,
        setype: yams_types.DefinitionName,
        tetype: yams_types.DefinitionName,
    ) -> bool:

        if (rschema.type, setype, tetype) in self._done:
            return False

        self._done.add((rschema.type, setype, tetype))

        if rschema.symmetric:
            self._done.add((rschema, tetype, setype))

        return True

    def nodes(self) -> Any:
        return self._nodes

    def edges(self) -> Any:
        return self._edges


class FullSchemaVisitor(SchemaVisitor):
    def __init__(self, schema: yams_types.Schema, skiptypes: Sequence = ()) -> None:
        super(FullSchemaVisitor, self).__init__(skiptypes)
        self.schema: yams_types.Schema = schema
        self._eindex: Dict[str, Any] = {
            eschema.type: eschema
            for eschema in schema.entities()
            if self.should_display_schema(eschema)
        }

    def nodes(self) -> Generator[Tuple[str, yams_types.EntitySchema], Any, None]:
        for eschema in sorted(self._eindex.values(), key=lambda x: x.type):
            yield eschema.type, eschema

    def edges(self) -> Generator[Tuple[str, str, Optional[yams_types.RelationSchema]], Any, None]:
        # Entities with inheritance relations.
        for eschema in sorted(self._eindex.values(), key=lambda x: x.type):
            if eschema.specializes():
                yield str(eschema), str(eschema.specializes()), None

        # Subject/object relations.
        for rschema in sorted(self.schema.relations(), key=lambda x: x.type):
            if not self.should_display_schema(rschema):
                continue

            for setype, tetype in sorted(rschema.relation_definitions):
                if not (setype in self._eindex and tetype in self._eindex):
                    continue

                if not self.display_rel(rschema, setype, tetype):
                    continue

                yield setype, tetype, rschema


class OneHopESchemaVisitor(SchemaVisitor):
    def __init__(self, eschema: yams_types.EntitySchema, skiptypes: Sequence = ()) -> None:
        super(OneHopESchemaVisitor, self).__init__(skiptypes)
        nodes = set()
        edges = set()

        nodes.add((eschema.type, eschema))

        for rschema in eschema.subject_relations():
            if not self.should_display_schema(rschema):
                continue

            for teschema in rschema.objects(eschema.type):
                nodes.add((teschema.type, teschema))
                if not self.display_rel(rschema, eschema.type, teschema.type):
                    continue

                edges.add((eschema.type, teschema.type, rschema))

        for rschema in eschema.object_relations():
            if not self.should_display_schema(rschema):
                continue

            for teschema in rschema.subjects(eschema.type):
                nodes.add((teschema.type, teschema))

                if not self.display_rel(rschema, teschema.type, eschema.type):
                    continue

                edges.add((teschema.type, eschema.type, rschema))

        # Inheritance relations.
        if eschema.specializes():
            entity_schema_specialisation = eschema.specializes()
            assert entity_schema_specialisation is not None
            nodes.add((entity_schema_specialisation.type, entity_schema_specialisation))
            # mypy: Argument 1 to "add" of "set" has incompatible type "Tuple[str, Any, None]";
            # mypy: expected "Tuple[str, str, RelationSchema]"
            # situation too complex for mypy to handle
            edges.add((eschema.type, eschema.specializes().type, None))  # type: ignore

        if eschema.specialized_by():
            for pschema in eschema.specialized_by():
                nodes.add((pschema.type, pschema))
                # mypy: Argument 1 to "add" of "set" has incompatible type "Tuple[str, str, None]";
                # mypy: expected "Tuple[str, str, RelationSchema]"
                # situation too complex for mypy to handle
                edges.add((pschema.type, eschema.type, None))  # type: ignore

        self._nodes = nodes
        self._edges = edges


class OneHopRSchemaVisitor(SchemaVisitor):
    def __init__(self, rschema: yams_types.RelationSchema, skiptypes: Sequence = ()) -> None:
        super(OneHopRSchemaVisitor, self).__init__(skiptypes)
        nodes = set()
        edges = set()

        for seschema in rschema.subjects():
            nodes.add((seschema.type, seschema))

            for oeschema in rschema.objects(seschema.type):
                nodes.add((oeschema.type, oeschema))

                if not self.display_rel(rschema, seschema.type, oeschema.type):
                    continue

                edges.add((seschema.type, oeschema.type, rschema))

        self._nodes = nodes
        self._edges = edges


def schema2dot(
    schema: Optional[yams_types.Schema] = None,
    outputfile: Optional[str] = None,
    skiptypes: Sequence = (),
    visitor: Optional[Union[SchemaVisitor, FullSchemaVisitor]] = None,
    prophdlr=None,
    size=None,
) -> Any:
    """write to the output stream a dot graph representing the given schema"""

    # mypy: Incompatible types in assignment (expression has type
    # mypy: "Union[SchemaVisitor, Schema, None]", variable has type "Optional[SchemaVisitor]")
    # mypy bug, Type is Optional[Union[SchemaVisitor, FullSchemaVisitor]] because "schema" will
    # either be None (or False...) or skipped but will never be returned from this expression
    visitor = visitor or (schema and FullSchemaVisitor(schema, skiptypes))  # type: ignore

    assert prophdlr or visitor is not None
    # mypy: Argument 1 to "SchemaDotPropsHandler" has incompatible type "Union[SchemaVisitor,
    # mypy: FullSchemaVisitor, None]"; expected "SchemaVisitor"
    # we check before that either we have a prophdlr or, if not, that visitor is not None
    prophdlr = prophdlr or SchemaDotPropsHandler(visitor)  # type: ignore

    schemaname: str
    if outputfile:
        schemaname = osp.splitext(osp.basename(outputfile))[0]
    else:
        schemaname = "Schema"

    generator = GraphGenerator(
        DotBackend(
            schemaname,
            "BT",
            ratio="compress",
            size=size,
            renderer="dot",
            additionnal_param={
                "overlap": "false",
                "splines": "true",
                # 'polylines':'true',
                "sep": "0.2",
            },
        )
    )

    return generator.generate(visitor, prophdlr, outputfile)


def run() -> None:
    """main routine when schema2dot is used as a script"""
    from yams.reader import SchemaLoader

    loader = SchemaLoader()

    if sys.argv[1:]:
        schema_dir: str = sys.argv[1]
    else:
        print("USAGE: schema2dot SCHEMA_DIR [OUTPUT FILE]")
        sys.exit(1)

    outputfile: Optional[str]

    if len(sys.argv) > 2:
        outputfile = sys.argv[2]
    else:
        outputfile = None

    schema = loader.load([schema_dir], "Test")  # type: ignore # uses old api

    schema2dot(schema, outputfile)


if __name__ == "__main__":
    run()
