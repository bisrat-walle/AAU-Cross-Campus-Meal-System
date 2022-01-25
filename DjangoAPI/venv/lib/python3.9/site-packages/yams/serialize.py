import logging
from io import StringIO
from typing import Dict
from logilab.common.graph import ordered_nodes

import yams.types as yams_types


def serialize_to_python(schema: yams_types.Schema) -> str:
    out = StringIO()
    w = out.write
    w("from yams.buildobjs import *\n\n")

    graph: Dict = {}

    for entity in schema.entities():
        targets = graph.setdefault(entity, set())
        if entity.final:
            continue
        base = entity._specialized_type
        if base is None:
            continue
        if isinstance(base, str) and ", " in base:
            targets |= set(base.split(", "))
        else:
            targets.add(base)

    for e in reversed(ordered_nodes(graph)):
        if not e.final:
            if e._specialized_type:
                base = e._specialized_type
            else:
                base = "EntityType"
            w("class %s(%s):\n" % (e.type, base))
            attr_defs = list(e.attribute_definitions())
            if attr_defs:
                for attr, obj in attr_defs:
                    w("    %s = %s()\n" % (attr.type, obj.type))
            else:
                w("    pass\n")
            w("\n")

    for r in schema.relations():
        if not r.final:
            if r.subjects() and r.objects():
                w("class %s(RelationDefinition):\n" % r.type)
                w("    subject = (%s,)\n" % ", ".join("'%s'" % x for x in r.subjects()))
                w("    object = (%s,)\n" % ", ".join("'%s'" % x for x in r.objects()))
                w("\n")
            else:
                logging.warning("relation definition %s missing subject/object" % r.type)

    return out.getvalue()
