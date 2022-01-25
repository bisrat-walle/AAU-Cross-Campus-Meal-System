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

"""Classes to define generic Entities/Relations schemas."""

from logilab.common.logging_ext import set_log_methods
from logilab.common.deprecation import (
    send_warning,
    DeprecationWarningKind,
    TargetRemovedDeprecationWarning,
)

import logging

import warnings
from copy import deepcopy
from itertools import chain
from typing import (
    Dict,
    Any,
    Type,
    TYPE_CHECKING,
    Sequence,
    Generator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from logilab.common.decorators import cached, clear_cache
from logilab.common.interface import implements
from logilab.common import deprecation

import yams
from yams import (
    BASE_TYPES,
    MARKER,
    ValidationError,
    BadSchemaDefinition,
    KNOWN_METAATTRIBUTES,
    convert_default_value,
    DEFAULT_ATTRPERMS,
    DEFAULT_COMPUTED_RELPERMS,
)
from yams.interfaces import (
    ISchema,
    IRelationSchema,
    IEntitySchema,
    IConstraint,
    IVocabularyConstraint,
)
from yams.constraints import BASE_CHECKERS, BASE_CONVERTERS, UniqueConstraint, BaseConstraint
import yams.types as yams_types

__docformat__: str = "restructuredtext en"

_: Type[str] = str


@deprecation.argument_renamed(old_name="rtype", new_name="relation_type")
def role_name(relation_type, role) -> str:
    """function to use for qualifying attribute / relation in ValidationError
    errors'dictionnary
    """
    return "%s-%s" % (relation_type, role)


def rehash(dictionary: Dict) -> dict:
    """this function manually builds a copy of `dictionary` but forces
    hash values to be recomputed. Note that dict(d) or d.copy() don't
    do that.

    It is used to :
      - circumvent Pyro / (un)pickle problems (hash mode is changed
        during reconstruction)
      - force to recompute keys' hash values. This is needed when a
        schema's type is changed because the schema's hash method is based
        on the type attribute. This problem is illusrated by the pseudo-code
        below :

        >>> topic = EntitySchema(type='Topic')
        >>> d = {topic : 'foo'}
        >>> d[topic]
        'foo'
        >>> d['Topic']
        'foo'
        >>> topic.type = 'folder'
        >>> topic in d
        False
        >>> 'Folder' in d
        False
        >>> 'Folder' in d.keys() # but it can be found "manually"
        True
        >>> d = rehash(d) # explicit rehash()
        >>> 'Folder' in d
        True
    """
    return _RetroCompatRelationsDict(item for item in dictionary.items())


class ERSchema:
    """Base class shared by entity and relation schema."""

    @deprecation.argument_renamed(old_name="erdef", new_name="entity_relation_definition")
    def __init__(
        self, schema: "Schema", entity_relation_definition: yams_types.RelationDefinition = None
    ) -> None:
        """
        Construct an ERSchema instance.

        :Parameters:
         - `schema`: (??)
         - `entity_relation_definition`: (??)
        """
        if entity_relation_definition is None:
            return

        self.schema = schema
        self.type: yams_types.DefinitionName = entity_relation_definition.name
        self.description: str = entity_relation_definition.description or ""
        self.package = entity_relation_definition.package

    def __eq__(self, other) -> bool:
        return self.type == getattr(other, "type", other)

    def __ne__(self, other) -> bool:
        return not (self == other)

    def __lt__(self, other) -> bool:
        return self.type < getattr(other, "type", other)

    def __hash__(self) -> int:
        try:
            return hash(self.type)
        except AttributeError:
            pass
        return hash(id(self))

    def __deepcopy__(self: "ERSchema", memo) -> "ERSchema":
        clone = self.__class__(deepcopy(self.schema, memo))
        memo[id(self)] = clone
        clone.type = deepcopy(self.type, memo)
        clone.__dict__ = deepcopy(self.__dict__, memo)

        return clone

    def __str__(self) -> str:
        return self.type


class PermissionMixIn:
    """mixin class for permissions handling"""

    # https://github.com/python/mypy/issues/5837
    if TYPE_CHECKING:

        def __init__(
            self,
            schema: Optional["Schema"],
            relation_definition: Optional[yams_types.EntityType],
            *args,
            **kwargs,
        ):
            self.permissions: yams_types.Permissions

            # fake init for mypy
            if relation_definition:
                self.permissions = relation_definition.__permissions__.copy()
            else:
                self.permissions = {}
            self.final: bool = True

        @property
        def ACTIONS(self) -> Tuple[str, ...]:
            return ("read", "add", "update", "delete")

        def advertise_new_add_permission(self) -> None:
            pass

    def action_permissions(self, action: str) -> yams_types.Permission:
        return self.permissions[action]

    def set_action_permissions(self, action: str, permissions: yams_types.Permission) -> None:
        assert type(permissions) is tuple, "permissions is expected to be a tuple not %s" % type(
            permissions
        )

        assert action in self.ACTIONS, "%s not in %s" % (action, self.ACTIONS)

        self.permissions[action] = permissions

    def check_permission_definitions(self) -> None:
        """check permissions are correctly defined"""

        # already initialized, check everything is fine
        for action, groups in self.permissions.items():
            assert action in self.ACTIONS, "unknown action %s for %s" % (action, self)

            assert isinstance(
                groups, tuple
            ), "permission for action %s of %s isn't a tuple as " "expected" % (action, self)

        if self.final:
            self.advertise_new_add_permission()

        for action in self.ACTIONS:
            assert (
                action in self.permissions
            ), "missing expected permissions for action %s for %s" % (action, self)


# Schema objects definition ###################################################


class TargetInlinedDeprecationWarning(TargetRemovedDeprecationWarning):
    def __init__(self, reason: str, kind: DeprecationWarningKind, name: str):
        super().__init__(reason=reason, kind=kind, name=name)
        self.operation = "inlined"  # type: ignore

    def render_result(self, old_name):
        return f"list({old_name}.values())"


class _RetroCompatRelationsDict(dict):
    def __init__(self, *args, **kwargs):
        self.old_name = None
        if "old_name" in kwargs:
            self.old_name = kwargs["old_name"]
            del kwargs["old_name"]

        super().__init__(*args, **kwargs)

    def __call__(self):
        if self.old_name:
            send_warning(
                (
                    f"schema.{self.old_name}() method has been remove and is deprecated, use "
                    f"list(schema.{self.old_name}.values()) instead"
                ),
                TargetInlinedDeprecationWarning,
                deprecation_class_kwargs={
                    "kind": DeprecationWarningKind.CALLABLE,
                    "name": self.old_name,
                },
                stacklevel=3,
                version="0.46.2",
                module_name="yams.schema",
            )
        return list(self.values())


@deprecation.attribute_renamed(old_name="subjrels", new_name="subject_relations")
@deprecation.attribute_renamed(old_name="objrels", new_name="object_relations")
class EntitySchema(PermissionMixIn, ERSchema):
    """An entity has a type, a set of subject and or object relations
    the entity schema defines the possible relations for a given type and some
    constraints on those relations.
    """

    __implements__ = IEntitySchema

    ACTIONS: Tuple[str, ...] = ("read", "add", "update", "delete")
    field_checkers: yams_types.Checkers = BASE_CHECKERS
    field_converters: yams_types.Converters = BASE_CONVERTERS

    # XXX set default values for those attributes on the class level since
    # they may be missing from schemas obtained by pyro
    _specialized_type: Optional[str] = None
    _specialized_by: List = []

    @deprecation.argument_renamed(old_name="rdef", new_name="relation_definition")
    def __init__(
        self,
        schema: "Schema" = None,
        relation_definition: yams_types.EntityType = None,
        *args,
        **kwargs,
    ) -> None:
        super(EntitySchema, self).__init__(schema, relation_definition, *args, **kwargs)

        if relation_definition is not None:
            # quick access to bounded relation schemas
            self.subject_relations: Dict[
                "RelationSchema", "RelationSchema"
            ] = _RetroCompatRelationsDict(old_name="subject_relations")
            self.object_relations: Dict[
                "RelationSchema", "RelationSchema"
            ] = _RetroCompatRelationsDict(old_name="object_relations")
            self._specialized_type = relation_definition.specialized_type
            self._specialized_by = relation_definition.specialized_by
            self.final: bool = self.type in BASE_TYPES
            self.permissions: Dict[
                str, Tuple[str, ...]
            ] = relation_definition.__permissions__.copy()
            self._unique_together: List = getattr(relation_definition, "__unique_together__", [])

        else:  # this happens during deep copy (cf. ERSchema.__deepcopy__)
            self._specialized_type = None
            self._specialized_by = []

    def check_unique_together(self) -> None:
        errors = []

        for unique_together in self._unique_together:
            for name in unique_together:
                try:
                    relation_schema = self.relation_definition(name, take_first=True)
                except KeyError:
                    errors.append("no such attribute or relation %s" % name)
                else:
                    if not (relation_schema.final or relation_schema.relation_type.inlined):
                        errors.append("%s is not an attribute or an inlined " "relation" % name)

        if errors:
            message = "invalid __unique_together__ specification for %s: %s" % (
                self,
                ", ".join(errors),
            )
            raise BadSchemaDefinition(message)

    def __repr__(self) -> str:
        return "<%s %s - %s>" % (
            self.type,
            [subject_relation.type for subject_relation in self.subject_relations.values()],
            [object_relation.type for object_relation in self.object_relations.values()],
        )

    def _rehash(self) -> None:
        self.subject_relations = rehash(self.subject_relations)
        self.object_relations = rehash(self.object_relations)

    def advertise_new_add_permission(self) -> None:
        pass

    # schema building methods #################################################

    @deprecation.argument_renamed(old_name="rschema", new_name="relation_schema")
    def add_subject_relation(self, relation_schema: "RelationSchema") -> None:
        """register the relation schema as possible subject relation"""
        self.subject_relations[relation_schema] = relation_schema

        clear_cache(self, "ordered_relations")
        clear_cache(self, "meta_attributes")

    @deprecation.argument_renamed(old_name="rschema", new_name="relation_schema")
    def add_object_relation(self, relation_schema: "RelationSchema") -> None:
        """register the relation schema as possible object relation"""
        self.object_relations[relation_schema] = relation_schema

    @deprecation.argument_renamed(old_name="rtype", new_name="relation_schema")
    def del_subject_relation(self, relation_schema: "RelationSchema") -> None:
        try:
            del self.subject_relations[relation_schema]
            clear_cache(self, "ordered_relations")
            clear_cache(self, "meta_attributes")
        except KeyError:
            pass  # XXX error should never pass silently

    @deprecation.argument_renamed(old_name="rtype", new_name="relation_type")
    def del_object_relation(self, relation_schema: "RelationSchema") -> None:
        if relation_schema in self.object_relations:
            del self.object_relations[relation_schema]

    # IEntitySchema interface #################################################

    # navigation ######################

    def specializes(self) -> Optional["EntitySchema"]:
        if self._specialized_type and self.schema is not None:
            return self.schema.entity_schema_for(yams_types.DefinitionName(self._specialized_type))
        return None

    def ancestors(self) -> List["EntitySchema"]:
        specializes = self.specializes()
        ancestors = []

        while specializes:
            ancestors.append(specializes)
            specializes = specializes.specializes()

        return ancestors

    def specialized_by(self, recursive: bool = True) -> List["EntitySchema"]:
        if not self.schema:
            return []

        entity_schema = self.schema.entity_schema_for
        subject_schemas = [entity_schema(entity_type) for entity_type in self._specialized_by]

        if recursive:
            for subject_schema in subject_schemas[:]:
                subject_schemas.extend(subject_schema.specialized_by(recursive=True))

        return subject_schemas

    @deprecation.argument_renamed(old_name="rtype", new_name="relation_type")
    def has_relation(self, relation_type: yams_types.RelationType, role: str) -> bool:
        if role == "subject":
            return relation_type in self.subject_relations

        return relation_type in self.object_relations

    @deprecation.argument_renamed(old_name="rtype", new_name="relation_type")
    @deprecation.argument_renamed(old_name="targettype", new_name="target_type")
    @deprecation.argument_renamed(old_name="takefirst", new_name="take_first")
    def relation_definition(
        self,
        relation_type: yams_types.DefinitionName,
        role: str = "subject",
        target_type: yams_types.DefinitionName = None,
        take_first: bool = False,
    ) -> yams_types.RelationDefinitionSchema:
        """return a relation definition schema for a relation of this entity type

        Notice that when target_type is not specified and the relation may lead
        to different entity types (ambiguous relation), one of them is picked
        randomly. If also take_first is False, a warning will be emitted.
        """
        assert self.schema is not None
        relation_schema = self.schema.relation_schema_for(relation_type)

        if target_type is None:
            if role == "subject":
                types = relation_schema.objects(self)
            else:
                types = relation_schema.subjects(self)

            if len(types) != 1 and not take_first:
                warnings.warn(
                    "[yams 0.38] no target_type specified and there are several "
                    "relation definitions for relation_type %s: %s. Yet you get the first "
                    "relation_definition."
                    % (relation_type, [entity_schema.type for entity_schema in types]),
                    Warning,
                    stacklevel=2,
                )

            target_type = types[0].type

        return relation_schema.role_relation_definition(self.type, target_type, role)

    rdef = deprecation.callable_renamed(old_name="rdef", new_function=relation_definition)

    @cached
    def ordered_relations(self) -> List["RelationSchema"]:
        """return subject relations in an ordered way"""
        # mypy: "RelationDefinitionSchema" has no attribute "order"
        # this is a dynamically setted attribue using self.__dict__.update(some_dict)
        return sorted(
            self.subject_relations.values(),
            key=lambda x: x.relation_definition(self, x.objects(self)[0]).order,  # type: ignore
        )

    _RelationDefinitionsReturnType = Generator[
        Tuple["RelationSchema", Tuple["EntitySchema", ...], str], Any, None
    ]

    @deprecation.argument_renamed(old_name="includefinal", new_name="include_final")
    def relation_definitions(self, include_final: bool = False) -> "_RelationDefinitionsReturnType":
        """return an iterator on relation definitions

        if include_final is false, only non attribute relation are returned

        a relation definition is a 3-uple :
        * schema of the (non final) relation
        * schemas of the possible destination entity types
        * a string telling if this is a 'subject' or 'object' relation
        """
        for relation_schema in self.ordered_relations():
            if include_final or not relation_schema.final:
                yield relation_schema, relation_schema.objects(self), "subject"

        for relation_schema in self.object_relations.values():
            yield relation_schema, relation_schema.subjects(self), "object"

    @deprecation.argument_renamed(old_name="rtype", new_name="relation_schema")
    def destination(self, relation_schema: "RelationSchema") -> "EntitySchema":
        """return the type or schema of entities related by the given subject relation

        `relation_schema` is expected to be a non ambiguous relation
        """
        relation_schema = self.subject_relations[relation_schema]
        object_types = relation_schema.objects(self.type)

        assert len(object_types) == 1, (
            self.type,
            str(relation_schema),
            [str(ot) for ot in object_types],
        )

        return object_types[0]

    # attributes description ###########

    def attribute_definitions(
        self,
    ) -> Generator[Tuple["RelationSchema", "EntitySchema"], Any, None]:
        """return an iterator on attribute definitions

        attribute relations are a subset of subject relations where the
        object's type is a final entity

        an attribute definition is a 2-uple :
        * schema of the (final) relation
        * schema of the destination entity type
        """
        for relation_schema in self.ordered_relations():
            if not relation_schema.final:
                continue

            yield relation_schema, relation_schema.objects(self)[0]

    def main_attribute(self) -> Optional["RelationSchema"]:
        """convenience method that returns the *main* (i.e. the first non meta)
        attribute defined in the entity schema
        """
        for relation_schema, _ in self.attribute_definitions():
            if not self.is_metadata(relation_schema):
                return relation_schema

        return None

    def defaults(self) -> Generator[Tuple["RelationSchema", Any], Any, None]:
        """return an iterator on (attribute name, default value)"""
        for relation_schema in self.subject_relations.values():
            if relation_schema.final:
                value = self.default(relation_schema.type)

                if value is not None:
                    yield relation_schema, value

    @deprecation.argument_renamed(old_name="rtype", new_name="relation_type")
    def default(self, relation_type: yams_types.DefinitionName) -> Any:
        """return the default value of a subject relation"""
        relation_definition = self.relation_definition(relation_type, take_first=True)
        # mypy: "RelationDefinitionSchema" has no attribute "default"
        # this is a dynamically setted attribue using self.__dict__.update(some_dict)
        default = relation_definition.default  # type: ignore

        if callable(default):
            default = default()

        if default is MARKER:
            default = None
        elif default is not None:
            return convert_default_value(relation_definition, default)

        return default

    @deprecation.argument_renamed(old_name="rtype", new_name="relation_type")
    def has_unique_values(self, relation_type: yams_types.DefinitionName) -> bool:
        """convenience method to check presence of the UniqueConstraint on a
        relation
        """
        return bool(self.relation_definition(relation_type).constraint_by_class(UniqueConstraint))

    # metadata attributes #############

    @cached
    def meta_attributes(self) -> Dict["RelationSchema", Tuple[str, str]]:
        """return a dictionnary defining meta-attributes:
        * key is an attribute schema
        * value is a 2-uple (metadata name, described attribute name)

        a metadata attribute is expected to be named using the following scheme:

          <described attribute name>_<metadata name>

        for instance content_format is the format metadata of the content
        attribute (if it exists).
        """
        meta_attributes = {}

        for relation_schema, _ in self.attribute_definitions():
            try:
                attribute, meta = relation_schema.type.rsplit("_", -1)
            except ValueError:
                continue

            if meta in KNOWN_METAATTRIBUTES and attribute in self.subject_relations:
                meta_attributes[relation_schema] = (meta, attribute)

        return meta_attributes

    @deprecation.argument_renamed(old_name="attr", new_name="attribute")
    def has_metadata(self, attribute, metadata) -> Optional["RelationSchema"]:
        """return metadata's relation schema if this entity has the given
        `metadata` field for the given `attribute` attribute
        """
        return self.subject_relations.get("%s_%s" % (attribute, metadata))

    @deprecation.argument_renamed(old_name="attr", new_name="attribute")
    def is_metadata(self, attribute) -> Optional[Tuple[str, str]]:
        """return a metadata for an attribute (None if unspecified)"""
        try:
            attribute, metadata = str(attribute).rsplit("_", 1)
        except ValueError:
            return None

        if metadata in KNOWN_METAATTRIBUTES and attribute in self.subject_relations:
            return (attribute, metadata)

        return None

    # full text indexation control #####

    def indexable_attributes(self) -> Generator["RelationSchema", Any, None]:
        """return the relation schema of attribtues to index"""
        for relation_schema in self.subject_relations.values():
            if relation_schema.final:
                try:
                    # mypy: "RelationDefinitionSchema" has no attribute "fulltextindexed"
                    # this is a dynamically setted attribue using self.__dict__.update(some_dict)
                    if self.relation_definition(relation_schema).fulltextindexed:  # type: ignore
                        yield relation_schema
                except AttributeError:
                    # fulltextindexed is only available on String / Bytes
                    continue

    def fulltext_relations(self) -> Generator[Tuple["RelationSchema", str], Any, None]:
        """return the (name, role) of relations to index"""
        for relation_schema in self.subject_relations.values():
            if not relation_schema.final and relation_schema.fulltext_container == "subject":
                yield relation_schema, "subject"

        for relation_schema in self.object_relations.values():
            if relation_schema.fulltext_container == "object":
                yield relation_schema, "object"

    def fulltext_containers(self) -> Generator[Tuple["RelationSchema", str], Any, None]:
        """return relations whose extremity points to an entity that should
        contains the full text index content of entities of this type
        """
        for relation_schema in self.subject_relations.values():
            if relation_schema.fulltext_container == "object":
                yield relation_schema, "object"

        for relation_schema in self.object_relations.values():
            if relation_schema.fulltext_container == "subject":
                yield relation_schema, "subject"

    # resource accessors ##############

    @deprecation.argument_renamed(old_name="skiprels", new_name="skip_relations")
    def is_subobject(
        self,
        strict: bool = False,
        skip_relations: Sequence[Tuple["RelationSchema", str]] = (),
    ) -> bool:
        """return True if this entity type is contained by another. If strict,
        return True if this entity type *must* be contained by another.
        """
        for relation_schema in self.object_relations.values():
            if (relation_schema, "object") in skip_relations:
                continue

            relation_definition = self.relation_definition(
                relation_schema.type, "object", take_first=True
            )

            # mypy: "RelationDefinitionSchema" has no attribute "composite"
            # this is a dynamically setted attribue using self.__dict__.update(some_dict)
            # same for cardinality just after
            if relation_definition.composite == "subject":  # type: ignore
                if not strict or relation_definition.cardinality[1] in "1+":  # type: ignore
                    return True

        for relation_schema in self.subject_relations.values():
            if (relation_schema, "subject") in skip_relations:
                continue

            if relation_schema.final:
                continue

            relation_definition = self.relation_definition(
                relation_schema.type, "subject", take_first=True
            )

            # mypy: "RelationDefinitionSchema" has no attribute "composite"
            # this is a dynamically setted attribue using self.__dict__.update(some_dict)
            # same for cardinality just after
            if relation_definition.composite == "object":  # type: ignore
                if not strict or relation_definition.cardinality[0] in "1+":  # type: ignore
                    return True

        return False

    # validation ######################

    def check(
        self,
        entity: Dict["RelationSchema", Any],
        creation: bool = False,
        _=None,
        relations: Optional[List["RelationSchema"]] = None,
    ) -> None:
        """check the entity and raises an ValidationError exception if it
        contains some invalid fields (ie some constraints failed)
        """

        if _ is not None:
            warnings.warn(
                "[yams 0.36] _ argument is deprecated, remove it", DeprecationWarning, stacklevel=2
            )

        # mypy: Name '_' already defined on line 593
        # we force redeclaration because of the previous if
        # we probably want to remove all this very old depreciation code tbh...
        _: Type[str] = str  # type: ignore
        errors: Dict[str, str] = {}
        message_arguments: Dict[str, Any] = {}
        i18nvalues: List[str] = []
        relations = relations or list(self.subject_relations.values())

        for relation_schema in relations:
            if not relation_schema.final:
                continue

            aschema = self.destination(relation_schema)
            qname = role_name(relation_schema, "subject")
            relation_definition = relation_schema.relation_definition(self.type, aschema.type)

            # don't care about rhs cardinality, always '*' (if it make senses)
            # mypy: "RelationDefinitionSchema" has no attribute "cardinality"
            # this is a dynamically setted attribue using self.__dict__.update(some_dict)
            card = relation_definition.cardinality[0]  # type: ignore

            assert card in "?1"

            required = card == "1"

            # check value according to their type
            if relation_schema in entity:
                value = entity[relation_schema]
            else:
                if creation and required:
                    # missing required attribute with no default on creation
                    # is not autorized
                    errors[qname] = _("required attribute")
                # on edition, missing attribute is considered as no changes
                continue

            # skip other constraint if value is None and None is allowed
            if value is None:
                if required:
                    errors[qname] = _("required attribute")

                continue

            if not aschema.check_value(value):
                errors[qname] = _('incorrect value (%(KEY-value)r) for type "%(KEY-type)s"')
                message_arguments[qname + "-value"] = value
                message_arguments[qname + "-type"] = aschema.type

                i18nvalues.append(qname + "-type")

                if isinstance(value, bytes) and aschema == "String":
                    errors[qname] += "; you might want to try unicode"

                continue

            # ensure value has the correct python type
            nvalue = aschema.convert_value(value)

            if nvalue != value:
                # don't change what's has not changed, who knows what's behind
                # this <entity> thing
                entity[relation_schema] = value = nvalue

            # check arbitrary constraints
            # mypy: "RelationDefinitionSchema" has no attribute "constraint"
            # this is a dynamically setted attribue using self.__dict__.update(some_dict)
            for constraint in relation_definition.constraints:  # type: ignore
                if not constraint.check(entity, relation_schema, value):
                    message, args = constraint.failed_message(qname, value, entity)
                    errors[qname] = message

                    message_arguments.update(args)

                    break

        if errors:
            raise ValidationError(entity, errors, message_arguments, i18nvalues)

    def check_value(self, value: Any) -> bool:
        """check the value of a final entity (ie a const value)"""
        return self.field_checkers[self.type](self, value)

    def convert_value(self, value: Any) -> Any:
        """check the value of a final entity (ie a const value)"""
        if self.type in self.field_converters:
            return self.field_converters[self.type](value)
        else:
            return value

    @deprecation.argument_renamed(old_name="rtype", new_name="relation_type")
    def vocabulary(self, relation_type: yams_types.DefinitionName) -> Tuple[str, ...]:
        """backward compat return the vocabulary of a subject relation"""
        constraint = self.relation_definition(relation_type).constraint_by_interface(
            IVocabularyConstraint
        )

        if constraint is None:
            raise AssertionError(f"field {relation_type} of entity {self} has no vocabulary")

        return constraint.vocabulary()  # type: ignore # we know it's a StaticVocabularyConstraint


@deprecation.attribute_renamed(old_name="rtype", new_name="relation_type")
class RelationDefinitionSchema(PermissionMixIn):
    """a relation definition is fully caracterized relation, eg

    <subject type> <relation type> <object type>
    """

    _RPROPERTIES: Dict[str, Any] = {
        "cardinality": None,
        "constraints": (),
        "order": 9999,
        "description": "",
        "infered": False,
        "permissions": None,
    }
    _NONFINAL_RPROPERTIES: Dict[str, Any] = {"composite": None}
    _FINAL_RPROPERTIES: Dict[str, Any] = {
        "default": None,
        "uid": False,
        "indexed": False,
        "formula": None,
    }
    # Use a TYPE_PROPERTIES dictionnary to store type-dependant parameters.
    # in certains situation in yams.register_base_type, the value of the
    # subdict can be None or Any
    BASE_TYPE_PROPERTIES: Dict[str, Dict[str, Any]] = {
        "String": {"fulltextindexed": False, "internationalizable": False},
        "Bytes": {"fulltextindexed": False},
    }

    @classmethod
    def ALL_PROPERTIES(cls) -> Set[str]:
        return set(
            chain(
                cls._RPROPERTIES,
                cls._NONFINAL_RPROPERTIES,
                cls._FINAL_RPROPERTIES,
                *cls.BASE_TYPE_PROPERTIES.values(),
            )
        )

    @deprecation.argument_renamed(old_name="rtype", new_name="relation_type")
    def __init__(
        self,
        subject: "EntitySchema",
        relation_type: "RelationSchema",
        object: "EntitySchema",
        package: str,
        values: Optional[Dict[str, Any]] = None,
    ) -> None:

        if values is not None:
            self.update(values)

        self.subject: "EntitySchema" = subject
        self.relation_type: "RelationSchema" = relation_type
        self.object: "EntitySchema" = object
        self.package: str = package
        self.infered: bool = False

    @property
    def ACTIONS(self) -> Tuple[str, ...]:
        if self.relation_type.final:
            return ("read", "add", "update")
        else:
            return ("read", "add", "delete")

    def update(self, values: Dict[str, Any]) -> None:
        # XXX check we're copying existent properties
        self.__dict__.update(values)

    def __str__(self) -> str:
        if self.object.final:
            return "attribute %s.%s[%s]" % (self.subject, self.relation_type, self.object)

        return "relation %s %s %s" % (self.subject, self.relation_type, self.object)

    def __repr__(self) -> str:
        return "<%s at @%#x>" % (self, id(self))

    def as_triple(self) -> Tuple["EntitySchema", "RelationSchema", "EntitySchema"]:
        return (self.subject, self.relation_type, self.object)

    def advertise_new_add_permission(self) -> None:
        """handle backward compatibility with pre-add permissions

        * if the update permission was () [empty tuple], use the
          default attribute permissions for `add`

        * else copy the `update` rule for `add`
        """
        if "add" not in self.permissions:
            if self.permissions["update"] == ():
                defaultaddperms = DEFAULT_ATTRPERMS["add"]
            else:
                defaultaddperms = self.permissions["update"]

            self.permissions["add"] = defaultaddperms
            warnings.warn(
                '[yams 0.39] %s: new "add" permissions on attribute '
                "set to %s by default, but you must make it explicit" % (self, defaultaddperms),
                DeprecationWarning,
            )

    @classmethod
    @deprecation.argument_renamed(old_name="desttype", new_name="destination_type")
    def rproperty_defs(cls, destination_type) -> Dict[str, Any]:
        """return a dictionary mapping property name to its definition for each
        allowable properties when the relation has `destination_type` as target entity's
        type
        """
        property_definitions = cls._RPROPERTIES.copy()

        if destination_type not in BASE_TYPES:
            property_definitions.update(cls._NONFINAL_RPROPERTIES)
        else:
            property_definitions.update(cls._FINAL_RPROPERTIES)
            property_definitions.update(cls.BASE_TYPE_PROPERTIES.get(destination_type, {}))

        return property_definitions

    def rproperties(self) -> Dict[str, Any]:
        """same as .rproperty_defs class method, but for instances (hence
        destination is known to be self.object).
        """
        return self.rproperty_defs(self.object)

    def get(self, key, default=None) -> Any:
        return getattr(self, key, default)

    @property
    # def final(self) -> bool:  # can't set it (bug): https://github.com/python/mypy/issues/4125
    def final(self):
        return self.relation_type.final

    def dump(
        self: "RelationDefinitionSchema", subject: "EntitySchema", object: "EntitySchema"
    ) -> "RelationDefinitionSchema":
        return self.__class__(subject, self.relation_type, object, self.package, self.__dict__)

    def role_cardinality(self, role) -> Any:
        # mypy: "RelationDefinitionSchema" has no attribute "cardinality"
        # this is a dynamically setted attribue using self.__dict__.update(some_dict)
        return self.cardinality[role == "object"]  # type: ignore

    @deprecation.argument_renamed(old_name="cstrtype", new_name="constraint_type")
    def constraint_by_type(self, constraint_type: str) -> Optional[BaseConstraint]:
        # mypy: "RelationDefinitionSchema" has no attribute "constraints"
        # this is a dynamically setted attribue using self.__dict__.update(some_dict)
        for constraint in self.constraints:  # type: ignore
            if constraint.type() == constraint_type:
                return constraint

        return None

    def constraint_by_class(self, cls: Type[BaseConstraint]) -> Optional[BaseConstraint]:
        # mypy: "RelationDefinitionSchema" has no attribute "constraints"
        # this is a dynamically setted attribue using self.__dict__.update(some_dict)
        for constraint in self.constraints:  # type: ignore
            if isinstance(constraint, cls):
                return constraint

        return None

    @deprecation.argument_renamed(old_name="iface", new_name="interface")
    def constraint_by_interface(self, interface: Type[IConstraint]) -> Optional[BaseConstraint]:
        # mypy: "RelationDefinitionSchema" has no attribute "constraints"
        # this is a dynamically setted attribue using self.__dict__.update(some_dict)
        for constraint in self.constraints:  # type: ignore
            if implements(constraint, interface):
                return constraint

        return None

    def check_permission_definitions(self) -> None:
        """check permissions are correctly defined"""
        super(RelationDefinitionSchema, self).check_permission_definitions()

        # mypy: "RelationDefinitionSchema" has no attribute "formula"
        # this is a dynamically setted attribue using self.__dict__.update(some_dict)
        if (
            self.final
            and self.formula  # type: ignore
            and (self.permissions["add"] or self.permissions["update"])
        ):
            raise BadSchemaDefinition(f"Cannot set add/update permissions on computed {self}")


@deprecation.attribute_renamed(old_name="rdefs", new_name="relation_definitions")
@deprecation.attribute_renamed(old_name="rdef_class", new_name="relation_definition_class")
@deprecation.attribute_renamed(old_name="_subj_schemas", new_name="_subject_schemas")
@deprecation.attribute_renamed(old_name="_obj_schemas", new_name="_object_schemas")
class RelationSchema(ERSchema):
    """A relation is a named and oriented link between two entities.
    A relation schema defines the possible types of both extremities.

    Cardinality between the two given entity's type is defined
    as a 2 characters string where each character is one of:
     - 1 <-> 1..1 <-> one and only one
     - ? <-> 0..1 <-> zero or one
     - + <-> 1..n <-> one or more
     - * <-> 0..n <-> zero or more
    """

    __implements__ = IRelationSchema
    symmetric: bool = False
    inlined: bool = False
    fulltext_container = None
    rule = None
    # if this relation is an attribute relation
    final: bool = False
    # only when rule is not None, for later propagation to
    # computed relation definitions
    permissions: Optional[yams_types.Permissions] = None
    relation_definition_class = RelationDefinitionSchema

    @deprecation.argument_renamed(old_name="rdef", new_name="relation_definition")
    def __init__(
        self, schema: "Schema", relation_definition: yams_types.RelationType = None, **kwargs
    ) -> None:
        # XXX why would relation_definition be None ?
        if relation_definition is not None:
            # XXX make this a factory and have two classes Relation and ComputedRelation ?
            if relation_definition.rule:
                self._init_computed_relation(relation_definition)
            else:
                self._init_relation(relation_definition)

            # mapping to subject/object with schema as key
            self._subject_schemas: Dict["EntitySchema", List["EntitySchema"]] = {}
            self._object_schemas: Dict["EntitySchema", List["EntitySchema"]] = {}

            # relation properties
            self.relation_definitions: Dict[
                Tuple[yams_types.DefinitionName, yams_types.DefinitionName],
                RelationDefinitionSchema,
            ] = {}

        super(RelationSchema, self).__init__(schema, relation_definition)

    @deprecation.argument_renamed(old_name="rdef", new_name="relation_definition")
    def _init_relation(self, relation_definition: yams_types.RelationType) -> None:
        if relation_definition.rule is not MARKER:
            raise BadSchemaDefinition("Relation has no rule attribute")

        # if this relation is symmetric/inlined
        self.symmetric = bool(relation_definition.symmetric)
        self.inlined = bool(relation_definition.inlined)

        # if full text content of subject/object entity should be added
        # to other side entity (the container)
        self.fulltext_container = relation_definition.fulltext_container or None

    @deprecation.argument_renamed(old_name="rdef", new_name="relation_definition")
    def _init_computed_relation(self, relation_definition: yams_types.RelationType) -> None:
        """computed relation are specific relation with only a rule attribute.

        Reponsibility to infer associated relation definitions is left to client
        code defining what's in rule (eg rql snippet in cubicweb).
        """
        for attribute in ("inlined", "symmetric", "fulltext_container"):
            if getattr(relation_definition, attribute, MARKER) is not MARKER:
                raise BadSchemaDefinition(f"Computed relation has no {attribute} attribute")

        if relation_definition.__permissions__ is MARKER:
            permissions = DEFAULT_COMPUTED_RELPERMS
        else:
            permissions = relation_definition.__permissions__

        self.rule = relation_definition.rule
        self.permissions = permissions

    def __repr__(self) -> str:
        return "<%s [%s]>" % (
            self.type,
            "; ".join("%s,%s" % (s, o) for (s, o), _ in self.relation_definitions.items()),
        )

    def _rehash(self) -> None:
        self._subject_schemas = rehash(self._subject_schemas)
        self._object_schemas = rehash(self._object_schemas)
        self.relation_definitions = rehash(self.relation_definitions)

    # schema building methods #################################################

    @deprecation.argument_renamed(old_name="subjschema", new_name="subject_schema")
    @deprecation.argument_renamed(old_name="objschema", new_name="object_schema")
    @deprecation.argument_renamed(old_name="rdef", new_name="relation_definition")
    def update(
        self,
        subject_schema: "EntitySchema",
        object_schema: "EntitySchema",
        relation_definition: yams_types.RelationDefinition,
    ) -> Optional[RelationDefinitionSchema]:
        """Allow this relation between the two given types schema"""
        if subject_schema.final:
            raise BadSchemaDefinition(
                f"final type {subject_schema} can't be the subject of a relation"
            )

        # check final consistency:
        # * a final relation only points to final entity types
        # * a non final relation only points to non final entity types
        final = object_schema.final

        for entity_schema in self.objects():
            if entity_schema is object_schema:
                continue

            if final != entity_schema.final:
                if final:
                    final_entity_schema = subject_schema
                    final_relation_schema, not_final_relation_schema = object_schema, entity_schema
                else:
                    final_entity_schema = self.subjects()[0]
                    final_relation_schema, not_final_relation_schema = entity_schema, object_schema

                raise BadSchemaDefinition(
                    f"ambiguous relation: '{final_entity_schema}.{self.type}' is final "
                    f"({final_relation_schema}) but not '{subject_schema}.{self.type}' "
                    f"({not_final_relation_schema})"
                )

        constraints = getattr(relation_definition, "constraints", None)

        if constraints:
            for constraint in constraints:
                constraint.check_consistency(subject_schema, object_schema, relation_definition)

        if (subject_schema, object_schema) in self.relation_definitions and self.symmetric:
            return None

        # update our internal struct
        if final and self.symmetric:
            raise BadSchemaDefinition("symmetric makes no sense on final relation")
        if final and self.inlined:
            raise BadSchemaDefinition("inlined makes no sense on final relation")
        if final and self.fulltext_container:
            raise BadSchemaDefinition("fulltext_container makes no sense on final relation")

        self.final = final
        relation_definitions = self.init_rproperties(
            subject_schema, object_schema, relation_definition
        )

        self._add_relation_definition(relation_definitions)

        return relation_definitions

    @deprecation.argument_renamed(old_name="rdef", new_name="relation_definition")
    def _add_relation_definition(
        self, relation_definition: yams_types.RelationDefinitionSchema
    ) -> None:
        # update our internal struct
        self.relation_definitions[
            (relation_definition.subject.type, relation_definition.object.type)
        ] = relation_definition
        self._update(relation_definition.subject, relation_definition.object)

        if self.symmetric:
            self._update(relation_definition.object, relation_definition.subject)

            if relation_definition.object != relation_definition.subject:
                self.relation_definitions[
                    (relation_definition.object.type, relation_definition.subject.type)
                ] = relation_definition

        # mypy:"RelationDefinitionSchema" has no attribute "cardinality"; maybe "role_cardinality"?
        # this is a dynamically setted attribue using self.__dict__.update(some_dict)
        if self.inlined and relation_definition.cardinality[0] in "*+":  # type: ignore
            raise BadSchemaDefinition(
                f"inlined relation {relation_definition} can't have multiple cardinality for its "
                "subject"
            )

        # update entity types schema
        relation_definition.subject.add_subject_relation(self)

        if self.symmetric:
            relation_definition.object.add_subject_relation(self)
        else:
            relation_definition.object.add_object_relation(self)

    _add_rdef = deprecation.callable_renamed(
        old_name="_add_rdef", new_function=_add_relation_definition
    )

    @deprecation.argument_renamed(old_name="subjectschema", new_name="subject_schema")
    @deprecation.argument_renamed(old_name="objectschema", new_name="object_schema")
    def _update(self, subject_schema: "EntitySchema", object_schema: "EntitySchema") -> None:
        object_types = self._subject_schemas.setdefault(subject_schema, [])

        if object_schema not in object_types:
            object_types.append(object_schema)

        subject_types = self._object_schemas.setdefault(object_schema, [])

        if subject_schema not in subject_types:
            subject_types.append(subject_schema)

    @deprecation.argument_renamed(old_name="subjschema", new_name="subject_schema")
    @deprecation.argument_renamed(old_name="objschema", new_name="object_schema")
    def del_relation_def(
        self,
        subject_schema: "EntitySchema",
        object_schema: "EntitySchema",
        _recursing: bool = False,
    ) -> bool:
        try:
            self._subject_schemas[subject_schema].remove(object_schema)

            if len(self._subject_schemas[subject_schema]) == 0:
                del self._subject_schemas[subject_schema]

                subject_schema.del_subject_relation(self)
        except (ValueError, KeyError):
            pass

        try:
            self._object_schemas[object_schema].remove(subject_schema)

            if len(self._object_schemas[object_schema]) == 0:
                del self._object_schemas[object_schema]

                object_schema.del_object_relation(self)
        except (ValueError, KeyError):
            pass

        try:
            del self.relation_definitions[(subject_schema.type, object_schema.type)]
        except KeyError:
            pass

        try:
            if self.symmetric and subject_schema != object_schema and not _recursing:
                self.del_relation_def(object_schema, subject_schema, True)
        except KeyError:
            pass

        if not self._object_schemas or not self._subject_schemas:
            assert not self._object_schemas and not self._subject_schemas

            return True

        return False

    # relation definitions properties handling ################################

    # XXX move to RelationDefinitionSchema

    @deprecation.argument_renamed(old_name="buildrdef", new_name="build_relation_definition")
    def init_rproperties(
        self,
        subject: "EntitySchema",
        object: "EntitySchema",
        build_relation_definition: yams_types.RelationDefinition,
    ) -> RelationDefinitionSchema:
        key = (subject.type, object.type)

        # raise an error if already defined unless the defined relalation has
        # been infered, in which case we may want to replace it
        if key in self.relation_definitions and not self.relation_definitions[key].infered:
            raise BadSchemaDefinition(f"({subject}, {object}) already defined for {self}")

        self.relation_definitions[key] = relation_definition = self.relation_definition_class(
            subject, self, object, build_relation_definition.package
        )

        for property_, default in relation_definition.rproperties().items():
            relation_definition_value = getattr(build_relation_definition, property_, MARKER)

            if relation_definition_value is MARKER:
                if property_ == "permissions":
                    relation_definition_value = default = build_relation_definition.get_permissions(
                        self.final
                    ).copy()

                if property_ == "cardinality":
                    default = (object in BASE_TYPES) and "?1" or "**"

            else:
                default = relation_definition_value

            setattr(relation_definition, property_, default)

        return relation_definition

    # IRelationSchema interface ###############################################

    def associations(self) -> List[Tuple["EntitySchema", List]]:
        """return a list of (subject, [objects]) defining between which types
        this relation may exists
        """

        # XXX deprecates in favor of iter_relation_definitions() ?
        return list(self._subject_schemas.items())

    @deprecation.argument_renamed(old_name="etype", new_name="entity_schema")
    def subjects(
        self, entity_schema: Optional["EntitySchema"] = None
    ) -> Tuple["EntitySchema", ...]:
        """Return a list of entity schemas which can be subject of this relation.

        If entity_schema is not None, return a list of schemas which can be subject of
        this relation with entity_schema as object.

        :raise `KeyError`: if entity_schema is not a subject entity type.
        """
        if entity_schema is None:
            return tuple(self._subject_schemas)

        if entity_schema in self._object_schemas:
            return tuple(self._object_schemas[entity_schema])
        else:
            raise KeyError(f"{self} does not have {entity_schema} as object")

    @deprecation.argument_renamed(old_name="etype", new_name="entity_schema")
    def objects(self, entity_schema: Optional["EntitySchema"] = None) -> Tuple["EntitySchema", ...]:
        """Return a list of entity schema which can be object of this relation.

        If entity_schema is not None, return a list of schemas which can be object of
        this relation with entity_schema as subject.

        :raise `KeyError`: if entity_schema is not an object entity type.
        """
        if entity_schema is None:
            return tuple(self._object_schemas)

        try:
            return tuple(self._subject_schemas[entity_schema])
        except KeyError:
            raise KeyError(f"{self} does not have {entity_schema} as subject")

    @deprecation.argument_renamed(old_name="etype", new_name="entity_schema")
    def targets(
        self, entity_schema: Optional["EntitySchema"] = None, role: str = "subject"
    ) -> Tuple["EntitySchema", ...]:
        """return possible target types with <entity_schema> as <x>"""
        if role == "subject":
            return self.objects(entity_schema)

        return self.subjects(entity_schema)

    def relation_definition(
        self, subject: yams_types.DefinitionName, object: yams_types.DefinitionName
    ) -> yams_types.RelationDefinitionSchema:
        """return the properties dictionary of a relation"""
        if (subject, object) in self.relation_definitions:
            return self.relation_definitions[(subject, object)]
        else:
            raise KeyError(f"{subject} {self} {object}")

    rdef = deprecation.callable_renamed(old_name="rdef", new_function=relation_definition)

    @deprecation.argument_renamed(old_name="etype", new_name="entity_type")
    @deprecation.argument_renamed(old_name="ttype", new_name="target_type")
    def role_relation_definition(
        self,
        entity_type: yams_types.DefinitionName,
        target_type: yams_types.DefinitionName,
        role: str,
    ) -> yams_types.RelationDefinitionSchema:
        if role == "subject":
            return self.relation_definitions[(entity_type, target_type)]

        return self.relation_definitions[(target_type, entity_type)]

    role_rdef = deprecation.callable_renamed(
        old_name="role_rdef", new_function=role_relation_definition
    )

    def check_permission_definitions(self) -> None:
        """check permissions are correctly defined"""
        for relation_definition in self.relation_definitions.values():
            relation_definition.check_permission_definitions()

        if (
            self.rule
            and self.permissions
            and (self.permissions.get("add") or self.permissions.get("delete"))
        ):
            raise BadSchemaDefinition(
                f"Cannot set add/delete permissions on computed relation {self.type}"
            )


class Schema:
    """set of entities and relations schema defining the possible data sets
    used in an application


    :type name: str
    :ivar name: name of the schema, usually the application identifier

    :type base: str
    :ivar base: path of the directory where the schema is defined
    """

    __implements__ = ISchema
    entity_class = EntitySchema
    relation_class = RelationSchema
    # relation that should not be infered according to entity type inheritance
    no_specialization_inference = ()

    # these are overridden by set_log_methods below
    # only defining here to prevent checkers from complaining
    info = warning = error = critical = exception = debug = lambda message, *a, **kw: None

    def __init__(self, name: str, construction_mode: str = "strict") -> None:
        super(Schema, self).__init__()
        self.name = name

        # with construction_mode != 'strict', no error when trying to add a
        # relation using an undefined entity type, simply log the error
        # right now, construction_mode != 'strict' is only used by migractions
        self.construction_mode = construction_mode
        self._entities: Dict = {}
        self._relations: Dict = {}

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._rehash()

    def _rehash(self) -> None:
        """rehash schema's internal structures"""
        for entity_schema in self._entities.values():
            entity_schema._rehash()

        for relation_schema in self._relations.values():
            relation_schema._rehash()

    def get(self, name: yams_types.DefinitionName, default=None) -> Any:
        if name in self:
            return self[name]
        else:
            return default

    def __getitem__(
        self, name: yams_types.DefinitionName
    ) -> Union["EntitySchema", "RelationSchema"]:
        try:
            return self.entity_schema_for(name)
        except KeyError:
            return self.relation_schema_for(name)

    def __contains__(self, name: yams_types.DefinitionName) -> bool:
        try:
            self[name]
        except KeyError:
            return False

        return True

    # schema building methods #################################################

    @deprecation.argument_renamed(old_name="edef", new_name="entity_definition")
    def add_entity_type(self, entity_definition: yams_types.EntityType) -> EntitySchema:
        """Add an entity schema definition for an entity's type.

        :type entity_definition: str
        :param entity_definition: the name of the entity type to define

        :raise `BadSchemaDefinition`: if the entity type is already defined
        :relation_type: `EntitySchema`
        :return: the newly created entity schema instance
        """
        entity_type = entity_definition.name

        if entity_type in self._entities:
            raise BadSchemaDefinition(f"entity type {entity_type} is already defined")

        entity_schema = self.entity_class(self, entity_definition)
        self._entities[entity_type] = entity_schema

        return entity_schema

    @deprecation.argument_renamed(old_name="oldname", new_name="old_name")
    @deprecation.argument_renamed(old_name="newname", new_name="new_name")
    def rename_entity_type(self, old_name: str, new_name: str) -> None:
        """renames an entity type and update internal structures accordingly"""
        entity_schema = self._entities.pop(old_name)
        entity_schema.type = new_name
        self._entities[new_name] = entity_schema

        # XXX are we renaming in enough places?
        # the erschema.type might be stored in other locations
        # this rename_entity_type seems dangeurous tbh
        for relation in self._relations.values():
            to_rename: List[
                Tuple[Tuple[str, str], Tuple[str, str], yams_types.RelationDefinition]
            ] = []

            for a_b, relation_definition in relation.relation_definitions.items():
                if a_b == (old_name, old_name):
                    to_rename.append((a_b, (new_name, new_name), relation_definition))
                elif a_b[0] == old_name:
                    to_rename.append((a_b, (new_name, a_b[1]), relation_definition))
                elif a_b[1] == old_name:
                    to_rename.append((a_b, (a_b[0], new_name), relation_definition))

            for old, new, relation_definition in to_rename:
                del relation.relation_definitions[old]
                relation.relation_definitions[new] = relation_definition

        # rebuild internal structures since eschema's hash value has changed
        self._rehash()

    @deprecation.argument_renamed(old_name="rtypedef", new_name="relation_type_definition")
    def add_relation_type(
        self, relation_type_definition: yams_types.RelationType
    ) -> RelationSchema:
        relation_type = relation_type_definition.name

        if relation_type in self._relations:
            raise BadSchemaDefinition(f"relation type {relation_type} is already defined")

        relation_schema = self.relation_class(self, relation_type_definition)
        self._relations[relation_type] = relation_schema

        return relation_schema

    @deprecation.argument_renamed(old_name="rdef", new_name="relation_definition")
    def add_relation_def(
        self, relation_definition: yams_types.RelationDefinition
    ) -> Optional[RelationDefinitionSchema]:
        """build a part of a relation schema:
        add a relation between two specific entity's types

        :relation_type: RelationSchema
        :return: the newly created or simply completed relation schema
        """
        relation_type = yams_types.DefinitionName(relation_definition.name)

        try:
            relation_schema = self.relation_schema_for(relation_type)
        except KeyError:
            # returns here are to break the function but don't return anything
            # shouldn't it raise instead?
            self._building_error(f"using unknown relation type in {relation_definition}")
            return None

        try:
            subject_schema = self.entity_schema_for(relation_definition.subject)
        except KeyError:
            self._building_error(
                f"using unknown type {repr(relation_definition.subject)} in relation "
                f"{relation_type}"
            )
            return None

        try:
            object_schema = self.entity_schema_for(relation_definition.object)
        except KeyError:
            self._building_error(
                f"using unknown type {repr(relation_definition.object)} in relation {relation_type}"
            )
            return None

        return relation_schema.update(subject_schema, object_schema, relation_definition)

    def _building_error(self, message, *args) -> None:
        if self.construction_mode == "strict":
            raise BadSchemaDefinition(message % args)

        self.critical(message, *args)

    @deprecation.argument_renamed(old_name="rtype", new_name="relation_type")
    @deprecation.argument_renamed(old_name="subjtype", new_name="subject_type")
    @deprecation.argument_renamed(old_name="objtype", new_name="object_type")
    def del_relation_def(
        self,
        subject_type: yams_types.DefinitionName,
        relation_type: yams_types.DefinitionName,
        object_type: yams_types.DefinitionName,
    ) -> None:

        subject_schema = self.entity_schema_for(subject_type)
        object_schema = self.entity_schema_for(object_type)
        relation_schema = self.relation_schema_for(relation_type)

        if relation_schema.del_relation_def(subject_schema, object_schema):
            del self._relations[relation_type]

    @deprecation.argument_renamed(old_name="rtype", new_name="relation_type")
    def del_relation_type(self, relation_type: "RelationSchema") -> None:
        # XXX don't iter directly on the dictionary since it may be changed
        # by del_relation_def
        for subject_type, object_type in list(
            self.relation_schema_for(relation_type.type).relation_definitions
        ):
            self.del_relation_def(subject_type, relation_type.type, object_type)

        if not self.relation_schema_for(relation_type.type).relation_definitions:
            del self._relations[relation_type]

    @deprecation.argument_renamed(old_name="etype", new_name="entity_type")
    def del_entity_type(self, entity_type: str) -> None:
        entity_schema = self._entities[entity_type]

        for relation_schema in list(entity_schema.subject_relations.values()):
            for object_type in relation_schema.objects(entity_type):
                self.del_relation_def(entity_schema, relation_schema, object_type)

        for relation_schema in list(entity_schema.object_relations.values()):
            for subject_type in relation_schema.subjects(entity_type):
                self.del_relation_def(subject_type, relation_schema, entity_schema)

        if entity_schema.specializes():
            entity_schema.specializes()._specialized_by.remove(entity_schema)

        if entity_schema.specialized_by():
            raise Exception(
                "can't remove entity type %s used as parent class by %s"
                % (entity_schema, ",".join(str(et) for et in entity_schema.specialized_by()))
            )

        del self._entities[entity_type]

        if entity_schema.final:
            yams.unregister_base_type(entity_type)

    def infer_specialization_rules(self) -> None:
        for relation_schema in self.relations():
            if relation_schema in self.no_specialization_inference:
                continue

            for (subject, object), relation_definition in list(
                relation_schema.relation_definitions.items()
            ):
                subject_schema = self.entity_schema_for(subject)
                object_schema = self.entity_schema_for(object)
                subject_entity_schemas = [subject_schema] + subject_schema.specialized_by(
                    recursive=True
                )
                object_entity_schemas = [object_schema] + object_schema.specialized_by(
                    recursive=True
                )

                for subject_schema in subject_entity_schemas:
                    for object_schema in object_entity_schemas:
                        # don't try to add an already defined relation
                        if (subject_schema, object_schema) in relation_schema.relation_definitions:
                            continue

                        this_relation_definition = relation_definition.dump(
                            subject_schema, object_schema
                        )
                        this_relation_definition.infered = True
                        relation_schema._add_relation_definition(this_relation_definition)

    def remove_infered_definitions(self) -> None:
        """remove any infered definitions added by
        `infer_specialization_rules`
        """
        for relation_schema in self.relations():
            if relation_schema.final:
                continue

            for (subject, object), relation_definition in list(
                relation_schema.relation_definitions.items()
            ):
                if relation_definition.infered:
                    subject_schema = self.entity_schema_for(subject)
                    object_schema = self.entity_schema_for(object)
                    relation_schema.del_relation_def(subject_schema, object_schema)

    def rebuild_infered_relations(self) -> None:
        """remove any infered definitions and rebuild them"""
        self.remove_infered_definitions()
        self.infer_specialization_rules()

    # ISchema interface #######################################################

    def entities(self) -> List["EntitySchema"]:
        """return a list of possible entity's type

        :relation_type: list
        :return: defined entity's types (str) or schemas (`EntitySchema`)
        """
        return list(self._entities.values())

    @deprecation.argument_renamed(old_name="etype", new_name="entity_type")
    def has_entity(self, entity_type: str) -> bool:
        """return true the type is defined in the schema

        :type entity_type: str
        :param entity_type: the entity's type

        :relation_type: bool
        :return:
          a boolean indicating whether this type is defined in this schema
        """
        return entity_type in self._entities

    @deprecation.argument_renamed(old_name="etype", new_name="entity_type")
    def entity_schema_for(self, entity_type: yams_types.DefinitionName) -> "EntitySchema":
        """return the entity's schema for the given type

        :relation_type: `EntitySchema`
        :raise `KeyError`: if the type is not defined as an entity
        """
        return self._entities[entity_type]

    eschema = deprecation.callable_renamed("eschema", entity_schema_for)

    def relations(self) -> List["RelationSchema"]:
        """return the list of possible relation'types

        :relation_type: list
        :return: defined relation's types (str) or schemas (`RelationSchema`)
        """
        return list(self._relations.values())

    @deprecation.argument_renamed(old_name="rtype", new_name="relation_type")
    def has_relation(self, relation_type: yams_types.DefinitionName) -> bool:
        """return true the relation is defined in the schema

        :type relation_type: str
        :param relation_type: the relation's type

        :relation_type: bool
        :return:
          a boolean indicating whether this type is defined in this schema
        """
        return relation_type in self._relations

    @deprecation.argument_renamed(old_name="rtype", new_name="relation_type")
    def relation_schema_for(self, relation_type: yams_types.DefinitionName) -> RelationSchema:
        """return the relation schema for the given type

        :relation_type: `RelationSchema`
        """
        if relation_type in self._relations:
            return self._relations[relation_type]
        else:
            raise KeyError(f"No relation named {relation_type} in schema")

    rschema = deprecation.callable_renamed("rschema", relation_schema_for)

    def finalize(self) -> None:
        """Finalize schema

        Can be used to, e.g., infer relations from inheritance, computed
        relations, etc.
        """
        self.infer_specialization_rules()


LOGGER = logging.getLogger("yams")
set_log_methods(Schema, LOGGER)
