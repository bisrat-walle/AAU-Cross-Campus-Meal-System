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
"""Classes used to build a schema."""

from typing import Optional, List
from warnings import warn
from copy import copy

from typing import Any, Generator, Tuple, Type, Union, Dict, Sequence, Iterable, Set, cast, TypeVar

from logilab.common import attrdict, deprecation, nullobject

import yams.types as yams_types

from yams import (
    BASE_TYPES,
    MARKER,
    BadSchemaDefinition,
    KNOWN_METAATTRIBUTES,
    DEFAULT_ETYPEPERMS,
    DEFAULT_RELPERMS,
    DEFAULT_ATTRPERMS,
    DEFAULT_COMPUTED_ATTRPERMS,
)
from yams.constraints import (
    SizeConstraint,
    UniqueConstraint,
    BaseConstraint,
    StaticVocabularyConstraint,
    FORMAT_CONSTRAINT,
)
from yams.schema import RelationDefinitionSchema

Defined = Dict[Union[str, Tuple[str, str, str]], Union["autopackage", "Definition"]]

__docformat__: str = "restructuredtext en"

# will be modified by the yams'reader when schema is
# beeing read
PACKAGE: str = "<builtin>"


__all__: Tuple[str, ...] = (
    "EntityType",
    "RelationType",
    "RelationDefinition",
    "SubjectRelation",
    "ObjectRelation",
    "RichString",
) + tuple(BASE_TYPES)

# EntityType properties
ETYPE_PROPERTIES: Tuple[str, ...] = ("description", "__permissions__", "__unique_together__")

# RelationType properties. Don't put description inside, handled specifically
RTYPE_PROPERTIES: Tuple[str, ...] = ("symmetric", "inlined", "fulltext_container")

# RelationDefinition properties have to be computed dynamically since new ones
# may be added at runtime


def _RELATION_DEFINITION_PROPERTIES() -> Tuple[str, ...]:
    base: Set[str] = RelationDefinitionSchema.ALL_PROPERTIES()

    # infered is an internal property and should not be specified explicitly
    base.remove("infered")

    # replace permissions by __permissions__ as it's spelled that way in schema
    # definition files
    base.remove("permissions")
    base.add("__permissions__")

    return tuple(base)


_RDEF_PROPERTIES = deprecation.callable_renamed(
    old_name="_RDEF_PROPERTIES", new_function=_RELATION_DEFINITION_PROPERTIES
)


# regroup all rtype/rdef properties as they may be defined one on each other in
# some cases


def _RELATION_PROPERTIES() -> Tuple[str, ...]:
    return RTYPE_PROPERTIES + _RELATION_DEFINITION_PROPERTIES()


_REL_PROPERTIES = deprecation.callable_renamed(
    old_name="_REL_PROPERTIES", new_function=_RELATION_PROPERTIES
)


# pre 0.37 backward compat
RDEF_PROPERTIES = ()  # stuff added here is also added to underlying dict, nevermind


CREATION_RANK: int = 0


def _add_constraint(kwargs: Dict[str, Any], constraint: BaseConstraint) -> None:
    """Add constraint to param kwargs."""
    constraints: List[BaseConstraint] = kwargs.setdefault("constraints", [])

    for number, existing_constraint in enumerate(constraints):
        if existing_constraint.__class__ is constraint.__class__:
            constraints[number] = constraint

            return

    constraints.append(constraint)


@deprecation.argument_renamed(old_name="rdef", new_name="relation_definition")
@deprecation.argument_renamed(old_name="insertidx", new_name="insert_index")
def _add_relation(
    relations: List["RelationDefinition"],
    relation_definition: "RelationDefinition",
    name: Optional[yams_types.DefinitionName] = None,
    insert_index: Optional[int] = None,
) -> None:
    """Add relation (param relation_definition) to list of relations (param relations)."""
    if name is not None:
        relation_definition.name = name

    if insert_index is None:
        insert_index = len(relations)

    cast(int, insert_index)

    relations.insert(insert_index, relation_definition)

    if getattr(relation_definition, "metadata", {}):
        # mypy: "RelationDefinition" has no attribute "metadata"
        # dynamic attribute tested in the if
        for meta_name, value in relation_definition.metadata.items():  # type: ignore
            assert meta_name in KNOWN_METAATTRIBUTES

            insert_index += 1  # insert meta after main
            meta_rel_name: str = "_".join(((name or relation_definition.name), meta_name))

            _add_relation(relations, value, meta_rel_name, insert_index)


_KwargsKeyType = TypeVar("_KwargsKeyType")


def _check_kwargs(kwargs: Dict[_KwargsKeyType, Any], attributes: Sequence[_KwargsKeyType]) -> None:
    """Check that all keys of kwargs are actual attributes."""
    for key in kwargs:
        if key not in attributes:
            raise BadSchemaDefinition("no such property %r in %r" % (key, attributes))


@deprecation.argument_renamed(old_name="fromobj", new_name="from_object")
@deprecation.argument_renamed(old_name="toobj", new_name="to_object")
def _copy_attributes(from_object: Any, to_object: Any, attributes: Iterable[str]) -> None:
    for attribute in attributes:
        value = getattr(from_object, attribute, MARKER)

        if value is MARKER:
            continue

        object_value = getattr(to_object, attribute, MARKER)

        if object_value is not MARKER and value != object_value:
            relation_name = getattr(to_object, "name", None) or to_object.__name__

            raise BadSchemaDefinition(
                "conflicting values %r/%r for property %s of relation %r"
                % (object_value, value, attribute, relation_name)
            )

        setattr(to_object, attribute, value)


def register_base_types(schema: yams_types.Schema) -> None:
    """add base (final) entity types to the given schema"""
    for entity_type in BASE_TYPES:
        entity_definition = EntityType(name=entity_type)

        schema.add_entity_type(entity_definition)


# first class schema definition objects #######################################


class autopackage(type):
    def __new__(
        mcs: "Type[autopackage]", name: str, bases: Tuple, classdict: Dict[str, Any]
    ) -> Any:

        classdict["package"] = PACKAGE

        return super(autopackage, mcs).__new__(mcs, name, bases, classdict)


class Definition(object, metaclass=autopackage):
    """Abstract class for entity / relation definition classes."""

    meta = MARKER
    description: Union[nullobject, str, None] = MARKER
    __permissions__ = MARKER

    def __init__(self, name=None) -> None:
        self.name: yams_types.DefinitionName = yams_types.DefinitionName(
            name or getattr(self, "name", None) or self.__class__.__name__
        )

        if self.__doc__:
            self.description = " ".join(self.__doc__.split())
            cast(str, self.description)

    def __repr__(self) -> str:
        return "<%s %r @%x>" % (self.__class__.__name__, self.name, id(self))

    @classmethod
    def expand_type_definitions(cls: Type["Definition"], defined: Defined) -> None:
        """Schema building step 1: register definition objects by adding them
        to the `defined` dictionnary.
        """
        raise NotImplementedError()

    @classmethod
    def expand_relation_definitions(
        cls: Type["Definition"], defined: Defined, schema: yams_types.Schema
    ) -> None:
        """Schema building step 2: register all relations definition,
        expanding wildcard if necessary.
        """
        raise NotImplementedError()

    def get_permissions(self, final: bool = False) -> yams_types.Permissions:
        if self.__permissions__ is MARKER:
            if final:
                return DEFAULT_ATTRPERMS

            return DEFAULT_RELPERMS

        return self.__permissions__

    @classmethod
    @deprecation.argument_renamed(old_name="perms", new_name="permissions")
    def set_permissions(cls: Type["Definition"], permissions: yams_types.Permissions) -> None:
        cls.__permissions__ = permissions


# classes used to define relationships within entity type classes ##################

# has to be defined before the metadefinition metaclass which "isinstance" this
# class
@deprecation.attribute_renamed(old_name="etype", new_name="entity_type")
class ObjectRelation:
    __permissions__ = MARKER
    cardinality = MARKER
    constraints = MARKER

    @deprecation.argument_renamed(old_name="etype", new_name="entity_type")
    def __init__(self, entity_type: str, override: bool = False, **kwargs) -> None:
        if self.__class__.__name__ == "ObjectRelation":
            warn(
                "[yams 0.29] ObjectRelation is deprecated, " "use RelationDefinition subclass",
                DeprecationWarning,
                stacklevel=2,
            )

        global CREATION_RANK
        CREATION_RANK += 1
        self.creation_rank: int = CREATION_RANK

        self.package: str = PACKAGE
        self.name: str = "<undefined>"
        self.entity_type: str = entity_type
        self.override: bool = override

        if self.constraints:
            self.constraints = list(self.constraints)

        if kwargs.pop("meta", None):
            warn("[yams 0.37.0] meta is deprecated", DeprecationWarning, stacklevel=3)

        try:
            _check_kwargs(kwargs, _RELATION_PROPERTIES())
        except BadSchemaDefinition as bad:
            # XXX (auc) bad field name + required attribute can lead there
            # instead of schema.py ~ 920
            bad_schema_definition = BadSchemaDefinition(
                "%s in relation to entity %r (also is %r defined ? "
                "(check two lines above in the backtrace))" % (bad.args, entity_type, entity_type)
            )
            # mypy: "BadSchemaDefinition" has no attribute "tb_offset"
            # hack to transport information
            bad_schema_definition.tb_offset = 2  # type: ignore

            raise bad_schema_definition

        self.__dict__.update(kwargs)

    def __repr__(self) -> str:
        return "%(name)s %(entity_type)s" % self.__dict__


class SubjectRelation(ObjectRelation):
    uid = MARKER
    indexed = MARKER
    fulltextindexed = MARKER
    internationalizable = MARKER
    default = MARKER

    def __repr__(self) -> str:
        return "%(entity_type)s %(name)s" % self.__dict__


class AbstractTypedAttribute(SubjectRelation):
    """AbstractTypedAttribute is not directly instantiable

    subclasses must provide a <entity_type> attribute to be instantiable
    """

    def __init__(
        self,
        metadata: Optional[Dict[str, "AbstractTypedAttribute"]] = None,
        required: bool = False,
        maxsize: Optional[int] = None,
        formula=MARKER,
        vocabulary: Optional[List[str]] = None,
        unique: Optional[bool] = None,
        override: bool = False,
        **kwargs,
    ):

        # Store metadata
        if metadata is None:
            metadata = {}
        self.metadata: Dict[Any, "AbstractTypedAttribute"] = metadata

        # transform "required" into "cardinality"
        if required:
            cardinality = "11"
        else:
            cardinality = "?1"
        kwargs["cardinality"] = cardinality

        # transform maxsize into SizeConstraint
        if maxsize is not None:
            _add_constraint(kwargs, SizeConstraint(max=maxsize))

        # formula
        self.formula = formula

        # transform vocabulary into StaticVocabularyConstraint
        if vocabulary is not None:
            self.set_vocabulary(vocabulary, kwargs)

        # transform unique into UniqueConstraint
        if unique:
            _add_constraint(kwargs, UniqueConstraint())

        # use the entity_type attribute provided by subclasses
        kwargs["override"] = override
        super(AbstractTypedAttribute, self).__init__(self.entity_type, **kwargs)

        # reassign creation rank
        #
        # Main attribute are marked as created before it's metadata.
        # order in meta data is preserved.
        if self.metadata:
            meta: List[AbstractTypedAttribute] = sorted(
                metadata.values(), key=lambda x: x.creation_rank
            )

            if meta[0].creation_rank < self.creation_rank:
                _previous: AbstractTypedAttribute = self

                for _next in meta:
                    if _previous.creation_rank < _next.creation_rank:
                        break

                    _previous.creation_rank, _next.creation_rank = (
                        _next.creation_rank,
                        _previous.creation_rank,
                    )
                    _next = _previous

    def set_vocabulary(self, vocabulary: List[str], kwargs=None) -> None:
        if kwargs is None:
            kwargs = self.__dict__

        # constraints = kwargs.setdefault('constraints', [])
        _add_constraint(kwargs, StaticVocabularyConstraint(vocabulary))

        if self.__class__.__name__ == "String":  # XXX
            max_size = max(len(x) for x in vocabulary)

            _add_constraint(kwargs, SizeConstraint(max=max_size))

    def __repr__(self) -> str:
        return "<%(name)s(%(entity_type)s)>" % self.__dict__


@deprecation.argument_renamed(old_name="etype", new_name="entity_type")
def make_type(entity_type: str) -> Type[AbstractTypedAttribute]:
    """create a python class for a Yams base type.

    Notice it is now possible to create a specific type with user-defined
    behaviour, e.g.:

        Geometry = make_type('Geometry') # (c.f. postgis)

    will allow the use of:

        Geometry(geom_type='POINT')

    in a Yams schema, provided in this example that `geom_type` is specified to
    the :func:`yams.register_base_type` function which should be called prior to
    make_type.
    """
    assert entity_type in BASE_TYPES
    return type(entity_type, (AbstractTypedAttribute,), {"entity_type": entity_type})


# build a specific class for each base type
class String(AbstractTypedAttribute):
    entity_type: str = "String"


class Password(AbstractTypedAttribute):
    entity_type: str = "Password"


class Bytes(AbstractTypedAttribute):
    entity_type: str = "Bytes"


class Int(AbstractTypedAttribute):
    entity_type: str = "Int"


class BigInt(AbstractTypedAttribute):
    entity_type: str = "BigInt"


class Float(AbstractTypedAttribute):
    entity_type: str = "Float"


class Boolean(AbstractTypedAttribute):
    entity_type: str = "Boolean"


class Decimal(AbstractTypedAttribute):
    entity_type: str = "Decimal"


class Time(AbstractTypedAttribute):
    entity_type: str = "Time"


class Date(AbstractTypedAttribute):
    entity_type: str = "Date"


class Datetime(AbstractTypedAttribute):
    entity_type: str = "Datetime"


class TZTime(AbstractTypedAttribute):
    entity_type: str = "TZTime"


class TZDatetime(AbstractTypedAttribute):
    entity_type: str = "TZDatetime"


class Interval(AbstractTypedAttribute):
    entity_type: str = "Interval"


# provides a RichString factory for convenience
def RichString(
    default_format: str = "text/plain",
    format_constraints: Optional[List[BaseConstraint]] = None,
    required: bool = False,
    maxsize: Optional[int] = None,
    formula=MARKER,
    vocabulary: Optional[List[str]] = None,
    unique: Optional[bool] = None,
    override: bool = False,
    **kwargs,
):
    """RichString is a convenience attribute type for attribute containing text
    in a format that should be specified in another attribute.

    The following declaration::

      class Card(EntityType):
          content = RichString(fulltextindexed=True, default_format='text/rest')

    is equivalent to::

      class Card(EntityType):
          content_format = String(internationalizable=True,
                                  default='text/rest', constraints=[FORMAT_CONSTRAINT])
          content  = String(fulltextindexed=True)
    """
    format_args = {"default": default_format, "maxsize": 50}

    if format_constraints is None:
        format_args["constraints"] = [FORMAT_CONSTRAINT]
    else:
        format_args["constraints"] = format_constraints

    # mypy: Argument 2 to "String" has incompatible type "**Dict[str, object]"; expected
    # mypy: "Optional[bool]"
    # really looks like mypy is failing on AbstractTypedAttribute constructor here
    meta: Dict[str, AbstractTypedAttribute] = {
        "format": String(internationalizable=True, **format_args)  # type: ignore
    }

    return String(
        metadata=meta,
        required=required,
        maxsize=maxsize,
        formula=formula,
        vocabulary=vocabulary,
        unique=unique,
        override=override,
        **kwargs,
    )


# other schema definition classes ##############################################


class metadefinition(autopackage):
    """Metaclass that builds the __relations__ attribute of EntityType's
    subclasses.
    """

    stacklevel = 3

    def __new__(
        mcs: "Type[metadefinition]", name: str, bases: Tuple, classdict: Dict[str, Any]
    ) -> Any:

        # Move (any) relation from the class dict to __relations__ attribute
        relations_list: List = classdict.setdefault("__relations__", [])
        relations: Dict[str, Any] = dict(
            (relation_definition.name, relation_definition)
            for relation_definition in relations_list
        )

        for relation_name, relation_definition in list(classdict.items()):
            if isinstance(relation_definition, ObjectRelation):
                # relation's name **must** be removed from class namespace
                # to avoid conflicts with instance's potential attributes
                del classdict[relation_name]

                relations[relation_name] = relation_definition

        # handle logical inheritance
        if "__specializes_schema__" in classdict:
            specialized = bases[0]
            classdict["__specializes__"] = specialized.__name__

            if "__specialized_by__" not in specialized.__dict__:
                specialized.__specialized_by__ = []

            specialized.__specialized_by__.append(name)

        # Initialize processed class
        class_definition = super(metadefinition, mcs).__new__(mcs, name, bases, classdict)

        for relation_name, relation_definition in relations.items():
            _add_relation(class_definition.__relations__, relation_definition, relation_name)

        # take base classes'relations into account
        for base in bases:
            for relation_definition in getattr(base, "__relations__", ()):
                if (
                    relation_definition.name not in relations
                    or not relations[relation_definition.name].override
                ):
                    if isinstance(relation_definition, RelationDefinition):
                        relation_definition = copy(relation_definition)

                        if relation_definition.subject == base.__name__:
                            relation_definition.subject = name

                        if relation_definition.object == base.__name__:
                            relation_definition.object = name

                    relations_list.append(relation_definition)
                else:
                    relations[
                        relation_definition.name
                    ].creation_rank = relation_definition.creation_rank

        # sort relations by creation rank
        class_definition.__relations__ = sorted(relations_list, key=lambda r: r.creation_rank)

        return class_definition


class EntityType(Definition, metaclass=metadefinition):
    # :FIXME reader magic forbids to define a docstring...
    #  an entity has attributes and can be linked to other entities by
    #  relations. Both entity attributes and relationships are defined by
    #  class attributes.
    #
    #  kwargs keys must have values in ETYPE_PROPERTIES
    #
    #  Example:
    #
    #  >>> class Project(EntityType):
    #  ...     name = String()
    #  >>>
    #
    #  After instanciation, EntityType can we altered with dedicated class methods:
    #
    #  .. currentmodule:: yams.buildobjs
    #
    #   .. automethod:: EntityType.extend
    #   .. automethod:: EntityType.add_relation
    #   .. automethod:: EntityType.insert_relation_after
    #   .. automethod:: EntityType.remove_relation
    #   .. automethod:: EntityType.get_relation
    #   .. automethod:: EntityType.get_relations

    __permissions__: yams_types.Permissions = DEFAULT_ETYPEPERMS

    def __init__(self, name: Optional[str] = None, **kwargs) -> None:
        super(EntityType, self).__init__(name)

        _check_kwargs(kwargs, ETYPE_PROPERTIES)

        self.__dict__.update(kwargs)
        self.specialized_type: Optional[str] = self.__class__.__dict__.get("__specializes__")

    def __str__(self) -> str:
        return "entity type %r" % self.name

    @property
    def specialized_by(self) -> List[str]:
        return self.__class__.__dict__.get("__specialized_by__", [])

    @classmethod
    def expand_type_definitions(cls: Type["EntityType"], defined: Defined) -> None:
        """Schema building step 1: register definition objects by adding
        them to the `defined` dictionnary.
        """
        name: str = getattr(cls, "name", cls.__name__)

        assert cls is not defined.get(name), "duplicate registration: %s" % name
        assert (
            name not in defined
        ), "type '%s' was already defined here %s, new definition here %s" % (
            name,
            defined[name].__module__,
            cls,
        )

        # mypy: "Type[EntityType]" has no attribute "_defined"
        # dynamic attribute
        # XXX may be used later (eg .add_relation())
        cls._defined: Defined = defined  # type: ignore

        defined[name] = cls

        # mypy: "Type[EntityType]" has no attribute "__relations__"
        # dynamically set attribute, full yams magic
        for relation in cls.__relations__:  # type: ignore
            cls._ensure_relation_type(relation)

    @classmethod
    def _ensure_relation_type(cls: Type["EntityType"], relation: ObjectRelation) -> bool:
        """Check the type the relation

        return False if the class is not yet finalized
        (XXX raise excep instead ?)"""

        relation_type = RelationType(relation.name)

        _copy_attributes(relation, relation_type, RTYPE_PROPERTIES)

        # assert hasattr(cls, '_defined'), "Type definition for %s not yet expanded.
        # you can't register new type through it" % cls

        if not hasattr(cls, "_defined"):
            return False

        # mypy: "Type[EntityType]" has no attribute "_defined"
        # dynamically set attribute
        defined = cls._defined  # type: ignore

        if relation.name in defined:
            _copy_attributes(relation_type, defined[relation.name], RTYPE_PROPERTIES)
        else:
            defined[relation.name] = relation_type

        return True

    @classmethod
    def expand_relation_definitions(
        cls: Type["EntityType"], defined: Defined, schema: yams_types.Schema
    ) -> None:
        """schema building step 2:

        register all relations definition, expanding wildcards if necessary
        """
        order: int = 1
        name: str = getattr(cls, "name", cls.__name__)
        relation_definitions_properties: Tuple[str, ...] = _RELATION_DEFINITION_PROPERTIES()

        # mypy: "Type[EntityType]" has no attribute "__relations__"
        # dynamically set attribute, full yams magic
        for relation in cls.__relations__:  # type: ignore
            if isinstance(relation, SubjectRelation):
                relation_definition = RelationDefinition(
                    subject=name,
                    name=relation.name,
                    object=relation.entity_type,
                    order=order,
                    package=relation.package,
                )

                _copy_attributes(relation, relation_definition, relation_definitions_properties)

            elif isinstance(relation, ObjectRelation):
                relation_definition = RelationDefinition(
                    subject=relation.entity_type,
                    name=relation.name,
                    object=name,
                    order=order,
                    package=relation.package,
                )

                _copy_attributes(relation, relation_definition, relation_definitions_properties)

            elif isinstance(relation, RelationDefinition):
                relation_definition = relation
            else:
                raise BadSchemaDefinition("dunno how to handle %s" % relation)

            order += 1
            relation_definition._add_relations(defined, schema)

    # methods that can be used to extend an existant schema definition ########

    @classmethod
    @deprecation.attribute_renamed(
        old_name="othermetadefcls", new_name="other_meta_definition_class"
    )
    def extend(cls: Type["EntityType"], other_meta_definition_class) -> None:
        """add all relations of ``other_meta_definition_class`` to the current class"""
        for relation_definition in other_meta_definition_class.__relations__:
            cls.add_relation(relation_definition)

    @classmethod
    @deprecation.argument_renamed(old_name="rdef", new_name="relation_definition")
    def add_relation(
        cls: Type["EntityType"], relation_definition: ObjectRelation, name: Optional[str] = None
    ) -> None:
        """Add ``relation_definition`` relation to the class"""
        if name:
            relation_definition.name = name

        if cls._ensure_relation_type(relation_definition):
            # mypy: "Type[EntityType]" has no attribute "__relations__"
            # dynamically set attribute, full yams magic
            _add_relation(cls.__relations__, relation_definition, name)  # type: ignore

            # mypy: "Type[EntityType]" has no attribute "_defined"
            # dynamically set attribute
            if (
                getattr(relation_definition, "metadata", {})
                and relation_definition not in cls._defined  # type: ignore
            ):
                for meta_name in relation_definition.metadata:  # type: ignore
                    format_attr_name = "_".join(((name or relation_definition.name), meta_name))
                    relation_definition = next(cls.get_relations(format_attr_name))
                    cls._ensure_relation_type(relation_definition)

        else:
            # mypy: "Type[EntityType]" has no attribute "__relations__"
            # dynamically set attribute, full yams magic
            _add_relation(cls.__relations__, relation_definition, name=name)  # type: ignore

    @classmethod
    @deprecation.argument_renamed(old_name="rdef", new_name="relation_definition")
    @deprecation.argument_renamed(old_name="afterrelname", new_name="after_relation_name")
    def insert_relation_after(
        cls: Type["EntityType"],
        after_relation_name: str,
        name: str,
        relation_definition: ObjectRelation,
    ) -> None:
        """Add ``relation_definition`` relation to the class right after another"""
        # FIXME change order of arguments to relation_definition, name, after_relation_name ?
        relation_definition.name = name
        cls._ensure_relation_type(relation_definition)

        # mypy: "Type[EntityType]" has no attribute "__relations__"
        # dynamically set attribute, full yams magic
        number = 0
        for number, rel in enumerate(cls.__relations__):  # type: ignore
            if rel.name == after_relation_name:
                break

        else:
            raise BadSchemaDefinition("can't find %s relation on %s" % (after_relation_name, cls))

        # mypy: "Type[EntityType]" has no attribute "__relations__"
        # dynamically set attribute, full yams magic
        _add_relation(cls.__relations__, relation_definition, name, number + 1)  # type: ignore

    @classmethod
    def remove_relation(cls: Type["EntityType"], name: str) -> None:
        """Remove relation from the class"""

        # mypy: "Type[EntityType]" has no attribute "__relations__"
        # dynamically set attribute, full yams magic
        for relation_definition in cls.get_relations(name):
            cls.__relations__.remove(relation_definition)  # type: ignore

    @classmethod
    def get_relations(cls: Type["EntityType"], name: str) -> Generator[ObjectRelation, Any, None]:
        """Iterate over relations definitions that match the ``name`` parameters

        It may iterate multiple definitions when the class is both object and
        sujet of a relation:
        """

        # mypy: "Type[EntityType]" has no attribute "__relations__"
        # dynamically set attribute, full yams magic
        for relation_definition in cls.__relations__[:]:  # type: ignore
            if relation_definition.name == name:
                yield relation_definition

    @classmethod
    def get_relation(cls: Type["EntityType"], name: str) -> ObjectRelation:
        """Return relation definitions by name. Fails if there is multiple one."""
        relations: Tuple[ObjectRelation, ...] = tuple(cls.get_relations(name))

        assert len(relations) == 1, "can't use get_relation for relation with multiple definitions"

        return relations[0]


class RelationType(Definition):
    symmetric = MARKER
    inlined = MARKER
    fulltext_container = MARKER
    rule = MARKER

    def __init__(self, name: Optional[str] = None, **kwargs) -> None:
        """kwargs must have values in RTYPE_PROPERTIES"""
        super(RelationType, self).__init__(name)

        if kwargs.pop("meta", None):
            warn("[yams 0.37] meta is deprecated", DeprecationWarning, stacklevel=2)

        _check_kwargs(kwargs, RTYPE_PROPERTIES + ("description", "__permissions__"))

        self.__dict__.update(kwargs)

    def __str__(self) -> str:
        return "relation type %r" % self.name

    @classmethod
    def expand_type_definitions(cls: Type["RelationType"], defined: Defined) -> None:
        """schema building step 1:

        register definition objects by adding them to the `defined` dictionnary
        """
        name: str = getattr(cls, "name", cls.__name__)

        if cls.__doc__ and not cls.description:
            cls.description = " ".join(cls.__doc__.split())

        if name in defined:
            if defined[name].__class__ is not RelationType:
                raise BadSchemaDefinition("duplicated relation type for %s" % name)

            # relation type created from a relation definition, override it
            all_properties = _RELATION_PROPERTIES() + ("subject", "object")

            _copy_attributes(defined[name], cls, all_properties)

        defined[name] = cls

    @classmethod
    def expand_relation_definitions(
        cls: Type["RelationType"], defined: Defined, schema: yams_types.Schema
    ) -> None:
        """schema building step 2:

        register all relations definition, expanding wildcard if necessary
        """
        name: str = getattr(cls, "name", cls.__name__)

        if getattr(cls, "subject", None) and getattr(cls, "object", None):
            # mypy: "Type[RelationType]" has no attribute "subject"
            # mypy: "Type[RelationType]" has no attribute "object"
            # dynamically set attributes
            relation_definition = RelationDefinition(
                subject=cls.subject,  # type: ignore
                name=name,  # type: ignore
                object=cls.object,  # type: ignore
            )

            relation_definition._add_relations(defined, schema)
            _copy_attributes(cls, relation_definition, _RELATION_DEFINITION_PROPERTIES())


class ComputedRelation(RelationType):
    __permissions__ = MARKER

    def __init__(self, name: Optional[str] = None, rule=None, **kwargs) -> None:
        if rule is not None:
            self.rule = rule

        super(ComputedRelation, self).__init__(name, **kwargs)


class RelationDefinition(Definition):
    # FIXME reader magic forbids to define a docstring...
    # """a relation is defined by a name, the entity types that can be
    # subject or object the relation, the cardinality, the constraints
    # and the symmetric property.
    # """

    subject: Union[nullobject, str, None] = MARKER
    object: Union[nullobject, str, None] = MARKER
    cardinality: Union[nullobject, str, None] = MARKER
    constraints = MARKER
    symmetric = MARKER
    inlined = MARKER
    formula = MARKER

    def __init__(
        self,
        subject: Optional[str] = None,
        name: Optional[str] = None,
        object: Optional[str] = None,
        package: Optional[str] = None,
        **kwargs,
    ) -> None:
        """kwargs keys must have values in _RELATION_DEFINITION_PROPERTIES()"""
        if subject:
            self.subject = subject
        else:
            self.subject = self.__class__.subject

        if object:
            self.object = object
        else:
            self.object = self.__class__.object

        super(RelationDefinition, self).__init__(name)

        global CREATION_RANK
        CREATION_RANK += 1
        self.creation_rank: int = CREATION_RANK

        self.package: str
        if package is not None:
            self.package = package
        elif self.package == "<builtin>":
            self.package = PACKAGE

        if kwargs.pop("meta", None):
            warn("[yams 0.37] meta is deprecated", DeprecationWarning)

        relation_definitions_properties: Tuple[str, ...] = _RELATION_DEFINITION_PROPERTIES()

        _check_kwargs(kwargs, relation_definitions_properties)
        _copy_attributes(attrdict(**kwargs), self, relation_definitions_properties)

        if self.constraints:
            self.constraints = list(self.constraints)

    def __str__(self) -> str:
        return "relation definition (%(subject)s %(name)s %(object)s)" % self.__dict__

    @classmethod
    def expand_type_definitions(cls: Type["RelationDefinition"], defined: Defined) -> None:
        """schema building step 1:

        register definition objects by adding them to the `defined` dictionnary
        """
        name: str = getattr(cls, "name", cls.__name__)
        relation_type: RelationType = RelationType(name)

        _copy_attributes(cls, relation_type, RTYPE_PROPERTIES)

        if name in defined:
            _copy_attributes(relation_type, defined[name], RTYPE_PROPERTIES)
        else:
            defined[name] = relation_type

        # subject and object in defined's keys are only strings not tuples
        if isinstance(cls.subject, tuple):
            subjects = cls.subject
        else:
            subjects = (cls.subject,)

        if isinstance(cls.object, tuple):
            objects = cls.object
        else:
            objects = (cls.object,)

        for subject in subjects:
            for object in objects:
                key = (subject, name, object)
                if key in defined:
                    raise BadSchemaDefinition(
                        "duplicated relation definition (%s) %s (%s.%s)"
                        % (defined[key], key, cls.__module__, cls.__name__)
                    )
                defined[key] = cls

        # XXX keep this for bw compat
        defined[(cls.subject, name, cls.object)] = cls

    @classmethod
    def expand_relation_definitions(
        cls: Type["RelationDefinition"], defined: Defined, schema: yams_types.Schema
    ) -> None:
        """schema building step 2:

        register all relations definition, expanding wildcard if necessary
        """
        assert cls.subject and cls.object, "%s; check the schema (%s, %s)" % (
            cls,
            cls.subject,
            cls.object,
        )
        cls()._add_relations(defined, schema)

    def _add_relations(self, defined: Defined, schema: yams_types.Schema) -> None:
        name: str = getattr(self, "name", self.__class__.__name__)
        relation_type: Union[autopackage, Definition] = defined[name]
        relation_definitions_properties: Tuple[str, ...] = _RELATION_DEFINITION_PROPERTIES()

        # copy relation definition attributes set on the relation type, beside
        # description
        _copy_attributes(
            relation_type, self, set(relation_definitions_properties) - set(("description",))
        )

        # process default cardinality and constraints if not set yet
        cardinality = self.cardinality
        if cardinality is MARKER:
            if self.object in BASE_TYPES:
                self.cardinality = "?1"
            else:
                self.cardinality = "**"
        else:
            assert isinstance(cardinality, str)
            assert len(cardinality) == 2
            assert cardinality[0] in "1?+*"
            assert cardinality[1] in "1?+*"

        if not self.constraints:
            self.constraints = ()

        relation_schema = schema.relation_schema_for(yams_types.DefinitionName(name))
        if relation_schema.rule:
            raise BadSchemaDefinition(
                'Cannot add relation definition "{}" because an '
                "homonymous computed relation already exists "
                'with rule "{}"'.format(relation_schema.type, relation_schema.rule)
            )

        if self.__permissions__ is MARKER:
            final: bool = next(iter(_actual_types(schema, self.object))) in BASE_TYPES

            if final:
                if self.formula is not MARKER:
                    permissions = DEFAULT_COMPUTED_ATTRPERMS
                else:
                    permissions = DEFAULT_ATTRPERMS
            else:
                permissions = DEFAULT_RELPERMS
        else:
            permissions = self.__permissions__

        for subject in _actual_types(schema, self.subject):
            for object in _actual_types(schema, self.object):
                relation_definition = RelationDefinition(
                    subject, name, object, __permissions__=permissions, package=self.package
                )

                _copy_attributes(self, relation_definition, relation_definitions_properties)
                schema.add_relation_def(relation_definition)


@deprecation.argument_renamed(old_name="etype", new_name="entity_type")
def _actual_types(
    schema: yams_types.Schema, entity_type: Union[str, list, Tuple[Any, Any]]
) -> Union[Generator[Any, Any, None], Tuple[Any], Tuple, List, Any]:
    if entity_type == "*":
        yield from (eschema.type for eschema in schema.entities() if not eschema.final)
    elif isinstance(entity_type, (list, tuple)):
        yield from entity_type
    elif isinstance(entity_type, str):
        yield entity_type
    else:
        raise RuntimeError(
            "Entity types must be strings or list/tuples of strings. "
            'SubjectRelation(Foo) is wrong, SubjectRelation("Foo") is correct. '
            "Hence, %r is not acceptable." % entity_type
        )
