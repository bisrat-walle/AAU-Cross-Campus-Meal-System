# copyright 2004-2010 LOGILAB S.A. (Paris, FRANCE), all rights reserved.
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
"""Public interfaces for yams.

"""

from typing import List, Any, Tuple, Iterator, Optional
import yams.types as yams_types

from logilab.common.interface import Interface

__docformat__: str = "restructuredtext en"

# remove this to avoid a dependency on rql for client code
# from rql.interfaces import ISchema as IRQLSchema, \
#     IRelationSchema as IRQLRelationSchema, IEntitySchema as IRQLEntitySchema


class ISchema(Interface):  # (IRQLSchema):
    """a schema is a collection of relation and entity schemas"""

    def entities(self, schema: Optional[yams_types.Schema] = None) -> List:
        """return a list of possible entity's type

        If schema is not None, return a list of schemas instead of types.
        """

    def has_entity(self, e_type) -> bool:
        """return true the type is defined in the schema"""

    def eschema(self, e_type) -> Any:
        """return the entity's schema for the given type"""

    def relations(self, schema: yams_types.Schema = None) -> List:
        """return the list of possible relation'types

        If schema is not None, return a list of schemas instead of relation's
        types.
        """

    def has_relation(self, rtype) -> bool:
        """return true the relation is defined in the schema"""

    def rschema(self, rtype) -> None:
        """return the relation schema for the given relation type"""


class IRelationSchema(Interface):  # (IRQLRelationSchema):
    """A relation is a named ordered link between two entities.
    A relation schema defines the possible types of both extremities.
    """

    def associations(self, schema: yams_types.Schema = None) -> List[Tuple[Any, List[Any]]]:
        """return a list of (subject_type, [object_types]) defining between
        which types this relation may exists

        If schema is not None, return a list of schemas instead of type.
        """

    def subjects(self, etype=None) -> List:
        """return a list of types which can be subject of this relation

        If e_type is not None, return a list of types which can be subject of
        this relation with e_type as object.
        If schema is not None, return a list of schemas instead of type.
        Raise KeyError if e_type is not known
        """

    def objects(self, etype=None) -> List:
        """return a list of types which can be object of this relation.

        If e_type is not None, return a list of types which can be object of
        this relation with e_type as subject.
        If schema is not None, return a list of schemas instead of type.
        Raise KeyError if e_type is not known.
        """


class IEntitySchema(Interface):  # (IRQLEntitySchema):
    """An entity has a type, a set of subject and or object relations.
    The entity schema defines the possible relations for a given type and some
    constraints on those relations.

    Attributes are defined with relations pointing to a 'final' entity
    """

    def subject_relations(self) -> List:
        """return a list of relations that may have this type of entity as
        subject

        If schema is not None, return a list of schemas instead of relation's
        types.
        """

    def object_relations(self) -> List:
        """return a list of relations that may have this type of entity as
        object

        If schema is not None, return a list of schemas instead of relation's
        types.
        """

    def subject_relation(self, rtype) -> Any:
        """return the relation schema for the rtype subject relation

        Raise KeyError if rtype is not known.
        """

    def object_relation(self, rtype) -> Any:
        """return the relation schema for the rtype object relation

        Raise KeyError if rtype is not known.
        """

    def relation_definitions(self) -> Iterator:
        """return an iterator on "real" relation definitions

        "real"  relations are a subset of subject relations where the
        object's type is not a final entity

        a relation definition is a 2-uple :
        * name of the relation
        * schema of the destination entity type
        """

    def attribute_definitions(self) -> Iterator:
        """return an iterator on attribute definitions

        attribute relations are a subset of subject relations where the
        object's type is a final entity

        an attribute definition is a 2-uple :
        * name of the relation
        * schema of the destination entity type
        """

    def is_final(self) -> bool:
        """return true if the entity is a final entity (ie cannot be used
        as subject of a relation)
        """

    def constraints(self, rtype) -> Any:
        """return the existing constraints on the <rtype> subject relation"""

    def default(self, rtype) -> Any:
        """return the default value of a subject relation"""

    def check(self, entity) -> None:
        """check the entity and raises an InvalidEntity exception if it
        contains some invalid fields (ie some constraints failed)
        """


class IConstraint(Interface):
    """Represents a constraint on a relation."""


class IVocabularyConstraint(IConstraint):
    """a specific constraint restricting the set of possible value for an
    attribute value
    """

    def vocabulary(self) -> List:
        """return a list of possible values for the attribute"""
