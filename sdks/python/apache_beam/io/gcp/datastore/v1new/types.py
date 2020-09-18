#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Beam Datastore types.
"""

# pytype: skip-file

from __future__ import absolute_import

import copy
from typing import Iterable
from typing import List
from typing import Optional
from typing import Text
from typing import Union

from google.cloud.datastore import entity
from google.cloud.datastore import key
from google.cloud.datastore import query

from apache_beam.options.value_provider import ValueProvider

__all__ = ['Query', 'Key', 'Entity']


class Query(object):
  def __init__(
      self,
      kind=None,
      project=None,
      namespace=None,
      ancestor=None,
      filters=(),
      projection=(),
      order=(),
      distinct_on=(),
      limit=None):
    """Represents a Datastore query.

    Args:
      kind: (str) The kind to query.
      project: (str) Required. Project associated with query.
      namespace: (str, ValueProvider(str)) (Optional) Namespace to restrict
        results to.
      ancestor: (:class:`~apache_beam.io.gcp.datastore.v1new.types.Key`)
        (Optional) key of the ancestor to which this query's results are
        restricted.
      filters: (sequence of tuple[str, str, str],
        sequence of
        tuple[ValueProvider(str), ValueProvider(str), ValueProvider(str)])
        Property filters applied by this query.
        The sequence is ``(property_name, operator, value)``.
      projection: (sequence of string) fields returned as part of query results.
      order: (sequence of string) field names used to order query results.
        Prepend ``-`` to a field name to sort it in descending order.
      distinct_on: (sequence of string) field names used to group query
        results.
      limit: (int) Maximum amount of results to return.
    """
    self.kind = kind
    self.project = project
    self.namespace = namespace
    self.ancestor = ancestor
    self.filters = filters or ()
    self.projection = projection
    self.order = order
    self.distinct_on = distinct_on
    self.limit = limit

  def _to_client_query(self, client):
    """
    Returns a ``google.cloud.datastore.query.Query`` instance that represents
    this query.

    Args:
      client: (``google.cloud.datastore.client.Client``) Datastore client
        instance to use.
    """
    ancestor_client_key = None
    if self.ancestor is not None:
      ancestor_client_key = self.ancestor.to_client_key()

    # Resolve ValueProvider arguments.
    self.filters = self._set_runtime_filters()
    if isinstance(self.namespace, ValueProvider):
      self.namespace = self.namespace.get()

    return query.Query(
        client,
        kind=self.kind,
        project=self.project,
        namespace=self.namespace,
        ancestor=ancestor_client_key,
        filters=self.filters,
        projection=self.projection,
        order=self.order,
        distinct_on=self.distinct_on)

  def _set_runtime_filters(self):
    """
    Extracts values from ValueProviders in `self.filters` if available
    :param filters: sequence of tuple[str, str, str] or
    sequence of tuple[ValueProvider, ValueProvider, ValueProvider]
    :return: tuple[str, str, str]
    """
    runtime_filters = []
    if not all(len(filter_tuple) == 3 for filter_tuple in self.filters):
      raise TypeError(
          '%s: filters must be a sequence of tuple with length=3'
          ' got %r instead' % (self.__class__.__name__, self.filters))

    for filter_type, filter_operator, filter_value in self.filters:
      if isinstance(filter_type, ValueProvider):
        filter_type = filter_type.get()
      if isinstance(filter_operator, ValueProvider):
        filter_operator = filter_operator.get()
      if isinstance(filter_value, ValueProvider):
        filter_value = filter_value.get()
      runtime_filters.append((filter_type, filter_operator, filter_value))

    return runtime_filters or ()

  def clone(self):
    return copy.copy(self)

  def __repr__(self):
    return (
        '<Query(kind=%s, project=%s, namespace=%s, ancestor=%s, filters=%s,'
        'projection=%s, order=%s, distinct_on=%s, limit=%s)>' % (
            self.kind,
            self.project,
            self.namespace,
            self.ancestor,
            self.filters,
            self.projection,
            self.order,
            self.distinct_on,
            self.limit))


class Key(object):
  def __init__(
      self,
      path_elements: List[Union[Text, int]],
      parent: Optional[Key] = None,
      project: Optional[Text] = None,
      namespace: Optional[Text] = None):
    """
    Represents a Datastore key.

    The partition ID is represented by its components: namespace and project.
    If key has a parent, project and namespace should either be unset or match
    the parent's.

    Args:
      path_elements: (list of str and int) Key path: an alternating sequence of
        kind and identifier. The kind must be of type ``str`` and identifier may
        be a ``str`` or an ``int``.
        If the last identifier is omitted this is an incomplete key, which is
        unsupported in ``WriteToDatastore`` and ``DeleteFromDatastore``.
        See :class:`google.cloud.datastore.key.Key` for more details.
      parent: (:class:`~apache_beam.io.gcp.datastore.v1new.types.Key`)
        (optional) Parent for this key.
      project: (str) Project ID. Required unless set by parent.
      namespace: (str) (optional) Namespace ID
    """
    # Verification or arguments is delegated to to_client_key().
    self.path_elements = tuple(path_elements)
    self.parent = parent
    self.namespace = namespace
    self.project = project

  @staticmethod
  def from_client_key(client_key):
    return Key(
        client_key.flat_path,
        project=client_key.project,
        namespace=client_key.namespace)

  def to_client_key(self):
    """
    Returns a :class:`google.cloud.datastore.key.Key` instance that represents
    this key.
    """
    parent = self.parent
    if parent is not None:
      parent = parent.to_client_key()
    return key.Key(
        *self.path_elements,
        parent=parent,
        namespace=self.namespace,
        project=self.project)

  def __eq__(self, other):
    if not isinstance(other, Key):
      return False
    if self.path_elements != other.path_elements:
      return False
    if self.project != other.project:
      return False
    if self.parent is not None and other.parent is not None:
      return self.parent == other.parent

    return self.parent is None and other.parent is None

  __hash__ = None  # type: ignore[assignment]

  def __repr__(self):
    return '<%s(%s, parent=%s, project=%s, namespace=%s)>' % (
        self.__class__.__name__,
        str(self.path_elements),
        str(self.parent),
        self.project,
        self.namespace)


class Entity(object):
  def __init__(self, key: Key, exclude_from_indexes: Iterable[str] = ()):
    """
    Represents a Datastore entity.

    Does not support the property value "meaning" field.

    Args:
      key: (Key) A complete Key representing this Entity.
      exclude_from_indexes: (iterable of str) List of property keys whose values
        should not be indexed for this entity.
    """
    self.key = key
    self.exclude_from_indexes = set(exclude_from_indexes)
    self.properties = {}

  def set_properties(self, property_dict):
    """Sets a dictionary of properties on this entity.

    Args:
      property_dict: A map from property name to value. See
        :class:`google.cloud.datastore.entity.Entity` documentation for allowed
        values.
    """
    self.properties.update(property_dict)

  @staticmethod
  def from_client_entity(client_entity):
    res = Entity(
        Key.from_client_key(client_entity.key),
        exclude_from_indexes=set(client_entity.exclude_from_indexes))
    for name, value in client_entity.items():
      if isinstance(value, key.Key):
        value = Key.from_client_key(value)
      if isinstance(value, entity.Entity):
        value = Entity.from_client_entity(value)
      res.properties[name] = value
    return res

  def to_client_entity(self):
    """
    Returns a :class:`google.cloud.datastore.entity.Entity` instance that
    represents this entity.
    """
    res = entity.Entity(
        key=self.key.to_client_key(),
        exclude_from_indexes=tuple(self.exclude_from_indexes))
    for name, value in self.properties.items():
      if isinstance(value, Key):
        if not value.project:
          value.project = self.key.project
        value = value.to_client_key()
      if isinstance(value, Entity):
        if not value.key.project:
          value.key.project = self.key.project
        value = value.to_client_entity()
      res[name] = value
    return res

  def __eq__(self, other):
    if not isinstance(other, Entity):
      return False
    return (
        self.key == other.key and
        self.exclude_from_indexes == other.exclude_from_indexes and
        self.properties == other.properties)

  __hash__ = None  # type: ignore[assignment]

  def __repr__(self):
    return "<%s(key=%s, exclude_from_indexes=%s) properties=%s>" % (
        self.__class__.__name__,
        str(self.key),
        str(self.exclude_from_indexes),
        str(self.properties))
