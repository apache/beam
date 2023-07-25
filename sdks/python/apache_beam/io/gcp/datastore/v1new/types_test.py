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

"""Unit tests for types module."""

# pytype: skip-file

import datetime
import logging
import unittest

import mock

# Protect against environments where datastore library is not available.
try:
  from google.cloud.datastore import client
  from google.cloud.datastore import entity
  from google.cloud.datastore import key
  from google.cloud.datastore.helpers import GeoPoint
  from apache_beam.io.gcp.datastore.v1new.types import Entity
  from apache_beam.io.gcp.datastore.v1new.types import Key
  from apache_beam.io.gcp.datastore.v1new.types import Query
  from apache_beam.options.value_provider import StaticValueProvider
except ImportError:
  client = None

_LOGGER = logging.getLogger(__name__)


@unittest.skipIf(client is None, 'Datastore dependencies are not installed')
class TypesTest(unittest.TestCase):
  _PROJECT = 'project'
  _NAMESPACE = 'namespace'

  def setUp(self):
    self._test_client = client.Client(
        project=self._PROJECT,
        namespace=self._NAMESPACE,
        # Don't do any network requests.
        _http=mock.MagicMock())

  def _assert_keys_equal(self, beam_type, client_type, expected_project):
    self.assertEqual(beam_type.path_elements[0], client_type.kind)
    self.assertEqual(beam_type.path_elements[1], client_type.id)
    self.assertEqual(expected_project, client_type.project)

  def testEntityToClientEntity(self):
    # Test conversion from Beam type to client type.
    k = Key(['kind', 1234], project=self._PROJECT)
    kc = k.to_client_key()
    exclude_from_indexes = ('datetime', 'key')
    e = Entity(k, exclude_from_indexes=exclude_from_indexes)
    properties = {
        'datetime': datetime.datetime.utcnow(),
        'key_ref': Key(['kind2', 1235]),
        'bool': True,
        'float': 1.21,
        'int': 1337,
        'unicode': 'text',
        'bytes': b'bytes',
        'geopoint': GeoPoint(0.123, 0.456),
        'none': None,
        'list': [1, 2, 3],
        'entity': Entity(Key(['kind', 111])),
        'dict': {
            'property': 5
        },
    }
    e.set_properties(properties)
    ec = e.to_client_entity()
    self.assertEqual(kc, ec.key)
    self.assertSetEqual(set(exclude_from_indexes), ec.exclude_from_indexes)
    self.assertEqual('kind', ec.kind)
    self.assertEqual(1234, ec.id)
    for name, unconverted in properties.items():
      converted = ec[name]
      if name == 'key_ref':
        self.assertNotIsInstance(converted, Key)
        self._assert_keys_equal(unconverted, converted, self._PROJECT)
      elif name == 'entity':
        self.assertNotIsInstance(converted, Entity)
        self.assertNotIsInstance(converted.key, Key)
        self._assert_keys_equal(unconverted.key, converted.key, self._PROJECT)
      else:
        self.assertEqual(unconverted, converted)

    # Test reverse conversion.
    entity_from_client_entity = Entity.from_client_entity(ec)
    self.assertEqual(e, entity_from_client_entity)

  def testEmbeddedClientEntityWithoutKey(self):
    client_entity = entity.Entity(key.Key('foo', project='bar'))
    entity_without_key = entity.Entity()
    entity_without_key['test'] = True
    client_entity['embedded'] = entity_without_key
    e = Entity.from_client_entity(client_entity)
    self.assertIsInstance(e.properties['embedded'], dict)

  def testKeyToClientKey(self):
    k = Key(['kind1', 'parent'],
            project=self._PROJECT,
            namespace=self._NAMESPACE)
    ck = k.to_client_key()
    self.assertEqual(self._PROJECT, ck.project)
    self.assertEqual(self._NAMESPACE, ck.namespace)
    self.assertEqual(('kind1', 'parent'), ck.flat_path)
    self.assertEqual('kind1', ck.kind)
    self.assertEqual('parent', ck.id_or_name)
    self.assertEqual(None, ck.parent)

    k2 = Key(['kind2', 1234], parent=k)
    ck2 = k2.to_client_key()
    self.assertEqual(self._PROJECT, ck2.project)
    self.assertEqual(self._NAMESPACE, ck2.namespace)
    self.assertEqual(('kind1', 'parent', 'kind2', 1234), ck2.flat_path)
    self.assertEqual('kind2', ck2.kind)
    self.assertEqual(1234, ck2.id_or_name)
    self.assertEqual(ck, ck2.parent)

  def testKeyFromClientKey(self):
    k = Key(['k1', 1234], project=self._PROJECT, namespace=self._NAMESPACE)
    kfc = Key.from_client_key(k.to_client_key())
    self.assertEqual(k, kfc)

    k2 = Key(['k2', 'adsf'], parent=k)
    kfc2 = Key.from_client_key(k2.to_client_key())
    # Converting a key with a parent to a client_key and back loses the parent:
    self.assertNotEqual(k2, kfc2)
    self.assertTupleEqual(('k1', 1234, 'k2', 'adsf'), kfc2.path_elements)
    self.assertIsNone(kfc2.parent)

    kfc3 = Key.from_client_key(kfc2.to_client_key())
    self.assertEqual(kfc2, kfc3)

    kfc4 = Key.from_client_key(kfc2.to_client_key())
    kfc4.project = 'other'
    self.assertNotEqual(kfc2, kfc4)

  def testKeyFromClientKeyNoNamespace(self):
    k = Key(['k1', 1234], project=self._PROJECT)
    ck = k.to_client_key()
    self.assertEqual(None, ck.namespace)  # Test that getter doesn't croak.
    kfc = Key.from_client_key(ck)
    self.assertEqual(k, kfc)

  def testKeyToClientKeyMissingProject(self):
    k = Key(['k1', 1234], namespace=self._NAMESPACE)
    with self.assertRaisesRegex(ValueError, r'project'):
      _ = Key.from_client_key(k.to_client_key())

  def testQuery(self):
    filters = [('property_name', '=', 'value')]
    projection = ['f1', 'f2']
    order = projection
    distinct_on = projection
    ancestor_key = Key(['kind', 'id'], project=self._PROJECT)
    q = Query(
        kind='kind',
        project=self._PROJECT,
        namespace=self._NAMESPACE,
        ancestor=ancestor_key,
        filters=filters,
        projection=projection,
        order=order,
        distinct_on=distinct_on)
    cq = q._to_client_query(self._test_client)
    self.assertEqual(self._PROJECT, cq.project)
    self.assertEqual(self._NAMESPACE, cq.namespace)
    self.assertEqual('kind', cq.kind)
    self.assertEqual(ancestor_key.to_client_key(), cq.ancestor)
    self.assertEqual(filters, cq.filters)
    self.assertEqual(projection, cq.projection)
    self.assertEqual(order, cq.order)
    self.assertEqual(distinct_on, cq.distinct_on)

    _LOGGER.info('query: %s', q)  # Test __repr__()

  def testValueProviderFilters(self):
    self.vp_filters = [
        [(
            StaticValueProvider(str, 'property_name'),
            StaticValueProvider(str, '='),
            StaticValueProvider(str, 'value'))],
        [(
            StaticValueProvider(str, 'property_name'),
            StaticValueProvider(str, '='),
            StaticValueProvider(str, 'value')),
         ('property_name', '=', 'value')],
    ]
    self.expected_filters = [
        [('property_name', '=', 'value')],
        [('property_name', '=', 'value'), ('property_name', '=', 'value')],
    ]

    for vp_filter, exp_filter in zip(self.vp_filters, self.expected_filters):
      q = Query(
          kind='kind',
          project=self._PROJECT,
          namespace=self._NAMESPACE,
          filters=vp_filter)
      cq = q._to_client_query(self._test_client)
      self.assertEqual(exp_filter, cq.filters)

      _LOGGER.info('query: %s', q)  # Test __repr__()

  def testValueProviderNamespace(self):
    self.vp_namespace = StaticValueProvider(str, 'vp_namespace')
    self.expected_namespace = 'vp_namespace'

    q = Query(kind='kind', project=self._PROJECT, namespace=self.vp_namespace)
    cq = q._to_client_query(self._test_client)
    self.assertEqual(self.expected_namespace, cq.namespace)

    _LOGGER.info('query: %s', q)  # Test __repr__()

  def testQueryEmptyNamespace(self):
    # Test that we can pass a namespace of None.
    self._test_client.namespace = None
    q = Query(project=self._PROJECT, namespace=None)
    cq = q._to_client_query(self._test_client)
    self.assertEqual(self._test_client.project, cq.project)
    self.assertEqual(None, cq.namespace)


if __name__ == '__main__':
  unittest.main()
