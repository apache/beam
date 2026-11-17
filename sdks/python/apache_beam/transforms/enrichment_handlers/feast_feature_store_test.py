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
import os
import unittest
from unittest import mock

from parameterized import parameterized

import apache_beam as beam

try:
  from apache_beam.transforms.enrichment_handlers import feast_feature_store
  from apache_beam.transforms.enrichment_handlers.feast_feature_store import FeastFeatureStoreEnrichmentHandler
  from apache_beam.transforms.enrichment_handlers.feast_feature_store_it_test import _entity_row_fn
  from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel
except ImportError:
  raise unittest.SkipTest(
      'Feast feature store test dependencies are not installed.')


class TestFeastFeatureStoreHandler(unittest.TestCase):
  def setUp(self) -> None:
    self.feature_store_yaml_file = (
        'gs://apache-beam-testing-enrichment/'
        'feast-feature-store/repos/ecommerce/'
        'feature_repo/feature_store.yaml')
    self.feature_service_name = 'demograph_service'

  def test_feature_store_yaml_path_exists(self):
    feature_store_yaml_path = 'gs://apache-beam-testing-enrichment/invalid.yaml'
    with self.assertRaises(ValueError):
      _ = FeastFeatureStoreEnrichmentHandler(
          entity_id='user_id',
          feature_store_yaml_path=feature_store_yaml_path,
          feature_service_name=self.feature_service_name,
      )

  def test_feast_enrichment_no_feature_service(self):
    """Test raising an error in case of no feature service name."""
    with self.assertRaises(ValueError):
      _ = FeastFeatureStoreEnrichmentHandler(
          entity_id='user_id',
          feature_store_yaml_path=self.feature_store_yaml_file,
      )

  @parameterized.expand([('user_id', _entity_row_fn), ('', None)])
  def test_feast_enrichment_invalid_args(self, entity_id, entity_row_fn):
    with self.assertRaises(ValueError):
      _ = FeastFeatureStoreEnrichmentHandler(
          feature_store_yaml_path=self.feature_store_yaml_file,
          entity_id=entity_id,
          entity_row_fn=entity_row_fn,
      )


class TestFeastFeatureStoreHandlerUnit(unittest.TestCase):
  """CPU-only unit tests that mock out GCS and the Feast feature store."""
  _MODULE = 'apache_beam.transforms.enrichment_handlers.feast_feature_store'
  _YAML_PATH = 'gs://dummy-bucket/feature_store.yaml'

  def _build_handler(self, **kwargs):
    # Skip the gs:// existence check so the handler can be built without GCS.
    with mock.patch(self._MODULE + '._validate_feature_store_yaml_path_exists'):
      return FeastFeatureStoreEnrichmentHandler(
          feature_store_yaml_path=self._YAML_PATH, **kwargs)

  def test_get_cache_key_with_mapped_entity_row_fn(self):
    # `entity_row_fn` maps the input column `id` to a differently-named entity
    # key `user_id`. The cache key must be derived from the value the function
    # returns, not by re-indexing the input row (which has no `user_id`).
    handler = self._build_handler(
        feature_names=['x'], entity_row_fn=lambda row: {'user_id': row.id})
    self.assertEqual(
        handler.get_cache_key(beam.Row(id=5)), 'entity_id: user_id: 5')

  def test_get_cache_key_with_compound_entity_row_fn(self):
    # Compound (multi-entity) mappings must produce a stable, order-independent
    # key.
    handler = self._build_handler(
        feature_names=['x'],
        entity_row_fn=lambda row: {
            'user_id': row.id, 'country': row.country
        })
    self.assertEqual(
        handler.get_cache_key(beam.Row(id=5, country='US')),
        'entity_id: country: US, user_id: 5')

  def test_call_raises_when_features_missing_and_level_raise(self):
    handler = self._build_handler(
        entity_id='user_id',
        feature_names=['state'],
        exception_level=ExceptionLevel.RAISE)
    handler.features = ['state']
    handler.store = mock.MagicMock()
    handler.store.get_online_features.return_value.to_dict.return_value = {
        'user_id': [1], 'state': [None]
    }
    with self.assertRaises(ValueError):
      handler(beam.Row(user_id=1))

  def test_call_warns_when_features_missing_and_level_warn(self):
    handler = self._build_handler(
        entity_id='user_id',
        feature_names=['state'],
        exception_level=ExceptionLevel.WARN)
    handler.features = ['state']
    handler.store = mock.MagicMock()
    handler.store.get_online_features.return_value.to_dict.return_value = {
        'user_id': [1], 'state': [None]
    }
    with self.assertLogs(feast_feature_store._LOGGER, level='WARNING') as logs:
      _, response = handler(beam.Row(user_id=1))
    self.assertTrue(any('state' in message for message in logs.output))
    self.assertIsNone(response.state)

  def test_call_does_not_warn_when_features_present(self):
    handler = self._build_handler(
        entity_id='user_id',
        feature_names=['state'],
        exception_level=ExceptionLevel.RAISE)
    handler.features = ['state']
    handler.store = mock.MagicMock()
    handler.store.get_online_features.return_value.to_dict.return_value = {
        'user_id': [1], 'state': ['CA']
    }
    _, response = handler(beam.Row(user_id=1))
    self.assertEqual(response.state, 'CA')

  def test_exit_removes_downloaded_yaml_file(self):
    gcs_file = mock.MagicMock()
    gcs_file.read.return_value = b'project: test\nprovider: local\n'
    mock_open = mock.MagicMock()
    mock_open.return_value.__enter__.return_value = gcs_file
    with mock.patch(self._MODULE + '.FileSystems.open', mock_open), \
        mock.patch(self._MODULE + '.FeatureStore', mock.MagicMock()):
      handler = self._build_handler(feature_names=['x'], entity_id='user_id')
      handler.__enter__()
      local_repo_path = handler._local_repo_path
      self.assertTrue(os.path.exists(local_repo_path))
      handler.__exit__(None, None, None)
      self.assertFalse(os.path.exists(local_repo_path))


if __name__ == '__main__':
  unittest.main()
