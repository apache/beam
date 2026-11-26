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
import unittest
from unittest import mock

import apache_beam as beam
from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel

# pylint: disable=ungrouped-imports
try:
  from google.api_core.exceptions import NotFound
  from google.api_core.exceptions import PermissionDenied

  from apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store import VertexAIFeatureStoreEnrichmentHandler
  from apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store import \
      VertexAIFeatureStoreLegacyEnrichmentHandler
except ImportError:
  raise unittest.SkipTest(
      'VertexAI Feature Store test dependencies '
      'are not installed.')


class TestVertexAIFeatureStoreHandlerInit(unittest.TestCase):
  def test_raise_error_duplicate_api_endpoint_online_store(self):
    with self.assertRaises(ValueError):
      _ = VertexAIFeatureStoreEnrichmentHandler(
          project='project',
          location='location',
          api_endpoint='location@google.com',
          feature_store_name='feature_store',
          feature_view_name='feature_view',
          row_key='row_key',
          client_options={'api_endpoint': 'region@google.com'},
      )

  @mock.patch(
      'apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store.'
      'aiplatform.gapic.FeatureOnlineStoreAdminServiceClient')
  def test_permission_denied_online_store(self, mock_admin_client):
    mock_admin_client.return_value.get_feature_online_store.side_effect = (
        PermissionDenied('permission denied'))
    with self.assertLogs(
        'apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store',
        level='WARNING') as logs:
      _ = VertexAIFeatureStoreEnrichmentHandler(
          project='project',
          location='location',
          api_endpoint='location@google.com',
          feature_store_name='feature_store',
          feature_view_name='feature_view',
          row_key='row_key',
      )
    self.assertIn('insufficient admin permission', logs.output[0])

  @mock.patch(
      'apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store.'
      'aiplatform.gapic.FeatureOnlineStoreAdminServiceClient')
  def test_raise_error_feature_store_not_found_online_store(
      self, mock_admin_client):
    mock_admin_client.return_value.get_feature_online_store.side_effect = (
        NotFound('not found'))
    with self.assertRaises(NotFound):
      _ = VertexAIFeatureStoreEnrichmentHandler(
          project='project',
          location='location',
          api_endpoint='location@google.com',
          feature_store_name='feature_store',
          feature_view_name='feature_view',
          row_key='row_key',
      )

  def test_raise_error_duplicate_api_endpoint_legacy_store(self):
    with self.assertRaises(ValueError):
      _ = VertexAIFeatureStoreLegacyEnrichmentHandler(
          project='project',
          location='location',
          api_endpoint='location@google.com',
          feature_store_id='feature_store',
          entity_type_id='entity_id',
          feature_ids=['feature1', 'feature2'],
          row_key='row_key',
          client_options={'api_endpoint': 'region@google.com'},
      )


class TestVertexAIFeatureStoreHandlerNotFound(unittest.TestCase):
  def _make_online_handler(self, exception_level):
    with mock.patch(
        'apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store.'
        'aiplatform.gapic.FeatureOnlineStoreAdminServiceClient'):
      handler = VertexAIFeatureStoreEnrichmentHandler(
          project='project',
          location='location',
          api_endpoint='location@google.com',
          feature_store_name='feature_store',
          feature_view_name='feature_view',
          row_key='user_id',
          exception_level=exception_level,
      )
    handler.client = mock.MagicMock()
    handler.feature_view_path = (
        'projects/project/locations/location/featureOnlineStores/'
        'feature_store/featureViews/feature_view')
    handler.client.fetch_feature_values.side_effect = NotFound('missing entity')
    return handler

  def _make_legacy_handler(self, exception_level):
    with mock.patch(
        'apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store.'
        'aiplatform.Featurestore'):
      handler = VertexAIFeatureStoreLegacyEnrichmentHandler(
          project='project',
          location='location',
          api_endpoint='location@google.com',
          feature_store_id='feature_store',
          entity_type_id='entity_type',
          feature_ids=['feature1'],
          row_key='user_id',
          exception_level=exception_level,
      )
    handler.client = mock.MagicMock()
    handler.entity_type_path = (
        'projects/project/locations/location/featurestores/'
        'feature_store/entityTypes/entity_type')
    handler.client.read_feature_values.side_effect = NotFound('missing entity')
    return handler

  @mock.patch(
      'apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store.'
      '_LOGGER.warning')
  def test_online_store_not_found_quiet(self, mock_warning):
    handler = self._make_online_handler(ExceptionLevel.QUIET)
    request = beam.Row(user_id='u_missing')
    response = handler(request)
    self.assertEqual(response, (request, beam.Row()))
    mock_warning.assert_not_called()

  def test_online_store_not_found_warn(self):
    handler = self._make_online_handler(ExceptionLevel.WARN)
    request = beam.Row(user_id='u_missing')
    with self.assertLogs(
        'apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store',
        level='WARNING') as logs:
      response = handler(request)
    self.assertEqual(response, (request, beam.Row()))
    self.assertIn('u_missing', logs.output[0])

  def test_online_store_not_found_raise(self):
    handler = self._make_online_handler(ExceptionLevel.RAISE)
    request = beam.Row(user_id='u_missing')
    with self.assertRaises(ValueError):
      handler(request)

  @mock.patch(
      'apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store.'
      '_LOGGER.warning')
  def test_legacy_store_not_found_quiet(self, mock_warning):
    handler = self._make_legacy_handler(ExceptionLevel.QUIET)
    request = beam.Row(user_id='u_missing')
    response = handler(request)
    self.assertEqual(response, (request, beam.Row()))
    mock_warning.assert_not_called()

  def test_legacy_store_not_found_warn(self):
    handler = self._make_legacy_handler(ExceptionLevel.WARN)
    request = beam.Row(user_id='u_missing')
    with self.assertLogs(
        'apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store',
        level='WARNING') as logs:
      response = handler(request)
    self.assertEqual(response, (request, beam.Row()))
    self.assertIn('u_missing', logs.output[0])

  def test_legacy_store_not_found_raise(self):
    handler = self._make_legacy_handler(ExceptionLevel.RAISE)
    request = beam.Row(user_id='u_missing')
    with self.assertRaises(ValueError):
      handler(request)


if __name__ == '__main__':
  unittest.main()
