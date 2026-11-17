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

try:
  from google.api_core.exceptions import NotFound
  from google.api_core.exceptions import PermissionDenied

  import apache_beam as beam
  from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel

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


class TestVertexAIFeatureStoreHandlerMissingEntity(unittest.TestCase):
  def _make_handler(self, legacy, exception_level):
    with mock.patch(
        'apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store.'
        'aiplatform') as mock_aiplatform:
      common = dict(
          project='project',
          location='us-central1',
          api_endpoint='us-central1-aiplatform.googleapis.com',
          row_key='user_id',
          exception_level=exception_level,
      )
      if legacy:
        handler = VertexAIFeatureStoreLegacyEnrichmentHandler(
            **common,
            feature_store_id='store',
            entity_type_id='entity',
            feature_ids=['feature'],
        )
        client = mock_aiplatform.gapic.FeaturestoreOnlineServingServiceClient
        client.return_value.entity_type_path.return_value = 'entity-type'
        client.return_value.read_feature_values.side_effect = NotFound(
            'missing entity')
      else:
        handler = VertexAIFeatureStoreEnrichmentHandler(
            **common, feature_store_name='store', feature_view_name='view')
        client = mock_aiplatform.gapic.FeatureOnlineStoreServiceClient
        client.return_value.feature_view_path.return_value = 'feature-view'
        client.return_value.fetch_feature_values.side_effect = NotFound(
            'missing entity')
      handler.__enter__()
    self.addCleanup(handler.__exit__, None, None, None)
    return handler

  def test_quiet_returns_empty_row_without_logging(self):
    request = beam.Row(user_id='missing')
    for legacy in (False, True):
      with self.subTest(legacy=legacy):
        handler = self._make_handler(legacy, ExceptionLevel.QUIET)
        with self.assertNoLogs(
            'apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store',
            level='WARNING'):
          actual_request, response = handler(request)
        self.assertEqual(actual_request, request)
        self.assertEqual(response._asdict(), {})

  def test_warn_logs_and_returns_empty_row(self):
    request = beam.Row(user_id='missing')
    for legacy in (False, True):
      with self.subTest(legacy=legacy):
        handler = self._make_handler(legacy, ExceptionLevel.WARN)
        with self.assertLogs(
            'apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store',
            level='WARNING') as logs:
          actual_request, response = handler(request)
        self.assertEqual(actual_request, request)
        self.assertEqual(response._asdict(), {})
        self.assertIn('has entity_id: missing', logs.output[0])

  def test_raise_reports_missing_entity(self):
    request = beam.Row(user_id='missing')
    for legacy in (False, True):
      with self.subTest(legacy=legacy):
        handler = self._make_handler(legacy, ExceptionLevel.RAISE)
        with self.assertRaisesRegex(ValueError, 'has entity_id: missing'):
          handler(request)


if __name__ == '__main__':
  unittest.main()
