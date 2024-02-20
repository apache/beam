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
import json
import logging

__all__ = [
    'VertexAIFeatureStoreEnrichmentHandler',
]

from typing import List

from google.cloud.aiplatform_v1 import FetchFeatureValuesRequest, FeatureOnlineStoreServiceClient

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler

_LOGGER = logging.getLogger(__name__)


class VertexAIFeatureStoreEnrichmentHandler(EnrichmentSourceHandler[beam.Row,
                                                                    beam.Row]):
  """Handler to interact with Vertex AI feature store using
  :class:`apache_beam.transforms.enrichment.Enrichment` transform.
  """
  def __init__(
      self,
      project: str,
      location: str,
      api_endpoint: str,
      feature_store_name: str,
      feature_view_name: str,
      entity_type_name: str,
      feature_ids: List[str]):
    """Initializes an instance of `VertexAIFeatureStoreEnrichmentHandler`.

    Args:
      project (str): The GCP project for the Vertex AI feature store.
      location (str): The region for the Vertex AI feature store.
      api_endpoint (str): The API endpoint for the Vertex AI feature store.
      feature_store_name (str): The name of the Vertex AI feature store.
      feature_view_name (str): The name of the feature view within the
        feature store.
      entity_type_name (str): The name of the entity type within the
        feature store.
      feature_ids (List[str]): A list of feature IDs to fetch
        from the feature store.
    """
    self.project = project
    self.location = location
    self.api_endpoint = api_endpoint
    self.feature_store_name = feature_store_name
    self.feature_view_name = feature_view_name
    self.entity_type_name = entity_type_name
    self.feature_ids = feature_ids

  def __enter__(self):
    self.client = FeatureOnlineStoreServiceClient(
        client_options={"api_endpoint": self.api_endpoint})

  def __call__(self, request, *args, **kwargs):
    entity_id = request._asdict()[self.entity_type_name]
    response = self.client.fetch_feature_values(
        FetchFeatureValuesRequest(
            feature_view=(
                "projects/%s/locations/%s/featureOnlineStores/%s/feature"
                "Views/%s" % (
                    self.project,
                    self.location,
                    self.feature_store_name,
                    self.feature_view_name)),
            data_key=entity_id,
        ))
    response_dict = json.loads(response.key_values)
    return request, response_dict

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.client.__exit__()

  def get_cache_key(self, request):
    return 'entity_id: %s'
