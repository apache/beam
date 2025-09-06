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
from dataclasses import dataclass, field
import logging
from collections.abc import Callable
from collections.abc import Mapping
from typing import Any, Union
from typing import Optional
import numpy

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel
import tecton

import sys
from io import StringIO

__all__ = [
    'TectonFeatureStoreEnrichmentHandler',
]

EntityRowFn = Callable[[beam.Row], Mapping[str, Any]]

_LOGGER = logging.getLogger(__name__)


@dataclass
class TectonConnectionConfig:
  """Configuration dataclass for Tecton connection parameters.

  This dataclass contains the essential connection parameters needed to
  establish a connection with a Tecton feature store instance.

  Attributes:
    url: The URL of the Tecton instance to connect to.
      Example: 'https://your-instance.tecton.ai'
    api_key: The API key for authenticating with the Tecton instance.
      This should be a valid API key with appropriate permissions.
  """
  url: str
  api_key: str

  def __post_init__(self):
    if not self.url:
      raise ValueError('Please provide a Tecton instance URL (`url`).')

    if not self.api_key:
      raise ValueError('Please provide an API key (`api_key`).')

@dataclass
class TectonFeaturesRetrievalConfig:
  """Configuration dataclass for Tecton feature retrieval parameters.

  This dataclass contains the parameters needed to retrieve features from
  a Tecton feature store, including entity identification and feature
  service configuration.

  Attributes:
    feature_service_name: The name of the feature service containing the
      features to fetch from the online Tecton feature store. This should
      match a feature service defined in your Tecton workspace.
    workspace_name: workspace name to use for feature retrieval.
    entity_id: The entity name for the entity associated with the features.
      The `entity_id` is used to extract the entity value from the input row.
      Please provide exactly one of `entity_id` or `entity_row_fn`.
    entity_row_fn: A lambda function that takes an input `beam.Row` and
      returns a dictionary with a mapping from the entity key column name to
      entity key value. It is used to build/extract the entity dict for
      feature retrieval. Please provide exactly one of `entity_id` or
      `entity_row_fn`.
    include_join_keys_in_response: Whether to include join keys as part of
      the response FeatureVector. Defaults to False.
    request_data: Optional mapping of request context parameters to pass to
      Tecton for real-time feature computation. These are typically used for
      RealtimeFeatureViews that depend on request-time data. Defaults to None.
    return_effective_times: Whether to include effective times when converting
      FeatureVector to dictionary. Effective times indicate when each feature
      value became valid. Defaults to False.
  """
  feature_service_name: str
  workspace_name: str
  entity_id: str = ""
  entity_row_fn: Optional[EntityRowFn] = None
  include_join_keys_in_response: bool = False
  request_data: Optional[Mapping[str, Any]] = None
  return_effective_times: bool = False

  def __post_init__(self):
    if not self.feature_service_name:
      raise ValueError(
          'Please provide a feature service name for the Tecton '
          'online feature store (`feature_service_name`).')

    if not self.workspace_name:
      raise ValueError(
          'Please provide a workspace name for the Tecton '
          'online feature store (`workspace_name`).')

    if ((not self.entity_row_fn and not self.entity_id) or
        bool(self.entity_row_fn and self.entity_id)):
      raise ValueError(
          "Please specify exactly one of a `entity_id` or a lambda "
          "function with `entity_row_fn` to extract the entity id "
          "from the input row.")

class TectonFeatureStoreEnrichmentHandler(EnrichmentSourceHandler[beam.Row,
                                                                 beam.Row]):
  """Enrichment handler to interact with the Tecton feature store.

  This handler fetches features from Tecton's online feature store using
  a feature service name.

  Use this handler with :class:`apache_beam.transforms.enrichment.Enrichment`
  transform. To filter the features to enrich, use the `join_fn` param in
  :class:`apache_beam.transforms.enrichment.Enrichment`.
  """
  def __init__(
      self,
      connection_config: TectonConnectionConfig,
      features_retrieval_config: TectonFeaturesRetrievalConfig,
      *,
      exception_level: ExceptionLevel = ExceptionLevel.WARN,
  ):
    """Initializes an instance of `TectonFeatureStoreEnrichmentHandler`.

    Args:
      connection_config: A `TectonConnectionConfig` dataclass containing
        connection parameters (url, workspace_name, api_key).
      features_retrieval_config: A `TectonFeaturesRetrievalConfig` dataclass
        containing feature retrieval parameters (feature_service_name,
        entity_id, entity_row_fn).
      exception_level: a `enum.Enum` value from
        `apache_beam.transforms.enrichment_handlers.utils.ExceptionLevel`
        to set the level when `None` feature values are fetched from the
        online Tecton store. Defaults to `ExceptionLevel.WARN`.
    """
    self._connection_config = connection_config
    self._features_retrieval_config = features_retrieval_config
    self._exception_level = exception_level

  def __enter__(self):
    """Connect to the Tecton feature store."""
    # Suppress Tecton SDK output to avoid cluttering test output. Redirect
    # stdout to suppress success messages.
    original_stdout = sys.stdout
    sys.stdout = StringIO()
    try:
      tecton.login(
        tecton_url=self._connection_config.url,
        tecton_api_key=self._connection_config.api_key)
    finally:
      sys.stdout = original_stdout

    self._feature_service = tecton.get_feature_service(
      name=self._features_retrieval_config.feature_service_name,
      workspace=self._features_retrieval_config.workspace_name)

  def __call__(self, request: beam.Row, *args, **kwargs):
    """Fetches feature values for an entity-id from the Tecton feature store."""
    if self._features_retrieval_config.entity_row_fn:
      entity = self._features_retrieval_config.entity_row_fn(request)
    else:
      request_dict = request._asdict()
      entity = {
          self._features_retrieval_config.entity_id:
          request_dict[self._features_retrieval_config.entity_id]
      }

    try:
      response = self._feature_service.get_online_features(
        join_keys=entity,
        include_join_keys_in_response=self._features_retrieval_config.include_join_keys_in_response,
        request_data=self._features_retrieval_config.request_data
      )

      feature_values = response.to_dict(
        self._features_retrieval_config.return_effective_times
      )
    except Exception as e:
      if self._exception_level == ExceptionLevel.RAISE:
        raise RuntimeError(
            f'Failed to fetch features from Tecton feature store: {e}')
      elif self._exception_level == ExceptionLevel.WARN:
        _LOGGER.warning(
            f'Failed to fetch features from Tecton feature store: {e}')
        feature_values = {}
      else:  # ExceptionLevel.QUIET
        feature_values = {}

    return request, beam.Row(**feature_values)

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Clean the Tecton feature store connection."""
    # Suppress Tecton SDK output during teardown. Redirect stdout to suppress
    # logout messages.
    original_stdout = sys.stdout
    sys.stdout = StringIO()
    try:
      tecton.logout()
    finally:
      sys.stdout = original_stdout

  def get_cache_key(self, request: beam.Row) -> str:
    """Returns a string formatted with unique entity-id for the feature values.
    """
    if self._features_retrieval_config.entity_row_fn:
      entity = self._features_retrieval_config.entity_row_fn(request)
      entity_id = list(entity.keys())[0]
    else:
      entity_id = self._features_retrieval_config.entity_id
    return f'entity_id: {request._asdict()[entity_id]}'

