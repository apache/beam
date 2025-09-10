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
from typing import Any, Dict
from typing import Optional

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel
from tecton_client import TectonClient, MetadataOptions, RequestOptions

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
    default_workspace_name: The name of the workspace containing the feature
      service. This is the workspace where your feature definitions are stored.
    api_key: The API key for authenticating with the Tecton instance.
      This should be a valid API key with appropriate permissions.
    kwargs: Additional keyword arguments for write operations. Enables forward
      compatibility with future Tecton connection parameters.
  """
  url: str
  default_workspace_name: str
  api_key: str
  kwargs: Dict[str, Any] = field(default_factory=dict)

  def __post_init__(self):
    if not self.url:
      raise ValueError('Please provide a Tecton instance URL (`url`).')

    if not self.default_workspace_name:
      raise ValueError(
          'Please provide a workspace name (`default_workspace_name`).')

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
    entity_id: The entity name for the entity associated with the features.
      The `entity_id` is used to extract the entity value from the input row.
      Please provide exactly one of `entity_id` or `entity_row_fn`.
    entity_row_fn: A lambda function that takes an input `beam.Row` and
      returns a dictionary with a mapping from the entity key column name to
      entity key value. It is used to build/extract the entity dict for
      feature retrieval. Please provide exactly one of `entity_id` or
      `entity_row_fn`.
    request_context_map: Optional mapping of request context parameters
      to pass to Tecton for feature computation. These are typically used
      for real-time features that depend on request-time data.
    workspace_name: Optional workspace name override. If not provided,
      uses the workspace from the connection config.
    allow_partial_results: Whether to allow partial results if some features
      fail to compute. Defaults to False.
    request_options: Optional RequestOptions for controlling request behavior.
      Defaults to None.
    metadata_options: Optional MetadataOptions for controlling what metadata
      is returned. Defaults to
      MetadataOptions(include_names=True, include_data_types=True).
    kwargs: Additional keyword arguments for feature retrieval. Enables forward
      compatibility with future Tecton feature retrieval parameters.
  """
  feature_service_name: str
  entity_id: str = ""
  entity_row_fn: Optional[EntityRowFn] = None
  request_context_map: Optional[Mapping[str, Any]] = None
  workspace_name: Optional[str] = None
  allow_partial_results: bool = False
  request_options: Optional[RequestOptions] = None
  metadata_options: Optional[MetadataOptions] = field(
      default_factory=lambda: MetadataOptions(include_names=True,
        include_data_types=True))
  kwargs: Dict[str, Any] = field(default_factory=dict)

  def __post_init__(self):
    if not self.feature_service_name:
      raise ValueError(
          'Please provide a feature service name for the Tecton '
          'online feature store (`feature_service_name`).')

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
    """Connect with the Tecton feature store."""
    self._client = TectonClient(
      **unpack_dataclass_with_kwargs(self._connection_config))

  def __call__(self, request: beam.Row, *args, **kwargs):
    """Fetches feature values for an entity-id from the Tecton feature store.

    Args:
      request: the input `beam.Row` to enrich.
    """
    if self._features_retrieval_config.entity_row_fn:
      entity = self._features_retrieval_config.entity_row_fn(request)
    else:
      request_dict = request._asdict()
      entity = {
          self._features_retrieval_config.entity_id:
          request_dict[self._features_retrieval_config.entity_id]
      }

    try:
      config = unpack_dataclass_with_kwargs(self._features_retrieval_config)
      config.pop('entity_id', None)
      config.pop('entity_row_fn', None)
      response = self._client.get_features(**config,join_key_map=entity)
      feature_values = response.get_features_dict()
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
    """Clean the instantiated Tecton client."""
    self._client._client.close()
    self._client = None

  def get_cache_key(self, request: beam.Row) -> str:
    """Returns a string formatted with unique entity-id for the feature values.
    """
    if self._features_retrieval_config.entity_row_fn:
      entity = self._features_retrieval_config.entity_row_fn(request)
      entity_id = list(entity.keys())[0]
    else:
      entity_id = self._features_retrieval_config.entity_id
    return f'entity_id: {request._asdict()[entity_id]}'


def unpack_dataclass_with_kwargs(dataclass_instance):
  """Unpacks dataclass fields into a flat dict, merging kwargs with precedence.

  Args:
    dataclass_instance: Dataclass instance to unpack.

  Returns:
    dict: Flattened dictionary with kwargs taking precedence over fields.
  """
  params: dict = dataclass_instance.__dict__.copy()
  nested_kwargs = params.pop('kwargs', {})
  return {**params, **nested_kwargs}
