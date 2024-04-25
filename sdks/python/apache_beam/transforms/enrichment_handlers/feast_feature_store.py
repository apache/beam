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
import logging
import tempfile
from pathlib import Path
from typing import Any
from typing import Callable
from typing import List
from typing import Mapping
from typing import Optional

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel
from feast import FeatureStore

__all__ = [
    'FeastFeatureStoreEnrichmentHandler',
]

EntityRowFn = Callable[[beam.Row], Mapping[str, Any]]

_LOGGER = logging.getLogger(__name__)

LOCAL_FEATURE_STORE_YAML_FILENAME = 'fs_yaml_file.yaml'


def download_fs_yaml_file(gcs_fs_yaml_file: str):
  """Download the feature store config file for Feast."""
  try:
    with FileSystems.open(gcs_fs_yaml_file, 'r') as gcs_file:
      with tempfile.NamedTemporaryFile(suffix=LOCAL_FEATURE_STORE_YAML_FILENAME,
                                       delete=False) as local_file:
        local_file.write(gcs_file.read())
        return Path(local_file.name)
  except Exception:
    raise RuntimeError(
        'error downloading the file %s locally to load the '
        'Feast feature store.' % gcs_fs_yaml_file)


def _validate_feature_names(feature_names, feature_service_name):
  """Check if one of `feature_names` or `feature_service_name` is provided."""
  if ((not feature_names and not feature_service_name) or
      bool(feature_names and feature_service_name)):
    raise ValueError(
        'Please provide exactly one of a list of feature names to fetch '
        'from online store (`feature_names`) or a feature service name for '
        'the Feast online feature store (`feature_service_name`).')


def _validate_feature_store_yaml_path_exists(fs_yaml_file):
  """Check if the feature store yaml path exists."""
  if not FileSystems.exists(fs_yaml_file):
    raise ValueError(
        'The feature store yaml path (%s) does not exist.' % fs_yaml_file)


def _validate_entity_key_exists(entity_id, entity_row_fn):
  """Checks if the entity key or a lambda to build entity key exists."""
  if ((not entity_row_fn and not entity_id) or
      bool(entity_row_fn and entity_id)):
    raise ValueError(
        "Please specify exactly one of a `entity_id` or a lambda "
        "function with `entity_row_fn` to extract the entity id "
        "from the input row.")


class FeastFeatureStoreEnrichmentHandler(EnrichmentSourceHandler[beam.Row,
                                                                 beam.Row]):
  """Enrichment handler to interact with the Feast feature store.

  To specify the features to fetch from Feast online store,
  please specify exactly one of `feature_names` or `feature_service_name`.

  Use this handler with :class:`apache_beam.transforms.enrichment.Enrichment`
  transform. To filter the features to enrich, use the `join_fn` param in
  :class:`apache_beam.transforms.enrichment.Enrichment`.
  """
  def __init__(
      self,
      feature_store_yaml_path: str,
      feature_names: Optional[List[str]] = None,
      feature_service_name: Optional[str] = "",
      full_feature_names: Optional[bool] = False,
      entity_id: str = "",
      *,
      entity_row_fn: Optional[EntityRowFn] = None,
      exception_level: ExceptionLevel = ExceptionLevel.WARN,
  ):
    """Initializes an instance of `FeastFeatureStoreEnrichmentHandler`.

    Args:
      feature_store_yaml_path (str): The path to a YAML configuration file for
        the Feast feature store. See
        https://docs.feast.dev/reference/feature-repository/feature-store-yaml
        for configuration options supported by Feast.
      feature_names: A list of feature names to be retrieved from the online
        Feast feature store.
      feature_service_name (str): The name of the feature service containing
        the features to fetch from the online Feast feature store.
      full_feature_names (bool): Whether to use full feature names
        (including namespaces, etc.). Defaults to False.
      entity_id (str): entity name for the entity associated with the features.
        The `entity_id` is used to extract the entity value from the input row.
        Please provide exactly one of `entity_id` or `entity_row_fn`.
      entity_row_fn: a lambda function that takes an input `beam.Row` and
        returns a dictionary with a mapping from the entity key column name to
        entity key value. It is used to build/extract the entity dict for
        feature retrieval. Please provide exactly one of `entity_id` or
        `entity_row_fn`.
        See https://docs.feast.dev/getting-started/concepts/feature-retrieval
        for more information.
      exception_level: a `enum.Enum` value from
        `apache_beam.transforms.enrichment_handlers.utils.ExceptionLevel`
        to set the level when `None` feature values are fetched from the
        online Feast store. Defaults to `ExceptionLevel.WARN`.
    """
    self.entity_id = entity_id
    self.feature_store_yaml_path = feature_store_yaml_path
    self.feature_names = feature_names
    self.feature_service_name = feature_service_name
    self.full_feature_names = full_feature_names
    self.entity_row_fn = entity_row_fn
    self._exception_level = exception_level
    _validate_entity_key_exists(self.entity_id, self.entity_row_fn)
    _validate_feature_store_yaml_path_exists(self.feature_store_yaml_path)
    _validate_feature_names(self.feature_names, self.feature_service_name)

  def __enter__(self):
    """Connect with the Feast feature store."""
    local_repo_path = download_fs_yaml_file(self.feature_store_yaml_path)
    try:
      self.store = FeatureStore(fs_yaml_file=local_repo_path)
    except Exception:
      raise RuntimeError(
          'Invalid feature store yaml file provided. Make sure '
          'the %s contains the valid configuration for Feast feature store.' %
          self.feature_store_yaml_path)
    if self.feature_service_name:
      try:
        self.features = self.store.get_feature_service(
            self.feature_service_name)
      except Exception:
        raise RuntimeError(
            'Could not find the feature service %s for the feature '
            'store configured in %s.' %
            (self.feature_service_name, self.feature_store_yaml_path))
    else:
      self.features = self.feature_names

  def __call__(self, request: beam.Row, *args, **kwargs):
    """Fetches feature values for an entity-id from the Feast feature store.

    Args:
      request: the input `beam.Row` to enrich.
    """
    if self.entity_row_fn:
      entity_dict = self.entity_row_fn(request)
    else:
      request_dict = request._asdict()
      entity_dict = {self.entity_id: request_dict[self.entity_id]}
    feature_values = self.store.get_online_features(
        features=self.features,
        entity_rows=[entity_dict],
        full_feature_names=self.full_feature_names).to_dict()
    # get_online_features() returns a list of feature values per entity-id.
    # Since we do this per entity, the list of feature values only contain
    # a single element at position 0.
    response_dict = {k: v[0] for k, v in feature_values.items()}
    return request, beam.Row(**response_dict)

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Clean the instantiated Feast feature store client."""
    self.store = None

  def get_cache_key(self, request: beam.Row) -> str:
    """Returns a string formatted with unique entity-id for the feature values.
    """
    if self.entity_row_fn:
      entity_dict = self.entity_row_fn(request)
      entity_id = list(entity_dict.keys())[0]
    else:
      entity_id = self.entity_id
    return 'entity_id: %s' % request._asdict()[entity_id]
