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

"""A connector for sending API requests to the GCP Recommendations AI
API (https://cloud.google.com/recommendations).
"""

from __future__ import absolute_import

from typing import Sequence
from typing import Tuple

from cachetools.func import ttl_cache
from google.api_core.retry import Retry

from apache_beam import pvalue
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.transforms import PTransform
from apache_beam.transforms.util import GroupIntoBatches
from apache_beam.utils import retry

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from google.cloud import recommendationengine
except ImportError:
  raise ImportError(
      'Google Cloud Recommendation AI not supported for this execution '
      'environment (could not import google.cloud.recommendationengine).')
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

__all__ = [
    'CreateCatalogItem',
    'WriteUserEvent',
    'ImportCatalogItems',
    'ImportUserEvents',
    'PredictUserEvent'
]

FAILED_CATALOG_ITEMS = "failed_catalog_items"
MAX_RETRIES = 5


@ttl_cache(maxsize=128, ttl=3600)
def get_recommendation_prediction_client():
  """Returns a Recommendation AI - Prediction Service client."""
  _client = recommendationengine.PredictionServiceClient()
  return _client


@ttl_cache(maxsize=128, ttl=3600)
def get_recommendation_catalog_client():
  """Returns a Recommendation AI - Catalog Service client."""
  _client = recommendationengine.CatalogServiceClient()
  return _client


@ttl_cache(maxsize=128, ttl=3600)
def get_recommendation_user_event_client():
  """Returns a Recommendation AI - UserEvent Service client."""
  _client = recommendationengine.UserEventServiceClient()
  return _client


class CreateCatalogItem(PTransform):
  """Creates catalogitem information.
    The ``PTransform`` returns a PCollectionTuple with a PCollections of
    successfully and failed created CatalogItems.

    Example usage::

      pipeline | CreateCatalogItem(
        project='example-gcp-project',
        catalog_name='my-catalog')
    """
  def __init__(
      self,
      project: str = None,
      retry: Retry = None,
      timeout: float = 120,
      metadata: Sequence[Tuple[str, str]] = (),
      catalog_name: str = "default_catalog"):
    """Initializes a :class:`CreateCatalogItem` transform.

        Args:
            project (str): Optional. GCP project name in which the catalog
              data will be imported.
            retry: Optional. Designation of what
              errors, if any, should be retried.
            timeout (float): Optional. The amount of time, in seconds, to wait
              for the request to complete.
            metadata: Optional. Strings which
              should be sent along with the request as metadata.
            catalog_name (str): Optional. Name of the catalog.
              Default: 'default_catalog'
        """
    self.project = project
    self.retry = retry
    self.timeout = timeout
    self.metadata = metadata
    self.catalog_name = catalog_name

  def expand(self, pcoll):
    if self.project is None:
      self.project = pcoll.pipeline.options.view_as(GoogleCloudOptions).project
    if self.project is None:
      raise ValueError(
          """GCP project name needs to be specified in "project" pipeline
            option""")
    return pcoll | ParDo(
        _CreateCatalogItemFn(
            self.project,
            self.retry,
            self.timeout,
            self.metadata,
            self.catalog_name))


class _CreateCatalogItemFn(DoFn):
  def __init__(
      self,
      project: str = None,
      retry: Retry = None,
      timeout: float = 120,
      metadata: Sequence[Tuple[str, str]] = (),
      catalog_name: str = None):
    self._client = None
    self.retry = retry
    self.timeout = timeout
    self.metadata = metadata
    self.parent = f"projects/{project}/locations/global/catalogs/{catalog_name}"
    self.counter = Metrics.counter(self.__class__, "api_calls")

  def setup(self):
    if self._client is None:
      self._client = get_recommendation_catalog_client()

  def process(self, element):
    catalog_item = recommendationengine.CatalogItem(element)
    request = recommendationengine.CreateCatalogItemRequest(
        parent=self.parent, catalog_item=catalog_item)

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_timeout_or_quota_issues_filter
    )
    def create_item():
      return self._client.create_catalog_item(
          request=request,
          retry=self.retry,
          timeout=self.timeout,
          metadata=self.metadata)

    try:
      created_catalog_item = create_item()
      self.counter.inc()
      yield recommendationengine.CatalogItem.to_dict(created_catalog_item)
    except Exception:
      yield pvalue.TaggedOutput(
          FAILED_CATALOG_ITEMS,
          recommendationengine.CatalogItem.to_dict(catalog_item))


class ImportCatalogItems(PTransform):
  """Imports catalogitems in bulk.
    The `PTransform` returns a PCollectionTuple with PCollections of
    successfully and failed imported CatalogItems.

    Example usage::

      pipeline
      | ImportCatalogItems(
          project='example-gcp-project',
          catalog_name='my-catalog')
    """
  def __init__(
      self,
      max_batch_size: int = 5000,
      project: str = None,
      retry: Retry = None,
      timeout: float = 120,
      metadata: Sequence[Tuple[str, str]] = (),
      catalog_name: str = "default_catalog"):
    """Initializes a :class:`ImportCatalogItems` transform

        Args:
            batch_size (int): Required. Maximum number of catalogitems per
              request.
            project (str): Optional. GCP project name in which the catalog
              data will be imported.
            retry: Optional. Designation of what
              errors, if any, should be retried.
            timeout (float): Optional. The amount of time, in seconds, to wait
              for the request to complete.
            metadata: Optional. Strings which
              should be sent along with the request as metadata.
            catalog_name (str): Optional. Name of the catalog.
              Default: 'default_catalog'
        """
    self.max_batch_size = max_batch_size
    self.project = project
    self.retry = retry
    self.timeout = timeout
    self.metadata = metadata
    self.catalog_name = catalog_name

  def expand(self, pcoll):
    if self.project is None:
      self.project = pcoll.pipeline.options.view_as(GoogleCloudOptions).project
    if self.project is None:
      raise ValueError(
          'GCP project name needs to be specified in "project" pipeline option')
    return (
        pcoll | GroupIntoBatches.WithShardedKey(self.max_batch_size) | ParDo(
            _ImportCatalogItemsFn(
                self.project,
                self.retry,
                self.timeout,
                self.metadata,
                self.catalog_name)))


class _ImportCatalogItemsFn(DoFn):
  def __init__(
      self,
      project=None,
      retry=None,
      timeout=120,
      metadata=None,
      catalog_name=None):
    self._client = None
    self.retry = retry
    self.timeout = timeout
    self.metadata = metadata
    self.parent = f"projects/{project}/locations/global/catalogs/{catalog_name}"
    self.counter = Metrics.counter(self.__class__, "api_calls")

  def setup(self):
    if self._client is None:
      self.client = get_recommendation_catalog_client()

  def process(self, element):
    catalog_items = [recommendationengine.CatalogItem(e) for e in element[1]]
    catalog_inline_source = recommendationengine.CatalogInlineSource(
        {"catalog_items": catalog_items})
    input_config = recommendationengine.InputConfig(
        catalog_inline_source=catalog_inline_source)

    request = recommendationengine.ImportCatalogItemsRequest(
        parent=self.parent, input_config=input_config)

    try:
      operation = self._client.import_catalog_items(
          request=request,
          retry=self.retry,
          timeout=self.timeout,
          metadata=self.metadata)
      self.counter.inc(len(catalog_items))
      yield operation.result()
    except Exception:
      yield pvalue.TaggedOutput(FAILED_CATALOG_ITEMS, catalog_items)


class WriteUserEvent(PTransform):
  """Write user event information.
    The `PTransform` returns a PCollectionTuple with PCollections of
    successfully and failed written UserEvents.

    Example usage::

      pipeline
      | WriteUserEvent(
          project='example-gcp-project',
          catalog_name='my-catalog',
          event_store='my_event_store')
    """
  def __init__(
      self,
      project: str = None,
      retry: Retry = None,
      timeout: float = 120,
      metadata: Sequence[Tuple[str, str]] = (),
      catalog_name: str = "default_catalog",
      event_store: str = "default_event_store"):
    """Initializes a :class:`WriteUserEvent` transform.

        Args:
            project (str): Optional. GCP project name in which the catalog
              data will be imported.
            retry: Optional. Designation of what
              errors, if any, should be retried.
            timeout (float): Optional. The amount of time, in seconds, to wait
              for the request to complete.
            metadata: Optional. Strings which
              should be sent along with the request as metadata.
            catalog_name (str): Optional. Name of the catalog.
              Default: 'default_catalog'
            event_store (str): Optional. Name of the event store.
              Default: 'default_event_store'
        """
    self.project = project
    self.retry = retry
    self.timeout = timeout
    self.metadata = metadata
    self.catalog_name = catalog_name
    self.event_store = event_store

  def expand(self, pcoll):
    if self.project is None:
      self.project = pcoll.pipeline.options.view_as(GoogleCloudOptions).project
    if self.project is None:
      raise ValueError(
          'GCP project name needs to be specified in "project" pipeline option')
    return pcoll | ParDo(
        _WriteUserEventFn(
            self.project,
            self.retry,
            self.timeout,
            self.metadata,
            self.catalog_name,
            self.event_store))


class _WriteUserEventFn(DoFn):
  FAILED_USER_EVENTS = "failed_user_events"

  def __init__(
      self,
      project=None,
      retry=None,
      timeout=120,
      metadata=None,
      catalog_name=None,
      event_store=None):
    self._client = None
    self.retry = retry
    self.timeout = timeout
    self.metadata = metadata
    self.parent = f"projects/{project}/locations/global/catalogs/"\
                  f"{catalog_name}/eventStores/{event_store}"
    self.counter = Metrics.counter(self.__class__, "api_calls")

  def setup(self):
    if self._client is None:
      self._client = get_recommendation_user_event_client()

  def process(self, element):
    user_event = recommendationengine.UserEvent(element)
    request = recommendationengine.WriteUserEventRequest(
        parent=self.parent, user_event=user_event)

    try:
      created_user_event = self._client.write_user_event(request)
      self.counter.inc()
      yield recommendationengine.UserEvent.to_dict(created_user_event)
    except Exception:
      yield pvalue.TaggedOutput(
          self.FAILED_USER_EVENTS,
          recommendationengine.UserEvent.to_dict(user_event))


class ImportUserEvents(PTransform):
  """Imports userevents in bulk.
    The `PTransform` returns a PCollectionTuple with PCollections of
    successfully and failed imported UserEvents.

    Example usage::

      pipeline
      | ImportUserEvents(
          project='example-gcp-project',
          catalog_name='my-catalog',
          event_store='my_event_store')
    """
  def __init__(
      self,
      max_batch_size: int = 5000,
      project: str = None,
      retry: Retry = None,
      timeout: float = 120,
      metadata: Sequence[Tuple[str, str]] = (),
      catalog_name: str = "default_catalog",
      event_store: str = "default_event_store"):
    """Initializes a :class:`WriteUserEvent` transform.

        Args:
            batch_size (int): Required. Maximum number of catalogitems
              per request.
            project (str): Optional. GCP project name in which the catalog
              data will be imported.
            retry: Optional. Designation of what
              errors, if any, should be retried.
            timeout (float): Optional. The amount of time, in seconds, to wait
              for the request to complete.
            metadata: Optional. Strings which
              should be sent along with the request as metadata.
            catalog_name (str): Optional. Name of the catalog.
              Default: 'default_catalog'
            event_store (str): Optional. Name of the event store.
              Default: 'default_event_store'
        """
    self.max_batch_size = max_batch_size
    self.project = project
    self.retry = retry
    self.timeout = timeout
    self.metadata = metadata
    self.catalog_name = catalog_name
    self.event_store = event_store

  def expand(self, pcoll):
    if self.project is None:
      self.project = pcoll.pipeline.options.view_as(GoogleCloudOptions).project
    if self.project is None:
      raise ValueError(
          'GCP project name needs to be specified in "project" pipeline option')
    return (
        pcoll | GroupIntoBatches.WithShardedKey(self.max_batch_size) | ParDo(
            _ImportUserEventsFn(
                self.project,
                self.retry,
                self.timeout,
                self.metadata,
                self.catalog_name,
                self.event_store)))


class _ImportUserEventsFn(DoFn):
  FAILED_USER_EVENTS = "failed_user_events"

  def __init__(
      self,
      project=None,
      retry=None,
      timeout=120,
      metadata=None,
      catalog_name=None,
      event_store=None):
    self._client = None
    self.retry = retry
    self.timeout = timeout
    self.metadata = metadata
    self.parent = f"projects/{project}/locations/global/catalogs/"\
                  f"{catalog_name}/eventStores/{event_store}"
    self.counter = Metrics.counter(self.__class__, "api_calls")

  def setup(self):
    if self._client is None:
      self.client = get_recommendation_user_event_client()

  def process(self, element):

    user_events = [recommendationengine.UserEvent(e) for e in element[1]]
    user_event_inline_source = recommendationengine.UserEventInlineSource(
        {"user_events": user_events})
    input_config = recommendationengine.InputConfig(
        user_event_inline_source=user_event_inline_source)

    request = recommendationengine.ImportUserEventsRequest(
        parent=self.parent, input_config=input_config)

    try:
      operation = self._client.write_user_event(request)
      self.counter.inc(len(user_events))
      yield recommendationengine.PredictResponse.to_dict(operation.result())
    except Exception:
      yield pvalue.TaggedOutput(self.FAILED_USER_EVENTS, user_events)


class PredictUserEvent(PTransform):
  """Make a recommendation prediction.
    The `PTransform` returns a PCollection

    Example usage::

      pipeline
      | PredictUserEvent(
          project='example-gcp-project',
          catalog_name='my-catalog',
          event_store='my_event_store',
          placement_id='recently_viewed_default')
    """
  def __init__(
      self,
      project: str = None,
      retry: Retry = None,
      timeout: float = 120,
      metadata: Sequence[Tuple[str, str]] = (),
      catalog_name: str = "default_catalog",
      event_store: str = "default_event_store",
      placement_id: str = None):
    """Initializes a :class:`PredictUserEvent` transform.

        Args:
            project (str): Optional. GCP project name in which the catalog
              data will be imported.
            retry: Optional. Designation of what
              errors, if any, should be retried.
            timeout (float): Optional. The amount of time, in seconds, to wait
              for the request to complete.
            metadata: Optional. Strings which
              should be sent along with the request as metadata.
            catalog_name (str): Optional. Name of the catalog.
              Default: 'default_catalog'
            event_store (str): Optional. Name of the event store.
              Default: 'default_event_store'
            placement_id (str): Required. ID of the recommendation engine
              placement. This id is used to identify the set of models that
              will be used to make the prediction.
        """
    self.project = project
    self.retry = retry
    self.timeout = timeout
    self.metadata = metadata
    self.placement_id = placement_id
    self.catalog_name = catalog_name
    self.event_store = event_store
    if placement_id is None:
      raise ValueError('placement_id must be specified')
    else:
      self.placement_id = placement_id

  def expand(self, pcoll):
    if self.project is None:
      self.project = pcoll.pipeline.options.view_as(GoogleCloudOptions).project
    if self.project is None:
      raise ValueError(
          'GCP project name needs to be specified in "project" pipeline option')
    return pcoll | ParDo(
        _PredictUserEventFn(
            self.project,
            self.retry,
            self.timeout,
            self.metadata,
            self.catalog_name,
            self.event_store,
            self.placement_id))


class _PredictUserEventFn(DoFn):
  FAILED_PREDICTIONS = "failed_predictions"

  def __init__(
      self,
      project=None,
      retry=None,
      timeout=120,
      metadata=None,
      catalog_name=None,
      event_store=None,
      placement_id=None):
    self._client = None
    self.retry = retry
    self.timeout = timeout
    self.metadata = metadata
    self.name = f"projects/{project}/locations/global/catalogs/"\
                f"{catalog_name}/eventStores/{event_store}/placements/"\
                f"{placement_id}"
    self.counter = Metrics.counter(self.__class__, "api_calls")

  def setup(self):
    if self._client is None:
      self._client = get_recommendation_prediction_client()

  def process(self, element):
    user_event = recommendationengine.UserEvent(element)
    request = recommendationengine.PredictRequest(
        name=self.name, user_event=user_event)

    try:
      prediction = self._client.predict(request)
      self.counter.inc()
      yield [
          recommendationengine.PredictResponse.to_dict(p)
          for p in prediction.pages
      ]
    except Exception:
      yield pvalue.TaggedOutput(self.FAILED_PREDICTIONS, user_event)
