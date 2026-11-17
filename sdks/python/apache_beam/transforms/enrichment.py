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
import sys
import traceback
from collections.abc import Callable
from collections.abc import Mapping
from datetime import timedelta
from typing import Any
from typing import Optional
from typing import TypeVar
from typing import Union

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.coders import coders
from apache_beam.io.requestresponse import DEFAULT_CACHE_ENTRY_TTL_SEC
from apache_beam.io.requestresponse import DEFAULT_TIMEOUT_SECS
from apache_beam.io.requestresponse import Caller
from apache_beam.io.requestresponse import DefaultThrottler
from apache_beam.io.requestresponse import ExponentialBackOffRepeater
from apache_beam.io.requestresponse import PreCallThrottler
from apache_beam.io.requestresponse import RedisCache
from apache_beam.io.requestresponse import Repeater
from apache_beam.io.requestresponse import RequestResponseIO
from apache_beam.typehints import typehints
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.typehints.schemas import named_fields_from_element_type

__all__ = [
    "EnrichmentSourceHandler",
    "Enrichment",
    "cross_join",
]

InputT = TypeVar('InputT')
OutputT = TypeVar('OutputT')

JoinFn = Callable[[dict[str, Any], dict[str, Any]], beam.Row]

VALID_ON_MISS_MODES = ('null', 'drop', 'dlq', 'fail')

_LOGGER = logging.getLogger(__name__)


def has_valid_redis_address(host: str, port: int) -> bool:
  """returns `True` if both host and port are not `None`."""
  if host and port:
    return True
  return False


def cross_join(
    left: dict[str, Any],
    right: dict[str, Any],
    prefix: Optional[str] = None) -> beam.Row:
  """performs a cross join on two `dict` objects.

    Joins the columns of the right row onto the left row.

    Args:
      left (dict[str, Any]): input request dictionary.
      right (dict[str, Any]): response dictionary from the API.
      prefix (Optional[str]): optional prefix to prepend to keys from `right`
        to avoid column name collisions.

    Returns:
      `beam.Row` containing the merged columns.
  """
  for k, v in right.items():
    target_key = f"{prefix}{k}" if prefix else k
    if target_key not in left:
      # Don't override the values in left.
      left[target_key] = v
    elif left[target_key] != v:
      _LOGGER.warning(
          '%s exists in the input row as well the row fetched '
          'from API but have different values - %s and %s. Using the input '
          'value (%s) for the enriched row. You can override this behavior by '
          'passing a custom `join_fn` to Enrichment transform.' %
          (target_key, left[target_key], v, left[target_key]))
  return beam.Row(**left)


class _EnrichmentJoinFn(beam.DoFn):
  """DoFn that joins `(request, response)` tuples with `on_miss` and `prefix`."""
  def __init__(
      self,
      join_fn: JoinFn,
      use_custom_types: bool = False,
      on_miss: str = 'null',
      prefix: Optional[str] = None,
      null_fields: Optional[list[str]] = None,
      source_handler: Optional[Any] = None,
      dead_letter_tag: Optional[str] = None,
  ):
    self._join_fn = join_fn
    self._use_custom_types = use_custom_types
    self._on_miss = on_miss
    self._prefix = prefix
    self._null_fields = list(null_fields) if null_fields else []
    self._source_handler = source_handler
    self._dead_letter_tag = dead_letter_tag

  def setup(self):
    if (not self._null_fields and self._source_handler is not None and
        hasattr(self._source_handler, 'get_output_schema')):
      try:
        self._null_fields = [
            name for name, _ in self._source_handler.get_output_schema()
        ]
      except Exception:
        pass

  @staticmethod
  def _is_miss(response: Any) -> bool:
    if response is None:
      return True
    if hasattr(response, '_asdict'):
      return len(response._asdict()) == 0
    if isinstance(response, Mapping):
      return len(response) == 0
    return False

  @staticmethod
  def _to_dict(obj: Any) -> dict[str, Any]:
    if hasattr(obj, '_asdict'):
      return dict(obj._asdict())
    return dict(obj)

  def process(self, element: tuple[Any, Any], *args, **kwargs):
    request, response = element
    if self._is_miss(response):
      if self._on_miss == 'drop':
        return
      if self._on_miss == 'fail':
        raise KeyError(f"Enrichment lookup missed for element: {request}")
      if self._on_miss == 'dlq':
        try:
          raise KeyError(f"Enrichment lookup missed for element: {request}")
        except Exception as exn:
          if self._dead_letter_tag:
            yield pvalue.TaggedOutput(
                self._dead_letter_tag,
                (
                    request,
                    (
                        type(exn),
                        repr(exn),
                        traceback.format_exception(*sys.exc_info()))))
            return
          raise

      # on_miss == 'null' (left join)
      if self._use_custom_types:
        yield self._join_fn(request, response)
        return
      left_dict = self._to_dict(request)
      right_dict: dict[str, Any] = {f: None for f in self._null_fields}
      if self._prefix:
        right_dict = {f"{self._prefix}{k}": v for k, v in right_dict.items()}
      yield self._join_fn(left_dict, right_dict)
      return

    try:
      if self._use_custom_types:
        yield self._join_fn(request, response)
      else:
        left_dict = self._to_dict(request)
        right_dict = self._to_dict(response)
        if not self._null_fields and right_dict:
          self._null_fields = list(right_dict.keys())
        if self._prefix:
          right_dict = {f"{self._prefix}{k}": v for k, v in right_dict.items()}
        yield self._join_fn(left_dict, right_dict)
    except Exception as exn:
      if self._dead_letter_tag:
        yield pvalue.TaggedOutput(
            self._dead_letter_tag,
            (
                request,
                (
                    type(exn),
                    repr(exn),
                    traceback.format_exception(*sys.exc_info()))))
      else:
        raise


class EnrichmentSourceHandler(Caller[InputT, OutputT]):
  """Wrapper class for `apache_beam.io.requestresponse.Caller`.

  Ensure that the implementation of ``__call__`` method returns a tuple
  of `beam.Row`  objects.
  """
  def get_cache_key(self, request: InputT) -> str:
    """Returns the request to be cached. This is how the response will be
    looked up in the cache as well.

    Implement this method to provide the key for the cache.
    By default, the entire request is stored as the cache key.

    For example, in `BigTableEnrichmentHandler`, the row key for the element
    is returned here.
    """
    return "request: %s" % request


class Enrichment(beam.PTransform[beam.PCollection[InputT],
                                 beam.PCollection[OutputT]]):
  """A :class:`apache_beam.transforms.enrichment.Enrichment` transform to
  enrich elements in a PCollection.

  Uses the :class:`apache_beam.transforms.enrichment.EnrichmentSourceHandler`
  to enrich elements by joining the metadata from external source.

  Processes an input :class:`~apache_beam.pvalue.PCollection` of `beam.Row` by
  applying a :class:`apache_beam.transforms.enrichment.EnrichmentSourceHandler`
  to each element and returning the enriched
  :class:`~apache_beam.pvalue.PCollection`.

  Args:
    source_handler: Handles source lookup and metadata retrieval.
      Implements the
      :class:`apache_beam.transforms.enrichment.EnrichmentSourceHandler`
    join_fn: A lambda function to join original element with lookup metadata.
      Defaults to `CROSS_JOIN`.
    timeout: (Optional) timeout for source requests. Defaults to 30 seconds.
    repeater: provides method to repeat failed requests to API due to service
      errors. Defaults to
      :class:`apache_beam.io.requestresponse.ExponentialBackOffRepeater` to
      repeat requests with exponential backoff.
    throttler: provides methods to pre-throttle a request. Defaults to
      :class:`apache_beam.io.requestresponse.DefaultThrottler` for
      client-side adaptive throttling using
      :class:`apache_beam.io.components.adaptive_throttler.AdaptiveThrottler`.
  """
  def __init__(
      self,
      source_handler: EnrichmentSourceHandler,
      join_fn: Optional[JoinFn] = None,
      timeout: Optional[float] = DEFAULT_TIMEOUT_SECS,
      repeater: Repeater = ExponentialBackOffRepeater(),
      throttler: PreCallThrottler = DefaultThrottler(),
      use_custom_types: bool = False,
      *,
      on_miss: Optional[str] = 'null',
      prefix: Optional[str] = None):
    normalized_on_miss = 'null' if on_miss is None else str(on_miss).lower()
    if normalized_on_miss not in VALID_ON_MISS_MODES:
      raise ValueError(
          f"Invalid on_miss mode '{on_miss}'. Expected one of: "
          f"{VALID_ON_MISS_MODES}.")
    self._cache = None
    self._source_handler = source_handler
    self._custom_join_fn = (
        join_fn if join_fn else getattr(source_handler, 'join_fn', None))
    self._join_fn = self._custom_join_fn or cross_join
    self._timeout = timeout
    self._repeater = repeater
    self._throttler = throttler
    self._use_custom_types = (
        source_handler.use_custom_types if hasattr(
            source_handler, 'use_custom_types') else use_custom_types)
    self._on_miss = normalized_on_miss
    self._prefix = prefix
    self._exception_handling_args: Optional[dict[str, Any]] = None

  def with_exception_handling(self, **kwargs):
    self._exception_handling_args = kwargs
    return self

  def _get_handler_schema(self) -> list[tuple[str, Any]]:
    if hasattr(self._source_handler, 'get_output_schema'):
      return list(self._source_handler.get_output_schema())
    for attr in (
        'select_fields',
        'column_names',
        'feature_ids',
        'feature_names',
        'output_fields',
    ):
      cols = getattr(self._source_handler, attr, None)
      if isinstance(cols, (list, tuple)) and cols and all(isinstance(c, str)
                                                          for c in cols):
        return [(c, Any) for c in cols]
    return []

  @staticmethod
  def _make_optional_type(field_type: Any) -> Any:
    if field_type is Any or field_type is typehints.Any:
      return field_type
    if (getattr(field_type, '__origin__', None) is Union and
        type(None) in getattr(field_type, '__args__', ())):
      return field_type
    return Optional[field_type]

  def infer_output_type(self, input_type):
    if self._use_custom_types or self._custom_join_fn is not None:
      return typehints.Any
    handler_schema = self._get_handler_schema()
    if not handler_schema:
      return typehints.Any
    try:
      if isinstance(input_type, RowTypeConstraint):
        input_fields = list(input_type._fields)
      else:
        input_fields = list(named_fields_from_element_type(input_type))
    except Exception:
      return typehints.Any
    if not input_fields:
      return typehints.Any

    output_fields = list(input_fields)
    existing_names = {name for name, _ in output_fields}
    for field_name, field_type in handler_schema:
      target_name = f"{self._prefix}{field_name}" if self._prefix else field_name
      if target_name not in existing_names:
        effective_type = (
            self._make_optional_type(field_type)
            if self._on_miss == 'null' else field_type)
        output_fields.append((target_name, effective_type))
        existing_names.add(target_name)
    return RowTypeConstraint.from_fields(output_fields)

  def expand(self,
             input_row: beam.PCollection[InputT]) -> beam.PCollection[OutputT]:
    if self._on_miss == 'dlq' and self._exception_handling_args is None:
      raise ValueError(
          "`on_miss='dlq'` requires `error_handling` (or "
          "`with_exception_handling`) to be configured.")

    # For caching with enrichment transform, enrichment handlers provide a
    # get_cache_key() method that returns a unique string formatted
    # request for that row.
    request_coder = coders.StrUtf8Coder()
    if self._cache:
      self._cache.request_coder = request_coder

    handler_schema = self._get_handler_schema()
    null_fields = [name for name, _ in handler_schema]
    inferred_type = self.infer_output_type(input_row.element_type)

    rrio = RequestResponseIO(
        caller=self._source_handler,
        timeout=self._timeout,
        repeater=self._repeater,
        cache=self._cache,
        throttler=self._throttler)

    if self._exception_handling_args is not None:
      main_tag = self._exception_handling_args.get('main_tag', 'good')
      dead_letter_tag = self._exception_handling_args.get(
          'dead_letter_tag', 'bad')
      rrio_result = (
          input_row
          | "Enrichment-RRIO" >>
          rrio.with_exception_handling(**self._exception_handling_args))
      fetched_data = rrio_result[main_tag]
      rrio_errors = rrio_result[dead_letter_tag]

      join_result = (
          fetched_data
          | "enrichment_join" >> beam.ParDo(
              _EnrichmentJoinFn(
                  join_fn=self._join_fn,
                  use_custom_types=self._use_custom_types,
                  on_miss=self._on_miss,
                  prefix=self._prefix,
                  null_fields=null_fields,
                  source_handler=self._source_handler,
                  dead_letter_tag=dead_letter_tag)).with_outputs(
                      dead_letter_tag, main=main_tag))
      main_output = join_result[main_tag]
      if inferred_type != typehints.Any:
        main_output.element_type = inferred_type
      all_errors = ((rrio_errors, join_result[dead_letter_tag])
                    | "FlattenEnrichmentErrors" >> beam.Flatten())
      return {main_tag: main_output, dead_letter_tag: all_errors}  # type: ignore[return-value]

    fetched_data = input_row | "Enrichment-RRIO" >> rrio

    # EnrichmentSourceHandler returns a tuple of (request,response).
    output = (
        fetched_data
        | "enrichment_join" >> beam.ParDo(
            _EnrichmentJoinFn(
                join_fn=self._join_fn,
                use_custom_types=self._use_custom_types,
                on_miss=self._on_miss,
                prefix=self._prefix,
                null_fields=null_fields,
                source_handler=self._source_handler)))
    if inferred_type != typehints.Any:
      output.element_type = inferred_type
    return output

  def with_redis_cache(
      self,
      host: str,
      port: int,
      time_to_live: Union[int, timedelta] = DEFAULT_CACHE_ENTRY_TTL_SEC,
      *,
      request_coder: Optional[coders.Coder] = None,
      response_coder: Optional[coders.Coder] = None,
      **kwargs,
  ):
    """Configure the Redis cache to use with enrichment transform.

    Args:
      host (str): The hostname or IP address of the Redis server.
      port (int): The port number of the Redis server.
      time_to_live: `(Union[int, timedelta])` The time-to-live (TTL) for
        records stored in Redis. Provide an integer (in seconds) or a
        `datetime.timedelta` object.
      request_coder: (Optional[`coders.Coder`]) coder for requests stored
        in Redis.
      response_coder: (Optional[`coders.Coder`]) coder for decoding responses
        received from Redis.
      kwargs: Optional additional keyword arguments that
        are required to connect to your redis server. Same as `redis.Redis()`.
    """
    if has_valid_redis_address(host, port):
      self._cache = RedisCache(  # type: ignore[assignment]
          host=host,
          port=port,
          time_to_live=time_to_live,
          request_coder=request_coder,
          response_coder=response_coder,
          **kwargs)
    return self
