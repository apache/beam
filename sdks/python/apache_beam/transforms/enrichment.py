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

from typing import Callable
from typing import Generic
from typing import Optional
from typing import TypeVar

import apache_beam as beam
from apache_beam.io.requestresponseio import Caller
from apache_beam.io.requestresponseio import CacheReader
from apache_beam.io.requestresponseio import CacheWriter
from apache_beam.io.requestresponseio import DEFAULT_TIMEOUT_SECS
from apache_beam.io.requestresponseio import PreCallThrottler
from apache_beam.io.requestresponseio import Repeater
from apache_beam.io.requestresponseio import RequestResponseIO
from apache_beam.io.requestresponseio import ShouldBackOff

__all__ = [
    "EnrichmentSourceHandler",
    "Enrichment",
]

InputT = TypeVar('InputT')
OutputT = TypeVar('OutputT')


def cross_join(element):
  """cross_join performs a cross join between two `beam.Row` objects.

    Joins the columns of the right `beam.Row` onto the left `beam.Row`.

    Args:
      element (Tuple): A tuple containing two beam.Row objects -
        request and response.

    Returns:
      beam.Row: `beam.Row` containing the merged columns.
  """
  right_dict = element[1].as_dict()
  left_dict = element[0].as_dict()
  for k, v in right_dict.items():
    left_dict[k] = v
  return beam.Row(**left_dict)


class EnrichmentSourceHandler(Caller):
  """Wrapper class for :class:`apache_beam.io.requestresponseio.Caller`.

  Ensure that the implementation of ``__call__`` method returns a tuple
  of `beam.Row`  objects.
  """

  pass


class Enrichment(beam.PTransform[beam.PCollection[InputT],
                                 beam.PCollection[OutputT]],
                 Generic[InputT, OutputT]):
  """A :class:`apache_beam.transforms.Enrichment` transform to enrich elements
  in a PCollection.

  Uses the :class:`apache_beam.transforms.EnrichmentSourceHandler` to enrich
  elements by joining the metadata from external source.

  Processes an input :class:`~apache_beam.pvalue.PCollection` of `beam.Row` by
  applying a :class:`apache_beam.transforms.EnrichmentSourceHandler` to each
  element and returning the enriched :class:`~apache_beam.pvalue.PCollection`.

  Args:
    source_handler: Handles source lookup and metadata retrieval.
      Implements the :class:`apache_beam.transforms.EnrichmentSourceHandler`
    join_fn: A lambda function to join original element with lookup metadata.
      Defaults to `CROSS_JOIN`.
    timeout: (Optional) timeout for source requests. Defaults to 30 seconds.
    should_backoff: (Optional) backoff strategy function.
    repeater: (Optional) retry Repeater.
    cache_reader: (Optional) CacheReader for reading cache.
    cache_writer: (Optional) CacheWriter for writing cache.
    throttler: (Optional) Throttler mechanism to throttle source requests.
  """
  def __init__(
      self,
      source_handler: EnrichmentSourceHandler,
      join_fn: Callable = cross_join,
      timeout: Optional[float] = DEFAULT_TIMEOUT_SECS,
      should_backoff: Optional[ShouldBackOff] = None,
      repeater: Optional[Repeater] = None,
      cache_reader: Optional[CacheReader] = None,
      cache_writer: Optional[CacheWriter] = None,
      throttler: Optional[PreCallThrottler] = None):
    self._source_handler = source_handler
    self._join_fn = join_fn
    self._timeout = timeout
    self._should_backoff = should_backoff
    self._repeater = repeater
    self._cache_reader = cache_reader
    self._cache_writer = cache_writer
    self._throttler = throttler
    self.output_type = None

  def expand(self,
             input_row: beam.PCollection[InputT]) -> beam.PCollection[OutputT]:
    fetched_data = input_row | RequestResponseIO(
        caller=self._source_handler,
        timeout=self._timeout,
        should_backoff=self._should_backoff,
        repeater=self._repeater,
        cache_reader=self._cache_reader,
        cache_writer=self._cache_writer,
        throttler=self._throttler).with_output_types(dict)

    # EnrichmentSourceHandler returns a tuple of (request,response).
    return fetched_data | beam.Map(self._join_fn)
