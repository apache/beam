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

from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import TypeVar

import apache_beam as beam
from apache_beam.io.requestresponse import DEFAULT_TIMEOUT_SECS
from apache_beam.io.requestresponse import Caller
from apache_beam.io.requestresponse import RequestResponseIO

__all__ = [
    "EnrichmentSourceHandler",
    "Enrichment",
    "cross_join",
]

InputT = TypeVar('InputT')
OutputT = TypeVar('OutputT')

JoinFn = Callable[[Tuple[Dict[str, Any], Dict[str, Any]]], beam.Row]


def cross_join(element: Tuple[Dict[str, Any], Dict[str, Any]]) -> beam.Row:
  """cross_join performs a cross join on two `dict` objects.

    Joins the columns of the right row onto the left row.

    Args:
      element (Tuple): A tuple containing two `dict` objects -
        request and response.

    Returns:
      `beam.Row` containing the merged columns.
  """
  left, right = element
  for k, v in right.items():
    if k not in left:
      # Don't override the values in left.
      left[k] = v
  return beam.Row(**left)


class EnrichmentSourceHandler(Caller[InputT, OutputT]):
  """Wrapper class for :class:`apache_beam.io.requestresponse.Caller`.

  Ensure that the implementation of ``__call__`` method returns a tuple
  of `beam.Row`  objects.
  """
  pass


class Enrichment(beam.PTransform[beam.PCollection[InputT],
                                 beam.PCollection[OutputT]]):
  """A :class:`apache_beam.transforms.enrichment.Enrichment` transform to
  enrich elements in a PCollection.
  **NOTE:** This transform and its implementation are under development and
  do not provide backward compatibility guarantees.
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
  """
  def __init__(
      self,
      source_handler: EnrichmentSourceHandler,
      join_fn: JoinFn = cross_join,
      timeout: Optional[float] = DEFAULT_TIMEOUT_SECS):
    self._source_handler = source_handler
    self._join_fn = join_fn
    self._timeout = timeout

  def expand(self,
             input_row: beam.PCollection[InputT]) -> beam.PCollection[OutputT]:
    fetched_data = input_row | RequestResponseIO(
        caller=self._source_handler, timeout=self._timeout)

    # EnrichmentSourceHandler returns a tuple of (request,response).
    return fetched_data | beam.Map(self._join_fn)
