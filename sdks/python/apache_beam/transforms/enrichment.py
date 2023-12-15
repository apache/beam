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

from typing import TypeVar, Callable, Optional

import apache_beam as beam
from apache_beam.io.requestresponseio import Caller
from apache_beam.io.requestresponseio import CacheReader
from apache_beam.io.requestresponseio import CacheWriter
from apache_beam.io.requestresponseio import DEFAULT_TIMEOUT_SECS
from apache_beam.io.requestresponseio import PreCallThrottler
from apache_beam.io.requestresponseio import Repeater
from apache_beam.io.requestresponseio import RequestResponseIO
from apache_beam.io.requestresponseio import ShouldBackOff

InputT = TypeVar('InputT')
OutputT = TypeVar('OutputT')


def cross_join(element):
  right_dict = element[1].as_dict()
  left_dict = element[0].as_dict()
  for k, v in right_dict.items():
    left_dict[k] = v
  return beam.Row(**left_dict)


class EnrichmentSourceHandler(Caller):
  pass


class HTTPSourceHandler(EnrichmentSourceHandler):
  def __init__(self, url):
    self._url = url

  def __call__(self, *args, **kwargs):
    pass


class Enrichment(beam.PTransform[beam.PCollection[InputT],
                                 beam.PCollection[OutputT]]):
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

  def expand(self, input_row: InputT) -> OutputT:
    fetched_data = input_row | RequestResponseIO(
        caller=self._source_handler,
        timeout=self._timeout,
        should_backoff=self._should_backoff,
        repeater=self._repeater,
        cache_reader=self._cache_reader,
        cache_writer=self._cache_writer,
        throttler=self._throttler).with_output_types(dict)

    # RequestResponseIO returns a tuple of (request,response)
    return fetched_data | beam.ParDo(self._join_fn)
