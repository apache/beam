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

"""Module for transforms that reifies and unreifies PCollection values with
window info.

For internal use only; no backwards-compatibility guarantees.
"""

# pytype: skip-file

from typing import Optional

import apache_beam as beam
from apache_beam.runners.interactive import cache_manager as cache
from apache_beam.testing import test_stream
from apache_beam.transforms.window import WindowedValue

READ_CACHE = 'ReadCache_'
WRITE_CACHE = 'WriteCache_'


class Reify(beam.DoFn):
  """Reifies elements with window info into windowed values.

  Internally used to capture window info with each element into cache for
  replayability.
  """
  def process(
      self,
      e,
      w=beam.DoFn.WindowParam,
      p=beam.DoFn.PaneInfoParam,
      t=beam.DoFn.TimestampParam):
    yield test_stream.WindowedValueHolder(WindowedValue(e, t, [w], p))


class Unreify(beam.DoFn):
  """Unreifies elements from windowed values.

  Cached values are elements with window info. This unpacks the elements.
  """
  def process(self, e):
    # Row coder was used when encoding windowed values.
    if isinstance(e, beam.Row) and hasattr(e, 'windowed_value'):
      yield e.windowed_value


def reify_to_cache(
    pcoll: beam.pvalue.PCollection,
    cache_key: str,
    cache_manager: cache.CacheManager,
    reify_label: Optional[str] = None,
    write_cache_label: Optional[str] = None,
    is_capture: bool = False) -> beam.pvalue.PValue:
  """Reifies elements into windowed values and write to cache.

  Args:
    pcoll: The PCollection to be cached.
    cache_key: The key of the cache.
    cache_manager: The cache manager to manage the cache.
    reify_label: (optional) A transform label for the Reify transform.
    write_cache_label: (optional) A transform label for the cache-writing
      transform.
    is_capture: Whether the cache is capturing a record of recordable sources.
  """
  if not reify_label:
    reify_label = '{}{}{}'.format('ReifyBefore_', WRITE_CACHE, cache_key)
  if not write_cache_label:
    write_cache_label = '{}{}'.format(WRITE_CACHE, cache_key)
  return (
      pcoll | reify_label >> beam.ParDo(Reify())
      | write_cache_label >> cache.WriteCache(
          cache_manager, cache_key, is_capture=is_capture))


def unreify_from_cache(
    pipeline: beam.Pipeline,
    cache_key: str,
    cache_manager: cache.CacheManager,
    element_type: Optional[type] = None,
    source_label: Optional[str] = None,
    unreify_label: Optional[str] = None) -> beam.pvalue.PCollection:
  """Reads from cache and unreifies elements from windowed values.

  pipeline: The pipeline that's reading from the cache.
  cache_key: The key of the cache.
  cache_manager: The cache manager to manage the cache.
  element_type: (optional) The element type of the PCollection's elements.
  source_label: (optional) A transform label for the cache-reading transform.
  unreify_label: (optional) A transform label for the Unreify transform.
  """
  if not source_label:
    source_label = '{}{}'.format(READ_CACHE, cache_key)
  if not unreify_label:
    unreify_label = '{}{}{}'.format('UnreifyAfter_', READ_CACHE, cache_key)
  read_cache = pipeline | source_label >> cache.ReadCache(
      cache_manager, cache_key)
  if element_type:
    # If the PCollection is schema-aware, explicitly sets the output types.
    return read_cache | unreify_label >> beam.ParDo(
        Unreify()).with_output_types(element_type)
  return read_cache | unreify_label >> beam.ParDo(Unreify())
