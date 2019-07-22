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

from __future__ import absolute_import

import collections
import itertools

import apache_beam as beam
from apache_beam import typehints
from apache_beam.internal.util import ArgumentPlaceholder
from apache_beam.transforms.combiners import _CurriedFn
from apache_beam.utils.windowed_value import WindowedValue


class LiftedCombinePerKey(beam.PTransform):
  """An implementation of CombinePerKey that does mapper-side pre-combining.
  """
  def __init__(self, combine_fn, args, kwargs):
    args_to_check = itertools.chain(args, kwargs.values())
    if isinstance(combine_fn, _CurriedFn):
      args_to_check = itertools.chain(args_to_check,
                                      combine_fn.args,
                                      combine_fn.kwargs.values())
    if any(isinstance(arg, ArgumentPlaceholder)
           for arg in args_to_check):
      # This isn't implemented in dataflow either...
      raise NotImplementedError('Deferred CombineFn side inputs.')
    self._combine_fn = beam.transforms.combiners.curry_combine_fn(
        combine_fn, args, kwargs)

  def expand(self, pcoll):
    return (
        pcoll
        | beam.ParDo(PartialGroupByKeyCombiningValues(self._combine_fn))
        | beam.GroupByKey()
        | beam.ParDo(FinishCombine(self._combine_fn)))


class PartialGroupByKeyCombiningValues(beam.DoFn):
  """Aggregates values into a per-key-window cache.

  As bundles are in-memory-sized, we don't bother flushing until the very end.
  """
  def __init__(self, combine_fn):
    self._combine_fn = combine_fn

  def start_bundle(self):
    self._cache = collections.defaultdict(self._combine_fn.create_accumulator)

  def process(self, element, window=beam.DoFn.WindowParam):
    k, vi = element
    self._cache[k, window] = self._combine_fn.add_input(self._cache[k, window],
                                                        vi)

  def finish_bundle(self):
    for (k, w), va in self._cache.items():
      # We compact the accumulator since a GBK (which necessitates encoding)
      # will follow.
      yield WindowedValue((k, self._combine_fn.compact(va)), w.end, (w,))

  def default_type_hints(self):
    hints = self._combine_fn.get_type_hints().copy()
    K = typehints.TypeVariable('K')
    if hints.input_types:
      args, kwargs = hints.input_types
      args = (typehints.Tuple[K, args[0]],) + args[1:]
      hints.set_input_types(*args, **kwargs)
    else:
      hints.set_input_types(typehints.Tuple[K, typehints.Any])
    hints.set_output_types(typehints.Tuple[K, typehints.Any])
    return hints


class FinishCombine(beam.DoFn):
  """Merges partially combined results.
  """
  def __init__(self, combine_fn):
    self._combine_fn = combine_fn

  def process(self, element):
    k, vs = element
    return [(
        k,
        self._combine_fn.extract_output(
            self._combine_fn.merge_accumulators(vs)))]

  def default_type_hints(self):
    hints = self._combine_fn.get_type_hints().copy()
    K = typehints.TypeVariable('K')
    hints.set_input_types(typehints.Tuple[K, typehints.Any])
    if hints.output_types:
      main_output_type = hints.simple_output_type('')
      hints.set_output_types(typehints.Tuple[K, main_output_type])
    return hints
