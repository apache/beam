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

# pytype: skip-file

import apache_beam as beam
from apache_beam.utils.windowed_value import WindowedValue

from apache_beam.transforms import core


class LiftedCombinePerKey(beam.PTransform):
  """An implementation of CombinePerKey that does mapper-side pre-combining.
  """
  def __init__(self, combine_fn, args, kwargs):
    side_inputs = _pack_side_inputs(args, kwargs)
    self._side_inputs: dict = side_inputs
    if not isinstance(combine_fn, core.CombineFn):
      combine_fn = core.CombineFn.from_callable(combine_fn)
    self._combine_fn = combine_fn

  def expand(self, pcoll):
    return (
        pcoll
        | beam.ParDo(
            PartialGroupByKeyCombiningValues(self._combine_fn),
            **self._side_inputs)
        | beam.GroupByKey()
        | beam.ParDo(FinishCombine(self._combine_fn), **self._side_inputs))


def _pack_side_inputs(side_input_args, side_input_kwargs):
  if len(side_input_args) >= 10:
    # If we have more than 10 side inputs, we can't use the
    # _side_input_arg_{i} as our keys since they won't sort
    # correctly. Just punt for now, more than 10 args probably
    # doesn't happen often.
    raise NotImplementedError
  side_inputs = {}
  for i, si in enumerate(side_input_args):
    side_inputs[f'_side_input_arg_{i}'] = si
  for k, v in side_input_kwargs.items():
    side_inputs[k] = v
  return side_inputs


def _unpack_side_inputs(side_inputs):
  side_input_args = []
  side_input_kwargs = {}
  for k, v in sorted(side_inputs.items(), key=lambda x: x[0]):
    if k.startswith('_side_input_arg_'):
      side_input_args.append(v)
    else:
      side_input_kwargs[k] = v
  return side_input_args, side_input_kwargs


class PartialGroupByKeyCombiningValues(beam.DoFn):
  """Aggregates values into a per-key-window cache.

  As bundles are in-memory-sized, we don't bother flushing until the very end.
  """
  def __init__(self, combine_fn):
    self._combine_fn = combine_fn
    self.side_input_args = []
    self.side_input_kwargs = {}

  def setup(self):
    self._combine_fn.setup()

  def start_bundle(self):
    self._cache = dict()
    self._cached_windowed_side_inputs = {}

  def process(self, element, window=beam.DoFn.WindowParam, **side_inputs):
    k, vi = element
    side_input_args, side_input_kwargs = _unpack_side_inputs(side_inputs)
    if (k, window) not in self._cache:
      self._cache[(k, window)] = self._combine_fn.create_accumulator(
          *side_input_args, **side_input_kwargs)

    self._cache[k, window] = self._combine_fn.add_input(
        self._cache[k, window], vi, *side_input_args, **side_input_kwargs)
    self._cached_windowed_side_inputs[window] = (
        side_input_args, side_input_kwargs)

  def finish_bundle(self):
    for (k, w), va in self._cache.items():
      # We compact the accumulator since a GBK (which necessitates encoding)
      # will follow.
      side_input_args, side_input_kwargs = (
          self._cached_windowed_side_inputs[w])
      yield WindowedValue((
          k,
          self._combine_fn.compact(va, *side_input_args, **side_input_kwargs)),
                          w.end, (w, ))

  def teardown(self):
    self._combine_fn.teardown()


class FinishCombine(beam.DoFn):
  """Merges partially combined results.
  """
  def __init__(self, combine_fn):
    self._combine_fn = combine_fn

  def setup(self):
    self._combine_fn.setup()

  def process(self, element, window=beam.DoFn.WindowParam, **side_inputs):

    k, vs = element
    side_input_args, side_input_kwargs = _unpack_side_inputs(side_inputs)
    return [(
        k,
        self._combine_fn.extract_output(
            self._combine_fn.merge_accumulators(
                vs, *side_input_args, **side_input_kwargs),
            *side_input_args,
            **side_input_kwargs))]

  def teardown(self):
    try:
      self._combine_fn.teardown()
    except AttributeError:
      pass
