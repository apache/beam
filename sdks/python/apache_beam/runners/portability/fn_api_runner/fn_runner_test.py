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

import collections
import gc
import logging
import os
import random
import re
import shutil
import tempfile
import threading
import time
import traceback
import typing
import unittest
import uuid
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Tuple
from typing import no_type_check

import hamcrest  # pylint: disable=ungrouped-imports
import numpy as np
import pytest
from hamcrest.core.matcher import Matcher
from hamcrest.core.string_description import StringDescription
from tenacity import retry
from tenacity import stop_after_attempt

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.io import restriction_trackers
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.metricbase import MetricName
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import DirectOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.portability import python_urns
from apache_beam.runners.portability import fn_api_runner
from apache_beam.runners.portability.fn_api_runner import fn_runner
from apache_beam.runners.sdf_utils import RestrictionTrackerView
from apache_beam.runners.worker import data_plane
from apache_beam.runners.worker import statesampler
from apache_beam.runners.worker.operations import InefficientExecutionWarning
from apache_beam.testing.synthetic_pipeline import SyntheticSDFAsSource
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.tools import utils
from apache_beam.transforms import environments
from apache_beam.transforms import userstate
from apache_beam.transforms import window
from apache_beam.utils import timestamp
from apache_beam.utils import windowed_value

if statesampler.FAST_SAMPLER:
  DEFAULT_SAMPLING_PERIOD_MS = statesampler.DEFAULT_SAMPLING_PERIOD_MS
else:
  DEFAULT_SAMPLING_PERIOD_MS = 0

_LOGGER = logging.getLogger(__name__)


def _matcher_or_equal_to(value_or_matcher):
  """Pass-thru for matchers, and wraps value inputs in an equal_to matcher."""
  if value_or_matcher is None:
    return None
  if isinstance(value_or_matcher, Matcher):
    return value_or_matcher
  return hamcrest.equal_to(value_or_matcher)


def has_urn_and_labels(mi, urn, labels):
  """Returns true if it the monitoring_info contains the labels and urn."""
  def contains_labels(mi, labels):
    # Check all the labels and their values exist in the monitoring_info
    return all(item in mi.labels.items() for item in labels.items())

  return contains_labels(mi, labels) and mi.urn == urn


class FnApiRunnerTest(unittest.TestCase):
  def create_pipeline(self, is_drain=False):
    return beam.Pipeline(runner=fn_api_runner.FnApiRunner(is_drain=is_drain))

  def test_assert_that(self):
    # TODO: figure out a way for fn_api_runner to parse and raise the
    # underlying exception.
    with self.assertRaisesRegex(Exception, 'Failed assert'):
      with self.create_pipeline() as p:
        assert_that(p | beam.Create(['a', 'b']), equal_to(['a']))

  def test_create(self):
    with self.create_pipeline() as p:
      assert_that(p | beam.Create(['a', 'b']), equal_to(['a', 'b']))

  def test_pardo(self):
    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create(['a', 'bc'])
          | beam.Map(lambda e: e * 2)
          | beam.Map(lambda e: e + 'x'))
      assert_that(res, equal_to(['aax', 'bcbcx']))

  def test_batch_pardo(self):
    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create(np.array([1, 2, 3], dtype=np.int64)).with_output_types(
              np.int64)
          | beam.ParDo(ArrayMultiplyDoFn())
          | beam.Map(lambda x: x * 3))

      assert_that(res, equal_to([6, 12, 18]))

  def test_batch_pardo_override_type_inference(self):
    class ArrayMultiplyDoFnOverride(beam.DoFn):
      def process_batch(self, batch, *unused_args,
                        **unused_kwargs) -> Iterator[np.ndarray]:
        assert isinstance(batch, np.ndarray)
        yield batch * 2

      # infer_output_type must be defined (when there's no process method),
      # otherwise we don't know the input type is the same as output type.
      def infer_output_type(self, input_type):
        return input_type

      def get_input_batch_type(self, input_element_type):
        from apache_beam.typehints.batch import NumpyArray
        return NumpyArray[input_element_type]

      def get_output_batch_type(self, input_element_type):
        return self.get_input_batch_type(input_element_type)

    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create(np.array([1, 2, 3], dtype=np.int64)).with_output_types(
              np.int64)
          | beam.ParDo(ArrayMultiplyDoFnOverride())
          | beam.Map(lambda x: x * 3))

      assert_that(res, equal_to([6, 12, 18]))

  @unittest.skip('https://github.com/apache/beam/issues/23944')
  def test_batch_pardo_trigger_flush(self):
    try:
      utils.check_compiled('apache_beam.coders.coder_impl')
    except RuntimeError:
      self.skipTest(
          'https://github.com/apache/beam/issues/21643: FnRunnerTest with '
          'non-trivial inputs flakes in non-cython environments')

    with self.create_pipeline() as p:
      res = (
          p
          # Pass more than GeneralPurposeConsumerSet.MAX_BATCH_SIZE elements
          # here to make sure we exercise the batch size limit.
          | beam.Create(np.array(range(5000),
                                 dtype=np.int64)).with_output_types(np.int64)
          | beam.ParDo(ArrayMultiplyDoFn())
          | beam.Map(lambda x: x * 3))

      assert_that(res, equal_to([i * 2 * 3 for i in range(5000)]))

  def test_batch_rebatch_pardos(self):
    # Should raise a warning about the rebatching that mentions:
    # - The consuming DoFn
    # - The output batch type of the producer
    # - The input batch type of the consumer
    with self.assertWarnsRegex(InefficientExecutionWarning,
                               (r'ListPlusOneDoFn.*NumpyArray.*List\[<class '
                                r'\'numpy.int64\'>\]')):
      with self.create_pipeline() as p:
        res = (
            p
            | beam.Create(np.array([1, 2, 3],
                                   dtype=np.int64)).with_output_types(np.int64)
            | beam.ParDo(ArrayMultiplyDoFn())
            | beam.ParDo(ListPlusOneDoFn())
            | beam.Map(lambda x: x * 3))

        assert_that(res, equal_to([9, 15, 21]))

  def test_batch_pardo_fusion_break(self):
    class NormalizeDoFn(beam.DoFn):
      @no_type_check
      def process_batch(
          self,
          batch: np.ndarray,
          mean: np.float64,
      ) -> Iterator[np.ndarray]:
        assert isinstance(batch, np.ndarray)
        yield batch - mean

      # infer_output_type must be defined (when there's no process method),
      # otherwise we don't know the input type is the same as output type.
      def infer_output_type(self, input_type):
        return np.float64

    with self.create_pipeline() as p:
      pc = (
          p
          | beam.Create(np.array([1, 2, 3], dtype=np.int64)).with_output_types(
              np.int64)
          | beam.ParDo(ArrayMultiplyDoFn()))

      res = (
          pc
          | beam.ParDo(
              NormalizeDoFn(),
              mean=beam.pvalue.AsSingleton(
                  pc | beam.CombineGlobally(beam.combiners.MeanCombineFn()))))
      assert_that(res, equal_to([-2, 0, 2]))

  def test_batch_pardo_dofn_params(self):
    class ConsumeParamsDoFn(beam.DoFn):
      @no_type_check
      def process_batch(
          self,
          batch: np.ndarray,
          ts=beam.DoFn.TimestampParam,
          pane_info=beam.DoFn.PaneInfoParam,
      ) -> Iterator[np.ndarray]:
        assert isinstance(batch, np.ndarray)
        assert isinstance(ts, timestamp.Timestamp)
        assert isinstance(pane_info, windowed_value.PaneInfo)

        yield batch * ts.seconds()

      # infer_output_type must be defined (when there's no process method),
      # otherwise we don't know the input type is the same as output type.
      def infer_output_type(self, input_type):
        return input_type

    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create(np.array(range(10), dtype=np.int64)).with_output_types(
              np.int64)
          | beam.Map(lambda t: window.TimestampedValue(t, int(t % 2))).
          with_output_types(np.int64)
          | beam.ParDo(ConsumeParamsDoFn()))

      assert_that(res, equal_to([0, 1, 0, 3, 0, 5, 0, 7, 0, 9]))

  def test_batch_pardo_window_param(self):
    class PerWindowDoFn(beam.DoFn):
      @no_type_check
      def process_batch(
          self,
          batch: np.ndarray,
          window=beam.DoFn.WindowParam,
      ) -> Iterator[np.ndarray]:
        yield batch * window.start.seconds()

      # infer_output_type must be defined (when there's no process method),
      # otherwise we don't know the input type is the same as output type.
      def infer_output_type(self, input_type):
        return input_type

    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create(np.array(range(10), dtype=np.int64)).with_output_types(
              np.int64)
          | beam.Map(lambda t: window.TimestampedValue(t, int(t))).
          with_output_types(np.int64)
          | beam.WindowInto(window.FixedWindows(5))
          | beam.ParDo(PerWindowDoFn()))

      assert_that(res, equal_to([0, 0, 0, 0, 0, 25, 30, 35, 40, 45]))

  def test_batch_pardo_overlapping_windows(self):
    class PerWindowDoFn(beam.DoFn):
      @no_type_check
      def process_batch(self,
                        batch: np.ndarray,
                        window=beam.DoFn.WindowParam) -> Iterator[np.ndarray]:
        yield batch * window.start.seconds()

      # infer_output_type must be defined (when there's no process method),
      # otherwise we don't know the input type is the same as output type.
      def infer_output_type(self, input_type):
        return input_type

    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create(np.array(range(10), dtype=np.int64)).with_output_types(
              np.int64)
          | beam.Map(lambda t: window.TimestampedValue(t, int(t))).
          with_output_types(np.int64)
          | beam.WindowInto(window.SlidingWindows(size=5, period=3))
          | beam.ParDo(PerWindowDoFn()))

      assert_that(res, equal_to([               0*-3, 1*-3, # [-3, 2)
                                 0*0, 1*0, 2*0, 3* 0, 4* 0, # [ 0, 5)
                                 3*3, 4*3, 5*3, 6* 3, 7* 3, # [ 3, 8)
                                 6*6, 7*6, 8*6, 9* 6,       # [ 6, 11)
                                 9*9                        # [ 9, 14)
                                 ]))

  def test_batch_to_element_pardo(self):
    class ArraySumDoFn(beam.DoFn):
      @beam.DoFn.yields_elements
      def process_batch(self, batch: np.ndarray, *unused_args,
                        **unused_kwargs) -> Iterator[np.int64]:
        yield batch.sum()

      def infer_output_type(self, input_type):
        assert input_type == np.int64
        return np.int64

    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create(np.array(range(100), dtype=np.int64)).with_output_types(
              np.int64)
          | beam.ParDo(ArrayMultiplyDoFn())
          | beam.ParDo(ArraySumDoFn())
          | beam.CombineGlobally(sum))

      assert_that(res, equal_to([99 * 50 * 2]))

  def test_element_to_batch_pardo(self):
    class ArrayProduceDoFn(beam.DoFn):
      @beam.DoFn.yields_batches
      def process(self, element: np.int64, *unused_args,
                  **unused_kwargs) -> Iterator[np.ndarray]:
        yield np.array([element] * int(element))

      # infer_output_type must be defined (when there's no process method),
      # otherwise we don't know the input type is the same as output type.
      def infer_output_type(self, input_type):
        return np.int64

    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create(np.array([1, 2, 3], dtype=np.int64)).with_output_types(
              np.int64)
          | beam.ParDo(ArrayProduceDoFn())
          | beam.ParDo(ArrayMultiplyDoFn())
          | beam.Map(lambda x: x * 3))

      assert_that(res, equal_to([6, 12, 12, 18, 18, 18]))

  @unittest.skip('https://github.com/apache/beam/issues/23944')
  def test_pardo_large_input(self):
    try:
      utils.check_compiled('apache_beam.coders.coder_impl')
    except RuntimeError:
      self.skipTest(
          'https://github.com/apache/beam/issues/21643: FnRunnerTest with '
          'non-trivial inputs flakes in non-cython environments')
    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create(np.array(range(5000),
                                 dtype=np.int64)).with_output_types(np.int64)
          | beam.Map(lambda e: e * 2)
          | beam.Map(lambda e: e + 3))
      assert_that(res, equal_to([(i * 2) + 3 for i in range(5000)]))

  def test_pardo_side_outputs(self):
    def tee(elem, *tags):
      for tag in tags:
        if tag in elem:
          yield beam.pvalue.TaggedOutput(tag, elem)

    with self.create_pipeline() as p:
      xy = (
          p
          | 'Create' >> beam.Create(['x', 'y', 'xy'])
          | beam.FlatMap(tee, 'x', 'y').with_outputs())
      assert_that(xy.x, equal_to(['x', 'xy']), label='x')
      assert_that(xy.y, equal_to(['y', 'xy']), label='y')

  def test_pardo_side_and_main_outputs(self):
    def even_odd(elem):
      yield elem
      yield beam.pvalue.TaggedOutput('odd' if elem % 2 else 'even', elem)

    with self.create_pipeline() as p:
      ints = p | beam.Create([1, 2, 3])
      named = ints | 'named' >> beam.FlatMap(even_odd).with_outputs(
          'even', 'odd', main='all')
      assert_that(named.all, equal_to([1, 2, 3]), label='named.all')
      assert_that(named.even, equal_to([2]), label='named.even')
      assert_that(named.odd, equal_to([1, 3]), label='named.odd')

      unnamed = ints | 'unnamed' >> beam.FlatMap(even_odd).with_outputs()
      unnamed[None] | beam.Map(id)  # pylint: disable=expression-not-assigned
      assert_that(unnamed[None], equal_to([1, 2, 3]), label='unnamed.all')
      assert_that(unnamed.even, equal_to([2]), label='unnamed.even')
      assert_that(unnamed.odd, equal_to([1, 3]), label='unnamed.odd')

  def test_pardo_side_inputs(self):
    def cross_product(elem, sides):
      for side in sides:
        yield elem, side

    with self.create_pipeline() as p:
      main = p | 'main' >> beam.Create(['a', 'b', 'c'])
      side = p | 'side' >> beam.Create(['x', 'y'])
      assert_that(
          main | beam.FlatMap(cross_product, beam.pvalue.AsList(side)),
          equal_to([('a', 'x'), ('b', 'x'), ('c', 'x'), ('a', 'y'), ('b', 'y'),
                    ('c', 'y')]))

  def test_pardo_side_input_dependencies(self):
    ##
    # The issue that this test surfaces is that whenever a PCollection is
    # consumed as main input by several stages, we have a bug.
    #
    # The bug is: A stage assumes that it has run if its upstream PCollection
    # has watermark=MAX. If multiple stages depend on a single PCollection, then
    # the first stage that runs it will set its watermar to MAX, and other
    # stages will think they've run.
    #
    # How to resolve?
    # Option1: to make sure that a PCollection's watermark only advances with
    #    its consumption by all consumers?
    #          - I tested this and it didn't seem fruitful/obvious. (YET)
    #
    # Option2: Change execution schema: A PCollection's watermark represents
    #    its *production* watermark, not its *consumption* watermark.(?)
    with self.create_pipeline() as p:
      inputs = [p | beam.Create([None])]
      for k in range(1, 10):
        inputs.append(
            inputs[0] | beam.ParDo(
                ExpectingSideInputsFn(f'Do{k}'),
                *[beam.pvalue.AsList(inputs[s]) for s in range(1, k)]))

  def test_flatmap_numpy_array(self):
    with self.create_pipeline() as p:
      pc = (
          p
          | beam.Create([np.array(range(10))])
          | beam.FlatMap(lambda arr: arr))

      assert_that(pc, equal_to([np.int64(i) for i in range(10)]))

  @unittest.skip('https://github.com/apache/beam/issues/21228')
  def test_pardo_side_input_sparse_dependencies(self):
    with self.create_pipeline() as p:
      inputs = []

      def choose_input(s):
        return inputs[(389 + s * 5077) % len(inputs)]

      for k in range(20):
        num_inputs = int((k * k % 16)**0.5)
        if num_inputs == 0:
          inputs.append(p | f'Create{k}' >> beam.Create([f'Create{k}']))
        else:
          inputs.append(
              choose_input(0) | beam.ParDo(
                  ExpectingSideInputsFn(f'Do{k}'),
                  *[
                      beam.pvalue.AsList(choose_input(s))
                      for s in range(1, num_inputs)
                  ]))

  def test_pardo_windowed_side_inputs(self):
    with self.create_pipeline() as p:
      # Now with some windowing.
      pcoll = p | beam.Create(list(
          range(10))) | beam.Map(lambda t: window.TimestampedValue(t, t))
      # Intentionally choosing non-aligned windows to highlight the transition.
      main = pcoll | 'WindowMain' >> beam.WindowInto(window.FixedWindows(5))
      side = pcoll | 'WindowSide' >> beam.WindowInto(window.FixedWindows(7))
      res = main | beam.Map(
          lambda x, s: (x, sorted(s)), beam.pvalue.AsList(side))
      assert_that(
          res,
          equal_to([
              # The window [0, 5) maps to the window [0, 7).
              (0, list(range(7))),
              (1, list(range(7))),
              (2, list(range(7))),
              (3, list(range(7))),
              (4, list(range(7))),
              # The window [5, 10) maps to the window [7, 14).
              (5, list(range(7, 10))),
              (6, list(range(7, 10))),
              (7, list(range(7, 10))),
              (8, list(range(7, 10))),
              (9, list(range(7, 10)))
          ]),
          label='windowed')

  def test_flattened_side_input(self, with_transcoding=True):
    with self.create_pipeline() as p:
      main = p | 'main' >> beam.Create([None])
      side1 = p | 'side1' >> beam.Create([('a', 1)])
      side2 = p | 'side2' >> beam.Create([('b', 2)])
      if with_transcoding:
        # Also test non-matching coder types (transcoding required)
        third_element = [('another_type')]
      else:
        third_element = [('b', 3)]
      side3 = p | 'side3' >> beam.Create(third_element)
      side = (side1, side2) | beam.Flatten()
      assert_that(
          main | beam.Map(lambda a, b: (a, b), beam.pvalue.AsDict(side)),
          equal_to([(None, {
              'a': 1, 'b': 2
          })]),
          label='CheckFlattenAsSideInput')
      assert_that((side, side3) | 'FlattenAfter' >> beam.Flatten(),
                  equal_to([('a', 1), ('b', 2)] + third_element),
                  label='CheckFlattenOfSideInput')

  def test_gbk_side_input(self):
    with self.create_pipeline() as p:
      main = p | 'main' >> beam.Create([None])
      side = p | 'side' >> beam.Create([('a', 1)]) | beam.GroupByKey()
      assert_that(
          main | beam.Map(lambda a, b: (a, b), beam.pvalue.AsDict(side)),
          equal_to([(None, {
              'a': [1]
          })]))

  def test_multimap_side_input(self):
    with self.create_pipeline() as p:
      main = p | 'main' >> beam.Create(['a', 'b'])
      side = p | 'side' >> beam.Create([('a', 1), ('b', 2), ('a', 3)])
      assert_that(
          main | beam.Map(
              lambda k, d: (k, sorted(d[k])), beam.pvalue.AsMultiMap(side)),
          equal_to([('a', [1, 3]), ('b', [2])]))

  def test_multimap_multiside_input(self):
    # A test where two transforms in the same stage consume the same PCollection
    # twice as side input.
    with self.create_pipeline() as p:
      main = p | 'main' >> beam.Create(['a', 'b'])
      side = p | 'side' >> beam.Create([('a', 1), ('b', 2), ('a', 3)])
      assert_that(
          main | 'first map' >> beam.Map(
              lambda k,
              d,
              l: (k, sorted(d[k]), sorted([e[1] for e in l])),
              beam.pvalue.AsMultiMap(side),
              beam.pvalue.AsList(side))
          | 'second map' >> beam.Map(
              lambda k,
              d,
              l: (k[0], sorted(d[k[0]]), sorted([e[1] for e in l])),
              beam.pvalue.AsMultiMap(side),
              beam.pvalue.AsList(side)),
          equal_to([('a', [1, 3], [1, 2, 3]), ('b', [2], [1, 2, 3])]))

  def test_multimap_side_input_type_coercion(self):
    with self.create_pipeline() as p:
      main = p | 'main' >> beam.Create(['a', 'b'])
      # The type of this side-input is forced to Any (overriding type
      # inference). Without type coercion to Tuple[Any, Any], the usage of this
      # side-input in AsMultiMap() below should fail.
      side = (
          p | 'side' >> beam.Create([('a', 1), ('b', 2),
                                     ('a', 3)]).with_output_types(typing.Any))
      assert_that(
          main | beam.Map(
              lambda k, d: (k, sorted(d[k])), beam.pvalue.AsMultiMap(side)),
          equal_to([('a', [1, 3]), ('b', [2])]))

  def test_pardo_unfusable_side_inputs(self):
    def cross_product(elem, sides):
      for side in sides:
        yield elem, side

    with self.create_pipeline() as p:
      pcoll = p | beam.Create(['a', 'b'])
      assert_that(
          pcoll | beam.FlatMap(cross_product, beam.pvalue.AsList(pcoll)),
          equal_to([('a', 'a'), ('a', 'b'), ('b', 'a'), ('b', 'b')]))

  def test_pardo_unfusable_side_inputs_with_separation(self):
    def cross_product(elem, sides):
      for side in sides:
        yield elem, side

    with self.create_pipeline() as p:
      pcoll = p | beam.Create(['a', 'b'])
      derived = ((pcoll, ) | beam.Flatten()
                 | beam.Map(lambda x: (x, x))
                 | beam.GroupByKey()
                 | 'Unkey' >> beam.Map(lambda kv: kv[0]))
      assert_that(
          pcoll | beam.FlatMap(cross_product, beam.pvalue.AsList(derived)),
          equal_to([('a', 'a'), ('a', 'b'), ('b', 'a'), ('b', 'b')]))

  def test_pardo_state_only(self):
    index_state_spec = userstate.CombiningValueStateSpec('index', sum)
    value_and_index_state_spec = userstate.ReadModifyWriteStateSpec(
        'value:index', StrUtf8Coder())

    # TODO(ccy): State isn't detected with Map/FlatMap.
    class AddIndex(beam.DoFn):
      def process(
          self,
          kv,
          index=beam.DoFn.StateParam(index_state_spec),
          value_and_index=beam.DoFn.StateParam(value_and_index_state_spec)):
        k, v = kv
        index.add(1)
        value_and_index.write('%s:%s' % (v, index.read()))
        yield k, v, index.read(), value_and_index.read()

    inputs = [('A', 'a')] * 2 + [('B', 'b')] * 3
    expected = [('A', 'a', 1, 'a:1'), ('A', 'a', 2, 'a:2'),
                ('B', 'b', 1, 'b:1'), ('B', 'b', 2, 'b:2'),
                ('B', 'b', 3, 'b:3')]

    with self.create_pipeline() as p:
      assert_that(
          p | beam.Create(inputs) | beam.ParDo(AddIndex()), equal_to(expected))

  @unittest.skip('TestStream not yet supported')
  def test_teststream_pardo_timers(self):
    timer_spec = userstate.TimerSpec('timer', userstate.TimeDomain.WATERMARK)

    class TimerDoFn(beam.DoFn):
      def process(self, element, timer=beam.DoFn.TimerParam(timer_spec)):
        unused_key, ts = element
        timer.set(ts)
        timer.set(2 * ts)

      @userstate.on_timer(timer_spec)
      def process_timer(self):
        yield 'fired'

    ts = (
        TestStream().add_elements([('k1', 10)])  # Set timer for 20
        .advance_watermark_to(100).add_elements([('k2', 100)
                                                 ])  # Set timer for 200
        .advance_watermark_to(1000))

    with self.create_pipeline() as p:
      _ = (
          p
          | ts
          | beam.ParDo(TimerDoFn())
          | beam.Map(lambda x, ts=beam.DoFn.TimestampParam: (x, ts)))

      #expected = [('fired', ts) for ts in (20, 200)]
      #assert_that(actual, equal_to(expected))

  def test_pardo_timers(self):
    timer_spec = userstate.TimerSpec('timer', userstate.TimeDomain.WATERMARK)
    state_spec = userstate.CombiningValueStateSpec('num_called', sum)

    class TimerDoFn(beam.DoFn):
      def process(self, element, timer=beam.DoFn.TimerParam(timer_spec)):
        unused_key, ts = element
        timer.set(ts)
        timer.set(2 * ts)

      @userstate.on_timer(timer_spec)
      def process_timer(
          self,
          ts=beam.DoFn.TimestampParam,
          timer=beam.DoFn.TimerParam(timer_spec),
          state=beam.DoFn.StateParam(state_spec)):
        if state.read() == 0:
          state.add(1)
          timer.set(timestamp.Timestamp(micros=2 * ts.micros))
        yield 'fired'

    with self.create_pipeline() as p:
      actual = (
          p
          | beam.Create([('k1', 10), ('k2', 100)])
          | beam.ParDo(TimerDoFn())
          | beam.Map(lambda x, ts=beam.DoFn.TimestampParam: (x, ts)))

      expected = [('fired', ts) for ts in (20, 200, 40, 400)]
      assert_that(actual, equal_to(expected))

  def test_pardo_timers_clear(self):
    timer_spec = userstate.TimerSpec('timer', userstate.TimeDomain.WATERMARK)
    clear_timer_spec = userstate.TimerSpec(
        'clear_timer', userstate.TimeDomain.WATERMARK)

    class TimerDoFn(beam.DoFn):
      def process(
          self,
          element,
          timer=beam.DoFn.TimerParam(timer_spec),
          clear_timer=beam.DoFn.TimerParam(clear_timer_spec)):
        unused_key, ts = element
        timer.set(ts)
        timer.set(2 * ts)
        clear_timer.set(ts)
        clear_timer.clear()

      @userstate.on_timer(timer_spec)
      def process_timer(self):
        yield 'fired'

      @userstate.on_timer(clear_timer_spec)
      def process_clear_timer(self):
        yield 'should not fire'

    with self.create_pipeline() as p:
      actual = (
          p
          | beam.Create([('k1', 10), ('k2', 100)])
          | beam.ParDo(TimerDoFn())
          | beam.Map(lambda x, ts=beam.DoFn.TimestampParam: (x, ts)))

      expected = [('fired', ts) for ts in (20, 200)]
      assert_that(actual, equal_to(expected))

  def test_pardo_state_timers(self):
    self._run_pardo_state_timers(windowed=False)

  def test_pardo_state_timers_non_standard_coder(self):
    self._run_pardo_state_timers(windowed=False, key_type=Any)

  def test_windowed_pardo_state_timers(self):
    self._run_pardo_state_timers(windowed=True)

  def _run_pardo_state_timers(self, windowed, key_type=None):
    """
    :param windowed: If True, uses an interval window, otherwise a global window
    :param key_type: Allows to override the inferred key type. This is useful to
    test the use of non-standard coders, e.g. Python's FastPrimitivesCoder.
    """
    state_spec = userstate.BagStateSpec('state', beam.coders.StrUtf8Coder())
    timer_spec = userstate.TimerSpec('timer', userstate.TimeDomain.WATERMARK)
    elements = list('abcdefgh')
    key = 'key'
    buffer_size = 3

    class BufferDoFn(beam.DoFn):
      def process(
          self,
          kv,
          ts=beam.DoFn.TimestampParam,
          timer=beam.DoFn.TimerParam(timer_spec),
          state=beam.DoFn.StateParam(state_spec)):
        _, element = kv
        state.add(element)
        buffer = state.read()
        # For real use, we'd keep track of this size separately.
        if len(list(buffer)) >= 3:
          state.clear()
          yield buffer
        else:
          timer.set(ts + 1)

      @userstate.on_timer(timer_spec)
      def process_timer(self, state=beam.DoFn.StateParam(state_spec)):
        buffer = state.read()
        state.clear()
        yield buffer

    def is_buffered_correctly(actual):
      # Pickling self in the closure for asserts gives errors (only on jenkins).
      self = FnApiRunnerTest('__init__')
      # Acutal should be a grouping of the inputs into batches of size
      # at most buffer_size, but the actual batching is nondeterministic
      # based on ordering and trigger firing timing.
      self.assertEqual(sorted(sum((list(b) for b in actual), [])), elements, actual)
      self.assertEqual(max(len(list(buffer)) for buffer in actual), buffer_size, actual)
      if windowed:
        # Elements were assigned to windows based on their parity.
        # Assert that each grouping consists of elements belonging to the
        # same window to ensure states and timers were properly partitioned.
        for b in actual:
          parity = set(ord(e) % 2 for e in b)
          self.assertEqual(1, len(parity), b)

    with self.create_pipeline() as p:
      actual = (
          p
          | beam.Create(elements)
          # Send even and odd elements to different windows.
          | beam.Map(lambda e: window.TimestampedValue(e, ord(e) % 2))
          | beam.WindowInto(
              window.FixedWindows(1) if windowed else window.GlobalWindows())
          | beam.Map(lambda x: (key, x)).with_output_types(
              Tuple[key_type if key_type else type(key), Any])
          | beam.ParDo(BufferDoFn()))

      assert_that(actual, is_buffered_correctly)

  def test_pardo_dynamic_timer(self):
    class DynamicTimerDoFn(beam.DoFn):
      dynamic_timer_spec = userstate.TimerSpec(
          'dynamic_timer', userstate.TimeDomain.WATERMARK)

      def process(
          self, element,
          dynamic_timer=beam.DoFn.TimerParam(dynamic_timer_spec)):
        dynamic_timer.set(element[1], dynamic_timer_tag=element[0])

      @userstate.on_timer(dynamic_timer_spec)
      def dynamic_timer_callback(
          self,
          tag=beam.DoFn.DynamicTimerTagParam,
          timestamp=beam.DoFn.TimestampParam):
        yield (tag, timestamp)

    with self.create_pipeline() as p:
      actual = (
          p
          | beam.Create([('key1', 10), ('key2', 20), ('key3', 30)])
          | beam.ParDo(DynamicTimerDoFn()))
      assert_that(actual, equal_to([('key1', 10), ('key2', 20), ('key3', 30)]))

  def test_sdf(self):
    class ExpandingStringsDoFn(beam.DoFn):
      def process(
          self,
          element,
          restriction_tracker=beam.DoFn.RestrictionParam(
              ExpandStringsProvider())):
        assert isinstance(restriction_tracker, RestrictionTrackerView)
        cur = restriction_tracker.current_restriction().start
        while restriction_tracker.try_claim(cur):
          yield element[cur]
          cur += 1

    with self.create_pipeline() as p:
      data = ['abc', 'defghijklmno', 'pqrstuv', 'wxyz']
      actual = (p | beam.Create(data) | beam.ParDo(ExpandingStringsDoFn()))
      assert_that(actual, equal_to(list(''.join(data))))

  def test_sdf_with_dofn_as_restriction_provider(self):
    class ExpandingStringsDoFn(beam.DoFn, ExpandStringsProvider):
      def process(
          self, element, restriction_tracker=beam.DoFn.RestrictionParam()):
        assert isinstance(restriction_tracker, RestrictionTrackerView)
        cur = restriction_tracker.current_restriction().start
        while restriction_tracker.try_claim(cur):
          yield element[cur]
          cur += 1

    with self.create_pipeline() as p:
      data = ['abc', 'defghijklmno', 'pqrstuv', 'wxyz']
      actual = (p | beam.Create(data) | beam.ParDo(ExpandingStringsDoFn()))
      assert_that(actual, equal_to(list(''.join(data))))

  def test_sdf_with_check_done_failed(self):
    class ExpandingStringsDoFn(beam.DoFn):
      def process(
          self,
          element,
          restriction_tracker=beam.DoFn.RestrictionParam(
              ExpandStringsProvider())):
        assert isinstance(restriction_tracker, RestrictionTrackerView)
        cur = restriction_tracker.current_restriction().start
        while restriction_tracker.try_claim(cur):
          yield element[cur]
          cur += 1
          return

    with self.assertRaises(Exception):
      with self.create_pipeline() as p:
        data = ['abc', 'defghijklmno', 'pqrstuv', 'wxyz']
        _ = (p | beam.Create(data) | beam.ParDo(ExpandingStringsDoFn()))

  def test_sdf_with_watermark_tracking(self):
    class ExpandingStringsDoFn(beam.DoFn):
      def process(
          self,
          element,
          restriction_tracker=beam.DoFn.RestrictionParam(
              ExpandStringsProvider()),
          watermark_estimator=beam.DoFn.WatermarkEstimatorParam(
              ManualWatermarkEstimator.default_provider())):
        cur = restriction_tracker.current_restriction().start
        while restriction_tracker.try_claim(cur):
          watermark_estimator.set_watermark(timestamp.Timestamp(cur))
          assert (
              watermark_estimator.current_watermark() == timestamp.Timestamp(
                  cur))
          yield element[cur]
          if cur % 2 == 1:
            restriction_tracker.defer_remainder(timestamp.Duration(micros=5))
            return
          cur += 1

    with self.create_pipeline() as p:
      data = ['abc', 'defghijklmno', 'pqrstuv', 'wxyz']
      actual = (p | beam.Create(data) | beam.ParDo(ExpandingStringsDoFn()))
      assert_that(actual, equal_to(list(''.join(data))))

  def test_sdf_with_dofn_as_watermark_estimator(self):
    class ExpandingStringsDoFn(beam.DoFn, beam.WatermarkEstimatorProvider):
      def initial_estimator_state(self, element, restriction):
        return None

      def create_watermark_estimator(self, state):
        return beam.io.watermark_estimators.ManualWatermarkEstimator(state)

      def process(
          self,
          element,
          restriction_tracker=beam.DoFn.RestrictionParam(
              ExpandStringsProvider()),
          watermark_estimator=beam.DoFn.WatermarkEstimatorParam(
              ManualWatermarkEstimator.default_provider())):
        cur = restriction_tracker.current_restriction().start
        while restriction_tracker.try_claim(cur):
          watermark_estimator.set_watermark(timestamp.Timestamp(cur))
          assert (
              watermark_estimator.current_watermark() == timestamp.Timestamp(
                  cur))
          yield element[cur]
          if cur % 2 == 1:
            restriction_tracker.defer_remainder(timestamp.Duration(micros=5))
            return
          cur += 1

    with self.create_pipeline() as p:
      data = ['abc', 'defghijklmno', 'pqrstuv', 'wxyz']
      actual = (p | beam.Create(data) | beam.ParDo(ExpandingStringsDoFn()))
      assert_that(actual, equal_to(list(''.join(data))))

  def run_sdf_initiated_checkpointing(self, is_drain=False):
    counter = beam.metrics.Metrics.counter('ns', 'my_counter')

    class ExpandStringsDoFn(beam.DoFn):
      def process(
          self,
          element,
          restriction_tracker=beam.DoFn.RestrictionParam(
              ExpandStringsProvider())):
        assert isinstance(restriction_tracker, RestrictionTrackerView)
        cur = restriction_tracker.current_restriction().start
        while restriction_tracker.try_claim(cur):
          counter.inc()
          yield element[cur]
          if cur % 2 == 1:
            restriction_tracker.defer_remainder()
            return
          cur += 1

    with self.create_pipeline(is_drain=is_drain) as p:
      data = ['abc', 'defghijklmno', 'pqrstuv', 'wxyz']
      actual = (p | beam.Create(data) | beam.ParDo(ExpandStringsDoFn()))

      assert_that(actual, equal_to(list(''.join(data))))

    if isinstance(p.runner, fn_api_runner.FnApiRunner):
      res = p.runner._latest_run_result
      counters = res.metrics().query(
          beam.metrics.MetricsFilter().with_name('my_counter'))['counters']
      self.assertEqual(1, len(counters))
      self.assertEqual(counters[0].committed, len(''.join(data)))

  def test_sdf_with_sdf_initiated_checkpointing(self):
    self.run_sdf_initiated_checkpointing(is_drain=False)

  def test_draining_sdf_with_sdf_initiated_checkpointing(self):
    self.run_sdf_initiated_checkpointing(is_drain=True)

  def test_sdf_default_truncate_when_bounded(self):
    class SimpleSDF(beam.DoFn):
      def process(
          self,
          element,
          restriction_tracker=beam.DoFn.RestrictionParam(
              OffsetRangeProvider(use_bounded_offset_range=True))):
        assert isinstance(restriction_tracker, RestrictionTrackerView)
        cur = restriction_tracker.current_restriction().start
        while restriction_tracker.try_claim(cur):
          yield cur
          cur += 1

    with self.create_pipeline(is_drain=True) as p:
      actual = (p | beam.Create([10]) | beam.ParDo(SimpleSDF()))
      assert_that(actual, equal_to(range(10)))

  def test_sdf_default_truncate_when_unbounded(self):
    class SimpleSDF(beam.DoFn):
      def process(
          self,
          element,
          restriction_tracker=beam.DoFn.RestrictionParam(
              OffsetRangeProvider(use_bounded_offset_range=False))):
        assert isinstance(restriction_tracker, RestrictionTrackerView)
        cur = restriction_tracker.current_restriction().start
        while restriction_tracker.try_claim(cur):
          yield cur
          cur += 1

    with self.create_pipeline(is_drain=True) as p:
      actual = (p | beam.Create([10]) | beam.ParDo(SimpleSDF()))
      assert_that(actual, equal_to([]))

  def test_sdf_with_truncate(self):
    class SimpleSDF(beam.DoFn):
      def process(
          self,
          element,
          restriction_tracker=beam.DoFn.RestrictionParam(
              OffsetRangeProviderWithTruncate())):
        assert isinstance(restriction_tracker, RestrictionTrackerView)
        cur = restriction_tracker.current_restriction().start
        while restriction_tracker.try_claim(cur):
          yield cur
          cur += 1

    with self.create_pipeline(is_drain=True) as p:
      actual = (p | beam.Create([10]) | beam.ParDo(SimpleSDF()))
      assert_that(actual, equal_to(range(5)))

  def test_group_by_key(self):
    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create([('a', 1), ('a', 2), ('b', 3)])
          | beam.GroupByKey()
          | beam.Map(lambda k_vs: (k_vs[0], sorted(k_vs[1]))))
      assert_that(res, equal_to([('a', [1, 2]), ('b', [3])]))

  # Runners may special case the Reshuffle transform urn.
  def test_reshuffle(self):
    with self.create_pipeline() as p:
      assert_that(
          p | beam.Create([1, 2, 3]) | beam.Reshuffle(), equal_to([1, 2, 3]))

  def test_flatten(self, with_transcoding=True):
    with self.create_pipeline() as p:
      if with_transcoding:
        # Additional element which does not match with the first type
        additional = [ord('d')]
      else:
        additional = ['d']
      res = (
          p | 'a' >> beam.Create(['a']),
          p | 'bc' >> beam.Create(['b', 'c']),
          p | 'd' >> beam.Create(additional)) | beam.Flatten()
      assert_that(res, equal_to(['a', 'b', 'c'] + additional))

  def test_flatten_same_pcollections(self, with_transcoding=True):
    with self.create_pipeline() as p:
      pc = p | beam.Create(['a', 'b'])
      assert_that((pc, pc, pc) | beam.Flatten(), equal_to(['a', 'b'] * 3))

  def test_combine_per_key(self):
    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create([('a', 1), ('a', 2), ('b', 3)])
          | beam.CombinePerKey(beam.combiners.MeanCombineFn()))
      assert_that(res, equal_to([('a', 1.5), ('b', 3.0)]))

  def test_read(self):
    # Can't use NamedTemporaryFile as a context
    # due to https://bugs.python.org/issue14243
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    try:
      temp_file.write(b'a\nb\nc')
      temp_file.close()
      with self.create_pipeline() as p:
        assert_that(
            p | beam.io.ReadFromText(temp_file.name), equal_to(['a', 'b', 'c']))
    finally:
      os.unlink(temp_file.name)

  def test_windowing(self):
    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create([1, 2, 100, 101, 102])
          | beam.Map(lambda t: window.TimestampedValue(('k', t), t))
          | beam.WindowInto(beam.transforms.window.Sessions(10))
          | beam.GroupByKey()
          | beam.Map(lambda k_vs1: (k_vs1[0], sorted(k_vs1[1]))))
      assert_that(res, equal_to([('k', [1, 2]), ('k', [100, 101, 102])]))

  def test_custom_merging_window(self):
    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create([1, 2, 100, 101, 102])
          | beam.Map(lambda t: window.TimestampedValue(('k', t), t))
          | beam.WindowInto(CustomMergingWindowFn())
          | beam.GroupByKey()
          | beam.Map(lambda k_vs1: (k_vs1[0], sorted(k_vs1[1]))))
      assert_that(
          res, equal_to([('k', [1]), ('k', [101]), ('k', [2, 100, 102])]))
    gc.collect()
    from apache_beam.runners.portability.fn_api_runner.execution import GenericMergingWindowFn
    self.assertEqual(GenericMergingWindowFn._HANDLES, {})

  def test_custom_window_type(self):
    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create([1, 2, 100, 101, 102])
          | beam.Map(lambda t: window.TimestampedValue(('k', t), t))
          | beam.WindowInto(EvenOddWindows())
          | beam.GroupByKey()
          | beam.Map(lambda k_vs1: (k_vs1[0], sorted(k_vs1[1]))))
      assert_that(
          res,
          equal_to([('k', [1]), ('k', [2]), ('k', [101]), ('k', [100, 102])]))

  @unittest.skip('BEAM-9119: test is flaky')
  def test_large_elements(self):
    with self.create_pipeline() as p:
      big = (
          p
          | beam.Create(['a', 'a', 'b'])
          |
          beam.Map(lambda x: (x, x * data_plane._DEFAULT_SIZE_FLUSH_THRESHOLD)))

      side_input_res = (
          big
          | beam.Map(
              lambda x,
              side: (x[0], side.count(x[0])),
              beam.pvalue.AsList(big | beam.Map(lambda x: x[0]))))
      assert_that(
          side_input_res,
          equal_to([('a', 2), ('a', 2), ('b', 1)]),
          label='side')

      gbk_res = (big | beam.GroupByKey() | beam.Map(lambda x: x[0]))
      assert_that(gbk_res, equal_to(['a', 'b']), label='gbk')

  def test_error_message_includes_stage(self):
    with self.assertRaises(BaseException) as e_cm:
      with self.create_pipeline() as p:

        def raise_error(x):
          raise RuntimeError(
              'This error is expected and does not indicate a test failure.')

        # pylint: disable=expression-not-assigned
        (
            p
            | beam.Create(['a', 'b'])
            | 'StageA' >> beam.Map(lambda x: x)
            | 'StageB' >> beam.Map(lambda x: x)
            | 'StageC' >> beam.Map(raise_error)
            | 'StageD' >> beam.Map(lambda x: x))
    message = e_cm.exception.args[0]
    self.assertIn('StageC', message)
    self.assertNotIn('StageB', message)

  def test_error_traceback_includes_user_code(self):
    def first(x):
      return second(x)

    def second(x):
      return third(x)

    def third(x):
      raise ValueError(
          'This error is expected and does not indicate a test failure.')

    try:
      with self.create_pipeline() as p:
        p | beam.Create([0]) | beam.Map(first)  # pylint: disable=expression-not-assigned
    except Exception:  # pylint: disable=broad-except
      message = traceback.format_exc()
    else:
      raise AssertionError('expected exception not raised')

    self.assertIn('first', message)
    self.assertIn('second', message)
    self.assertIn('third', message)

  def test_no_subtransform_composite(self):
    class First(beam.PTransform):
      def expand(self, pcolls):
        return pcolls[0]

    with self.create_pipeline() as p:
      pcoll_a = p | 'a' >> beam.Create(['a'])
      pcoll_b = p | 'b' >> beam.Create(['b'])
      assert_that((pcoll_a, pcoll_b) | First(), equal_to(['a']))

  def test_metrics(self, check_gauge=True):
    p = self.create_pipeline()

    counter = beam.metrics.Metrics.counter('ns', 'counter')
    distribution = beam.metrics.Metrics.distribution('ns', 'distribution')
    gauge = beam.metrics.Metrics.gauge('ns', 'gauge')
    string_set = beam.metrics.Metrics.string_set('ns', 'string_set')

    elements = ['a', 'zzz']
    pcoll = p | beam.Create(elements)
    # pylint: disable=expression-not-assigned
    pcoll | 'count1' >> beam.FlatMap(lambda x: counter.inc())
    pcoll | 'count2' >> beam.FlatMap(lambda x: counter.inc(len(x)))
    pcoll | 'dist' >> beam.FlatMap(lambda x: distribution.update(len(x)))
    pcoll | 'gauge' >> beam.FlatMap(lambda x: gauge.set(3))
    pcoll | 'string_set' >> beam.FlatMap(lambda x: string_set.add(x))

    res = p.run()
    res.wait_until_finish()

    t1, t2 = res.metrics().query(beam.metrics.MetricsFilter()
                                 .with_name('counter'))['counters']
    self.assertEqual(t1.committed + t2.committed, 6)

    dist, = res.metrics().query(beam.metrics.MetricsFilter()
                                .with_name('distribution'))['distributions']
    self.assertEqual(
        dist.committed.data, beam.metrics.cells.DistributionData(4, 2, 1, 3))
    self.assertEqual(dist.committed.mean, 2.0)

    if check_gauge:
      gaug, = res.metrics().query(beam.metrics.MetricsFilter()
                                  .with_name('gauge'))['gauges']
      self.assertEqual(gaug.committed.value, 3)

    str_set, = res.metrics().query(beam.metrics.MetricsFilter()
                                  .with_name('string_set'))['string_sets']
    self.assertEqual(str_set.committed, set(elements))

  def test_callbacks_with_exception(self):
    elements_list = ['1', '2']

    def raise_expetion():
      raise Exception('raise exception when calling callback')

    class FinalizebleDoFnWithException(beam.DoFn):
      def process(
          self, element, bundle_finalizer=beam.DoFn.BundleFinalizerParam):
        bundle_finalizer.register(raise_expetion)
        yield element

    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create(elements_list)
          | beam.ParDo(FinalizebleDoFnWithException()))
      assert_that(res, equal_to(['1', '2']))

  def test_register_finalizations(self):
    event_recorder = EventRecorder(tempfile.gettempdir())

    class FinalizableSplittableDoFn(beam.DoFn):
      def process(
          self,
          element,
          bundle_finalizer=beam.DoFn.BundleFinalizerParam,
          restriction_tracker=beam.DoFn.RestrictionParam(
              OffsetRangeProvider(
                  use_bounded_offset_range=True, checkpoint_only=True))):
        # We use SDF to enforce finalization call happens by using
        # self-initiated checkpoint.
        if 'finalized' in event_recorder.events():
          restriction_tracker.try_claim(
              restriction_tracker.current_restriction().start)
          yield element
          restriction_tracker.try_claim(element)
          return
        if restriction_tracker.try_claim(
            restriction_tracker.current_restriction().start):
          bundle_finalizer.register(lambda: event_recorder.record('finalized'))
          # We sleep here instead of setting a resume time since the resume time
          # doesn't need to be honored.
          time.sleep(1)
          restriction_tracker.defer_remainder()

    with self.create_pipeline() as p:
      max_retries = 100
      res = (
          p
          | beam.Create([max_retries])
          | beam.ParDo(FinalizableSplittableDoFn()))
      assert_that(res, equal_to([max_retries]))

    event_recorder.cleanup()

  def test_sdf_synthetic_source(self):
    common_attrs = {
        'key_size': 1,
        'value_size': 1,
        'initial_splitting_num_bundles': 2,
        'initial_splitting_desired_bundle_size': 2,
        'sleep_per_input_record_sec': 0,
        'initial_splitting': 'const'
    }
    num_source_description = 5
    min_num_record = 10
    max_num_record = 20

    # pylint: disable=unused-variable
    source_descriptions = ([
        dict({'num_records': random.randint(min_num_record, max_num_record)},
             **common_attrs) for i in range(0, num_source_description)
    ])
    total_num_records = 0
    for source in source_descriptions:
      total_num_records += source['num_records']

    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create(source_descriptions)
          | beam.ParDo(SyntheticSDFAsSource())
          | beam.combiners.Count.Globally())
      assert_that(res, equal_to([total_num_records]))

  def test_create_value_provider_pipeline_option(self):
    # Verify that the runner can execute a pipeline when there are value
    # provider pipeline options
    # pylint: disable=unused-variable
    class FooOptions(PipelineOptions):
      @classmethod
      def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--foo", help='a value provider argument', default="bar")

    RuntimeValueProvider.set_runtime_options({})

    with self.create_pipeline() as p:
      assert_that(p | beam.Create(['a', 'b']), equal_to(['a', 'b']))

  def _test_pack_combiners(self, assert_using_counter_names):
    counter = beam.metrics.Metrics.counter('ns', 'num_values')

    def min_with_counter(values):
      counter.inc()
      return min(values)

    def max_with_counter(values):
      counter.inc()
      return max(values)

    class PackableCombines(beam.PTransform):
      def annotations(self):
        return {python_urns.APPLY_COMBINER_PACKING: b''}

      def expand(self, pcoll):
        assert_that(
            pcoll | 'PackableMin' >> beam.CombineGlobally(min_with_counter),
            equal_to([10]),
            label='AssertMin')
        assert_that(
            pcoll | 'PackableMax' >> beam.CombineGlobally(max_with_counter),
            equal_to([30]),
            label='AssertMax')

    with self.create_pipeline() as p:
      _ = p | beam.Create([10, 20, 30]) | PackableCombines()

    res = p.result

    packed_step_name_regex = (
        r'.*Packed.*PackableMin.*CombinePerKey.*PackableMax.*CombinePerKey.*' +
        'Pack.*')

    counters = res.metrics().query(beam.metrics.MetricsFilter())['counters']
    step_names = set(m.key.step for m in counters if m.key.step)
    pipeline_options = p._options
    if assert_using_counter_names:
      if pipeline_options.view_as(StandardOptions).streaming:
        self.assertFalse(
            any(re.match(packed_step_name_regex, s) for s in step_names))
      else:
        self.assertTrue(
            any(re.match(packed_step_name_regex, s) for s in step_names))

  def test_pack_combiners(self):
    self._test_pack_combiners(assert_using_counter_names=True)

  def test_group_by_key_with_empty_pcoll_elements(self):
    with self.create_pipeline() as p:
      res = (
          p
          | beam.Create([('test_key', 'test_value')])
          | beam.Filter(lambda x: False)
          | beam.GroupByKey())
      assert_that(res, equal_to([]))


# These tests are kept in a separate group so that they are
# not ran in the FnApiRunnerTestWithBundleRepeat which repeats
# bundle processing. This breaks the byte sampling metrics as
# it makes the probability of sampling far too small
# upon repeating bundle processing due to unncessarily incrementing
# the sampling counter.
class FnApiRunnerMetricsTest(unittest.TestCase):
  def assert_has_counter(
      self, mon_infos, urn, labels, value=None, ge_value=None):
    # TODO(ajamato): Consider adding a matcher framework
    found = 0
    matches = []
    for mi in mon_infos:
      if has_urn_and_labels(mi, urn, labels):
        extracted_value = monitoring_infos.extract_counter_value(mi)
        if ge_value is not None:
          if extracted_value >= ge_value:
            found = found + 1
        elif value is not None:
          if extracted_value == value:
            found = found + 1
        else:
          found = found + 1
    ge_value_str = {'ge_value': ge_value} if ge_value else ''
    value_str = {'value': value} if value else ''
    self.assertEqual(
        1,
        found,
        "Found (%s, %s) Expected only 1 monitoring_info for %s." % (
            found,
            matches,
            (urn, labels, value_str, ge_value_str),
        ))

  def assert_has_distribution(
      self, mon_infos, urn, labels, sum=None, count=None, min=None, max=None):
    # TODO(ajamato): Consider adding a matcher framework
    sum = _matcher_or_equal_to(sum)
    count = _matcher_or_equal_to(count)
    min = _matcher_or_equal_to(min)
    max = _matcher_or_equal_to(max)
    found = 0
    description = StringDescription()
    for mi in mon_infos:
      if has_urn_and_labels(mi, urn, labels):
        (extracted_count, extracted_sum, extracted_min,
         extracted_max) = monitoring_infos.extract_distribution(mi)
        increment = 1
        if sum is not None:
          description.append_text(' sum: ')
          sum.describe_to(description)
          if not sum.matches(extracted_sum):
            increment = 0
        if count is not None:
          description.append_text(' count: ')
          count.describe_to(description)
          if not count.matches(extracted_count):
            increment = 0
        if min is not None:
          description.append_text(' min: ')
          min.describe_to(description)
          if not min.matches(extracted_min):
            increment = 0
        if max is not None:
          description.append_text(' max: ')
          max.describe_to(description)
          if not max.matches(extracted_max):
            increment = 0
        found += increment
    self.assertEqual(
        1,
        found,
        "Found (%s) Expected only 1 monitoring_info for %s." % (
            found,
            (urn, labels, str(description)),
        ))

  def create_pipeline(self):
    return beam.Pipeline(runner=fn_api_runner.FnApiRunner())

  def test_element_count_metrics(self):
    class GenerateTwoOutputs(beam.DoFn):
      def process(self, element):
        yield str(element) + '1'
        yield beam.pvalue.TaggedOutput('SecondOutput', str(element) + '2')
        yield beam.pvalue.TaggedOutput('SecondOutput', str(element) + '2')
        yield beam.pvalue.TaggedOutput('ThirdOutput', str(element) + '3')

    class PassThrough(beam.DoFn):
      def process(self, element):
        yield element

    p = self.create_pipeline()

    # Produce enough elements to make sure byte sampling occurs.
    num_source_elems = 100
    pcoll = p | beam.Create(['a%d' % i for i in range(num_source_elems)],
                            reshuffle=False)

    # pylint: disable=expression-not-assigned
    pardo = (
        'StepThatDoesTwoOutputs' >> beam.ParDo(
            GenerateTwoOutputs()).with_outputs(
                'SecondOutput', 'ThirdOutput', main='FirstAndMainOutput'))

    # Actually feed pcollection to pardo
    second_output, third_output, first_output = (pcoll | pardo)

    # consume some of elements
    merged = ((first_output, second_output, third_output) | beam.Flatten())
    merged | ('PassThrough') >> beam.ParDo(PassThrough())
    second_output | ('PassThrough2') >> beam.ParDo(PassThrough())

    res = p.run()
    res.wait_until_finish()

    result_metrics = res.monitoring_metrics()

    counters = result_metrics.monitoring_infos()
    # All element count and byte count metrics must have a PCOLLECTION_LABEL.
    self.assertFalse([
        x for x in counters if x.urn in [
            monitoring_infos.ELEMENT_COUNT_URN,
            monitoring_infos.SAMPLED_BYTE_SIZE_URN
        ] and monitoring_infos.PCOLLECTION_LABEL not in x.labels
    ])
    try:
      labels = {
          monitoring_infos.PCOLLECTION_LABEL: 'ref_PCollection_PCollection_1'
      }
      self.assert_has_counter(
          counters, monitoring_infos.ELEMENT_COUNT_URN, labels, 1)

      # Create output.
      labels = {
          monitoring_infos.PCOLLECTION_LABEL: 'ref_PCollection_PCollection_3'
      }
      self.assert_has_counter(
          counters,
          monitoring_infos.ELEMENT_COUNT_URN,
          labels,
          num_source_elems)
      self.assert_has_distribution(
          counters,
          monitoring_infos.SAMPLED_BYTE_SIZE_URN,
          labels,
          min=hamcrest.greater_than(0),
          max=hamcrest.greater_than(0),
          sum=hamcrest.greater_than(0),
          count=hamcrest.greater_than(0))

      # GenerateTwoOutputs, main output.
      labels = {
          monitoring_infos.PCOLLECTION_LABEL: 'ref_PCollection_PCollection_4'
      }
      self.assert_has_counter(
          counters,
          monitoring_infos.ELEMENT_COUNT_URN,
          labels,
          num_source_elems)
      self.assert_has_distribution(
          counters,
          monitoring_infos.SAMPLED_BYTE_SIZE_URN,
          labels,
          min=hamcrest.greater_than(0),
          max=hamcrest.greater_than(0),
          sum=hamcrest.greater_than(0),
          count=hamcrest.greater_than(0))

      # GenerateTwoOutputs, "SecondOutput" output.
      labels = {
          monitoring_infos.PCOLLECTION_LABEL: 'ref_PCollection_PCollection_5'
      }
      self.assert_has_counter(
          counters,
          monitoring_infos.ELEMENT_COUNT_URN,
          labels,
          2 * num_source_elems)
      self.assert_has_distribution(
          counters,
          monitoring_infos.SAMPLED_BYTE_SIZE_URN,
          labels,
          min=hamcrest.greater_than(0),
          max=hamcrest.greater_than(0),
          sum=hamcrest.greater_than(0),
          count=hamcrest.greater_than(0))

      # GenerateTwoOutputs, "ThirdOutput" output.
      labels = {
          monitoring_infos.PCOLLECTION_LABEL: 'ref_PCollection_PCollection_6'
      }
      self.assert_has_counter(
          counters,
          monitoring_infos.ELEMENT_COUNT_URN,
          labels,
          num_source_elems)
      self.assert_has_distribution(
          counters,
          monitoring_infos.SAMPLED_BYTE_SIZE_URN,
          labels,
          min=hamcrest.greater_than(0),
          max=hamcrest.greater_than(0),
          sum=hamcrest.greater_than(0),
          count=hamcrest.greater_than(0))

      # Skipping other pcollections due to non-deterministic naming for multiple
      # outputs.
      # Flatten/Read, main output.
      labels = {
          monitoring_infos.PCOLLECTION_LABEL: 'ref_PCollection_PCollection_7'
      }
      self.assert_has_counter(
          counters,
          monitoring_infos.ELEMENT_COUNT_URN,
          labels,
          4 * num_source_elems)
      self.assert_has_distribution(
          counters,
          monitoring_infos.SAMPLED_BYTE_SIZE_URN,
          labels,
          min=hamcrest.greater_than(0),
          max=hamcrest.greater_than(0),
          sum=hamcrest.greater_than(0),
          count=hamcrest.greater_than(0))

      # PassThrough, main output
      labels = {
          monitoring_infos.PCOLLECTION_LABEL: 'ref_PCollection_PCollection_8'
      }
      self.assert_has_counter(
          counters,
          monitoring_infos.ELEMENT_COUNT_URN,
          labels,
          4 * num_source_elems)
      self.assert_has_distribution(
          counters,
          monitoring_infos.SAMPLED_BYTE_SIZE_URN,
          labels,
          min=hamcrest.greater_than(0),
          max=hamcrest.greater_than(0),
          sum=hamcrest.greater_than(0),
          count=hamcrest.greater_than(0))

      # PassThrough2, main output
      labels = {
          monitoring_infos.PCOLLECTION_LABEL: 'ref_PCollection_PCollection_9'
      }
      self.assert_has_counter(
          counters,
          monitoring_infos.ELEMENT_COUNT_URN,
          labels,
          num_source_elems)
      self.assert_has_distribution(
          counters,
          monitoring_infos.SAMPLED_BYTE_SIZE_URN,
          labels,
          min=hamcrest.greater_than(0),
          max=hamcrest.greater_than(0),
          sum=hamcrest.greater_than(0),
          count=hamcrest.greater_than(0))
    except:
      print(res._monitoring_infos_by_stage)
      raise

  def test_non_user_metrics(self):
    p = self.create_pipeline()

    pcoll = p | beam.Create(['a', 'zzz'])
    # pylint: disable=expression-not-assigned
    pcoll | 'MyStep' >> beam.FlatMap(lambda x: None)
    res = p.run()
    res.wait_until_finish()

    result_metrics = res.monitoring_metrics()
    all_metrics_via_montoring_infos = result_metrics.query()

    def assert_counter_exists(metrics, namespace, name, step):
      found = 0
      metric_key = MetricKey(step, MetricName(namespace, name))
      for m in metrics['counters']:
        if m.key == metric_key:
          found = found + 1
      self.assertEqual(
          1, found, "Did not find exactly 1 metric for %s." % metric_key)

    urns = [
        monitoring_infos.START_BUNDLE_MSECS_URN,
        monitoring_infos.PROCESS_BUNDLE_MSECS_URN,
        monitoring_infos.FINISH_BUNDLE_MSECS_URN,
        monitoring_infos.TOTAL_MSECS_URN,
    ]
    for urn in urns:
      split = urn.split(':')
      namespace = split[0]
      name = ':'.join(split[1:])
      assert_counter_exists(
          all_metrics_via_montoring_infos,
          namespace,
          name,
          step='Create/Impulse')
      assert_counter_exists(
          all_metrics_via_montoring_infos, namespace, name, step='MyStep')

  # Due to somewhat non-deterministic nature of state sampling and sleep,
  # this test is flaky when state duration is low.
  # Since increasing state duration significantly would also slow down
  # the test suite, we are retrying twice on failure as a mitigation.
  @retry(reraise=True, stop=stop_after_attempt(3))
  def test_progress_metrics(self):
    p = self.create_pipeline()

    _ = (
        p
        | beam.Create([0, 0, 0, 5e-3 * DEFAULT_SAMPLING_PERIOD_MS],
                      reshuffle=False)
        | beam.Map(time.sleep)
        | beam.Map(lambda x: ('key', x))
        | beam.GroupByKey()
        | 'm_out' >> beam.FlatMap(
            lambda x: [
                1,
                2,
                3,
                4,
                5,
                beam.pvalue.TaggedOutput('once', x),
                beam.pvalue.TaggedOutput('twice', x),
                beam.pvalue.TaggedOutput('twice', x)
            ]))

    res = p.run()
    res.wait_until_finish()

    def has_mi_for_ptransform(mon_infos, ptransform):
      for mi in mon_infos:
        if ptransform in mi.labels[monitoring_infos.PTRANSFORM_LABEL]:
          return True
      return False

    try:
      # Test the new MonitoringInfo monitoring format.
      self.assertEqual(3, len(res._monitoring_infos_by_stage))
      pregbk_mis, postgbk_mis = [
          mi for stage, mi in res._monitoring_infos_by_stage.items() if stage]

      if not has_mi_for_ptransform(pregbk_mis, 'Create/Map(decode)'):
        # The monitoring infos above are actually unordered. Swap.
        pregbk_mis, postgbk_mis = postgbk_mis, pregbk_mis

      # pregbk monitoring infos
      labels = {
          monitoring_infos.PCOLLECTION_LABEL: 'ref_PCollection_PCollection_3'
      }
      self.assert_has_counter(
          pregbk_mis, monitoring_infos.ELEMENT_COUNT_URN, labels, value=4)
      self.assert_has_distribution(
          pregbk_mis, monitoring_infos.SAMPLED_BYTE_SIZE_URN, labels)

      labels = {
          monitoring_infos.PCOLLECTION_LABEL: 'ref_PCollection_PCollection_4'
      }
      self.assert_has_counter(
          pregbk_mis, monitoring_infos.ELEMENT_COUNT_URN, labels, value=4)
      self.assert_has_distribution(
          pregbk_mis, monitoring_infos.SAMPLED_BYTE_SIZE_URN, labels)

      labels = {monitoring_infos.PTRANSFORM_LABEL: 'Map(sleep)'}
      self.assert_has_counter(
          pregbk_mis,
          monitoring_infos.TOTAL_MSECS_URN,
          labels,
          ge_value=4 * DEFAULT_SAMPLING_PERIOD_MS)

      # postgbk monitoring infos
      labels = {
          monitoring_infos.PCOLLECTION_LABEL: 'ref_PCollection_PCollection_6'
      }
      self.assert_has_counter(
          postgbk_mis, monitoring_infos.ELEMENT_COUNT_URN, labels, value=1)
      self.assert_has_distribution(
          postgbk_mis, monitoring_infos.SAMPLED_BYTE_SIZE_URN, labels)

      labels = {
          monitoring_infos.PCOLLECTION_LABEL: 'ref_PCollection_PCollection_7'
      }
      self.assert_has_counter(
          postgbk_mis, monitoring_infos.ELEMENT_COUNT_URN, labels, value=5)
      self.assert_has_distribution(
          postgbk_mis, monitoring_infos.SAMPLED_BYTE_SIZE_URN, labels)
    except:
      print(res._monitoring_infos_by_stage)
      raise


class FnApiRunnerTestWithGrpc(FnApiRunnerTest):
  def create_pipeline(self, is_drain=False):
    return beam.Pipeline(
        runner=fn_api_runner.FnApiRunner(
            default_environment=environments.EmbeddedPythonGrpcEnvironment.
            default(),
            is_drain=is_drain))


class FnApiRunnerTestWithDisabledCaching(FnApiRunnerTest):
  def create_pipeline(self, is_drain=False):
    return beam.Pipeline(
        runner=fn_api_runner.FnApiRunner(
            default_environment=environments.EmbeddedPythonGrpcEnvironment(
                state_cache_size=0,
                data_buffer_time_limit_ms=0,
                capabilities=environments.python_sdk_capabilities(),
                artifacts=()),
            is_drain=is_drain))


class FnApiRunnerTestWithMultiWorkers(FnApiRunnerTest):
  def create_pipeline(self, is_drain=False):
    pipeline_options = PipelineOptions(direct_num_workers=2)
    p = beam.Pipeline(
        runner=fn_api_runner.FnApiRunner(is_drain=is_drain),
        options=pipeline_options)
    #TODO(https://github.com/apache/beam/issues/19936): Fix these tests.
    p._options.view_as(DebugOptions).experiments.remove('beam_fn_api')
    return p

  def test_metrics(self):
    raise unittest.SkipTest("This test is for a single worker only.")

  def test_sdf_with_sdf_initiated_checkpointing(self):
    raise unittest.SkipTest("This test is for a single worker only.")

  def test_draining_sdf_with_sdf_initiated_checkpointing(self):
    raise unittest.SkipTest("This test is for a single worker only.")

  def test_sdf_with_watermark_tracking(self):
    raise unittest.SkipTest("This test is for a single worker only.")

  def test_sdf_with_dofn_as_watermark_estimator(self):
    raise unittest.SkipTest("This test is for a single worker only.")

  def test_register_finalizations(self):
    raise unittest.SkipTest("This test is for a single worker only.")


class FnApiRunnerTestWithGrpcAndMultiWorkers(FnApiRunnerTest):
  def create_pipeline(self, is_drain=False):
    pipeline_options = PipelineOptions(
        direct_num_workers=2, direct_running_mode='multi_threading')
    p = beam.Pipeline(
        runner=fn_api_runner.FnApiRunner(is_drain=is_drain),
        options=pipeline_options)
    #TODO(https://github.com/apache/beam/issues/19936): Fix these tests.
    p._options.view_as(DebugOptions).experiments.remove('beam_fn_api')
    return p

  def test_metrics(self):
    raise unittest.SkipTest("This test is for a single worker only.")

  def test_sdf_with_sdf_initiated_checkpointing(self):
    raise unittest.SkipTest("This test is for a single worker only.")

  def test_draining_sdf_with_sdf_initiated_checkpointing(self):
    raise unittest.SkipTest("This test is for a single worker only.")

  def test_sdf_with_watermark_tracking(self):
    raise unittest.SkipTest("This test is for a single worker only.")

  def test_sdf_with_dofn_as_watermark_estimator(self):
    raise unittest.SkipTest("This test is for a single worker only.")

  def test_register_finalizations(self):
    raise unittest.SkipTest("This test is for a single worker only.")


class FnApiRunnerTestWithBundleRepeat(FnApiRunnerTest):
  def create_pipeline(self, is_drain=False):
    return beam.Pipeline(
        runner=fn_api_runner.FnApiRunner(bundle_repeat=3, is_drain=is_drain))

  def test_register_finalizations(self):
    raise unittest.SkipTest("TODO: Avoid bundle finalizations on repeat.")


class FnApiRunnerTestWithBundleRepeatAndMultiWorkers(FnApiRunnerTest):
  def create_pipeline(self, is_drain=False):
    pipeline_options = PipelineOptions(direct_num_workers=2)
    p = beam.Pipeline(
        runner=fn_api_runner.FnApiRunner(bundle_repeat=3, is_drain=is_drain),
        options=pipeline_options)
    #TODO(https://github.com/apache/beam/issues/19936): Fix these tests.
    p._options.view_as(DebugOptions).experiments.remove('beam_fn_api')
    return p

  def test_register_finalizations(self):
    raise unittest.SkipTest("TODO: Avoid bundle finalizations on repeat.")

  def test_metrics(self):
    raise unittest.SkipTest("This test is for a single worker only.")

  def test_sdf_with_sdf_initiated_checkpointing(self):
    raise unittest.SkipTest("This test is for a single worker only.")

  def test_draining_sdf_with_sdf_initiated_checkpointing(self):
    raise unittest.SkipTest("This test is for a single worker only.")

  def test_sdf_with_watermark_tracking(self):
    raise unittest.SkipTest("This test is for a single worker only.")

  def test_sdf_with_dofn_as_watermark_estimator(self):
    raise unittest.SkipTest("This test is for a single worker only.")


class FnApiRunnerSplitTest(unittest.TestCase):
  def create_pipeline(self, is_drain=False):
    # Must be GRPC so we can send data and split requests concurrent
    # to the bundle process request.
    return beam.Pipeline(
        runner=fn_api_runner.FnApiRunner(
            default_environment=environments.EmbeddedPythonGrpcEnvironment.
            default(),
            is_drain=is_drain))

  def test_checkpoint(self):
    # This split manager will get re-invoked on each smaller split,
    # so N times for N elements.
    element_counter = ElementCounter()

    def split_manager(num_elements):
      # Send at least one element so it can make forward progress.
      element_counter.reset()
      breakpoint = element_counter.set_breakpoint(1)
      # Cede control back to the runner so data can be sent.
      yield
      breakpoint.wait()
      # Split as close to current as possible.
      split_result = yield 0.0
      # Verify we split at exactly the first element.
      self.verify_channel_split(split_result, 0, 1)
      # Continue processing.
      breakpoint.clear()

    self.run_split_pipeline(split_manager, list('abc'), element_counter)

  def test_split_half(self):
    total_num_elements = 25
    seen_bundle_sizes = []
    element_counter = ElementCounter()

    def split_manager(num_elements):
      seen_bundle_sizes.append(num_elements)
      if num_elements == total_num_elements:
        element_counter.reset()
        breakpoint = element_counter.set_breakpoint(5)
        yield
        breakpoint.wait()
        # Split the remainder (20, then 10, elements) in half.
        split1 = yield 0.5
        self.verify_channel_split(split1, 14, 15)  # remainder is 15 to end
        split2 = yield 0.5
        self.verify_channel_split(split2, 9, 10)  # remainder is 10 to end
        breakpoint.clear()

    self.run_split_pipeline(
        split_manager, range(total_num_elements), element_counter)
    self.assertEqual([25, 15], seen_bundle_sizes)

  def run_split_pipeline(self, split_manager, elements, element_counter=None):
    with fn_runner.split_manager('Identity', split_manager):
      with self.create_pipeline() as p:
        res = (
            p
            | beam.Create(elements)
            | beam.Reshuffle()
            | 'Identity' >> beam.Map(lambda x: x)
            | beam.Map(lambda x: element_counter.increment() or x))
        assert_that(res, equal_to(elements))

  def run_sdf_checkpoint(self, is_drain=False):
    element_counter = ElementCounter()

    def split_manager(num_elements):
      if num_elements > 0:
        element_counter.reset()
        breakpoint = element_counter.set_breakpoint(1)
        yield
        breakpoint.wait()
        yield 0
        breakpoint.clear()

    # Everything should be perfectly split.

    elements = [2, 3]
    expected_groups = [[(2, 0)], [(2, 1)], [(3, 0)], [(3, 1)], [(3, 2)]]
    self.run_sdf_split_pipeline(
        split_manager,
        elements,
        element_counter,
        expected_groups,
        is_drain=is_drain)

  def run_sdf_split_half(self, is_drain=False):
    element_counter = ElementCounter()
    is_first_bundle = True

    def split_manager(num_elements):
      nonlocal is_first_bundle
      if is_first_bundle and num_elements > 0:
        is_first_bundle = False
        breakpoint = element_counter.set_breakpoint(1)
        yield
        breakpoint.wait()
        split1 = yield 0.5
        split2 = yield 0.5
        split3 = yield 0.5
        self.verify_channel_split(split1, 0, 1)
        self.verify_channel_split(split2, -1, 1)
        self.verify_channel_split(split3, -1, 1)
        breakpoint.clear()

    elements = [4, 4]
    expected_groups = [[(4, 0)], [(4, 1)], [(4, 2), (4, 3)], [(4, 0), (4, 1),
                                                              (4, 2), (4, 3)]]

    self.run_sdf_split_pipeline(
        split_manager,
        elements,
        element_counter,
        expected_groups,
        is_drain=is_drain)

  def run_split_crazy_sdf(self, seed=None, is_drain=False):
    if seed is None:
      seed = random.randrange(1 << 20)
    r = random.Random(seed)
    element_counter = ElementCounter()

    def split_manager(num_elements):
      if num_elements > 0:
        element_counter.reset()
        wait_for = r.randrange(num_elements)
        breakpoint = element_counter.set_breakpoint(wait_for)
        yield
        breakpoint.wait()
        yield r.random()
        yield r.random()
        breakpoint.clear()

    try:
      elements = [r.randrange(5, 10) for _ in range(5)]
      self.run_sdf_split_pipeline(
          split_manager, elements, element_counter, is_drain=is_drain)
    except Exception:
      _LOGGER.error('test_split_crazy_sdf.seed = %s', seed)
      raise

  def test_nosplit_sdf(self):
    def split_manager(num_elements):
      yield

    elements = [1, 2, 3]
    expected_groups = [[(e, k) for k in range(e)] for e in elements]
    self.run_sdf_split_pipeline(
        split_manager, elements, ElementCounter(), expected_groups)

  def test_checkpoint_sdf(self):
    self.run_sdf_checkpoint(is_drain=False)

  def test_checkpoint_draining_sdf(self):
    self.run_sdf_checkpoint(is_drain=True)

  def test_split_half_sdf(self):
    self.run_sdf_split_half(is_drain=False)

  def test_split_half_draining_sdf(self):
    self.run_sdf_split_half(is_drain=True)

  def test_split_crazy_sdf(self, seed=None):
    self.run_split_crazy_sdf(seed=seed, is_drain=False)

  def test_split_crazy_draining_sdf(self, seed=None):
    self.run_split_crazy_sdf(seed=seed, is_drain=True)

  def run_sdf_split_pipeline(
      self,
      split_manager,
      elements,
      element_counter,
      expected_groups=None,
      is_drain=False):
    # Define an SDF that for each input x produces [(x, k) for k in range(x)].

    class EnumerateProvider(beam.transforms.core.RestrictionProvider):
      def initial_restriction(self, element):
        return restriction_trackers.OffsetRange(0, element)

      def create_tracker(self, restriction):
        return restriction_trackers.OffsetRestrictionTracker(restriction)

      def split(self, element, restriction):
        # Don't do any initial splitting to simplify test.
        return [restriction]

      def restriction_size(self, element, restriction):
        return restriction.size()

      def is_bounded(self):
        return True

    class EnumerateSdf(beam.DoFn):
      def process(
          self,
          element,
          restriction_tracker=beam.DoFn.RestrictionParam(EnumerateProvider())):
        to_emit = []
        cur = restriction_tracker.current_restriction().start
        while restriction_tracker.try_claim(cur):
          to_emit.append((element, cur))
          element_counter.increment()
          cur += 1
        # Emitting in batches for tighter testing.
        yield to_emit

    expected = [(e, k) for e in elements for k in range(e)]

    with fn_runner.split_manager('SDF', split_manager):
      with self.create_pipeline(is_drain=is_drain) as p:
        grouped = (
            p
            | beam.Create(elements, reshuffle=False)
            | 'SDF' >> beam.ParDo(EnumerateSdf()))
        flat = grouped | beam.FlatMap(lambda x: x)
        assert_that(flat, equal_to(expected))
        if expected_groups:
          assert_that(grouped, equal_to(expected_groups), label='CheckGrouped')

  def test_time_based_split_manager(self):

    elements = [str(x) for x in range(100)]

    class BundleCountingDoFn(beam.DoFn):
      def process(self, element):
        time.sleep(0.005)
        yield element

      def finish_bundle(self):
        yield window.GlobalWindows.windowed_value('endOfBundle')

    with self.create_pipeline() as p:
      p._options.view_as(DirectOptions).direct_test_splits = {
          'SplitMarker': {
              'timings': [0, .05], 'fractions': [0.5, 0.5]
          }
      }
      assert_that(
          p
          | beam.Create(elements)
          | 'SplitMarker' >> beam.ParDo(BundleCountingDoFn()),
          # We split the first bundle twice (once at 50%, and again at 50% of
          # what was left). All returned split remainders get processed
          # (together) in a (single) subsequent bundle.
          equal_to(elements + ['endOfBundle'] * 2))

  def verify_channel_split(self, split_result, last_primary, first_residual):
    self.assertEqual(1, len(split_result.channel_splits), split_result)
    channel_split, = split_result.channel_splits
    self.assertEqual(last_primary, channel_split.last_primary_element)
    self.assertEqual(first_residual, channel_split.first_residual_element)
    # There should be a primary and residual application for each element
    # not covered above.
    self.assertEqual(
        first_residual - last_primary - 1,
        len(split_result.primary_roots),
        split_result.primary_roots)
    self.assertEqual(
        first_residual - last_primary - 1,
        len(split_result.residual_roots),
        split_result.residual_roots)


class ElementCounter(object):
  """Used to wait until a certain number of elements are seen."""
  def __init__(self):
    self._cv = threading.Condition()
    self.reset()

  def reset(self):
    with self._cv:
      self._breakpoints = collections.defaultdict(list)
      self._count = 0

  def increment(self):
    with self._cv:
      self._count += 1
      self._cv.notify_all()
      breakpoints = list(self._breakpoints[self._count])
    for breakpoint in breakpoints:
      breakpoint.wait()

  def set_breakpoint(self, value):
    with self._cv:
      event = threading.Event()
      self._breakpoints[value].append(event)

    class Breakpoint(object):
      @staticmethod
      def wait(timeout=10):
        with self._cv:
          start = time.time()
          while self._count < value:
            elapsed = time.time() - start
            if elapsed > timeout:
              raise RuntimeError('Timed out waiting for %s' % value)
            self._cv.wait(timeout - elapsed)

      @staticmethod
      def clear():
        event.set()

    return Breakpoint()

  def __reduce__(self):
    # Ensure we get the same element back through a pickling round-trip.
    name = uuid.uuid4().hex
    _pickled_element_counters[name] = self
    return _unpickle_element_counter, (name, )


_pickled_element_counters: Dict[str, ElementCounter] = {}


def _unpickle_element_counter(name):
  return _pickled_element_counters[name]


class EventRecorder(object):
  """Used to be registered as a callback in bundle finalization.

  The reason why records are written into a tmp file is, the in-memory dataset
  cannot keep callback records when passing into one DoFn.
  """
  def __init__(self, tmp_dir):
    self.tmp_dir = os.path.join(tmp_dir, uuid.uuid4().hex)
    os.mkdir(self.tmp_dir)

  def record(self, content):
    file_path = os.path.join(self.tmp_dir, uuid.uuid4().hex + '.txt')
    with open(file_path, 'w') as f:
      f.write(content)

  def events(self):
    content = []
    record_files = [
        f for f in os.listdir(self.tmp_dir)
        if os.path.isfile(os.path.join(self.tmp_dir, f))
    ]
    for file in record_files:
      with open(os.path.join(self.tmp_dir, file), 'r') as f:
        content.append(f.read())
    return sorted(content)

  def cleanup(self):
    shutil.rmtree(self.tmp_dir)


class ExpandStringsProvider(beam.transforms.core.RestrictionProvider):
  """A RestrictionProvider that used for sdf related tests."""
  def initial_restriction(self, element):
    return restriction_trackers.OffsetRange(0, len(element))

  def create_tracker(self, restriction):
    return restriction_trackers.OffsetRestrictionTracker(restriction)

  def split(self, element, restriction):
    desired_bundle_size = restriction.size() // 2
    return restriction.split(desired_bundle_size)

  def restriction_size(self, element, restriction):
    return restriction.size()


class UnboundedOffsetRestrictionTracker(
    restriction_trackers.OffsetRestrictionTracker):
  def is_bounded(self):
    return False


class OffsetRangeProvider(beam.transforms.core.RestrictionProvider):
  def __init__(self, use_bounded_offset_range, checkpoint_only=False):
    self.use_bounded_offset_range = use_bounded_offset_range
    self.checkpoint_only = checkpoint_only

  def initial_restriction(self, element):
    return restriction_trackers.OffsetRange(0, element)

  def create_tracker(self, restriction):
    if self.checkpoint_only:

      class CheckpointOnlyOffsetRestrictionTracker(
          restriction_trackers.OffsetRestrictionTracker):
        def try_split(self, unused_fraction_of_remainder):
          return super().try_split(0.0)

      return CheckpointOnlyOffsetRestrictionTracker(restriction)
    if self.use_bounded_offset_range:
      return restriction_trackers.OffsetRestrictionTracker(restriction)
    return UnboundedOffsetRestrictionTracker(restriction)

  def split(self, element, restriction):
    return [restriction]

  def restriction_size(self, element, restriction):
    return restriction.size()


class OffsetRangeProviderWithTruncate(OffsetRangeProvider):
  def __init__(self):
    super().__init__(True)

  def truncate(self, element, restriction):
    return restriction_trackers.OffsetRange(
        restriction.start, restriction.stop // 2)


class FnApiBasedLullLoggingTest(unittest.TestCase):
  def create_pipeline(self):
    return beam.Pipeline(
        runner=fn_api_runner.FnApiRunner(
            default_environment=environments.EmbeddedPythonGrpcEnvironment.
            default(),
            progress_request_frequency=0.5))


class StateBackedTestElementType(object):
  live_element_count = 0

  def __init__(self, num_elements, unused):
    self.num_elements = num_elements
    StateBackedTestElementType.live_element_count += 1
    # Due to using state backed iterable, we expect there is a few instances
    # alive at any given time.
    if StateBackedTestElementType.live_element_count > 5:
      raise RuntimeError('Too many live instances.')

  def __del__(self):
    StateBackedTestElementType.live_element_count -= 1

  def __reduce__(self):
    return (self.__class__, (self.num_elements, 'x' * self.num_elements))


@pytest.mark.it_validatesrunner
class FnApiBasedStateBackedCoderTest(unittest.TestCase):
  def create_pipeline(self):
    return beam.Pipeline(
        runner=fn_api_runner.FnApiRunner(use_state_iterables=True))

  def test_gbk_many_values(self):
    with self.create_pipeline() as p:
      # The number of integers could be a knob to test against
      # different runners' default settings on page size.
      VALUES_PER_ELEMENT = 300
      NUM_OF_ELEMENTS = 200

      r = (
          p
          | beam.Create([None])
          | beam.FlatMap(
              lambda x: ((1, StateBackedTestElementType(VALUES_PER_ELEMENT, _))
                         for _ in range(NUM_OF_ELEMENTS)))
          | beam.GroupByKey()
          | beam.MapTuple(lambda _, vs: sum(e.num_elements for e in vs)))

      assert_that(r, equal_to([VALUES_PER_ELEMENT * NUM_OF_ELEMENTS]))


# TODO(robertwb): Why does pickling break when this is inlined?
class CustomMergingWindowFn(window.WindowFn):
  def assign(self, assign_context):
    return [
        window.IntervalWindow(
            assign_context.timestamp, assign_context.timestamp + 1000)
    ]

  def merge(self, merge_context):
    evens = [w for w in merge_context.windows if w.start % 2 == 0]
    if evens:
      merge_context.merge(
          evens,
          window.IntervalWindow(
              min(w.start for w in evens), max(w.end for w in evens)))

  def get_window_coder(self):
    return coders.IntervalWindowCoder()


class ColoredFixedWindow(window.BoundedWindow):
  def __init__(self, end, color):
    super().__init__(end)
    self.color = color

  def __hash__(self):
    return hash((self.end, self.color))

  def __eq__(self, other):
    return (
        type(self) == type(other) and self.end == other.end and
        self.color == other.color)


class ColoredFixedWindowCoder(beam.coders.Coder):
  kv_coder = beam.coders.TupleCoder(
      [beam.coders.TimestampCoder(), beam.coders.StrUtf8Coder()])

  def encode(self, colored_window):
    return self.kv_coder.encode((colored_window.end, colored_window.color))

  def decode(self, encoded_window):
    return ColoredFixedWindow(*self.kv_coder.decode(encoded_window))

  def is_deterministic(self):
    return True


class EvenOddWindows(window.NonMergingWindowFn):
  def assign(self, context):
    timestamp = context.timestamp
    return [
        ColoredFixedWindow(
            timestamp - timestamp % 10 + 10,
            'red' if timestamp.micros // 1000000 % 2 else 'black')
    ]

  def get_window_coder(self):
    return ColoredFixedWindowCoder()


class ExpectingSideInputsFn(beam.DoFn):
  def __init__(self, name):
    self._name = name

  def default_label(self):
    return self._name

  def process(self, element, *side_inputs):
    logging.info('Running %s (side inputs: %s)', self._name, side_inputs)
    if not all(list(s) for s in side_inputs):
      raise ValueError(f'Missing data in side input {side_inputs}')
    yield self._name


class ArrayMultiplyDoFn(beam.DoFn):
  def process_batch(self, batch: np.ndarray, *unused_args,
                    **unused_kwargs) -> Iterator[np.ndarray]:
    assert isinstance(batch, np.ndarray)
    # GeneralPurposeConsumerSet should limit batches to MAX_BATCH_SIZE (4096)
    # elements
    assert np.size(batch, axis=0) <= 4096
    yield batch * 2

  # infer_output_type must be defined (when there's no process method),
  # otherwise we don't know the input type is the same as output type.
  def infer_output_type(self, input_type):
    return input_type


class ListPlusOneDoFn(beam.DoFn):
  def process_batch(self, batch: List[np.int64], *unused_args,
                    **unused_kwargs) -> Iterator[List[np.int64]]:
    assert isinstance(batch, list)
    yield [element + 1 for element in batch]

  # infer_output_type must be defined (when there's no process method),
  # otherwise we don't know the input type is the same as output type.
  def infer_output_type(self, input_type):
    return input_type


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
