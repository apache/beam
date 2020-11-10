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

"""Unit tests for the triggering classes."""

# pytype: skip-file

from __future__ import absolute_import

import collections
import json
import os.path
import pickle
import random
import unittest
from builtins import range
from builtins import zip

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import
import yaml

import apache_beam as beam
from apache_beam import coders
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.portability import common_urns
from apache_beam.runners import pipeline_context
from apache_beam.runners.direct.clock import TestClock
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import ptransform
from apache_beam.transforms import trigger
from apache_beam.transforms.core import Windowing
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.trigger import AfterAll
from apache_beam.transforms.trigger import AfterAny
from apache_beam.transforms.trigger import AfterCount
from apache_beam.transforms.trigger import AfterEach
from apache_beam.transforms.trigger import AfterProcessingTime
from apache_beam.transforms.trigger import AfterWatermark
from apache_beam.transforms.trigger import Always
from apache_beam.transforms.trigger import DefaultTrigger
from apache_beam.transforms.trigger import GeneralTriggerDriver
from apache_beam.transforms.trigger import InMemoryUnmergedState
from apache_beam.transforms.trigger import Repeatedly
from apache_beam.transforms.trigger import TriggerFn
from apache_beam.transforms.trigger import _Never
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import IntervalWindow
from apache_beam.transforms.window import Sessions
from apache_beam.transforms.window import TimestampCombiner
from apache_beam.transforms.window import TimestampedValue
from apache_beam.transforms.window import WindowedValue
from apache_beam.transforms.window import WindowFn
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.windowed_value import PaneInfoTiming


class CustomTimestampingFixedWindowsWindowFn(FixedWindows):
  """WindowFn for testing custom timestamping."""
  def get_transformed_output_time(self, unused_window, input_timestamp):
    return input_timestamp + 100


class TriggerTest(unittest.TestCase):
  def run_trigger_simple(
      self,
      window_fn,
      trigger_fn,
      accumulation_mode,
      timestamped_data,
      expected_panes,
      *groupings,
      **kwargs):
    # Groupings is a list of integers indicating the (uniform) size of bundles
    # to try. For example, if timestamped_data has elements [a, b, c, d, e]
    # then groupings=(5, 2) would first run the test with everything in the same
    # bundle, and then re-run the test with bundling [a, b], [c, d], [e].
    # A negative value will reverse the order, e.g. -2 would result in bundles
    # [e, d], [c, b], [a].  This is useful for deterministic triggers in testing
    # that the output is not a function of ordering or bundling.
    # If empty, defaults to bundles of size 1 in the given order.
    late_data = kwargs.pop('late_data', [])
    assert not kwargs

    def bundle_data(data, size):
      if size < 0:
        data = list(data)[::-1]
        size = -size
      bundle = []
      for timestamp, elem in data:
        windows = window_fn.assign(WindowFn.AssignContext(timestamp, elem))
        bundle.append(WindowedValue(elem, timestamp, windows))
        if len(bundle) == size:
          yield bundle
          bundle = []
      if bundle:
        yield bundle

    if not groupings:
      groupings = [1]
    for group_by in groupings:
      self.run_trigger(
          window_fn,
          trigger_fn,
          accumulation_mode,
          bundle_data(timestamped_data, group_by),
          bundle_data(late_data, group_by),
          expected_panes)

  def run_trigger(
      self,
      window_fn,
      trigger_fn,
      accumulation_mode,
      bundles,
      late_bundles,
      expected_panes):
    actual_panes = collections.defaultdict(list)
    allowed_lateness = Duration(
        micros=int(common_urns.constants.MAX_TIMESTAMP_MILLIS.constant) * 1000)
    driver = GeneralTriggerDriver(
        Windowing(
            window_fn,
            trigger_fn,
            accumulation_mode,
            allowed_lateness=allowed_lateness),
        TestClock())
    state = InMemoryUnmergedState()

    for bundle in bundles:
      for wvalue in driver.process_elements(state,
                                            bundle,
                                            MIN_TIMESTAMP,
                                            MIN_TIMESTAMP):
        window, = wvalue.windows
        self.assertEqual(window.max_timestamp(), wvalue.timestamp)
        actual_panes[window].append(set(wvalue.value))

    while state.timers:
      for timer_window, (name, time_domain,
                         timestamp) in state.get_and_clear_timers():
        for wvalue in driver.process_timer(timer_window,
                                           name,
                                           time_domain,
                                           timestamp,
                                           state,
                                           MIN_TIMESTAMP):
          window, = wvalue.windows
          self.assertEqual(window.max_timestamp(), wvalue.timestamp)
          actual_panes[window].append(set(wvalue.value))

    for bundle in late_bundles:
      for wvalue in driver.process_elements(state,
                                            bundle,
                                            MAX_TIMESTAMP,
                                            MAX_TIMESTAMP):
        window, = wvalue.windows
        self.assertEqual(window.max_timestamp(), wvalue.timestamp)
        actual_panes[window].append(set(wvalue.value))

      while state.timers:
        for timer_window, (name, time_domain,
                           timestamp) in state.get_and_clear_timers():
          for wvalue in driver.process_timer(timer_window,
                                             name,
                                             time_domain,
                                             timestamp,
                                             state,
                                             MAX_TIMESTAMP):
            window, = wvalue.windows
            self.assertEqual(window.max_timestamp(), wvalue.timestamp)
            actual_panes[window].append(set(wvalue.value))

    self.assertEqual(expected_panes, actual_panes)

  def test_fixed_watermark(self):
    self.run_trigger_simple(
        FixedWindows(10),  # pyformat break
        AfterWatermark(),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (2, 'b'), (13, 'c')],
        {IntervalWindow(0, 10): [set('ab')],
         IntervalWindow(10, 20): [set('c')]},
        1,
        2,
        3,
        -3,
        -2,
        -1)

  def test_fixed_watermark_with_early(self):
    self.run_trigger_simple(
        FixedWindows(10),  # pyformat break
        AfterWatermark(early=AfterCount(2)),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (2, 'b'), (3, 'c')],
        {IntervalWindow(0, 10): [set('ab'), set('abc')]},
        2)
    self.run_trigger_simple(
        FixedWindows(10),  # pyformat break
        AfterWatermark(early=AfterCount(2)),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (2, 'b'), (3, 'c')],
        {IntervalWindow(0, 10): [set('abc'), set('abc')]},
        3)

  def test_fixed_watermark_with_early_late(self):
    self.run_trigger_simple(
        FixedWindows(100),  # pyformat break
        AfterWatermark(early=AfterCount(3),
                       late=AfterCount(2)),
        AccumulationMode.DISCARDING,
        zip(range(9), 'abcdefghi'),
        {IntervalWindow(0, 100): [
            set('abcd'), set('efgh'),  # early
            set('i'),                  # on time
            set('vw'), set('xy')       # late
            ]},
        2,
        late_data=zip(range(5), 'vwxyz'))

  def test_sessions_watermark_with_early_late(self):
    self.run_trigger_simple(
        Sessions(10),  # pyformat break
        AfterWatermark(early=AfterCount(2),
                       late=AfterCount(1)),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (15, 'b'), (7, 'c'), (30, 'd')],
        {
            IntervalWindow(1, 25): [
                set('abc'),                # early
                set('abc'),                # on time
                set('abcxy')               # late
            ],
            IntervalWindow(30, 40): [
                set('d'),                  # on time
            ],
            IntervalWindow(1, 40): [
                set('abcdxyz')             # late
            ],
        },
        2,
        late_data=[(1, 'x'), (2, 'y'), (21, 'z')])

  def test_fixed_after_count(self):
    self.run_trigger_simple(
        FixedWindows(10),  # pyformat break
        AfterCount(2),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (2, 'b'), (3, 'c'), (11, 'z')],
        {IntervalWindow(0, 10): [set('ab')]},
        1,
        2)
    self.run_trigger_simple(
        FixedWindows(10),  # pyformat break
        AfterCount(2),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (2, 'b'), (3, 'c'), (11, 'z')],
        {IntervalWindow(0, 10): [set('abc')]},
        3,
        4)

  def test_fixed_after_first(self):
    self.run_trigger_simple(
        FixedWindows(10),  # pyformat break
        AfterAny(AfterCount(2), AfterWatermark()),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (2, 'b'), (3, 'c')],
        {IntervalWindow(0, 10): [set('ab')]},
        1,
        2)
    self.run_trigger_simple(
        FixedWindows(10),  # pyformat break
        AfterAny(AfterCount(5), AfterWatermark()),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (2, 'b'), (3, 'c')],
        {IntervalWindow(0, 10): [set('abc')]},
        1,
        2,
        late_data=[(1, 'x'), (2, 'y'), (3, 'z')])

  def test_repeatedly_after_first(self):
    self.run_trigger_simple(
        FixedWindows(100),  # pyformat break
        Repeatedly(AfterAny(AfterCount(3), AfterWatermark())),
        AccumulationMode.ACCUMULATING,
        zip(range(7), 'abcdefg'),
        {IntervalWindow(0, 100): [
            set('abc'),
            set('abcdef'),
            set('abcdefg'),
            set('abcdefgx'),
            set('abcdefgxy'),
            set('abcdefgxyz')]},
        1,
        late_data=zip(range(3), 'xyz'))

  def test_sessions_after_all(self):
    self.run_trigger_simple(
        Sessions(10),  # pyformat break
        AfterAll(AfterCount(2), AfterWatermark()),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (2, 'b'), (3, 'c')],
        {IntervalWindow(1, 13): [set('abc')]},
        1,
        2)
    self.run_trigger_simple(
        Sessions(10),  # pyformat break
        AfterAll(AfterCount(5), AfterWatermark()),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (2, 'b'), (3, 'c')],
        {IntervalWindow(1, 13): [set('abcxy')]},
        1,
        2,
        late_data=[(1, 'x'), (2, 'y'), (3, 'z')])

  def test_sessions_default(self):
    self.run_trigger_simple(
        Sessions(10),  # pyformat break
        DefaultTrigger(),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (2, 'b')],
        {IntervalWindow(1, 12): [set('ab')]},
        1,
        2,
        -2,
        -1)

    self.run_trigger_simple(
        Sessions(10),  # pyformat break
        AfterWatermark(),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (2, 'b'), (15, 'c'), (16, 'd'), (30, 'z'), (9, 'e'),
         (10, 'f'), (30, 'y')],
        {IntervalWindow(1, 26): [set('abcdef')],
         IntervalWindow(30, 40): [set('yz')]},
        1,
        2,
        3,
        4,
        5,
        6,
        -4,
        -2,
        -1)

  def test_sessions_watermark(self):
    self.run_trigger_simple(
        Sessions(10),  # pyformat break
        AfterWatermark(),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (2, 'b')],
        {IntervalWindow(1, 12): [set('ab')]},
        1,
        2,
        -2,
        -1)

  def test_sessions_after_count(self):
    self.run_trigger_simple(
        Sessions(10),  # pyformat break
        AfterCount(2),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (15, 'b'), (6, 'c'), (30, 's'), (31, 't'), (50, 'z'),
         (50, 'y')],
        {IntervalWindow(1, 25): [set('abc')],
         IntervalWindow(30, 41): [set('st')],
         IntervalWindow(50, 60): [set('yz')]},
        1,
        2,
        3)

  def test_sessions_repeatedly_after_count(self):
    self.run_trigger_simple(
        Sessions(10),  # pyformat break
        Repeatedly(AfterCount(2)),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (15, 'b'), (6, 'c'), (2, 'd'), (7, 'e')],
        {IntervalWindow(1, 25): [set('abc'), set('abcde')]},
        1,
        3)
    self.run_trigger_simple(
        Sessions(10),  # pyformat break
        Repeatedly(AfterCount(2)),
        AccumulationMode.DISCARDING,
        [(1, 'a'), (15, 'b'), (6, 'c'), (2, 'd'), (7, 'e')],
        {IntervalWindow(1, 25): [set('abc'), set('de')]},
        1,
        3)

  def test_sessions_after_each(self):
    self.run_trigger_simple(
        Sessions(10),  # pyformat break
        AfterEach(AfterCount(2), AfterCount(3)),
        AccumulationMode.ACCUMULATING,
        zip(range(10), 'abcdefghij'),
        {IntervalWindow(0, 11): [set('ab')],
         IntervalWindow(0, 15): [set('abcdef')]},
        2)

    self.run_trigger_simple(
        Sessions(10),  # pyformat break
        Repeatedly(AfterEach(AfterCount(2), AfterCount(3))),
        AccumulationMode.ACCUMULATING,
        zip(range(10), 'abcdefghij'),
        {IntervalWindow(0, 11): [set('ab')],
         IntervalWindow(0, 15): [set('abcdef')],
         IntervalWindow(0, 17): [set('abcdefgh')]},
        2)

  def test_picklable_output(self):
    global_window = (trigger.GlobalWindow(), )
    driver = trigger.BatchGlobalTriggerDriver()
    unpicklable = (WindowedValue(k, 0, global_window) for k in range(10))
    with self.assertRaises(TypeError):
      pickle.dumps(unpicklable)
    for unwindowed in driver.process_elements(None, unpicklable, None, None):
      self.assertEqual(
          pickle.loads(pickle.dumps(unwindowed)).value, list(range(10)))


class RunnerApiTest(unittest.TestCase):
  def test_trigger_encoding(self):
    for trigger_fn in (DefaultTrigger(),
                       AfterAll(AfterCount(1), AfterCount(10)),
                       AfterAny(AfterCount(10), AfterCount(100)),
                       AfterWatermark(early=AfterCount(1000)),
                       AfterWatermark(early=AfterCount(1000),
                                      late=AfterCount(1)),
                       Repeatedly(AfterCount(100)),
                       trigger.OrFinally(AfterCount(3), AfterCount(10))):
      context = pipeline_context.PipelineContext()
      self.assertEqual(
          trigger_fn,
          TriggerFn.from_runner_api(trigger_fn.to_runner_api(context), context))


class TriggerPipelineTest(unittest.TestCase):
  def test_after_count(self):
    with TestPipeline() as p:

      def construct_timestamped(k_t):
        return TimestampedValue((k_t[0], k_t[1]), k_t[1])

      def format_result(k_v):
        return ('%s-%s' % (k_v[0], len(k_v[1])), set(k_v[1]))

      result = (
          p
          | beam.Create([1, 2, 3, 4, 5, 10, 11])
          | beam.FlatMap(lambda t: [('A', t), ('B', t + 5)])
          | beam.Map(construct_timestamped)
          | beam.WindowInto(
              FixedWindows(10),
              trigger=AfterCount(3),
              accumulation_mode=AccumulationMode.DISCARDING)
          | beam.GroupByKey()
          | beam.Map(format_result))
      assert_that(
          result,
          equal_to(
              list({
                  'A-5': {1, 2, 3, 4, 5},
                  # A-10, A-11 never emitted due to AfterCount(3) never firing.
                  'B-4': {6, 7, 8, 9},
                  'B-3': {10, 15, 16},
              }.items())))

  def test_always(self):
    with TestPipeline() as p:

      def construct_timestamped(k_t):
        return TimestampedValue((k_t[0], k_t[1]), k_t[1])

      def format_result(k_v):
        return ('%s-%s' % (k_v[0], len(k_v[1])), set(k_v[1]))

      result = (
          p
          | beam.Create([1, 1, 2, 3, 4, 5, 10, 11])
          | beam.FlatMap(lambda t: [('A', t), ('B', t + 5)])
          | beam.Map(construct_timestamped)
          | beam.WindowInto(
              FixedWindows(10),
              trigger=Always(),
              accumulation_mode=AccumulationMode.DISCARDING)
          | beam.GroupByKey()
          | beam.Map(format_result))
      assert_that(
          result,
          equal_to(
              list({
                  'A-2': {10, 11},
                  # Elements out of windows are also emitted.
                  'A-6': {1, 2, 3, 4, 5},
                  # A,1 is emitted twice.
                  'B-5': {6, 7, 8, 9},
                  # B,6 is emitted twice.
                  'B-3': {10, 15, 16},
              }.items())))

  def test_never(self):
    with TestPipeline() as p:

      def construct_timestamped(k_t):
        return TimestampedValue((k_t[0], k_t[1]), k_t[1])

      def format_result(k_v):
        return ('%s-%s' % (k_v[0], len(k_v[1])), set(k_v[1]))

      result = (
          p
          | beam.Create([1, 1, 2, 3, 4, 5, 10, 11])
          | beam.FlatMap(lambda t: [('A', t), ('B', t + 5)])
          | beam.Map(construct_timestamped)
          | beam.WindowInto(
              FixedWindows(10),
              trigger=_Never(),
              accumulation_mode=AccumulationMode.DISCARDING)
          | beam.GroupByKey()
          | beam.Map(format_result))
      assert_that(
          result,
          equal_to(
              list({
                  'A-2': {10, 11},
                  'A-6': {1, 2, 3, 4, 5},
                  'B-5': {6, 7, 8, 9},
                  'B-3': {10, 15, 16},
              }.items())))

  def test_multiple_accumulating_firings(self):
    # PCollection will contain elements from 1 to 10.
    elements = [i for i in range(1, 11)]

    ts = TestStream().advance_watermark_to(0)
    for i in elements:
      ts.add_elements([('key', str(i))])
      if i % 5 == 0:
        ts.advance_watermark_to(i)
        ts.advance_processing_time(5)
    ts.advance_watermark_to_infinity()

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    with TestPipeline(options=options) as p:
      records = (
          p
          | ts
          | beam.WindowInto(
              FixedWindows(10),
              accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
              trigger=AfterWatermark(
                  early=AfterAll(AfterCount(1), AfterProcessingTime(5))))
          | beam.GroupByKey()
          | beam.FlatMap(lambda x: x[1]))

    # The trigger should fire twice. Once after 5 seconds, and once after 10.
    # The firings should accumulate the output.
    first_firing = [str(i) for i in elements if i <= 5]
    second_firing = [str(i) for i in elements]
    assert_that(records, equal_to(first_firing + second_firing))

  def test_on_pane_watermark_hold_no_pipeline_stall(self):
    """A regression test added for
    https://issues.apache.org/jira/browse/BEAM-10054."""
    START_TIMESTAMP = 1534842000

    test_stream = TestStream()
    test_stream.add_elements(['a'])
    test_stream.advance_processing_time(START_TIMESTAMP + 1)
    test_stream.advance_watermark_to(START_TIMESTAMP + 1)
    test_stream.add_elements(['b'])
    test_stream.advance_processing_time(START_TIMESTAMP + 2)
    test_stream.advance_watermark_to(START_TIMESTAMP + 2)

    with TestPipeline(options=PipelineOptions(['--streaming'])) as p:
      # pylint: disable=expression-not-assigned
      (
          p
          | 'TestStream' >> test_stream
          | 'timestamp' >>
          beam.Map(lambda x: beam.window.TimestampedValue(x, START_TIMESTAMP))
          | 'kv' >> beam.Map(lambda x: (x, x))
          | 'window_1m' >> beam.WindowInto(
              beam.window.FixedWindows(60),
              trigger=trigger.AfterAny(
                  trigger.AfterProcessingTime(3600), trigger.AfterWatermark()),
              accumulation_mode=trigger.AccumulationMode.DISCARDING)
          | 'group_by_key' >> beam.GroupByKey()
          | 'filter' >> beam.Map(lambda x: x))


class TranscriptTest(unittest.TestCase):

  # We must prepend an underscore to this name so that the open-source unittest
  # runner does not execute this method directly as a test.
  @classmethod
  def _create_test(cls, spec):
    counter = 0
    name = spec.get('name', 'unnamed')
    unique_name = 'test_' + name
    while hasattr(cls, unique_name):
      counter += 1
      unique_name = 'test_%s_%d' % (name, counter)
    test_method = lambda self: self._run_log_test(spec)
    test_method.__name__ = unique_name
    test_method.__test__ = True
    setattr(cls, unique_name, test_method)

  # We must prepend an underscore to this name so that the open-source unittest
  # runner does not execute this method directly as a test.
  @classmethod
  def _create_tests(cls, transcript_filename):
    for spec in yaml.load_all(open(transcript_filename),
                              Loader=yaml.SafeLoader):
      cls._create_test(spec)

  def _run_log_test(self, spec):
    if 'error' in spec:
      self.assertRaisesRegex(Exception, spec['error'], self._run_log, spec)
    else:
      self._run_log(spec)

  def _run_log(self, spec):
    def parse_int_list(s):
      """Parses strings like '[1, 2, 3]'."""
      s = s.strip()
      assert s[0] == '[' and s[-1] == ']', s
      if not s[1:-1].strip():
        return []
      return [int(x) for x in s[1:-1].split(',')]

    def split_args(s):
      """Splits 'a, b, [c, d]' into ['a', 'b', '[c, d]']."""
      args = []
      start = 0
      depth = 0
      for ix in range(len(s)):
        c = s[ix]
        if c in '({[':
          depth += 1
        elif c in ')}]':
          depth -= 1
        elif c == ',' and depth == 0:
          args.append(s[start:ix].strip())
          start = ix + 1
      assert depth == 0, s
      args.append(s[start:].strip())
      return args

    def parse(s, names):
      """Parse (recursive) 'Foo(arg, kw=arg)' for Foo in the names dict."""
      s = s.strip()
      if s in names:
        return names[s]
      elif s[0] == '[':
        return parse_int_list(s)
      elif '(' in s:
        assert s[-1] == ')', s
        callee = parse(s[:s.index('(')], names)
        posargs = []
        kwargs = {}
        for arg in split_args(s[s.index('(') + 1:-1]):
          if '=' in arg:
            kw, value = arg.split('=', 1)
            kwargs[kw] = parse(value, names)
          else:
            posargs.append(parse(arg, names))
        return callee(*posargs, **kwargs)
      else:
        try:
          return int(s)
        except ValueError:
          raise ValueError('Unknown function: %s' % s)

    def parse_fn(s, names):
      """Like parse(), but implicitly calls no-arg constructors."""
      fn = parse(s, names)
      if isinstance(fn, type):
        return fn()
      return fn

    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.transforms import window as window_module
    # pylint: enable=wrong-import-order, wrong-import-position
    window_fn_names = dict(window_module.__dict__)
    # yapf: disable
    window_fn_names.update({
        'CustomTimestampingFixedWindowsWindowFn':
            CustomTimestampingFixedWindowsWindowFn
    })
    # yapf: enable
    trigger_names = {'Default': DefaultTrigger}
    trigger_names.update(trigger.__dict__)

    window_fn = parse_fn(
        spec.get('window_fn', 'GlobalWindows'), window_fn_names)
    trigger_fn = parse_fn(spec.get('trigger_fn', 'Default'), trigger_names)
    accumulation_mode = getattr(
        AccumulationMode, spec.get('accumulation_mode', 'ACCUMULATING').upper())
    timestamp_combiner = getattr(
        TimestampCombiner,
        spec.get('timestamp_combiner', 'OUTPUT_AT_EOW').upper())
    allowed_lateness = spec.get('allowed_lateness', 0.000)

    def only_element(xs):
      x, = list(xs)
      return x

    transcript = [only_element(line.items()) for line in spec['transcript']]

    self._execute(
        window_fn,
        trigger_fn,
        accumulation_mode,
        timestamp_combiner,
        allowed_lateness,
        transcript,
        spec)


def _windowed_value_info(windowed_value):
  # Currently some runners operate at the millisecond level, and some at the
  # microsecond level.  Trigger transcript timestamps are expressed as
  # integral units of the finest granularity, whatever that may be.
  # In these tests we interpret them as integral seconds and then truncate
  # the results to integral seconds to allow for portability across
  # different sub-second resolutions.
  window, = windowed_value.windows
  return {
      'window': [int(window.start), int(window.max_timestamp())],
      'values': sorted(windowed_value.value),
      'timestamp': int(windowed_value.timestamp),
      'index': windowed_value.pane_info.index,
      'nonspeculative_index': windowed_value.pane_info.nonspeculative_index,
      'early': windowed_value.pane_info.timing == PaneInfoTiming.EARLY,
      'late': windowed_value.pane_info.timing == PaneInfoTiming.LATE,
      'final': windowed_value.pane_info.is_last,
  }


def _windowed_value_info_map_fn(
    k,
    vs,
    window=beam.DoFn.WindowParam,
    t=beam.DoFn.TimestampParam,
    p=beam.DoFn.PaneInfoParam):
  return (
      k,
      _windowed_value_info(
          WindowedValue(vs, windows=[window], timestamp=t, pane_info=p)))


def _windowed_value_info_check(actual, expected, key=None):

  key_string = ' for %s' % key if key else ''

  def format(panes):
    return '\n[%s]\n' % '\n '.join(
        str(pane)
        for pane in sorted(panes, key=lambda pane: pane.get('timestamp', None)))

  if len(actual) > len(expected):
    raise AssertionError(
        'Unexpected output%s: expected %s but got %s' %
        (key_string, format(expected), format(actual)))
  elif len(expected) > len(actual):
    raise AssertionError(
        'Unmatched output%s: expected %s but got %s' %
        (key_string, format(expected), format(actual)))
  else:

    def diff(actual, expected):
      for key in sorted(expected.keys(), reverse=True):
        if key in actual:
          if actual[key] != expected[key]:
            return key

    for output in actual:
      diffs = [diff(output, pane) for pane in expected]
      if all(diffs):
        raise AssertionError(
            'Unmatched output%s: %s not found in %s (diffs in %s)' %
            (key_string, output, format(expected), diffs))


class _ConcatCombineFn(beam.CombineFn):
  create_accumulator = lambda self: []  # type: ignore[var-annotated]
  add_input = lambda self, acc, element: acc.append(element) or acc
  merge_accumulators = lambda self, accs: sum(accs, [])  # type: ignore[var-annotated]
  extract_output = lambda self, acc: acc


class TriggerDriverTranscriptTest(TranscriptTest):
  def _execute(
      self,
      window_fn,
      trigger_fn,
      accumulation_mode,
      timestamp_combiner,
      allowed_lateness,
      transcript,
      unused_spec):

    driver = GeneralTriggerDriver(
        Windowing(
            window_fn,
            trigger_fn,
            accumulation_mode,
            timestamp_combiner,
            allowed_lateness),
        TestClock())
    state = InMemoryUnmergedState()
    output = []
    watermark = MIN_TIMESTAMP

    def fire_timers():
      to_fire = state.get_and_clear_timers(watermark)
      while to_fire:
        for timer_window, (name, time_domain, t_timestamp) in to_fire:
          for wvalue in driver.process_timer(timer_window,
                                             name,
                                             time_domain,
                                             t_timestamp,
                                             state):
            output.append(_windowed_value_info(wvalue))
        to_fire = state.get_and_clear_timers(watermark)

    for action, params in transcript:

      if action != 'expect':
        # Fail if we have output that was not expected in the transcript.
        self.assertEqual([],
                         output,
                         msg='Unexpected output: %s before %s: %s' %
                         (output, action, params))

      if action == 'input':
        bundle = [
            WindowedValue(t, t, window_fn.assign(WindowFn.AssignContext(t, t)))
            for t in params
        ]
        output = [
            _windowed_value_info(wv) for wv in driver.process_elements(
                state, bundle, watermark, watermark)
        ]
        fire_timers()

      elif action == 'watermark':
        watermark = params
        fire_timers()

      elif action == 'expect':
        for expected_output in params:
          for candidate in output:
            if all(candidate[k] == expected_output[k] for k in candidate
                   if k in expected_output):
              output.remove(candidate)
              break
          else:
            self.fail('Unmatched output %s in %s' % (expected_output, output))

      elif action == 'state':
        # TODO(robertwb): Implement once we support allowed lateness.
        pass

      else:
        self.fail('Unknown action: ' + action)

    # Fail if we have output that was not expected in the transcript.
    self.assertEqual([], output, msg='Unexpected output: %s' % output)


class BaseTestStreamTranscriptTest(TranscriptTest):
  """A suite of TestStream-based tests based on trigger transcript entries.
  """
  def _execute(
      self,
      window_fn,
      trigger_fn,
      accumulation_mode,
      timestamp_combiner,
      allowed_lateness,
      transcript,
      spec):

    runner_name = TestPipeline().runner.__class__.__name__
    if runner_name in spec.get('broken_on', ()):
      self.skipTest('Known to be broken on %s' % runner_name)

    is_order_agnostic = (
        isinstance(trigger_fn, DefaultTrigger) and
        accumulation_mode == AccumulationMode.ACCUMULATING)

    if is_order_agnostic:
      reshuffle_seed = random.randrange(1 << 20)
      keys = [
          u'original',
          u'reversed',
          u'reshuffled(%s)' % reshuffle_seed,
          u'one-element-bundles',
          u'one-element-bundles-reversed',
          u'two-element-bundles'
      ]
    else:
      keys = [u'key1', u'key2']

    # Elements are encoded as a json strings to allow other languages to
    # decode elements while executing the test stream.
    # TODO(BEAM-8600): Eliminate these gymnastics.
    test_stream = TestStream(coder=coders.StrUtf8Coder()).with_output_types(str)
    for action, params in transcript:
      if action == 'expect':
        test_stream.add_elements([json.dumps(('expect', params))])
      else:
        test_stream.add_elements([json.dumps(('expect', []))])
        if action == 'input':

          def keyed(key, values):
            return [json.dumps(('input', (key, v))) for v in values]

          if is_order_agnostic:
            # Must match keys above.
            test_stream.add_elements(keyed('original', params))
            test_stream.add_elements(keyed('reversed', reversed(params)))
            r = random.Random(reshuffle_seed)
            reshuffled = list(params)
            r.shuffle(reshuffled)
            test_stream.add_elements(
                keyed('reshuffled(%s)' % reshuffle_seed, reshuffled))
            for v in params:
              test_stream.add_elements(keyed('one-element-bundles', [v]))
            for v in reversed(params):
              test_stream.add_elements(
                  keyed('one-element-bundles-reversed', [v]))
            for ix in range(0, len(params), 2):
              test_stream.add_elements(
                  keyed('two-element-bundles', params[ix:ix + 2]))
          else:
            for key in keys:
              test_stream.add_elements(keyed(key, params))
        elif action == 'watermark':
          test_stream.advance_watermark_to(params)
        elif action == 'clock':
          test_stream.advance_processing_time(params)
        elif action == 'state':
          pass  # Requires inspection of implementation details.
        else:
          raise ValueError('Unexpected action: %s' % action)
    test_stream.add_elements([json.dumps(('expect', []))])
    test_stream.advance_watermark_to_infinity()

    read_test_stream = test_stream | beam.Map(json.loads)

    class Check(beam.DoFn):
      """A StatefulDoFn that verifies outputs are produced as expected.

      This DoFn takes in two kinds of inputs, actual outputs and
      expected outputs.  When an actual output is received, it is buffered
      into state, and when an expected output is received, this buffered
      state is retrieved and compared against the expected value(s) to ensure
      they match.

      The key is ignored, but all items must be on the same key to share state.
      """
      def __init__(self, allow_out_of_order=True):
        # Some runners don't support cross-stage TestStream semantics.
        self.allow_out_of_order = allow_out_of_order

      def process(
          self,
          element,
          seen=beam.DoFn.StateParam(
              beam.transforms.userstate.BagStateSpec(
                  'seen', beam.coders.FastPrimitivesCoder())),
          expected=beam.DoFn.StateParam(
              beam.transforms.userstate.BagStateSpec(
                  'expected', beam.coders.FastPrimitivesCoder()))):
        key, (action, data) = element

        if self.allow_out_of_order:
          if action == 'expect' and not list(seen.read()):
            if data:
              expected.add(data)
            return
          elif action == 'actual' and list(expected.read()):
            seen.add(data)
            all_data = list(seen.read())
            all_expected = list(expected.read())
            if len(all_data) == len(all_expected[0]):
              expected.clear()
              for expect in all_expected[1:]:
                expected.add(expect)
              action, data = 'expect', all_expected[0]
            else:
              return

        if action == 'actual':
          seen.add(data)

        elif action == 'expect':
          actual = list(seen.read())
          seen.clear()
          _windowed_value_info_check(actual, data, key)

        else:
          raise ValueError('Unexpected action: %s' % action)

    @ptransform.ptransform_fn
    def CheckAggregation(inputs_and_expected, aggregation):
      # Split the test stream into a branch of to-be-processed elements, and
      # a branch of expected results.
      inputs, expected = (
          inputs_and_expected
          | beam.MapTuple(
              lambda tag, value: beam.pvalue.TaggedOutput(tag, value),
              ).with_outputs('input', 'expect'))

      # Process the inputs with the given windowing to produce actual outputs.
      outputs = (
          inputs
          | beam.MapTuple(
              lambda key, value: TimestampedValue((key, value), value))
          | beam.WindowInto(
              window_fn,
              trigger=trigger_fn,
              accumulation_mode=accumulation_mode,
              timestamp_combiner=timestamp_combiner,
              allowed_lateness=allowed_lateness)
          | aggregation
          | beam.MapTuple(_windowed_value_info_map_fn)
          # Place outputs back into the global window to allow flattening
          # and share a single state in Check.
          | 'Global' >> beam.WindowInto(beam.transforms.window.GlobalWindows()))
      # Feed both the expected and actual outputs to Check() for comparison.
      tagged_expected = (
          expected | beam.FlatMap(
              lambda value: [(key, ('expect', value)) for key in keys]))
      tagged_outputs = (
          outputs | beam.MapTuple(lambda key, value: (key, ('actual', value))))
      # pylint: disable=expression-not-assigned
      ([tagged_expected, tagged_outputs]
       | beam.Flatten()
       | beam.ParDo(Check(self.allow_out_of_order)))

    with TestPipeline() as p:
      # TODO(BEAM-8601): Pass this during pipeline construction.
      p._options.view_as(StandardOptions).streaming = True

      # We can have at most one test stream per pipeline, so we share it.
      inputs_and_expected = p | read_test_stream
      _ = inputs_and_expected | CheckAggregation(beam.GroupByKey())
      _ = inputs_and_expected | CheckAggregation(
          beam.CombinePerKey(_ConcatCombineFn()))


class TestStreamTranscriptTest(BaseTestStreamTranscriptTest):
  allow_out_of_order = False


class WeakTestStreamTranscriptTest(BaseTestStreamTranscriptTest):
  allow_out_of_order = True


class BatchTranscriptTest(TranscriptTest):
  def _execute(
      self,
      window_fn,
      trigger_fn,
      accumulation_mode,
      timestamp_combiner,
      allowed_lateness,
      transcript,
      spec):
    if timestamp_combiner == TimestampCombiner.OUTPUT_AT_EARLIEST_TRANSFORMED:
      self.skipTest(
          'Non-fnapi timestamp combiner: %s' % spec.get('timestamp_combiner'))

    if accumulation_mode != AccumulationMode.ACCUMULATING:
      self.skipTest('Batch mode only makes sense for accumulating.')

    watermark = MIN_TIMESTAMP
    for action, params in transcript:
      if action == 'watermark':
        watermark = params
      elif action == 'input':
        if any(t <= watermark for t in params):
          self.skipTest('Batch mode never has late data.')

    inputs = sum([vs for action, vs in transcript if action == 'input'], [])
    final_panes_by_window = {}
    for action, params in transcript:
      if action == 'expect':
        for expected in params:
          trimmed = {}
          for field in ('window', 'values', 'timestamp'):
            if field in expected:
              trimmed[field] = expected[field]
          final_panes_by_window[tuple(expected['window'])] = trimmed
    final_panes = list(final_panes_by_window.values())

    if window_fn.is_merging():
      merged_away = set()

      class MergeContext(WindowFn.MergeContext):
        def merge(_, to_be_merged, merge_result):
          for window in to_be_merged:
            if window != merge_result:
              merged_away.add(window)

      all_windows = [IntervalWindow(*pane['window']) for pane in final_panes]
      window_fn.merge(MergeContext(all_windows))
      final_panes = [
          pane for pane in final_panes
          if IntervalWindow(*pane['window']) not in merged_away
      ]

    with TestPipeline() as p:
      input_pc = (
          p
          | beam.Create(inputs)
          | beam.Map(lambda t: TimestampedValue(('key', t), t))
          | beam.WindowInto(
              window_fn,
              trigger=trigger_fn,
              accumulation_mode=accumulation_mode,
              timestamp_combiner=timestamp_combiner,
              allowed_lateness=allowed_lateness))

      grouped = input_pc | 'Grouped' >> (
          beam.GroupByKey()
          | beam.MapTuple(_windowed_value_info_map_fn)
          | beam.MapTuple(lambda _, value: value))

      combined = input_pc | 'Combined' >> (
          beam.CombinePerKey(_ConcatCombineFn())
          | beam.MapTuple(_windowed_value_info_map_fn)
          | beam.MapTuple(lambda _, value: value))

      assert_that(
          grouped,
          lambda actual: _windowed_value_info_check(actual, final_panes),
          label='CheckGrouped')

      assert_that(
          combined,
          lambda actual: _windowed_value_info_check(actual, final_panes),
          label='CheckCombined')


TRANSCRIPT_TEST_FILE = os.path.join(
    os.path.dirname(__file__),
    '..',
    'testing',
    'data',
    'trigger_transcripts.yaml')
if os.path.exists(TRANSCRIPT_TEST_FILE):
  TriggerDriverTranscriptTest._create_tests(TRANSCRIPT_TEST_FILE)
  TestStreamTranscriptTest._create_tests(TRANSCRIPT_TEST_FILE)
  WeakTestStreamTranscriptTest._create_tests(TRANSCRIPT_TEST_FILE)
  BatchTranscriptTest._create_tests(TRANSCRIPT_TEST_FILE)

if __name__ == '__main__':
  unittest.main()
