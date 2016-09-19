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

import collections
import os.path
import pickle
import unittest

import yaml

import apache_beam as beam
from apache_beam.pipeline import Pipeline
from apache_beam.transforms import trigger
from apache_beam.transforms.core import Windowing
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.trigger import AfterAll
from apache_beam.transforms.trigger import AfterCount
from apache_beam.transforms.trigger import AfterEach
from apache_beam.transforms.trigger import AfterFirst
from apache_beam.transforms.trigger import AfterWatermark
from apache_beam.transforms.trigger import DefaultTrigger
from apache_beam.transforms.trigger import GeneralTriggerDriver
from apache_beam.transforms.trigger import InMemoryUnmergedState
from apache_beam.transforms.trigger import Repeatedly
from apache_beam.transforms.util import assert_that, equal_to
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import IntervalWindow
from apache_beam.transforms.window import MIN_TIMESTAMP
from apache_beam.transforms.window import OutputTimeFn
from apache_beam.transforms.window import Sessions
from apache_beam.transforms.window import TimestampedValue
from apache_beam.transforms.window import WindowedValue
from apache_beam.transforms.window import WindowFn


class CustomTimestampingFixedWindowsWindowFn(FixedWindows):
  """WindowFn for testing custom timestamping."""

  def get_transformed_output_time(self, unused_window, input_timestamp):
    return input_timestamp + 100


class TriggerTest(unittest.TestCase):

  def run_trigger_simple(self, window_fn, trigger_fn, accumulation_mode,
                         timestamped_data, expected_panes, *groupings,
                         **kwargs):
    late_data = kwargs.pop('late_data', [])
    assert not kwargs

    def bundle_data(data, size):
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
      bundles = []
      bundle = []
      for timestamp, elem in timestamped_data:
        windows = window_fn.assign(WindowFn.AssignContext(timestamp, elem))
        bundle.append(WindowedValue(elem, timestamp, windows))
        if len(bundle) == group_by:
          bundles.append(bundle)
          bundle = []
      bundles.append(bundle)
      self.run_trigger(window_fn, trigger_fn, accumulation_mode,
                       bundle_data(timestamped_data, group_by),
                       bundle_data(late_data, group_by),
                       expected_panes)

  def run_trigger(self, window_fn, trigger_fn, accumulation_mode,
                  bundles, late_bundles,
                  expected_panes):
    actual_panes = collections.defaultdict(list)
    driver = GeneralTriggerDriver(
        Windowing(window_fn, trigger_fn, accumulation_mode))
    state = InMemoryUnmergedState()

    for bundle in bundles:
      for wvalue in driver.process_elements(state, bundle, MIN_TIMESTAMP):
        window, = wvalue.windows
        actual_panes[window].append(set(wvalue.value))

    while state.timers:
      for timer_window, (name, time_domain, timestamp) in (
          state.get_and_clear_timers()):
        for wvalue in driver.process_timer(
            timer_window, name, time_domain, timestamp, state):
          window, = wvalue.windows
          actual_panes[window].append(set(wvalue.value))

    for bundle in late_bundles:
      for wvalue in driver.process_elements(state, bundle, MIN_TIMESTAMP):
        window, = wvalue.windows
        actual_panes[window].append(set(wvalue.value))

      while state.timers:
        for timer_window, (name, time_domain, timestamp) in (
            state.get_and_clear_timers()):
          for wvalue in driver.process_timer(
              timer_window, name, time_domain, timestamp, state):
            window, = wvalue.windows
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
        3)

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
        AfterFirst(AfterCount(2), AfterWatermark()),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (2, 'b'), (3, 'c')],
        {IntervalWindow(0, 10): [set('ab')]},
        1,
        2)
    self.run_trigger_simple(
        FixedWindows(10),  # pyformat break
        AfterFirst(AfterCount(5), AfterWatermark()),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (2, 'b'), (3, 'c')],
        {IntervalWindow(0, 10): [set('abc')]},
        1,
        2,
        late_data=[(1, 'x'), (2, 'y'), (3, 'z')])

  def test_repeatedly_after_first(self):
    self.run_trigger_simple(
        FixedWindows(100),  # pyformat break
        Repeatedly(AfterFirst(AfterCount(3), AfterWatermark())),
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
        2)

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
        6)

  def test_sessions_watermark(self):
    self.run_trigger_simple(
        Sessions(10),  # pyformat break
        AfterWatermark(),
        AccumulationMode.ACCUMULATING,
        [(1, 'a'), (2, 'b')],
        {IntervalWindow(1, 12): [set('ab')]},
        1,
        2)

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
        6)

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
    global_window = trigger.GlobalWindow(),
    driver = trigger.DefaultGlobalBatchTriggerDriver()
    unpicklable = (WindowedValue(k, 0, global_window)
                   for k in range(10))
    with self.assertRaises(TypeError):
      pickle.dumps(unpicklable)
    for unwindowed in driver.process_elements(None, unpicklable, None):
      self.assertEqual(pickle.loads(pickle.dumps(unwindowed)).value,
                       range(10))

class TriggerPipelineTest(unittest.TestCase):

  def test_after_count(self):
    p = Pipeline('DirectPipelineRunner')
    result = (p
              | beam.Create([1, 2, 3, 4, 5, 10, 11])
              | beam.FlatMap(lambda t: [('A', t), ('B', t + 5)])
              | beam.Map(lambda (k, t): TimestampedValue((k, t), t))
              | beam.WindowInto(FixedWindows(10), trigger=AfterCount(3),
                                accumulation_mode=AccumulationMode.DISCARDING)
              | beam.GroupByKey()
              | beam.Map(lambda (k, v): ('%s-%s' % (k, len(v)), set(v))))
    assert_that(result, equal_to(
        {
            'A-5': {1, 2, 3, 4, 5},
            # A-10, A-11 never emitted due to AfterCount(3) never firing.
            'B-4': {6, 7, 8, 9},
            'B-3': {10, 15, 16},
        }.iteritems()))


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
    setattr(cls, unique_name, lambda self: self._run_log_test(spec))

  # We must prepend an underscore to this name so that the open-source unittest
  # runner does not execute this method directly as a test.
  @classmethod
  def _create_tests(cls, transcript_filename):
    for spec in yaml.load_all(open(transcript_filename)):
      cls._create_test(spec)

  def _run_log_test(self, spec):
    if 'error' in spec:
      self.assertRaisesRegexp(
          AssertionError, spec['error'], self._run_log, spec)
    else:
      self._run_log(spec)

  def _run_log(self, spec):

    def parse_int_list(s):
      """Parses strings like '[1, 2, 3]'."""
      s = s.strip()
      assert s[0] == '[' and s[-1] == ']', s
      if not s[1:-1].strip():
        return []
      else:
        return [int(x) for x in s[1:-1].split(',')]

    def split_args(s):
      """Splits 'a, b, [c, d]' into ['a', 'b', '[c, d]']."""
      args = []
      start = 0
      depth = 0
      for ix in xrange(len(s)):
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
      else:
        return fn

    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.transforms import window as window_module
    from apache_beam.transforms import trigger as trigger_module
    # pylint: enable=wrong-import-order, wrong-import-position
    window_fn_names = dict(window_module.__dict__)
    window_fn_names.update({'CustomTimestampingFixedWindowsWindowFn':
                            CustomTimestampingFixedWindowsWindowFn})
    trigger_names = {'Default': DefaultTrigger}
    trigger_names.update(trigger_module.__dict__)

    window_fn = parse_fn(spec.get('window_fn', 'GlobalWindows'),
                         window_fn_names)
    trigger_fn = parse_fn(spec.get('trigger_fn', 'Default'), trigger_names)
    accumulation_mode = getattr(
        AccumulationMode, spec.get('accumulation_mode', 'ACCUMULATING').upper())
    output_time_fn = getattr(
        OutputTimeFn, spec.get('output_time_fn', 'OUTPUT_AT_EOW').upper())

    driver = GeneralTriggerDriver(
        Windowing(window_fn, trigger_fn, accumulation_mode, output_time_fn))
    state = InMemoryUnmergedState()
    output = []
    watermark = MIN_TIMESTAMP

    def fire_timers():
      to_fire = state.get_and_clear_timers(watermark)
      while to_fire:
        for timer_window, (name, time_domain, t_timestamp) in to_fire:
          for wvalue in driver.process_timer(
              timer_window, name, time_domain, t_timestamp, state):
            window, = wvalue.windows
            output.append({'window': [window.start, window.end - 1],
                           'values': sorted(wvalue.value),
                           'timestamp': wvalue.timestamp})
        to_fire = state.get_and_clear_timers(watermark)

    for line in spec['transcript']:

      action, params = line.items()[0]

      if action != 'expect':
        # Fail if we have output that was not expected in the transcript.
        self.assertEquals(
            [], output, msg='Unexpected output: %s before %s' % (output, line))

      if action == 'input':
        bundle = [
            WindowedValue(t, t, window_fn.assign(WindowFn.AssignContext(t, t)))
            for t in params]
        output = [{'window': [wvalue.windows[0].start,
                              wvalue.windows[0].end - 1],
                   'values': sorted(wvalue.value),
                   'timestamp': wvalue.timestamp}
                  for wvalue
                  in driver.process_elements(state, bundle, watermark)]
        fire_timers()

      elif action == 'watermark':
        watermark = params
        fire_timers()

      elif action == 'expect':
        for expected_output in params:
          for candidate in output:
            if all(candidate[k] == expected_output[k]
                   for k in candidate if k in expected_output):
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
    self.assertEquals([], output, msg='Unexpected output: %s' % output)


TRANSCRIPT_TEST_FILE = os.path.join(os.path.dirname(__file__),
                                    'trigger_transcripts.yaml')
if os.path.exists(TRANSCRIPT_TEST_FILE):
  TranscriptTest._create_tests(TRANSCRIPT_TEST_FILE)


if __name__ == '__main__':
  unittest.main()
