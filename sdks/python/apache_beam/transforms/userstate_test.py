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

"""Unit tests for the Beam State and Timer API interfaces."""
from __future__ import absolute_import

import unittest

import mock

import apache_beam as beam
from apache_beam.coders import BytesCoder
from apache_beam.coders import IterableCoder
from apache_beam.coders import VarIntCoder
from apache_beam.runners.common import DoFnSignature
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import equal_to
from apache_beam.transforms import userstate
from apache_beam.transforms.combiners import ToListCombineFn
from apache_beam.transforms.combiners import TopCombineFn
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import CombiningValueStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import get_dofn_specs
from apache_beam.transforms.userstate import is_stateful_dofn
from apache_beam.transforms.userstate import on_timer
from apache_beam.transforms.userstate import validate_stateful_dofn


class TestStatefulDoFn(DoFn):
  """An example stateful DoFn with state and timers."""

  BUFFER_STATE_1 = BagStateSpec('buffer', BytesCoder())
  BUFFER_STATE_2 = BagStateSpec('buffer2', VarIntCoder())
  EXPIRY_TIMER_1 = TimerSpec('expiry1', TimeDomain.WATERMARK)
  EXPIRY_TIMER_2 = TimerSpec('expiry2', TimeDomain.WATERMARK)
  EXPIRY_TIMER_3 = TimerSpec('expiry3', TimeDomain.WATERMARK)

  def process(self, element, t=DoFn.TimestampParam,
              buffer_1=DoFn.StateParam(BUFFER_STATE_1),
              buffer_2=DoFn.StateParam(BUFFER_STATE_2),
              timer_1=DoFn.TimerParam(EXPIRY_TIMER_1),
              timer_2=DoFn.TimerParam(EXPIRY_TIMER_2)):
    yield element

  @on_timer(EXPIRY_TIMER_1)
  def on_expiry_1(self,
                  buffer=DoFn.StateParam(BUFFER_STATE_1),
                  timer_1=DoFn.TimerParam(EXPIRY_TIMER_1),
                  timer_2=DoFn.TimerParam(EXPIRY_TIMER_2),
                  timer_3=DoFn.TimerParam(EXPIRY_TIMER_3)):
    yield 'expired1'

  @on_timer(EXPIRY_TIMER_2)
  def on_expiry_2(self,
                  buffer=DoFn.StateParam(BUFFER_STATE_2),
                  timer_2=DoFn.TimerParam(EXPIRY_TIMER_2),
                  timer_3=DoFn.TimerParam(EXPIRY_TIMER_3)):
    yield 'expired2'

  @on_timer(EXPIRY_TIMER_3)
  def on_expiry_3(self,
                  buffer_1=DoFn.StateParam(BUFFER_STATE_1),
                  buffer_2=DoFn.StateParam(BUFFER_STATE_2),
                  timer_3=DoFn.TimerParam(EXPIRY_TIMER_3)):
    yield 'expired3'


class InterfaceTest(unittest.TestCase):

  def _validate_dofn(self, dofn):
    # Construction of DoFnSignature performs validation of the given DoFn.
    # In particular, it ends up calling userstate._validate_stateful_dofn.
    # That behavior is explicitly tested below in test_validate_dofn()
    return DoFnSignature(dofn)

  @mock.patch(
      'apache_beam.transforms.userstate.validate_stateful_dofn')
  def test_validate_dofn(self, unused_mock):
    dofn = TestStatefulDoFn()
    self._validate_dofn(dofn)
    userstate.validate_stateful_dofn.assert_called_with(dofn)

  def test_spec_construction(self):
    BagStateSpec('statename', VarIntCoder())
    with self.assertRaises(AssertionError):
      BagStateSpec(123, VarIntCoder())
    CombiningValueStateSpec('statename', VarIntCoder(), TopCombineFn(10))
    with self.assertRaises(AssertionError):
      CombiningValueStateSpec(123, VarIntCoder(), TopCombineFn(10))
    with self.assertRaises(TypeError):
      CombiningValueStateSpec('statename', VarIntCoder(), object())
    # BagStateSpec('bag', )
    # TODO: add more spec tests
    with self.assertRaises(ValueError):
      DoFn.TimerParam(BagStateSpec('elements', BytesCoder()))

    TimerSpec('timer', TimeDomain.WATERMARK)
    TimerSpec('timer', TimeDomain.REAL_TIME)
    with self.assertRaises(ValueError):
      TimerSpec('timer', 'bogus_time_domain')
    with self.assertRaises(ValueError):
      DoFn.StateParam(TimerSpec('timer', TimeDomain.WATERMARK))

  def test_param_construction(self):
    with self.assertRaises(ValueError):
      DoFn.StateParam(TimerSpec('timer', TimeDomain.WATERMARK))
    with self.assertRaises(ValueError):
      DoFn.TimerParam(BagStateSpec('elements', BytesCoder()))

  def test_stateful_dofn_detection(self):
    self.assertFalse(is_stateful_dofn(DoFn()))
    self.assertTrue(is_stateful_dofn(TestStatefulDoFn()))

  def test_good_signatures(self):
    class BasicStatefulDoFn(DoFn):
      BUFFER_STATE = BagStateSpec('buffer', BytesCoder())
      EXPIRY_TIMER = TimerSpec('expiry1', TimeDomain.WATERMARK)

      def process(self, element, buffer=DoFn.StateParam(BUFFER_STATE),
                  timer1=DoFn.TimerParam(EXPIRY_TIMER)):
        yield element

      @on_timer(EXPIRY_TIMER)
      def expiry_callback(self, element, timer=DoFn.TimerParam(EXPIRY_TIMER)):
        yield element

    # Validate get_dofn_specs() and timer callbacks in
    # DoFnSignature.
    stateful_dofn = BasicStatefulDoFn()
    signature = self._validate_dofn(stateful_dofn)
    expected_specs = (set([BasicStatefulDoFn.BUFFER_STATE]),
                      set([BasicStatefulDoFn.EXPIRY_TIMER]))
    self.assertEqual(expected_specs,
                     get_dofn_specs(stateful_dofn))
    self.assertEqual(
        stateful_dofn.expiry_callback,
        signature.timer_methods[BasicStatefulDoFn.EXPIRY_TIMER].method_value)

    stateful_dofn = TestStatefulDoFn()
    signature = self._validate_dofn(stateful_dofn)
    expected_specs = (set([TestStatefulDoFn.BUFFER_STATE_1,
                           TestStatefulDoFn.BUFFER_STATE_2]),
                      set([TestStatefulDoFn.EXPIRY_TIMER_1,
                           TestStatefulDoFn.EXPIRY_TIMER_2,
                           TestStatefulDoFn.EXPIRY_TIMER_3]))
    self.assertEqual(expected_specs,
                     get_dofn_specs(stateful_dofn))
    self.assertEqual(
        stateful_dofn.on_expiry_1,
        signature.timer_methods[TestStatefulDoFn.EXPIRY_TIMER_1].method_value)
    self.assertEqual(
        stateful_dofn.on_expiry_2,
        signature.timer_methods[TestStatefulDoFn.EXPIRY_TIMER_2].method_value)
    self.assertEqual(
        stateful_dofn.on_expiry_3,
        signature.timer_methods[TestStatefulDoFn.EXPIRY_TIMER_3].method_value)

  def test_bad_signatures(self):
    # (1) The same state parameter is duplicated on the process method.
    class BadStatefulDoFn1(DoFn):
      BUFFER_STATE = BagStateSpec('buffer', BytesCoder())

      def process(self, element, b1=DoFn.StateParam(BUFFER_STATE),
                  b2=DoFn.StateParam(BUFFER_STATE)):
        yield element

    with self.assertRaises(ValueError):
      self._validate_dofn(BadStatefulDoFn1())

    # (2) The same timer parameter is duplicated on the process method.
    class BadStatefulDoFn2(DoFn):
      TIMER = TimerSpec('timer', TimeDomain.WATERMARK)

      def process(self, element, t1=DoFn.TimerParam(TIMER),
                  t2=DoFn.TimerParam(TIMER)):
        yield element

    with self.assertRaises(ValueError):
      self._validate_dofn(BadStatefulDoFn2())

    # (3) The same state parameter is duplicated on the on_timer method.
    class BadStatefulDoFn3(DoFn):
      BUFFER_STATE = BagStateSpec('buffer', BytesCoder())
      EXPIRY_TIMER_1 = TimerSpec('expiry1', TimeDomain.WATERMARK)
      EXPIRY_TIMER_2 = TimerSpec('expiry2', TimeDomain.WATERMARK)

      @on_timer(EXPIRY_TIMER_1)
      def expiry_callback(self, element, b1=DoFn.StateParam(BUFFER_STATE),
                          b2=DoFn.StateParam(BUFFER_STATE)):
        yield element

    with self.assertRaises(ValueError):
      self._validate_dofn(BadStatefulDoFn3())

    # (4) The same timer parameter is duplicated on the on_timer method.
    class BadStatefulDoFn4(DoFn):
      BUFFER_STATE = BagStateSpec('buffer', BytesCoder())
      EXPIRY_TIMER_1 = TimerSpec('expiry1', TimeDomain.WATERMARK)
      EXPIRY_TIMER_2 = TimerSpec('expiry2', TimeDomain.WATERMARK)

      @on_timer(EXPIRY_TIMER_1)
      def expiry_callback(self, element, t1=DoFn.TimerParam(EXPIRY_TIMER_2),
                          t2=DoFn.TimerParam(EXPIRY_TIMER_2)):
        yield element

    with self.assertRaises(ValueError):
      self._validate_dofn(BadStatefulDoFn4())

  def test_validation_typos(self):
    # (1) Here, the user mistakenly used the same timer spec twice for two
    # different timer callbacks.
    with self.assertRaisesRegexp(
        ValueError,
        r'Multiple on_timer callbacks registered for TimerSpec\(expiry1\).'):
      class StatefulDoFnWithTimerWithTypo1(DoFn):  # pylint: disable=unused-variable
        BUFFER_STATE = BagStateSpec('buffer', BytesCoder())
        EXPIRY_TIMER_1 = TimerSpec('expiry1', TimeDomain.WATERMARK)
        EXPIRY_TIMER_2 = TimerSpec('expiry2', TimeDomain.WATERMARK)

        def process(self, element):
          pass

        @on_timer(EXPIRY_TIMER_1)
        def on_expiry_1(self, buffer_state=DoFn.StateParam(BUFFER_STATE)):
          yield 'expired1'

        # Note that we mistakenly associate this with the first timer.
        @on_timer(EXPIRY_TIMER_1)
        def on_expiry_2(self, buffer_state=DoFn.StateParam(BUFFER_STATE)):
          yield 'expired2'

    # (2) Here, the user mistakenly used the same callback name and overwrote
    # the first on_expiry_1 callback.
    class StatefulDoFnWithTimerWithTypo2(DoFn):
      BUFFER_STATE = BagStateSpec('buffer', BytesCoder())
      EXPIRY_TIMER_1 = TimerSpec('expiry1', TimeDomain.WATERMARK)
      EXPIRY_TIMER_2 = TimerSpec('expiry2', TimeDomain.WATERMARK)

      def process(self, element,
                  timer1=DoFn.TimerParam(EXPIRY_TIMER_1),
                  timer2=DoFn.TimerParam(EXPIRY_TIMER_2)):
        pass

      @on_timer(EXPIRY_TIMER_1)
      def on_expiry_1(self, buffer_state=DoFn.StateParam(BUFFER_STATE)):
        yield 'expired1'

      # Note that we mistakenly reuse the "on_expiry_1" name; this is valid
      # syntactically in Python.
      @on_timer(EXPIRY_TIMER_2)
      def on_expiry_1(self, buffer_state=DoFn.StateParam(BUFFER_STATE)):
        yield 'expired2'

      # Use a stable string value for matching.
      def __repr__(self):
        return 'StatefulDoFnWithTimerWithTypo2'

    dofn = StatefulDoFnWithTimerWithTypo2()
    with self.assertRaisesRegexp(
        ValueError,
        (r'The on_timer callback for TimerSpec\(expiry1\) is not the '
         r'specified .on_expiry_1 method for DoFn '
         r'StatefulDoFnWithTimerWithTypo2 \(perhaps it was overwritten\?\).')):
      validate_stateful_dofn(dofn)

    # (2) Here, the user forgot to add an on_timer decorator for 'expiry2'
    class StatefulDoFnWithTimerWithTypo3(DoFn):
      BUFFER_STATE = BagStateSpec('buffer', BytesCoder())
      EXPIRY_TIMER_1 = TimerSpec('expiry1', TimeDomain.WATERMARK)
      EXPIRY_TIMER_2 = TimerSpec('expiry2', TimeDomain.WATERMARK)

      def process(self, element,
                  timer1=DoFn.TimerParam(EXPIRY_TIMER_1),
                  timer2=DoFn.TimerParam(EXPIRY_TIMER_2)):
        pass

      @on_timer(EXPIRY_TIMER_1)
      def on_expiry_1(self, buffer_state=DoFn.StateParam(BUFFER_STATE)):
        yield 'expired1'

      def on_expiry_2(self, buffer_state=DoFn.StateParam(BUFFER_STATE)):
        yield 'expired2'

      # Use a stable string value for matching.
      def __repr__(self):
        return 'StatefulDoFnWithTimerWithTypo3'

    dofn = StatefulDoFnWithTimerWithTypo3()
    with self.assertRaisesRegexp(
        ValueError,
        (r'DoFn StatefulDoFnWithTimerWithTypo3 has a TimerSpec without an '
         r'associated on_timer callback: TimerSpec\(expiry2\).')):
      validate_stateful_dofn(dofn)


class StatefulDoFnOnDirectRunnerTest(unittest.TestCase):
  # pylint: disable=expression-not-assigned
  all_records = None

  def setUp(self):
    # Use state on the TestCase class, since other references would be pickled
    # into a closure and not have the desired side effects.
    #
    # TODO(BEAM-5295): Use assert_that after it works for the cases here in
    # streaming mode.
    StatefulDoFnOnDirectRunnerTest.all_records = []

  def record_dofn(self):
    class RecordDoFn(DoFn):
      def process(self, element):
        StatefulDoFnOnDirectRunnerTest.all_records.append(element)

    return RecordDoFn()

  def test_simple_stateful_dofn(self):
    class SimpleTestStatefulDoFn(DoFn):
      BUFFER_STATE = BagStateSpec('buffer', BytesCoder())
      EXPIRY_TIMER = TimerSpec('expiry', TimeDomain.WATERMARK)

      def process(self, element, buffer=DoFn.StateParam(BUFFER_STATE),
                  timer1=DoFn.TimerParam(EXPIRY_TIMER)):
        unused_key, value = element
        buffer.add(b'A' + str(value).encode('latin1'))
        timer1.set(20)

      @on_timer(EXPIRY_TIMER)
      def expiry_callback(self, buffer=DoFn.StateParam(BUFFER_STATE),
                          timer=DoFn.TimerParam(EXPIRY_TIMER)):
        yield b''.join(sorted(buffer.read()))

    with TestPipeline() as p:
      test_stream = (TestStream()
                     .advance_watermark_to(10)
                     .add_elements([1, 2])
                     .add_elements([3])
                     .advance_watermark_to(25)
                     .add_elements([4]))
      (p
       | test_stream
       | beam.Map(lambda x: ('mykey', x))
       | beam.ParDo(SimpleTestStatefulDoFn())
       | beam.ParDo(self.record_dofn()))

    # Two firings should occur: once after element 3 since the timer should
    # fire after the watermark passes time 20, and another time after element
    # 4, since the timer issued at that point should fire immediately.
    self.assertEqual(
        [b'A1A2A3', b'A1A2A3A4'],
        StatefulDoFnOnDirectRunnerTest.all_records)

  def test_stateful_dofn_nonkeyed_input(self):
    p = TestPipeline()
    values = p | beam.Create([1, 2, 3])
    with self.assertRaisesRegexp(
        ValueError,
        ('Input elements to the transform .* with stateful DoFn must be '
         'key-value pairs.')):
      values | beam.ParDo(TestStatefulDoFn())

  def test_simple_stateful_dofn_combining(self):
    class SimpleTestStatefulDoFn(DoFn):
      BUFFER_STATE = CombiningValueStateSpec(
          'buffer',
          IterableCoder(VarIntCoder()), ToListCombineFn())
      EXPIRY_TIMER = TimerSpec('expiry1', TimeDomain.WATERMARK)

      def process(self, element, buffer=DoFn.StateParam(BUFFER_STATE),
                  timer1=DoFn.TimerParam(EXPIRY_TIMER)):
        unused_key, value = element
        buffer.add(value)
        timer1.set(20)

      @on_timer(EXPIRY_TIMER)
      def expiry_callback(self, buffer=DoFn.StateParam(BUFFER_STATE),
                          timer=DoFn.TimerParam(EXPIRY_TIMER)):
        yield ''.join(str(x) for x in sorted(buffer.read()))

    with TestPipeline() as p:
      test_stream = (TestStream()
                     .advance_watermark_to(10)
                     .add_elements([1, 2])
                     .add_elements([3])
                     .advance_watermark_to(25)
                     .add_elements([4]))
      (p
       | test_stream
       | beam.Map(lambda x: ('mykey', x))
       | beam.ParDo(SimpleTestStatefulDoFn())
       | beam.ParDo(self.record_dofn()))

    self.assertEqual(
        ['123', '1234'],
        StatefulDoFnOnDirectRunnerTest.all_records)

  def test_timer_output_timestamp(self):
    class TimerEmittingStatefulDoFn(DoFn):
      EMIT_TIMER_1 = TimerSpec('emit1', TimeDomain.WATERMARK)
      EMIT_TIMER_2 = TimerSpec('emit2', TimeDomain.WATERMARK)
      EMIT_TIMER_3 = TimerSpec('emit3', TimeDomain.WATERMARK)

      def process(self, element,
                  timer1=DoFn.TimerParam(EMIT_TIMER_1),
                  timer2=DoFn.TimerParam(EMIT_TIMER_2),
                  timer3=DoFn.TimerParam(EMIT_TIMER_3)):
        timer1.set(10)
        timer2.set(20)
        timer3.set(30)

      @on_timer(EMIT_TIMER_1)
      def emit_callback_1(self):
        yield 'timer1'

      @on_timer(EMIT_TIMER_2)
      def emit_callback_2(self):
        yield 'timer2'

      @on_timer(EMIT_TIMER_3)
      def emit_callback_3(self):
        yield 'timer3'

    class TimestampReifyingDoFn(DoFn):
      def process(self, element, ts=DoFn.TimestampParam):
        yield (element, int(ts))

    with TestPipeline() as p:
      test_stream = (TestStream()
                     .advance_watermark_to(10)
                     .add_elements([1]))
      (p
       | test_stream
       | beam.Map(lambda x: ('mykey', x))
       | beam.ParDo(TimerEmittingStatefulDoFn())
       | beam.ParDo(TimestampReifyingDoFn())
       | beam.ParDo(self.record_dofn()))

    self.assertEqual(
        [('timer1', 10), ('timer2', 20), ('timer3', 30)],
        sorted(StatefulDoFnOnDirectRunnerTest.all_records))

  def test_index_assignment(self):
    class IndexAssigningStatefulDoFn(DoFn):
      INDEX_STATE = BagStateSpec('index', VarIntCoder())

      def process(self, element, state=DoFn.StateParam(INDEX_STATE)):
        unused_key, value = element
        next_index, = list(state.read()) or [0]
        yield (value, next_index)
        state.clear()
        state.add(next_index + 1)

    with TestPipeline() as p:
      test_stream = (TestStream()
                     .advance_watermark_to(10)
                     .add_elements(['A', 'B'])
                     .add_elements(['C'])
                     .advance_watermark_to(25)
                     .add_elements(['D']))
      (p
       | test_stream
       | beam.Map(lambda x: ('mykey', x))
       | beam.ParDo(IndexAssigningStatefulDoFn())
       | beam.ParDo(self.record_dofn()))

    self.assertEqual(
        [('A', 0), ('B', 1), ('C', 2), ('D', 3)],
        StatefulDoFnOnDirectRunnerTest.all_records)

  def test_hash_join(self):
    class HashJoinStatefulDoFn(DoFn):
      BUFFER_STATE = BagStateSpec('buffer', BytesCoder())
      UNMATCHED_TIMER = TimerSpec('unmatched', TimeDomain.WATERMARK)

      def process(self, element, state=DoFn.StateParam(BUFFER_STATE),
                  timer=DoFn.TimerParam(UNMATCHED_TIMER)):
        key, value = element
        existing_values = list(state.read())
        if not existing_values:
          state.add(value)
          timer.set(100)
        else:
          yield b'Record<%s,%s,%s>' % (key, existing_values[0], value)
          state.clear()
          timer.clear()

      @on_timer(UNMATCHED_TIMER)
      def expiry_callback(self, state=DoFn.StateParam(BUFFER_STATE)):
        buffered = list(state.read())
        assert len(buffered) == 1, buffered
        state.clear()
        yield b'Unmatched<%s>' % (buffered[0],)

    with TestPipeline() as p:
      test_stream = (TestStream()
                     .advance_watermark_to(10)
                     .add_elements([(b'A', b'a'), (b'B', b'b')])
                     .add_elements([(b'A', b'aa'), (b'C', b'c')])
                     .advance_watermark_to(25)
                     .add_elements([(b'A', b'aaa'), (b'B', b'bb')])
                     .add_elements([(b'D', b'd'), (b'D', b'dd'), (b'D', b'ddd'),
                                    (b'D', b'dddd')])
                     .advance_watermark_to(125)
                     .add_elements([(b'C', b'cc')]))
      (p
       | test_stream
       | beam.ParDo(HashJoinStatefulDoFn())
       | beam.ParDo(self.record_dofn()))

    equal_to(StatefulDoFnOnDirectRunnerTest.all_records)(
        [b'Record<A,a,aa>', b'Record<B,b,bb>', b'Record<D,d,dd>',
         b'Record<D,ddd,dddd>', b'Unmatched<aaa>', b'Unmatched<c>',
         b'Unmatched<cc>'])


if __name__ == '__main__':
  unittest.main()
