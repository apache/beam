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

"""Unit tests for user state and timers on the BundleBasedDirectRunner."""
from __future__ import absolute_import

import unittest

import apache_beam as beam
from apache_beam.coders import BytesCoder
from apache_beam.coders import IterableCoder
from apache_beam.coders import VarIntCoder
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.combiners import ToListCombineFn
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import CombiningValueStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer


class StatefulDoFnOnDirectRunnerTest(unittest.TestCase):
  # pylint: disable=expression-not-assigned
  all_records = None

  def setUp(self):
    # Use state on the TestCase class, since other references would be pickled
    # into a closure and not have the desired side effects.
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
        buffer.add('A' + str(value))
        timer1.set(20)

      @on_timer(EXPIRY_TIMER)
      def expiry_callback(self, buffer=DoFn.StateParam(BUFFER_STATE),
                          timer=DoFn.TimerParam(EXPIRY_TIMER)):
        yield ''.join(sorted(buffer.read()))

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
        ['A1A2A3', 'A1A2A3A4'],
        StatefulDoFnOnDirectRunnerTest.all_records)

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
        timer1.set(20)
        timer2.set(40)
        timer3.set(60)

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
        [('timer1', 20), ('timer2', 40), ('timer3', 60)],
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
      BUFFER_STATE = BagStateSpec('buffer', VarIntCoder())
      UNMATCHED_TIMER = TimerSpec('unmatched', TimeDomain.WATERMARK)

      def process(self, element, state=DoFn.StateParam(BUFFER_STATE),
                  timer=DoFn.TimerParam(UNMATCHED_TIMER)):
        key, value = element
        existing_values = list(state.read())
        if not existing_values:
          state.add(value)
          timer.set(100)
        else:
          yield 'Record<%s,%s,%s>' % (key, existing_values[0], value)
          state.clear()
          timer.clear()

      @on_timer(UNMATCHED_TIMER)
      def expiry_callback(self, state=DoFn.StateParam(BUFFER_STATE)):
        buffered = list(state.read())
        assert len(buffered) == 1, buffered
        state.clear()
        yield 'Unmatched<%s>' % (buffered[0],)

    with TestPipeline() as p:
      test_stream = (TestStream()
                     .advance_watermark_to(10)
                     .add_elements([('k1', 1), ('k2', 2)])
                     .add_elements([('k1', 3), ('k3', 4)])
                     .advance_watermark_to(25)
                     .add_elements([('k1', 5), ('k2', 6)])
                     .add_elements([('k4', 7), ('k4', 8), ('k4', 9),
                                    ('k4', 10)])
                     .advance_watermark_to(125)
                     .add_elements([('k3', 11)]))
      (p
       | test_stream
       | beam.ParDo(HashJoinStatefulDoFn())
       | beam.ParDo(self.record_dofn()))

    # Two firings should occur: once after element 3 since the timer should
    # fire after the watermark passes time 20, and another time after element
    # 4, since the timer issued at that point should fire immediately.
    self.assertEqual(
        ['Record<k1,1,3>', 'Record<k2,2,6>', 'Record<k4,7,8>',
         'Record<k4,9,10>', 'Unmatched<5>', 'Unmatched<4>',
         'Unmatched<11>'],
        StatefulDoFnOnDirectRunnerTest.all_records)


if __name__ == '__main__':
  unittest.main()
