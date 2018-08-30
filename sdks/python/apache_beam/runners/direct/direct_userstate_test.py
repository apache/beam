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

import mock

import apache_beam as beam
from apache_beam.coders import BytesCoder
from apache_beam.coders import IterableCoder
from apache_beam.coders import VarIntCoder
from apache_beam.runners.common import DoFnSignature
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import ElementEvent
from apache_beam.testing.test_stream import ProcessingTimeEvent
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.test_stream import WatermarkEvent
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.combiners import ToListCombineFn
from apache_beam.transforms.combiners import TopCombineFn
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import CombiningValueStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import UserStateUtils
from apache_beam.transforms.userstate import on_timer

class StatefulDoFnOnDirectRunnerTest(unittest.TestCase):
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
      EXPIRY_TIMER = TimerSpec('expiry1', TimeDomain.WATERMARK)

      def process(self, element, buffer=DoFn.StateParam(BUFFER_STATE),
                  timer1=DoFn.TimerParam(EXPIRY_TIMER)):
        buffer.add('A' + str(element))
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
      pcoll = p | test_stream
      records = pcoll | beam.ParDo(SimpleTestStatefulDoFn())
      records | beam.ParDo(self.record_dofn())

    # Two firings should occur: once after element 3, and another time after
    # element 4.
    self.assertEqual(['A1A2A3', 'A1A2A3A4'],
        StatefulDoFnOnDirectRunnerTest.all_records)

  # def test_simple_stateful_dofn_combining(self):
  #   class SimpleTestStatefulDoFn(DoFn):
  #     BUFFER_STATE = CombiningValueStateSpec('buffer', IterableCoder(VarIntCoder()), ToListCombineFn())
  #     EXPIRY_TIMER = TimerSpec('expiry1', TimeDomain.WATERMARK)

  #     def process(self, element, buffer=DoFn.StateParam(BUFFER_STATE),
  #                 timer1=DoFn.TimerParam(EXPIRY_TIMER)):
  #       buffer.add(element)
  #       timer1.set(20)

  #     @on_timer(EXPIRY_TIMER)
  #     def expiry_callback(self, buffer=DoFn.StateParam(BUFFER_STATE),
  #                         timer=DoFn.TimerParam(EXPIRY_TIMER)):
  #       print 'YO', buffer.read()
  #       yield ''.join(str(x) for x in sorted(buffer.read()))

  #   with TestPipeline() as p:
  #     test_stream = (TestStream()
  #                    .advance_watermark_to(10)
  #                    .add_elements([1, 2])
  #                    .add_elements([3])
  #                    .advance_watermark_to(25)
  #                    .add_elements([4]))
  #     pcoll = p | test_stream
  #     records = pcoll | beam.ParDo(SimpleTestStatefulDoFn())
  #     records | beam.ParDo(self.record_dofn())

  #   self.assertEqual(['123', '1234'],
  #       StatefulDoFnOnDirectRunnerTest.all_records)


if __name__ == '__main__':
  unittest.main()
