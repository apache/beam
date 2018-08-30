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
      'apache_beam.transforms.userstate.UserStateUtils.validate_stateful_dofn')
  def test_validate_dofn(self, unused_mock):
    dofn = TestStatefulDoFn()
    self._validate_dofn(dofn)
    UserStateUtils.validate_stateful_dofn.assert_called_with(dofn)

  def test_spec_construction(self):
    BagStateSpec('statename', VarIntCoder())
    with self.assertRaises(AssertionError):
      BagStateSpec(123, VarIntCoder())
    CombiningValueStateSpec('statename', VarIntCoder(), TopCombineFn(10))
    with self.assertRaises(AssertionError):
      CombiningValueStateSpec(123, VarIntCoder(), TopCombineFn(10))
    with self.assertRaises(AssertionError):
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
    self.assertFalse(UserStateUtils.is_stateful_dofn(DoFn()))
    self.assertTrue(UserStateUtils.is_stateful_dofn(TestStatefulDoFn()))

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

    # Validate UserStateUtils.get_dofn_specs() and timer callbacks in
    # DoFnSignature.
    stateful_dofn = BasicStatefulDoFn()
    signature = self._validate_dofn(stateful_dofn)
    expected_specs = (set([BasicStatefulDoFn.BUFFER_STATE]),
                      set([BasicStatefulDoFn.EXPIRY_TIMER]))
    self.assertEqual(expected_specs,
                      UserStateUtils.get_dofn_specs(stateful_dofn))
    self.assertEqual(stateful_dofn.expiry_callback,
        signature.timer_methods[BasicStatefulDoFn.EXPIRY_TIMER].method_value)

    stateful_dofn = TestStatefulDoFn()
    signature = self._validate_dofn(stateful_dofn)
    expected_specs = (set([TestStatefulDoFn.BUFFER_STATE_1,
                           TestStatefulDoFn.BUFFER_STATE_2]),
                      set([TestStatefulDoFn.EXPIRY_TIMER_1,
                           TestStatefulDoFn.EXPIRY_TIMER_2,
                           TestStatefulDoFn.EXPIRY_TIMER_3]))
    self.assertEqual(expected_specs,
                      UserStateUtils.get_dofn_specs(stateful_dofn))
    self.assertEqual(stateful_dofn.on_expiry_1,
        signature.timer_methods[TestStatefulDoFn.EXPIRY_TIMER_1].method_value)
    self.assertEqual(stateful_dofn.on_expiry_2,
        signature.timer_methods[TestStatefulDoFn.EXPIRY_TIMER_2].method_value)
    self.assertEqual(stateful_dofn.on_expiry_3,
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
      UserStateUtils.validate_stateful_dofn(dofn)

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
      UserStateUtils.validate_stateful_dofn(dofn)


if __name__ == '__main__':
  unittest.main()
