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

"""Unit tests for bundle processing."""
# pytype: skip-file

from __future__ import absolute_import

import unittest

from apache_beam.runners.worker.bundle_processor import DataInputOperation
from apache_beam.runners.worker.bundle_processor import FnApiUserStateContext
from apache_beam.runners.worker.bundle_processor import TimerInfo
from apache_beam.runners.worker.data_plane import SizeBasedBufferingClosableOutputStream
from apache_beam.transforms import userstate
from apache_beam.transforms.window import GlobalWindow


class FnApiUserStateContextTest(unittest.TestCase):
  def testOutputTimerTimestamp(self):
    class Coder(object):
      """Dummy coder to capture the timer result befor encoding."""
      def encode_to_stream(self, timer, *args, **kwargs):
        self.timer = timer

    coder = Coder()

    ctx = FnApiUserStateContext(None, 'transform_id', None, None)
    ctx.add_timer_info(
        'ts-event-timer',
        TimerInfo(coder, SizeBasedBufferingClosableOutputStream()))
    ctx.add_timer_info(
        'ts-proc-timer',
        TimerInfo(coder, SizeBasedBufferingClosableOutputStream()))

    timer_spec1 = userstate.TimerSpec(
        'event-timer', userstate.TimeDomain.WATERMARK)
    timer_spec2 = userstate.TimerSpec(
        'proc-timer', userstate.TimeDomain.REAL_TIME)

    # Set event time timer
    event_timer = ctx.get_timer(timer_spec1, 'key', GlobalWindow, 23, None)
    event_timer.set(42)
    # Output timestamp should be equal to the fire timestamp
    self.assertEquals(coder.timer.hold_timestamp, 42)

    # Set processing time timer
    proc_timer = ctx.get_timer(timer_spec2, 'key', GlobalWindow, 23, None)
    proc_timer.set(42)
    # Output timestamp should be equal to the input timestamp
    self.assertEquals(coder.timer.hold_timestamp, 23)


class SplitTest(unittest.TestCase):
  def split(
      self,
      index,
      current_element_progress,
      fraction_of_remainder,
      buffer_size,
      allowed=(),
      sdf=False):
    return DataInputOperation._compute_split(
        index,
        current_element_progress,
        float('inf'),
        fraction_of_remainder,
        buffer_size,
        allowed_split_points=allowed,
        try_split=lambda frac: element_split(frac, 0)[1:3] if sdf else None)

  def sdf_split(self, *args, **kwargs):
    return self.split(*args, sdf=True, **kwargs)

  def test_simple_split(self):
    # Split as close to the beginning as possible.
    self.assertEqual(self.split(0, 0, 0, 16), simple_split(1))
    # The closest split is at 4, even when just above or below it.
    self.assertEqual(self.split(0, 0, 0.24, 16), simple_split(4))
    self.assertEqual(self.split(0, 0, 0.25, 16), simple_split(4))
    self.assertEqual(self.split(0, 0, 0.26, 16), simple_split(4))
    # Split the *remainder* in half.
    self.assertEqual(self.split(0, 0, 0.5, 16), simple_split(8))
    self.assertEqual(self.split(2, 0, 0.5, 16), simple_split(9))
    self.assertEqual(self.split(6, 0, 0.5, 16), simple_split(11))

  def test_split_with_element_progress(self):
    # Progress into the active element influences where the split of the
    # remainder falls.
    self.assertEqual(self.split(0, 0.5, 0.25, 4), simple_split(1))
    self.assertEqual(self.split(0, 0.9, 0.25, 4), simple_split(2))
    self.assertEqual(self.split(1, 0.0, 0.25, 4), simple_split(2))
    self.assertEqual(self.split(1, 0.1, 0.25, 4), simple_split(2))

  def test_split_with_element_allowed_splits(self):
    # The desired split point is at 4.
    self.assertEqual(
        self.split(0, 0, 0.25, 16, allowed=(2, 3, 4, 5)), simple_split(4))
    # If we can't split at 4, choose the closest possible split point.
    self.assertEqual(
        self.split(0, 0, 0.25, 16, allowed=(2, 3, 5)), simple_split(5))
    self.assertEqual(
        self.split(0, 0, 0.25, 16, allowed=(2, 3, 6)), simple_split(3))

    # Also test the case where all possible split points lie above or below
    # the desired split point.
    self.assertEqual(
        self.split(0, 0, 0.25, 16, allowed=(5, 6, 7)), simple_split(5))
    self.assertEqual(
        self.split(0, 0, 0.25, 16, allowed=(1, 2, 3)), simple_split(3))

    # We have progressed beyond all possible split points, so can't split.
    self.assertEqual(self.split(5, 0, 0.25, 16, allowed=(1, 2, 3)), None)

  def test_sdf_split(self):
    # Split between future elements at element boundaries.
    self.assertEqual(self.sdf_split(0, 0, 0.51, 4), simple_split(2))
    self.assertEqual(self.sdf_split(0, 0, 0.49, 4), simple_split(2))
    self.assertEqual(self.sdf_split(0, 0, 0.26, 4), simple_split(1))
    self.assertEqual(self.sdf_split(0, 0, 0.25, 4), simple_split(1))

    # If the split falls inside the first, splittable element, split there.
    self.assertEqual(
        self.sdf_split(0, 0, 0.20, 4),
        (-1, ['Primary(0.8)'], ['Residual(0.2)'], 1))
    # The choice of split depends on the progress into the first element.
    self.assertEqual(
        self.sdf_split(0, 0, .125, 4),
        (-1, ['Primary(0.5)'], ['Residual(0.5)'], 1))
    # Here we are far enough into the first element that splitting at 0.2 of the
    # remainder falls outside the first element.
    self.assertEqual(self.sdf_split(0, .5, 0.2, 4), simple_split(1))

    # Verify the above logic when we are partially through the stream.
    self.assertEqual(self.sdf_split(2, 0, 0.6, 4), simple_split(3))
    self.assertEqual(self.sdf_split(2, 0.9, 0.6, 4), simple_split(4))
    self.assertEqual(
        self.sdf_split(2, 0.5, 0.2, 4),
        (1, ['Primary(0.6)'], ['Residual(0.4)'], 3))

  def test_sdf_split_with_allowed_splits(self):
    # This is where we would like to split, when all split points are available.
    self.assertEqual(
        self.sdf_split(2, 0, 0.2, 5, allowed=(1, 2, 3, 4, 5)),
        (1, ['Primary(0.6)'], ['Residual(0.4)'], 3))
    # We can't split element at index 2, because 3 is not a split point.
    self.assertEqual(
        self.sdf_split(2, 0, 0.2, 5, allowed=(1, 2, 4, 5)), simple_split(4))
    # We can't even split element at index 4 as above, because 4 is also not a
    # split point.
    self.assertEqual(
        self.sdf_split(2, 0, 0.2, 5, allowed=(1, 2, 5)), simple_split(5))
    # We can't split element at index 2, because 2 is not a split point.
    self.assertEqual(
        self.sdf_split(2, 0, 0.2, 5, allowed=(1, 3, 4, 5)), simple_split(3))


def simple_split(first_residual_index):
  return first_residual_index - 1, [], [], first_residual_index


def element_split(frac, index):
  return (
      index - 1, ['Primary(%0.1f)' % frac], ['Residual(%0.1f)' % (1 - frac)],
      index + 1)


if __name__ == '__main__':
  unittest.main()
