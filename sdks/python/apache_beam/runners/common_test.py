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

from __future__ import absolute_import

import threading
import time
import unittest

import apache_beam as beam
from apache_beam.io.concat_source_test import RangeSource
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.common import DoFnSignature
from apache_beam.runners.common import _RestrictionTrackerView
from apache_beam.runners.common import _ThreadsafeRestrictionTracker
from apache_beam.runners.common import _ThreadsafeWatermarkEstimator
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.core import DoFn
from apache_beam.utils import timestamp


class DoFnSignatureTest(unittest.TestCase):
  def test_dofn_validate_process_error(self):
    class MyDoFn(DoFn):
      def process(self, element, w1=DoFn.WindowParam, w2=DoFn.WindowParam):
        pass

    with self.assertRaises(ValueError):
      DoFnSignature(MyDoFn())

  def test_dofn_validate_start_bundle_error(self):
    class MyDoFn(DoFn):
      def process(self, element):
        pass

      def start_bundle(self, w1=DoFn.WindowParam):
        pass

    with self.assertRaises(ValueError):
      DoFnSignature(MyDoFn())

  def test_dofn_validate_finish_bundle_error(self):
    class MyDoFn(DoFn):
      def process(self, element):
        pass

      def finish_bundle(self, w1=DoFn.WindowParam):
        pass

    with self.assertRaises(ValueError):
      DoFnSignature(MyDoFn())


class DoFnProcessTest(unittest.TestCase):
  # pylint: disable=expression-not-assigned
  all_records = None

  def setUp(self):
    DoFnProcessTest.all_records = []

  def record_dofn(self):
    class RecordDoFn(DoFn):
      def process(self, element):
        DoFnProcessTest.all_records.append(element)

    return RecordDoFn()

  def test_dofn_process_keyparam(self):
    class DoFnProcessWithKeyparam(DoFn):
      def process(self, element, mykey=DoFn.KeyParam):
        yield "{key}-verify".format(key=mykey)

    pipeline_options = PipelineOptions()

    with TestPipeline(options=pipeline_options) as p:
      test_stream = (TestStream().advance_watermark_to(10).add_elements([1, 2]))
      (
          p
          | test_stream
          | beam.Map(lambda x: (x, "some-value"))
          | "window_into" >> beam.WindowInto(
              window.FixedWindows(5),
              accumulation_mode=trigger.AccumulationMode.DISCARDING)
          | beam.ParDo(DoFnProcessWithKeyparam())
          | beam.ParDo(self.record_dofn()))

    self.assertEqual(['1-verify', '2-verify'],
                     sorted(DoFnProcessTest.all_records))

  def test_dofn_process_keyparam_error_no_key(self):
    class DoFnProcessWithKeyparam(DoFn):
      def process(self, element, mykey=DoFn.KeyParam):
        yield "{key}-verify".format(key=mykey)

    pipeline_options = PipelineOptions()
    with self.assertRaises(ValueError),\
         TestPipeline(options=pipeline_options) as p:
      test_stream = (TestStream().advance_watermark_to(10).add_elements([1, 2]))
      (p | test_stream | beam.ParDo(DoFnProcessWithKeyparam()))


class _ThreadsafeRestrictionTrackerTest(unittest.TestCase):

  def test_initialization(self):
    with self.assertRaises(ValueError):
      _ThreadsafeRestrictionTracker(RangeSource(0, 1), threading.Lock())

  def test_defer_remainder_with_wrong_time_type(self):
    threadsafe_tracker = _ThreadsafeRestrictionTracker(
        OffsetRestrictionTracker(OffsetRange(0, 10)), threading.Lock())
    with self.assertRaises(ValueError):
      threadsafe_tracker.defer_remainder(10)

  def test_self_checkpoint_immediately(self):
    restriction_tracker = OffsetRestrictionTracker(OffsetRange(0, 10))
    threadsafe_tracker = _ThreadsafeRestrictionTracker(
        restriction_tracker, threading.Lock())
    threadsafe_tracker.defer_remainder()
    deferred_residual, deferred_time = threadsafe_tracker.deferred_status()
    expected_residual = OffsetRange(0, 10)
    self.assertEqual(deferred_residual, expected_residual)
    self.assertTrue(isinstance(deferred_time, timestamp.Duration))
    self.assertEqual(deferred_time, 0)

  def test_self_checkpoint_with_relative_time(self):
    threadsafe_tracker = _ThreadsafeRestrictionTracker(
        OffsetRestrictionTracker(OffsetRange(0, 10)), threading.Lock())
    threadsafe_tracker.defer_remainder(timestamp.Duration(100))
    time.sleep(2)
    _, deferred_time = threadsafe_tracker.deferred_status()
    self.assertTrue(isinstance(deferred_time, timestamp.Duration))
    # The expectation = 100 - 2 - some_delta
    self.assertTrue(deferred_time <= 98)

  def test_self_checkpoint_with_absolute_time(self):
    threadsafe_tracker = _ThreadsafeRestrictionTracker(
        OffsetRestrictionTracker(OffsetRange(0, 10)), threading.Lock())
    now = timestamp.Timestamp.now()
    schedule_time = now + timestamp.Duration(100)
    self.assertTrue(isinstance(schedule_time, timestamp.Timestamp))
    threadsafe_tracker.defer_remainder(schedule_time)
    time.sleep(2)
    _, deferred_time = threadsafe_tracker.deferred_status()
    self.assertTrue(isinstance(deferred_time, timestamp.Duration))
    # The expectation =
    # schedule_time - the time when deferred_status is called - some_delta
    self.assertTrue(deferred_time <= 98)


class RestrictionTrackerViewTest(unittest.TestCase):

  def test_initialization(self):
    with self.assertRaises(ValueError):
      _RestrictionTrackerView(OffsetRestrictionTracker(OffsetRange(0, 10)))

  def test_api_expose(self):
    threadsafe_tracker = _ThreadsafeRestrictionTracker(
        OffsetRestrictionTracker(OffsetRange(0, 10)), threading.Lock())
    tracker_view = _RestrictionTrackerView(threadsafe_tracker)
    current_restriction = tracker_view.current_restriction()
    self.assertEqual(current_restriction, OffsetRange(0, 10))
    self.assertTrue(tracker_view.try_claim(0))
    tracker_view.defer_remainder()
    deferred_remainder, deferred_watermark = (
        threadsafe_tracker.deferred_status())
    self.assertEqual(deferred_remainder, OffsetRange(1, 10))
    self.assertEqual(deferred_watermark, timestamp.Duration())

  def test_non_expose_apis(self):
    threadsafe_tracker = _ThreadsafeRestrictionTracker(
        OffsetRestrictionTracker(OffsetRange(0, 10)), threading.Lock())
    tracker_view = _RestrictionTrackerView(threadsafe_tracker)
    with self.assertRaises(AttributeError):
      tracker_view.check_done()
    with self.assertRaises(AttributeError):
      tracker_view.current_progress()
    with self.assertRaises(AttributeError):
      tracker_view.try_split()
    with self.assertRaises(AttributeError):
      tracker_view.deferred_status()


class ThreadsafeWatermarkEstimatorTest(unittest.TestCase):

  def test_initialization(self):
    with self.assertRaises(ValueError):
      _ThreadsafeWatermarkEstimator(None, threading.Lock())

  def test_get_estimator_state(self):
    estimator = _ThreadsafeWatermarkEstimator(
        ManualWatermarkEstimator(None), threading.Lock())
    self.assertIsNone(estimator.get_estimator_state())
    estimator.set_watermark(timestamp.Timestamp(10))
    self.assertEqual(estimator.get_estimator_state(), timestamp.Timestamp(10))

  def test_track_timestamp(self):
    estimator = _ThreadsafeWatermarkEstimator(
        ManualWatermarkEstimator(None), threading.Lock())
    estimator.observe_timestamp(timestamp.Timestamp(10))
    self.assertIsNone(estimator.current_watermark())
    estimator.set_watermark(timestamp.Timestamp(20))
    self.assertEqual(estimator.current_watermark(), timestamp.Timestamp(20))

  def test_non_exsited_attr(self):
    estimator = _ThreadsafeWatermarkEstimator(
        ManualWatermarkEstimator(None), threading.Lock())
    with self.assertRaises(AttributeError):
      estimator.non_existed_call()

if __name__ == '__main__':
  unittest.main()
