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

import unittest

import hamcrest as hc

import apache_beam as beam
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.common import DoFnSignature
from apache_beam.runners.common import PerWindowInvoker
from apache_beam.runners.sdf_utils import SplitResultPrimary
from apache_beam.runners.sdf_utils import SplitResultResidual
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.core import RestrictionProvider
from apache_beam.transforms.window import IntervalWindow
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.windowed_value import WindowedValue


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

  def test_unbounded_element_process_fn(self):
    class UnboundedDoFn(DoFn):
      @DoFn.unbounded_per_element()
      def process(self, element):
        pass

    class BoundedDoFn(DoFn):
      def process(self, element):
        pass

    signature = DoFnSignature(UnboundedDoFn())
    self.assertTrue(signature.is_unbounded_per_element())
    signature = DoFnSignature(BoundedDoFn())
    self.assertFalse(signature.is_unbounded_per_element())


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


class TestOffsetRestrictionProvider(RestrictionProvider):
  def restriction_size(self, element, restriction):
    return restriction.size()


class PerWindowInvokerSplitTest(unittest.TestCase):
  def setUp(self):
    self.window1 = IntervalWindow(0, 10)
    self.window2 = IntervalWindow(10, 20)
    self.window3 = IntervalWindow(20, 30)
    self.windowed_value = WindowedValue(
        'a', 57, (self.window1, self.window2, self.window3))
    self.restriction = OffsetRange(0, 100)
    self.watermark_estimator_state = Timestamp(21)
    self.restriction_provider = TestOffsetRestrictionProvider()
    self.watermark_estimator = ManualWatermarkEstimator(Timestamp(42))
    self.maxDiff = None

  def create_split_in_window(self, offset_index, windows):
    return (
        SplitResultPrimary(
            primary_value=WindowedValue(((
                'a',
                (OffsetRange(0, offset_index), self.watermark_estimator_state)),
                                         offset_index),
                                        57,
                                        windows)),
        SplitResultResidual(
            residual_value=WindowedValue(((
                'a',
                (
                    OffsetRange(offset_index, 100),
                    self.watermark_estimator.get_estimator_state())),
                                          100 - offset_index),
                                         57,
                                         windows),
            current_watermark=self.watermark_estimator.current_watermark(),
            deferred_timestamp=None))

  def create_split_across_windows(self, primary_windows, residual_windows):
    primary = SplitResultPrimary(
        primary_value=WindowedValue(
            (('a', (OffsetRange(0, 100), self.watermark_estimator_state)), 100),
            57,
            primary_windows)) if primary_windows else None
    residual = SplitResultResidual(
        residual_value=WindowedValue(
            (('a', (OffsetRange(0, 100), self.watermark_estimator_state)), 100),
            57,
            residual_windows),
        current_watermark=None,
        deferred_timestamp=None) if residual_windows else None
    return primary, residual

  def test_non_window_observing_checkpoint(self):
    # test checkpoint
    restriction_tracker = OffsetRestrictionTracker(self.restriction)
    restriction_tracker.try_claim(30)
    (primaries, residuals, stop_index) = PerWindowInvoker._try_split(
        0.0,
        None,
        None,
        self.windowed_value,
        self.restriction,
        self.watermark_estimator_state,
        self.restriction_provider,
        restriction_tracker,
        self.watermark_estimator)
    expected_primary_split, expected_residual_split = (
        self.create_split_in_window(31, self.windowed_value.windows))
    self.assertEqual([expected_primary_split], primaries)
    self.assertEqual([expected_residual_split], residuals)
    # We don't expect the stop index to be set for non window observing splits
    self.assertIsNone(stop_index)

  def test_non_window_observing_split(self):
    restriction_tracker = OffsetRestrictionTracker(self.restriction)
    restriction_tracker.try_claim(30)
    (primaries, residuals, stop_index) = PerWindowInvoker._try_split(
        0.1,
        None,
        None,
        self.windowed_value,
        self.restriction,
        self.watermark_estimator_state,
        self.restriction_provider,
        restriction_tracker,
        self.watermark_estimator)
    expected_primary_split, expected_residual_split = (
        self.create_split_in_window(37, self.windowed_value.windows))
    self.assertEqual([expected_primary_split], primaries)
    self.assertEqual([expected_residual_split], residuals)
    # We don't expect the stop index to be set for non window observing splits
    self.assertIsNone(stop_index)

  def test_non_window_observing_split_when_restriction_is_done(self):
    restriction_tracker = OffsetRestrictionTracker(self.restriction)
    restriction_tracker.try_claim(100)
    self.assertIsNone(
        PerWindowInvoker._try_split(
            0.1,
            None,
            None,
            self.windowed_value,
            self.restriction,
            self.watermark_estimator_state,
            self.restriction_provider,
            restriction_tracker,
            self.watermark_estimator))

  def test_window_observing_checkpoint_on_first_window(self):
    restriction_tracker = OffsetRestrictionTracker(self.restriction)
    restriction_tracker.try_claim(30)
    (primaries, residuals, stop_index) = PerWindowInvoker._try_split(
        0.0,
        0,
        3,
        self.windowed_value,
        self.restriction,
        self.watermark_estimator_state,
        self.restriction_provider,
        restriction_tracker,
        self.watermark_estimator)
    expected_primary_split, expected_residual_split = (
        self.create_split_in_window(31, (self.window1, )))
    _, expected_residual_windows = (
        self.create_split_across_windows(None, (self.window2, self.window3,)))
    hc.assert_that(primaries, hc.contains_inanyorder(expected_primary_split))
    hc.assert_that(
        residuals,
        hc.contains_inanyorder(
            expected_residual_split,
            expected_residual_windows,
        ))
    self.assertEqual(stop_index, 1)

  def test_window_observing_checkpoint_on_first_window_after_prior_split(self):
    restriction_tracker = OffsetRestrictionTracker(self.restriction)
    restriction_tracker.try_claim(30)
    (primaries, residuals, stop_index) = PerWindowInvoker._try_split(
        0.0,
        0,
        2,  # stop index < len(windows) representing a prior split had occurred
        self.windowed_value,
        self.restriction,
        self.watermark_estimator_state,
        self.restriction_provider,
        restriction_tracker,
        self.watermark_estimator)
    expected_primary_split, expected_residual_split = (
        self.create_split_in_window(31, (self.window1, )))
    _, expected_residual_windows = (
        self.create_split_across_windows(None, (self.window2, )))
    hc.assert_that(primaries, hc.contains_inanyorder(expected_primary_split))
    hc.assert_that(
        residuals,
        hc.contains_inanyorder(
            expected_residual_split,
            expected_residual_windows,
        ))
    self.assertEqual(stop_index, 1)

  def test_window_observing_split_on_first_window(self):
    restriction_tracker = OffsetRestrictionTracker(self.restriction)
    restriction_tracker.try_claim(30)
    (primaries, residuals, stop_index) = PerWindowInvoker._try_split(
        0.2,
        0,
        3,
        self.windowed_value,
        self.restriction,
        self.watermark_estimator_state,
        self.restriction_provider,
        restriction_tracker,
        self.watermark_estimator)
    # 20% of 2.7 windows = 20% of 270 offset left = 54 offset
    # 30 + 54 = 84 split offset
    expected_primary_split, expected_residual_split = (
        self.create_split_in_window(84, (self.window1, )))
    _, expected_residual_windows = (
        self.create_split_across_windows(None, (self.window2, self.window3, )))
    hc.assert_that(primaries, hc.contains_inanyorder(expected_primary_split))
    hc.assert_that(
        residuals,
        hc.contains_inanyorder(
            expected_residual_split,
            expected_residual_windows,
        ))
    self.assertEqual(stop_index, 1)

  def test_window_observing_split_on_middle_window(self):
    restriction_tracker = OffsetRestrictionTracker(self.restriction)
    restriction_tracker.try_claim(30)
    (primaries, residuals, stop_index) = PerWindowInvoker._try_split(
        0.2,
        1,
        3,
        self.windowed_value,
        self.restriction,
        self.watermark_estimator_state,
        self.restriction_provider,
        restriction_tracker,
        self.watermark_estimator)
    # 20% of 1.7 windows = 20% of 170 offset left = 34 offset
    # 30 + 34 = 64 split offset
    expected_primary_split, expected_residual_split = (
        self.create_split_in_window(64, (self.window2, )))
    expected_primary_windows, expected_residual_windows = (
        self.create_split_across_windows((self.window1, ), (self.window3, )))
    hc.assert_that(
        primaries,
        hc.contains_inanyorder(
            expected_primary_split,
            expected_primary_windows,
        ))
    hc.assert_that(
        residuals,
        hc.contains_inanyorder(
            expected_residual_split,
            expected_residual_windows,
        ))
    self.assertEqual(stop_index, 2)

  def test_window_observing_split_on_last_window(self):
    restriction_tracker = OffsetRestrictionTracker(self.restriction)
    restriction_tracker.try_claim(30)
    (primaries, residuals, stop_index) = PerWindowInvoker._try_split(
        0.2,
        2,
        3,
        self.windowed_value,
        self.restriction,
        self.watermark_estimator_state,
        self.restriction_provider,
        restriction_tracker,
        self.watermark_estimator)
    # 20% of 0.7 windows = 20% of 70 offset left = 14 offset
    # 30 + 14 = 44 split offset
    expected_primary_split, expected_residual_split = (
        self.create_split_in_window(44, (self.window3, )))
    expected_primary_windows, _ = (
        self.create_split_across_windows((self.window1, self.window2, ), None))
    hc.assert_that(
        primaries,
        hc.contains_inanyorder(
            expected_primary_split,
            expected_primary_windows,
        ))
    hc.assert_that(residuals, hc.contains_inanyorder(expected_residual_split, ))
    self.assertEqual(stop_index, 3)

  def test_window_observing_split_on_first_window_fallback(self):
    restriction_tracker = OffsetRestrictionTracker(self.restriction)
    restriction_tracker.try_claim(100)
    # We assume that we can't split this fully claimed restriction
    self.assertIsNone(restriction_tracker.try_split(0))
    (primaries, residuals, stop_index) = PerWindowInvoker._try_split(
        0.0,
        0,
        3,
        self.windowed_value,
        self.restriction,
        self.watermark_estimator_state,
        self.restriction_provider,
        restriction_tracker,
        self.watermark_estimator)
    expected_primary_windows, expected_residual_windows = (
        self.create_split_across_windows(
            (self.window1, ), (self.window2, self.window3, )))
    hc.assert_that(
        primaries, hc.contains_inanyorder(
            expected_primary_windows,
        ))
    hc.assert_that(
        residuals, hc.contains_inanyorder(
            expected_residual_windows,
        ))
    self.assertEqual(stop_index, 1)

  def test_window_observing_split_on_middle_window_fallback(self):
    restriction_tracker = OffsetRestrictionTracker(self.restriction)
    restriction_tracker.try_claim(100)
    # We assume that we can't split this fully claimed restriction
    self.assertIsNone(restriction_tracker.try_split(0))
    (primaries, residuals, stop_index) = PerWindowInvoker._try_split(
        0.0,
        1,
        3,
        self.windowed_value,
        self.restriction,
        self.watermark_estimator_state,
        self.restriction_provider,
        restriction_tracker,
        self.watermark_estimator)
    expected_primary_windows, expected_residual_windows = (
        self.create_split_across_windows(
            (self.window1, self.window2, ), (self.window3, )))
    hc.assert_that(
        primaries, hc.contains_inanyorder(
            expected_primary_windows,
        ))
    hc.assert_that(
        residuals, hc.contains_inanyorder(
            expected_residual_windows,
        ))
    self.assertEqual(stop_index, 2)

  def test_window_observing_split_on_last_window_when_split_not_possible(self):
    restriction_tracker = OffsetRestrictionTracker(self.restriction)
    restriction_tracker.try_claim(100)
    # We assume that we can't split this fully claimed restriction
    self.assertIsNone(restriction_tracker.try_split(0))
    self.assertIsNone(
        PerWindowInvoker._try_split(
            0.0,
            2,
            3,
            self.windowed_value,
            self.restriction,
            self.watermark_estimator_state,
            self.restriction_provider,
            restriction_tracker,
            self.watermark_estimator))

  def test_window_observing_split_on_window_boundary_round_up(self):
    restriction_tracker = OffsetRestrictionTracker(self.restriction)
    restriction_tracker.try_claim(30)
    (primaries, residuals, stop_index) = PerWindowInvoker._try_split(
        0.6,
        0,
        3,
        self.windowed_value,
        self.restriction,
        self.watermark_estimator_state,
        self.restriction_provider,
        restriction_tracker,
        self.watermark_estimator)
    # 60% of 2.7 windows = 60% of 270 offset left = 162 offset
    # 30 + 162 = 192 offset --> round to end of window 2
    expected_primary_windows, expected_residual_windows = (
        self.create_split_across_windows(
            (self.window1, self.window2, ), (self.window3, )))
    hc.assert_that(
        primaries, hc.contains_inanyorder(
            expected_primary_windows,
        ))
    hc.assert_that(
        residuals, hc.contains_inanyorder(
            expected_residual_windows,
        ))
    self.assertEqual(stop_index, 2)

  def test_window_observing_split_on_window_boundary_round_down(self):
    restriction_tracker = OffsetRestrictionTracker(self.restriction)
    restriction_tracker.try_claim(30)
    (primaries, residuals, stop_index) = PerWindowInvoker._try_split(
        0.3,
        0,
        3,
        self.windowed_value,
        self.restriction,
        self.watermark_estimator_state,
        self.restriction_provider,
        restriction_tracker,
        self.watermark_estimator)
    # 30% of 2.7 windows = 30% of 270 offset left = 81 offset
    # 30 + 81 = 111 offset --> round to end of window 1
    expected_primary_windows, expected_residual_windows = (
        self.create_split_across_windows(
            (self.window1, ), (self.window2, self.window3, )))
    hc.assert_that(
        primaries, hc.contains_inanyorder(
            expected_primary_windows,
        ))
    hc.assert_that(
        residuals, hc.contains_inanyorder(
            expected_residual_windows,
        ))
    self.assertEqual(stop_index, 1)

  def test_window_observing_split_on_window_boundary_round_down_on_last_window(
      self):
    restriction_tracker = OffsetRestrictionTracker(self.restriction)
    restriction_tracker.try_claim(30)
    (primaries, residuals, stop_index) = PerWindowInvoker._try_split(
        0.9,
        0,
        3,
        self.windowed_value,
        self.restriction,
        self.watermark_estimator_state,
        self.restriction_provider,
        restriction_tracker,
        self.watermark_estimator)
    # 90% of 2.7 windows = 90% of 270 offset left = 243 offset
    # 30 + 243 = 273 offset --> prefer a split so round to end of window 2
    # instead of no split
    expected_primary_windows, expected_residual_windows = (
        self.create_split_across_windows(
            (self.window1, self.window2, ), (self.window3, )))
    hc.assert_that(
        primaries, hc.contains_inanyorder(
            expected_primary_windows,
        ))
    hc.assert_that(
        residuals, hc.contains_inanyorder(
            expected_residual_windows,
        ))
    self.assertEqual(stop_index, 2)


if __name__ == '__main__':
  unittest.main()
