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

import logging
import shutil
import sys
import unittest

from parameterized import param
from parameterized import parameterized
from parameterized import parameterized_class

import apache_beam as beam
from apache_beam.examples.cookbook.ordered_window_elements.streaming import BufferStateType
from apache_beam.examples.cookbook.ordered_window_elements.streaming import OrderedWindowElements
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.periodicsequence import RebaseMode
from apache_beam.utils.timestamp import Timestamp

logging.basicConfig(level=logging.WARNING)

ENABLE_LOGGING = False
WINDOW_SIZE = 3
FIRE_INTERVAL = 0.5


def _maybe_log_elements(pcoll, prefix="result="):
  if ENABLE_LOGGING:
    return pcoll | beam.LogElements(
        prefix=prefix,
        level=logging.WARNING,
        with_timestamp=True,
        with_window=True,
        use_epoch_time=True)
  else:
    return pcoll


# Creates an unbounded source via `PeriodicImpulse`, simulating a continuous
# stream of elements fired at a fixed interval. This method is closer to
# real-world streaming but is sensitive to system load and can cause test
# flakiness.
# If the test runner is slow or under heavy load, elements may be delayed and
# processed in a single large bundle. This can defeat the purpose of testing
# time-based logic, as the elements will not arrive distributed over time as
# intended.
def _create_periodic_impulse_stream(elements: list[int]):
  now = Timestamp.now()
  length = len(elements)
  fire_interval = FIRE_INTERVAL
  return PeriodicImpulse(
      data=[(Timestamp.of(e), e) for e in elements],
      fire_interval=fire_interval,
      start_timestamp=now,
      stop_timestamp=now + length * fire_interval,
      rebase=RebaseMode.REBASE_ALL,
  )


# Creates an unbounded source via `TestStream`, allowing precise control over
# watermarks and element emission for deterministic testing scenarios. However,
# it is an instantaneous data stream and it is less realistic than the stream
# from `PeriodicImpulse`.
def _create_test_stream(elements: list[int]):
  test_stream = TestStream()
  wm = None
  for e in elements:
    test_stream.add_elements([e], event_timestamp=e)
    if wm is None or wm < e:
      wm = e
      test_stream.advance_watermark_to(wm)

  test_stream.advance_watermark_to_infinity()
  return test_stream


_go_installed = shutil.which('go') is not None
_in_windows = sys.platform == "win32"


@unittest.skipUnless(_go_installed, 'Go is not installed.')
# TODO: Go environments is not configured correctly on Windows test boxes.
@unittest.skipIf(_in_windows, reason="Not supported on Windows")
@parameterized_class(
    'buffer_state_type',
    [
        (BufferStateType.ORDERED_LIST, ),
        (BufferStateType.BAG, ),
        (BufferStateType.VALUE, ),
    ])
class OrderedWindowElementsTest(unittest.TestCase):
  def setUp(self) -> None:
    self.options = PipelineOptions([
        "--streaming",
        "--environment_type=LOOPBACK",
        "--runner=PrismRunner",
        "--prism_log_kind=dev",
        # # run on an external Portable Runner for debugging
        # "--runner=PortableRunner",
        # "--job_endpoint=localhost:8073",
    ])

    # # dataflow runner option
    # self.options = PipelineOptions([
    #     "--streaming",
    #     "--runner=DataflowRunner",
    #     "--temp_location=gs://shunping-test/anomaly-temp",
    #     "--staging_location=gs://shunping-test/anomaly-temp",
    #     "--project=apache-beam-testing",
    #     "--region=us-central1",
    #     "--sdk_location=dist/apache_beam-2.69.0.dev0.tar.gz",
    #     #"--pickle_library=dill",
    #     #"--save_main_session",
    # ])

  def test_default(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_test_stream([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
          | OrderedWindowElements(
              WINDOW_SIZE,
              stop_timestamp=13,
              buffer_state_type=self.buffer_state_type))
      result = _maybe_log_elements(result)
      assert_that(result, equal_to([
          [0, 1, 2],
          [3, 4, 5],
          [6, 7, 8],
          [9],
      ]))

  def test_slide_interval(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_test_stream([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
          | OrderedWindowElements(WINDOW_SIZE, 1, stop_timestamp=13))
      result = _maybe_log_elements(result)
      assert_that(
          result,
          equal_to([
              [0, 1, 2],
              [1, 2, 3],
              [2, 3, 4],
              [3, 4, 5],
              [4, 5, 6],
              [5, 6, 7],
              [6, 7, 8],
              [7, 8, 9],
              [8, 9],
              [9],
          ]))

  def test_keyed_input(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_test_stream([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
          | beam.WithKeys("my_key")  # key is present in the output
          | OrderedWindowElements(WINDOW_SIZE, stop_timestamp=13))
      result = _maybe_log_elements(result)
      assert_that(
          result,
          equal_to([
              ("my_key", [1, 2]),
              ("my_key", [3, 4, 5]),
              ("my_key", [6, 7, 8]),
              ("my_key", [9, 10]),
          ]))

  @parameterized.expand([
      param(fill_window_start=False),
      param(fill_window_start=True),
  ])
  def test_non_zero_offset_and_default_value(self, fill_window_start):
    if fill_window_start:
      expected = [
          [-100,
           0],  # window [-2, 1), and the start is filled with default value
          [1, 2, 3],  # window [1, 4)
          [4, 5, 6],
          [7, 8, 9],
      ]
    else:
      expected = [
          [0],  # window [-2, 1)
          [1, 2, 3],  # window [1, 4)
          [4, 5, 6],
          [7, 8, 9],
      ]

    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_test_stream([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
          | OrderedWindowElements(
              WINDOW_SIZE,
              offset=1,
              default_start_value=-100,
              fill_start_if_missing=fill_window_start,
              stop_timestamp=13))
      result = _maybe_log_elements(result)
      assert_that(result, equal_to(expected))

  @parameterized.expand([
      param(fill_window_start=False),
      param(fill_window_start=True),
  ])
  def test_ordered_data_with_gap(self, fill_window_start):
    if fill_window_start:
      expected = [
          [0, 1, 2],
          [3, 4],
          [4],  # window [6, 9) is empty, so the start is filled. Same as below.
          [4],  # window [9, 12) is empty
          [4],  # window [12, 15) is empty
          [4, 16, 17],  # window [15, 18) misses the start as well.
          [18, 19, 20],
      ]
    else:
      expected = [
          [0, 1, 2],
          [3, 4],
          [],  # window [6, 9) is empty
          [],  # window [9, 12) is empty
          [],  # window [12, 15) is empty
          [16, 17],
          [18, 19, 20],
      ]
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_test_stream([0, 1, 2, 3, 4, 16, 17, 18, 19, 20])
          | OrderedWindowElements(
              WINDOW_SIZE,
              fill_start_if_missing=fill_window_start,
              stop_timestamp=23))
      result = _maybe_log_elements(result)
      assert_that(result, equal_to(expected))

  def test_single_late_data_with_no_allowed_lateness(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_test_stream([0, 1, 2, 3, 4, 6, 7, 8, 9, 5])
          | OrderedWindowElements(WINDOW_SIZE, stop_timestamp=13))
      result = _maybe_log_elements(result)
      assert_that(
          result,
          equal_to([
              [0, 1, 2],
              [3, 4],  # 5 is late and discarded
              [6, 7, 8],
              [9],
          ]))

  def test_single_late_data_with_allowed_lateness(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_test_stream([0, 1, 2, 3, 4, 6, 7, 8, 9, 5])
          | OrderedWindowElements(
              WINDOW_SIZE, allowed_lateness=4, stop_timestamp=17))
      result = _maybe_log_elements(result)
      assert_that(
          result,
          equal_to([
              [0, 1, 2],
              # allow late data up to:
              # 9 (watermark before late data) - 4 (allowed lateness) = 5
              [3, 4, 5],
              [6, 7, 8],
              [9],
          ]))

  @parameterized.expand([
      param(fill_start=False),
      param(fill_start=True),
  ])
  def test_reversed_ordered_data_with_allowed_lateness(self, fill_start):
    if fill_start:
      expected = [
          # allow late data up to:
          # 9 (watermark before late data) - 5 (allowed lateness) = 4
          [None, 4, 5],
          [6, 7, 8],
          [9],
          [9],
          [9],
      ]
    else:
      expected = [
          [4, 5],
          [6, 7, 8],
          [9],
          [],
          [],
      ]
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_test_stream([9, 8, 7, 6, 5, 4, 3, 2, 1, 0])
          | OrderedWindowElements(
              WINDOW_SIZE,
              fill_start_if_missing=fill_start,
              allowed_lateness=5,
              stop_timestamp=25))
      result = _maybe_log_elements(result)
      assert_that(result, equal_to(expected))

  def test_multiple_late_data_with_allowed_lateness(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_test_stream([1, 2, 9, 3, 14, 7, 5, 12, 16, 17])
          | OrderedWindowElements(
              WINDOW_SIZE,
              1,
              allowed_lateness=6,
              fill_start_if_missing=True,
              stop_timestamp=28))
      result = _maybe_log_elements(result)
      assert_that(
          result,
          equal_to([
              [1, 2, 3],
              [2, 3],
              [3],
              [3],
              [3],
              [3],
              [3, 9],
              [3, 9],
              [9],
              [9, 12],
              [9, 12],
              [12, 14],
              [12, 14],
              [14, 16],
              [14, 16, 17],
              [16, 17],
              [17],
              [17],
          ]))


if __name__ == '__main__':
  unittest.main()
