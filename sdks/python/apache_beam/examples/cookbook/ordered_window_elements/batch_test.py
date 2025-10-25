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
import random
import shutil
import sys
import unittest

from parameterized import param
from parameterized import parameterized

import apache_beam as beam
from apache_beam.examples.cookbook.ordered_window_elements.batch import OrderedWindowElements
from apache_beam.examples.cookbook.ordered_window_elements.batch import WindowGapStrategy
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils.timestamp import Timestamp

logging.basicConfig(level=logging.INFO)
#logging.basicConfig(level=logging.WARNING)

options = PipelineOptions([
    "--environment_type=LOOPBACK",
    "--runner=PrismRunner",  #"--runner=FnApiRunner",
    "--prism_log_kind=dev",
    # "--runner=PortableRunner",
    # "--job_endpoint=localhost:8073",
])

ENABLE_LOGGING = False
WINDOW_SIZE = 3


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


def _create_input_batch(elements: list[int], shuffle_data=True):
  if shuffle_data:
    random.shuffle(elements)
  return beam.Create([(Timestamp.of(e), e) for e in elements])


def _create_input_batch_without_timestamp(
    elements: list[int], shuffle_data=True):
  if shuffle_data:
    random.shuffle(elements)
  return beam.Create(elements)


def _convert_timestamp_to_int():
  return beam.MapTuple(
      lambda window, elements:
      ((int(window[0].micros // 1e6), int(window[1].micros // 1e6)),
       [(int(t.micros // 1e6), v) for t, v in elements]))


_go_installed = shutil.which('go') is not None
_in_windows = sys.platform == "win32"


@unittest.skipUnless(_go_installed, 'Go is not installed.')
# TODO: Go environments is not configured correctly on Windows test boxes.
@unittest.skipIf(_in_windows, reason="Not supported on Windows")
class OrderedWindowElementsTest(unittest.TestCase):
  def setUp(self) -> None:
    self.options = PipelineOptions([
        "--environment_type=LOOPBACK",
        "--runner=PrismRunner",
        "--prism_log_kind=dev",
        # # run on an external Portable Runner for debugging
        # "--runner=PortableRunner",
        # "--job_endpoint=localhost:8073",
    ])

    # # dataflow runner option
    # self.options = PipelineOptions([
    #     "--runner=DataflowRunner",
    #     "--temp_location=gs://shunping-test/anomaly-temp",
    #     "--staging_location=gs://shunping-test/anomaly-temp",
    #     "--project=apache-beam-testing",
    #     "--region=us-central1",
    #     "--sdk_location=dist/apache_beam-2.70.0.dev0.tar.gz",
    #     #"--pickle_library=dill",
    #     #"--save_main_session",
    # ])

  def test_default(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_input_batch([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
          | OrderedWindowElements(WINDOW_SIZE))
      result = _maybe_log_elements(result) | _convert_timestamp_to_int()
      assert_that(
          result,
          equal_to([
              ((0, 3), [(0, 0), (1, 1), (2, 2)]),
              ((3, 6), [(3, 3), (4, 4), (5, 5)]),
              ((6, 9), [(6, 6), (7, 7), (8, 8)]),
              ((9, 12), [(9, 9)]),
          ]))

  def test_timestamp_func(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_input_batch_without_timestamp(
              [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
          | OrderedWindowElements(
              WINDOW_SIZE, timestamp=lambda x: Timestamp.of(x)))
      result = _maybe_log_elements(result) | _convert_timestamp_to_int()
      assert_that(
          result,
          equal_to([
              ((0, 3), [(0, 0), (1, 1), (2, 2)]),
              ((3, 6), [(3, 3), (4, 4), (5, 5)]),
              ((6, 9), [(6, 6), (7, 7), (8, 8)]),
              ((9, 12), [(9, 9)]),
          ]))

  def test_offset(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_input_batch([2, 3, 4, 5, 6, 7, 8, 9])
          | OrderedWindowElements(WINDOW_SIZE, offset=2))
      result = _maybe_log_elements(result) | _convert_timestamp_to_int()
      assert_that(
          result,
          equal_to([
              ((2, 5), [(2, 2), (3, 3), (4, 4)]),  # window start at 2
              ((5, 8), [(5, 5), (6, 6), (7, 7)]),
              ((8, 11), [(8, 8), (9, 9)])
          ]))

  def test_slide_interval(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_input_batch([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
          | OrderedWindowElements(WINDOW_SIZE, slide_interval=1))
      result = _maybe_log_elements(result) | _convert_timestamp_to_int()
      assert_that(
          result,
          equal_to([
              ((-2, 1), [(0, 0)]),
              ((-1, 2), [(0, 0), (1, 1)]),
              ((0, 3), [(0, 0), (1, 1), (2, 2)]),
              ((1, 4), [(1, 1), (2, 2), (3, 3)]),
              ((2, 5), [(2, 2), (3, 3), (4, 4)]),
              ((3, 6), [(3, 3), (4, 4), (5, 5)]),
              ((4, 7), [(4, 4), (5, 5), (6, 6)]),
              ((5, 8), [(5, 5), (6, 6), (7, 7)]),
              ((6, 9), [(6, 6), (7, 7), (8, 8)]),
              ((7, 10), [(7, 7), (8, 8), (9, 9)]),
              ((8, 11), [(8, 8), (9, 9)]),
              ((9, 12), [(9, 9)]),
          ]))

  @parameterized.expand([
      param(
          empty_window_strategy=WindowGapStrategy.DISCARD,
          window_start_gap_strategy=WindowGapStrategy.IGNORE),
      param(
          empty_window_strategy=WindowGapStrategy.DISCARD,
          window_start_gap_strategy=WindowGapStrategy.FORWARD_FILL),
      param(
          empty_window_strategy=WindowGapStrategy.IGNORE,
          window_start_gap_strategy=WindowGapStrategy.IGNORE),
      param(
          empty_window_strategy=WindowGapStrategy.IGNORE,
          window_start_gap_strategy=WindowGapStrategy.FORWARD_FILL),
      param(
          empty_window_strategy=WindowGapStrategy.FORWARD_FILL,
          window_start_gap_strategy=WindowGapStrategy.IGNORE),
      param(
          empty_window_strategy=WindowGapStrategy.FORWARD_FILL,
          window_start_gap_strategy=WindowGapStrategy.FORWARD_FILL),
  ])
  def test_gaps(self, empty_window_strategy, window_start_gap_strategy):
    if empty_window_strategy == WindowGapStrategy.DISCARD:
      if window_start_gap_strategy == WindowGapStrategy.IGNORE:
        expected = [
            ((0, 3), [(1, 1), (2, 2)]),
            ((3, 6), [(3, 3), (4, 4)]),
            # empty windows (6, 9), (9, 12), (12, 15) are discarded
            ((15, 18), [(16, 16)]),
            ((18, 21), [(20, 20)]),
        ]
      elif window_start_gap_strategy == WindowGapStrategy.FORWARD_FILL:
        expected = [
            # fill the beginning of (0, 3) with default value `None`
            ((0, 3), [(0, None), (1, 1), (2, 2)]),
            ((3, 6), [(3, 3), (4, 4)]),
            # fill the beginning of (15, 18) with 4 from Timestamp(4)
            ((15, 18), [(15, 4), (16, 16)]),
            # fill the beginning of (18, 21) with 16 from Timestamp(16)
            ((18, 21), [(18, 16), (20, 20)]),
        ]
    elif empty_window_strategy == WindowGapStrategy.IGNORE:
      if window_start_gap_strategy == WindowGapStrategy.IGNORE:
        expected = [
            ((0, 3), [(1, 1), (2, 2)]),
            ((3, 6), [(3, 3), (4, 4)]),
            ((6, 9), []),  # empty windows are kept
            ((9, 12), []),
            ((12, 15), []),
            ((15, 18), [(16, 16)]),
            ((18, 21), [(20, 20)]),
        ]
      elif window_start_gap_strategy == WindowGapStrategy.FORWARD_FILL:
        expected = [
            ((0, 3), [(0, None), (1, 1), (2, 2)]),
            ((3, 6), [(3, 3), (4, 4)]),
            ((6, 9), []),
            ((9, 12), []),
            ((12, 15), []),
            ((15, 18), [(15, 4), (16, 16)]),
            ((18, 21), [(18, 16), (20, 20)]),
        ]
    elif empty_window_strategy == WindowGapStrategy.FORWARD_FILL:
      if window_start_gap_strategy == WindowGapStrategy.IGNORE:
        expected = [
            ((0, 3), [(1, 1), (2, 2)]),
            ((3, 6), [(3, 3), (4, 4)]),
            ((6, 9), [(6, 4)]),  # empty windows are forward filled
            ((9, 12), [(9, 4)]),
            ((12, 15), [(12, 4)]),
            ((15, 18), [(16, 16)]),
            ((18, 21), [(20, 20)]),
        ]
      elif window_start_gap_strategy == WindowGapStrategy.FORWARD_FILL:
        expected = [
            ((0, 3), [(0, None), (1, 1), (2, 2)]),
            ((3, 6), [(3, 3), (4, 4)]),
            ((6, 9), [(6, 4)]),
            ((9, 12), [(9, 4)]),
            ((12, 15), [(12, 4)]),
            ((15, 18), [(15, 4), (16, 16)]),
            ((18, 21), [(18, 16), (20, 20)]),
        ]

    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_input_batch([1, 2, 3, 4, 16, 20])
          | OrderedWindowElements(
              WINDOW_SIZE,
              empty_window_strategy=empty_window_strategy,
              window_start_gap_strategy=window_start_gap_strategy))
      result = _maybe_log_elements(result) | _convert_timestamp_to_int()
      assert_that(result, equal_to(expected))

  @parameterized.expand([
      param(
          empty_window_strategy=WindowGapStrategy.DISCARD,
          window_start_gap_strategy=WindowGapStrategy.IGNORE),
      param(
          empty_window_strategy=WindowGapStrategy.DISCARD,
          window_start_gap_strategy=WindowGapStrategy.FORWARD_FILL),
      param(
          empty_window_strategy=WindowGapStrategy.IGNORE,
          window_start_gap_strategy=WindowGapStrategy.IGNORE),
      param(
          empty_window_strategy=WindowGapStrategy.IGNORE,
          window_start_gap_strategy=WindowGapStrategy.FORWARD_FILL),
      param(
          empty_window_strategy=WindowGapStrategy.FORWARD_FILL,
          window_start_gap_strategy=WindowGapStrategy.IGNORE),
      param(
          empty_window_strategy=WindowGapStrategy.FORWARD_FILL,
          window_start_gap_strategy=WindowGapStrategy.FORWARD_FILL),
  ])
  def test_long_slide(self, empty_window_strategy, window_start_gap_strategy):
    if empty_window_strategy == WindowGapStrategy.DISCARD:
      if window_start_gap_strategy == WindowGapStrategy.IGNORE:
        expected = [((0, 2), [(0, 0)]), ((5, 7), [(6, 6)]),
                    ((15, 17), [(16, 16)])]
      elif window_start_gap_strategy == WindowGapStrategy.FORWARD_FILL:
        expected = [((0, 2), [(0, 0)]), ((5, 7), [(5, 4), (6, 6)]),
                    ((15, 17), [(15, 7), (16, 16)])]
    elif empty_window_strategy == WindowGapStrategy.IGNORE:
      if window_start_gap_strategy == WindowGapStrategy.IGNORE:
        expected = [((0, 2), [(0, 0)]), ((5, 7), [(6, 6)]), ((10, 12), []),
                    ((15, 17), [(16, 16)])]
      elif window_start_gap_strategy == WindowGapStrategy.FORWARD_FILL:
        expected = [((0, 2), [(0, 0)]), ((5, 7), [(5, 4), (6, 6)]),
                    ((10, 12), []), ((15, 17), [(15, 7), (16, 16)])]
    elif empty_window_strategy == WindowGapStrategy.FORWARD_FILL:
      if window_start_gap_strategy == WindowGapStrategy.IGNORE:
        expected = [((0, 2), [(0, 0)]), ((5, 7), [(6, 6)]),
                    ((10, 12), [(10, 7)]), ((15, 17), [(16, 16)])]
      elif window_start_gap_strategy == WindowGapStrategy.FORWARD_FILL:
        expected = [((0, 2), [(0, 0)]), ((5, 7), [(5, 4), (6, 6)]),
                    ((10, 12), [(10, 7)]), ((15, 17), [(15, 7), (16, 16)])]
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_input_batch([0, 2, 4, 6, 7, 16])
          | OrderedWindowElements(
              2,
              5,
              0,
              -100,
              empty_window_strategy=empty_window_strategy,
              window_start_gap_strategy=window_start_gap_strategy)
      )  # window size < slide interval
      result = _maybe_log_elements(result) | _convert_timestamp_to_int()
      assert_that(result, equal_to(expected))


if __name__ == '__main__':
  unittest.main()
