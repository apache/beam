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

"""Test for OrderedBatchElements."""

import logging
import unittest

import apache_beam as beam
from apache_beam.examples.cookbook.ordered_batch_elements import OrderedBatchElements  # pylint: disable=line-too-long
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.periodicsequence import RebaseMode
from apache_beam.utils.timestamp import Timestamp

logging.basicConfig(level=logging.WARNING)

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


def _create_input_stream(elements: list[int]):
  now = Timestamp.now()
  length = len(elements)
  fire_interval = 0.1
  return PeriodicImpulse(
      data=[(Timestamp.of(e), e) for e in elements],
      fire_interval=fire_interval,
      start_timestamp=now,
      stop_timestamp=now + length * fire_interval,
      rebase=RebaseMode.REBASE_ALL,
  )


class OrderedBatchElementsTest(unittest.TestCase):
  def setUp(self) -> None:
    self.options = PipelineOptions([
        "--streaming",
        "--environment_type=LOOPBACK",
        "--runner=PrismRunner",
        "--prism_log_kind=dev",
    ])

    # # dataflow runner option
    # self.options = PipelineOptions([
    #     "--streaming",
    #     "--runner=DataflowRunner",
    #     "--temp_location=[TEMP_LOCATION],
    #     "--staging_location=[STAGING_LOCATION],
    #     "--project=[PROJECT_ID],
    #     "--region=[PROJECT_REGION]",
    #     "--sdk_location=[LOCAL_SDK_TAR_BALL]",
    # ])

  def test_default(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_input_stream([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
          | OrderedBatchElements(WINDOW_SIZE))
      result = _maybe_log_elements(result)
      assert_that(
          result,
          equal_to([
              [0, 1, 2],
              [3, 4, 5],
              [6, 7, 8],
              [9],
              [9],
              [9],
              [9],
              [9],
              [9],
          ]))

  def test_non_zero_offset_and_default_value(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_input_stream([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
          | OrderedBatchElements(WINDOW_SIZE, 1, -100))
      result = _maybe_log_elements(result)
      assert_that(
          result,
          equal_to([
              [-100, 0],  # default value is -100
              [1, 2, 3],
              [4, 5, 6],
              [7, 8, 9],
              [9],
              [9],
              [9],
              [9],
              [9],
          ]))

  def test_keyed_input(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_input_stream([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
          | beam.WithKeys("my_key")  # key is present in the output
          | OrderedBatchElements(WINDOW_SIZE))
      result = _maybe_log_elements(result)
      assert_that(
          result,
          equal_to([
              ("my_key", [None, 1, 2]),
              ("my_key", [3, 4, 5]),
              ("my_key", [6, 7, 8]),
              ("my_key", [9, 10]),
              ("my_key", [10]),
              ("my_key", [10]),
              ("my_key", [10]),
              ("my_key", [10]),
              ("my_key", [10]),
          ]))

  def test_ordered_data_with_gap(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_input_stream([0, 1, 2, 3, 4, 16, 17, 18, 19, 20])
          | OrderedBatchElements(WINDOW_SIZE))
      result = _maybe_log_elements(result)
      assert_that(
          result,
          equal_to([
              [0, 1, 2],
              [3, 4],
              [4],  # window[6-9] is empty and we add the last seen element
              [4],  # window[9-12] is empty
              [4],  # window[12-15] is empty
              [4, 16, 17],
              [18, 19, 20],
              [20],
              [20],
              [20],
              [20],
              [20],
          ]))

  def test_single_late_data_with_no_allowed_lateness(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_input_stream([0, 1, 2, 3, 4, 6, 7, 8, 9, 5])
          | OrderedBatchElements(WINDOW_SIZE))
      result = _maybe_log_elements(result)
      assert_that(
          result,
          equal_to([
              [0, 1, 2],
              [3, 4],  # 5 is late and discarded
              [6, 7, 8],
              [9],
              [9],
              [9],
              [9],
              [9],
              [9],
          ]))

  def test_single_late_data_with_allowed_lateness(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_input_stream([0, 1, 2, 3, 4, 6, 7, 8, 9, 5])
          | OrderedBatchElements(WINDOW_SIZE, allowed_lateness=4))
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
              [9],
              [9],
              [9],
              [9],
              [9],
          ]))

  def test_reversed_ordered_data_with_allowed_lateness(self):
    with TestPipeline(options=self.options) as p:
      result = (
          p | _create_input_stream([9, 8, 7, 6, 5, 4, 3, 2, 1, 0])
          | OrderedBatchElements(WINDOW_SIZE, allowed_lateness=5))
      result = _maybe_log_elements(result)
      assert_that(
          result,
          equal_to([
              # allow late data up to:
              # 9 (watermark before late data) - 5 (allowed lateness) = 4
              [None, 4, 5],
              [6, 7, 8],
              [9],
              [9],
              [9],
              [9],
              [9],
              [9],
          ]))


if __name__ == '__main__':
  unittest.main()
