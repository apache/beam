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
import unittest

import apache_beam as beam
from apache_beam.examples.inference.anomaly_detection.timesfm_anomaly_detection.ordered_sliding_window import FillGapsFn
from apache_beam.examples.inference.anomaly_detection.timesfm_anomaly_detection.ordered_sliding_window import \
    OrderedSlidingWindowFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.utils.timestamp import Timestamp

_LOGGER = logging.getLogger(__name__)


class DoFnTests(unittest.TestCase):
  def test_pipeline_with_periodic_stream_data(self):

    WINDOW_SIZE = 10
    SLIDE_INTERVAL = 3
    EXPECTED_INTERVAL = 1  # per second expected interval

    data = []
    for i in range(20):
      ts = i
      data.append((Timestamp(ts), i))

    expected = [
        (
            0,
            (
                Timestamp(0),
                Timestamp(10),
                [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0])),
        (
            0,
            (
                Timestamp(3),
                Timestamp(13),
                [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0])),
        (
            0,
            (
                Timestamp(6),
                Timestamp(16),
                [6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0])),
        (
            0,
            (
                Timestamp(9),
                Timestamp(19),
                [9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0])),
        (
            0,
            (
                Timestamp(12),
                Timestamp(22),
                [12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 'NaN',
                 'NaN'])),
        (
            0,
            (
                Timestamp(15),
                Timestamp(25), [
                    15.0,
                    16.0,
                    17.0,
                    18.0,
                    19.0,
                    'NaN',
                    'NaN',
                    'NaN',
                    'NaN',
                    'NaN'
                ])),
        (
            0,
            (
                Timestamp(18),
                Timestamp(28),
                [
                    18.0,
                    19.0,
                    'NaN',
                    'NaN',
                    'NaN',
                    'NaN',
                    'NaN',
                    'NaN',
                    'NaN',
                    'NaN'
                ]))
    ]

    options = PipelineOptions([
        "--streaming",
        "--environment_type=LOOPBACK",
        "--runner=PrismRunner",
    ])
    with beam.Pipeline(options=options) as p:
      output = (
          p
          | PeriodicImpulse(data=data, fire_interval=0.01)
          | beam.WithKeys(0)
          | "SlidingWindow" >> beam.ParDo(
              OrderedSlidingWindowFn(
                  window_size=WINDOW_SIZE, slide_interval=SLIDE_INTERVAL))
          | "FillGaps" >> beam.ParDo(
              FillGapsFn(expected_interval=EXPECTED_INTERVAL)))

      assert_that(output, equal_to(expected))

  def test_pipeline_with_periodic_stream_data_with_missing_values(self):

    WINDOW_SIZE = 10
    SLIDE_INTERVAL = 3
    EXPECTED_INTERVAL = 1  # per second expected interval

    data = []
    timestamps_to_drop = {5, 6, 7, 25, 26, 31, 32, 33}
    for i in range(20):
      if i not in timestamps_to_drop:
        ts = i
        data.append((Timestamp(ts), i))

    expected = [
        (
            0,
            (
                Timestamp(0),
                Timestamp(10),
                [0.0, 1.0, 2.0, 3.0, 4.0, 'NaN', 'NaN', 'NaN', 8.0, 9.0])),
        (
            0,
            (
                Timestamp(3),
                Timestamp(13),
                [3.0, 4.0, 'NaN', 'NaN', 'NaN', 8.0, 9.0, 10.0, 11.0, 12.0])),
        (
            0,
            (
                Timestamp(6),
                Timestamp(16),
                ['NaN', 'NaN', 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0])),
        (
            0,
            (
                Timestamp(9),
                Timestamp(19),
                [9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0])),
        (
            0,
            (
                Timestamp(12),
                Timestamp(22),
                [12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 'NaN',
                 'NaN'])),
        (
            0,
            (
                Timestamp(15),
                Timestamp(25), [
                    15.0,
                    16.0,
                    17.0,
                    18.0,
                    19.0,
                    'NaN',
                    'NaN',
                    'NaN',
                    'NaN',
                    'NaN'
                ])),
        (
            0,
            (
                Timestamp(18),
                Timestamp(28),
                [
                    18.0,
                    19.0,
                    'NaN',
                    'NaN',
                    'NaN',
                    'NaN',
                    'NaN',
                    'NaN',
                    'NaN',
                    'NaN'
                ]))
    ]

    options = PipelineOptions([
        "--streaming",
        "--environment_type=LOOPBACK",
        "--runner=PrismRunner",
    ])
    with beam.Pipeline(options=options) as p:
      output = (
          p
          | PeriodicImpulse(data=data, fire_interval=0.01)
          | beam.WithKeys(0)
          | "SlidingWindow" >> beam.ParDo(
              OrderedSlidingWindowFn(
                  window_size=WINDOW_SIZE, slide_interval=SLIDE_INTERVAL))
          | "FillGaps" >> beam.ParDo(
              FillGapsFn(expected_interval=EXPECTED_INTERVAL)))

      assert_that(output, equal_to(expected))

  @unittest.skip("This test is skipped")
  def test_pipeline_with_late_data(self):
    WINDOW_SIZE = 10
    SLIDE_INTERVAL = 3
    EXPECTED_INTERVAL = 1

    data = [(Timestamp(0), 0.0), (Timestamp(2), 2.0), (Timestamp(3), 3.0),
            (Timestamp(4), 4.0), (Timestamp(5), 5.0), (Timestamp(6), 6.0),
            (Timestamp(7), 7.0), (Timestamp(8), 8.0), (Timestamp(9), 9.0),
            (Timestamp(10), 10.0), (Timestamp(11), 11.0), (Timestamp(12), 12.0),
            (Timestamp(13), 13.0), (Timestamp(14), 14.0), (Timestamp(15), 15.0),
            (Timestamp(1), 1.0)]

    expected = [
        (
            0,
            (
                Timestamp(0),
                Timestamp(10),
                [0.0, 'NaN', 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0])),
        (
            0,
            (
                Timestamp(3),
                Timestamp(13),
                [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0])),
        (
            0,
            (
                Timestamp(6),
                Timestamp(16),
                [6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0])),
        (
            0,
            (
                Timestamp(9),
                Timestamp(19), [9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0])),
        (0, (Timestamp(12), Timestamp(22), [12.0, 13.0, 14.0, 15.0])),
        (0, (Timestamp(15), Timestamp(25), [15.0]))
    ]

    options = PipelineOptions([
        "--streaming",
        "--environment_type=LOOPBACK",
        "--runner=PrismRunner",
    ])
    with beam.Pipeline(options=options) as p:
      output = (
          p
          | "PeriodicLateStream" >> PeriodicImpulse(data=data, fire_interval=1)
          | "KeyLateData" >> beam.WithKeys(lambda x: 0)
          | "SlidingWindowLateData" >> beam.ParDo(
              OrderedSlidingWindowFn(
                  window_size=WINDOW_SIZE, slide_interval=SLIDE_INTERVAL))
          | "FillGapsLateData" >> beam.ParDo(
              FillGapsFn(expected_interval=EXPECTED_INTERVAL)))
      assert_that(output, equal_to(expected))


if __name__ == '__main__':
  unittest.main()
