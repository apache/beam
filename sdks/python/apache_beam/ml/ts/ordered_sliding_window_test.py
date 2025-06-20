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

import apache_beam as beam
from ordered_sliding_window import OrderedSlidingWindowFn, FillGapsFn
import unittest
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
import math
from apache_beam.utils.timestamp import Timestamp, MIN_TIMESTAMP
from util import PeriodicStream
import random
import logging
from apache_beam.options.pipeline_options import PipelineOptions
import numpy as np

logging.basicConfig(level=logging.INFO)

def format_for_comparison(element):
    """Converts np.nan in the data list to the string 'NaN' for stable comparison."""
    key, (start_ts, end_ts, data_list) = element
    formatted_list = ['NaN' if isinstance(x, float) and np.isnan(x) else x for x in data_list]
    return (key, (start_ts, end_ts, formatted_list))

class DoFnTests(unittest.TestCase):

    def test_pipeline_with_periodic_stream_data(self):
        """Tests the pipeline using the specific data sequence from the user's example."""
        
        WINDOW_SIZE =10
        SLIDE_INTERVAL = 3
        EXPECTED_INTERVAL = 1 # per second expected interval
        
        data = []
        for i in range(20):
            ts = i
            data.append((Timestamp(ts), i))

        # random.shuffle(data)
        print([i[1] for i in data])


        expected = [
            (0, (Timestamp(0), Timestamp(10), [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0])),
            (0, (Timestamp(3), Timestamp(13), [3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0])),
            (0, (Timestamp(6), Timestamp(16), [6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0])),
            (0, (Timestamp(9), Timestamp(19), [9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0])),
            (0, (Timestamp(12), Timestamp(22), [12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0])),
            (0, (Timestamp(15), Timestamp(25), [15.0, 16.0, 17.0, 18.0, 19.0])),
            (0, (Timestamp(18), Timestamp(28), [18.0, 19.0]))
        ]

        # 3. Pipeline using PeriodicStream

        options = PipelineOptions([
            "--streaming",
            "--environment_type=LOOPBACK",
            "--runner=PrismRunner",
        ])
        with beam.Pipeline(options=options) as p:
            output = (
                p
                | PeriodicStream(data, interval=0.01)
                | beam.WithKeys(0)
                | "SlidingWindow" >> beam.ParDo(OrderedSlidingWindowFn(window_size=WINDOW_SIZE, slide_interval=SLIDE_INTERVAL))
                | "FillGaps" >> beam.ParDo(FillGapsFn(expected_interval=EXPECTED_INTERVAL))
                | 'Format For Comparison' >> beam.Map(format_for_comparison)

            )

            assert_that(output, equal_to(expected))

    def test_pipeline_with_periodic_stream_data_with_missing_values(self):
        """Tests the pipeline using the specific data sequence from the user's example."""
        
        WINDOW_SIZE =10
        SLIDE_INTERVAL = 3
        EXPECTED_INTERVAL = 1 # per second expected interval
        
        data = []
        timestamps_to_drop = {5, 6, 7, 25, 26, 31, 32, 33}
        for i in range(20):
            # Check if the integer value should be dropped
            if i not in timestamps_to_drop:
                ts = i
                data.append((Timestamp(ts), i))

        # random.shuffle(data)
        print([i[1] for i in data])


        expected = [
            (0, (Timestamp(0), Timestamp(10), [0.0, 1.0, 2.0, 3.0, 4.0, 'NaN', 'NaN', 'NaN', 8.0, 9.0])),
            (0, (Timestamp(3), Timestamp(13), [3.0, 4.0, 'NaN', 'NaN', 'NaN', 8.0, 9.0, 10.0, 11.0, 12.0])),
            (0, (Timestamp(6), Timestamp(16), [8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0])),
            (0, (Timestamp(9), Timestamp(19), [9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0])),
            (0, (Timestamp(12), Timestamp(22), [12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0])),
            (0, (Timestamp(15), Timestamp(25), [15.0, 16.0, 17.0, 18.0, 19.0])),
            (0, (Timestamp(18), Timestamp(28), [18.0, 19.0]))
        ]

        # 3. Pipeline using PeriodicStream

        options = PipelineOptions([
            "--streaming",
            "--environment_type=LOOPBACK",
            "--runner=PrismRunner",
        ])
        with beam.Pipeline(options=options) as p:
            output = (
                p
                | PeriodicStream(data, interval=0.01)
                | beam.WithKeys(0)
                | "SlidingWindow" >> beam.ParDo(OrderedSlidingWindowFn(window_size=WINDOW_SIZE, slide_interval=SLIDE_INTERVAL))
                | "FillGaps" >> beam.ParDo(FillGapsFn(expected_interval=EXPECTED_INTERVAL))
                | 'Format For Comparison' >> beam.Map(format_for_comparison)
            )

            assert_that(output, equal_to(expected))


if __name__ == '__main__':
    unittest.main()
 