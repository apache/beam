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

"""Integration tests for cross-language transform expansion."""

# pytype: skip-file

import time
import unittest

import pytest

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import is_empty
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.periodicsequence import PeriodicSequence


@unittest.skipIf(
    not TestPipeline().get_pipeline_options().view_as(
        StandardOptions).streaming,
    "Watermark tests are only valid for streaming jobs.")
class PeriodicSequenceIT(unittest.TestCase):
  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)

  @pytest.mark.it_postcommit
  @pytest.mark.sickbay_direct
  @pytest.mark.sickbay_spark
  @pytest.mark.timeout(
      1800)  # Timeout after 30 minutes to give Dataflow some extra time
  def test_periodicsequence_outputs_valid_watermarks_it(self):
    """Tests periodic sequence with watermarks on dataflow.
    For testing that watermarks are being correctly emitted,
    we make sure that there's not a long gap between an element being
    emitted and being correctly aggregated.
    """
    class FindLongGaps(DoFn):
      def process(self, element):
        emitted_at, unused_count = element
        processed_at = time.time()
        if processed_at - emitted_at > 25:
          yield (
              'Elements emitted took too long to process.',
              emitted_at,
              processed_at)

    start_time = time.time()
    # Run long enough for Dataflow to start up
    duration_sec = 540
    end_time = start_time + duration_sec
    interval = 1

    res = (
        self.test_pipeline
        | 'ImpulseElement' >> beam.Create([(start_time, end_time, interval)])
        | 'ImpulseSeqGen' >> PeriodicSequence()
        | 'MapToCurrentTime' >> beam.Map(lambda element: time.time())
        | 'window_into' >> beam.WindowInto(
            window.FixedWindows(2),
            accumulation_mode=trigger.AccumulationMode.DISCARDING)
        | beam.combiners.Count.PerElement()
        | beam.ParDo(FindLongGaps()))
    assert_that(res, is_empty())

    self.test_pipeline.run().wait_until_finish()


if __name__ == '__main__':
  unittest.main()
