# -*- coding: utf-8 -*-
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

"""Test for the flight delay example."""

# pytype: skip-file

from __future__ import absolute_import

import logging
import os
import unittest
import uuid

import pandas as pd
import pytest

from apache_beam.examples.dataframe import flight_delays
from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline


class FlightDelaysTest(unittest.TestCase):
  EXPECTED = {
      '2012-12-23': [
          ('AA', 20.082559339525282, 12.825593395252838),
          ('AS', 5.0456273764258555, 1.0722433460076046),
          ('B6', 20.646569646569645, 16.405405405405407),
          ('DL', 5.241148325358852, -3.2401913875598085),
          ('EV', 9.982053838484546, 4.40777666999003),
          ('F9', 23.67883211678832, 25.27007299270073),
          ('FL', 4.4602272727272725, -0.8352272727272727),
          ('HA', -1.0829015544041452, 0.010362694300518135),
          ('MQ', 8.912912912912914, 3.6936936936936937),
          ('OO', 30.526699029126213, 31.17961165048544),
          ('UA', 19.142555438225976, 11.07180570221753),
          ('US', 3.092541436464088, -2.350828729281768),
          ('VX', 62.755102040816325, 62.61224489795919),
          ('WN', 12.05824508320726, 6.713313161875946),
          ('YV', 16.155844155844157, 13.376623376623376),
      ],
      '2012-12-24': [
          ('AA', 7.049086757990867, -1.5970319634703196),
          ('AS', 0.5917602996254682, -2.2659176029962547),
          ('B6', 8.070993914807302, 2.73630831643002),
          ('DL', 3.700745473908413, -2.2396166134185305),
          ('EV', 7.322115384615385, 2.3653846153846154),
          ('F9', 13.786764705882351, 15.5),
          ('FL', 2.416909620991253, 2.224489795918368),
          ('HA', -2.6785714285714284, -2.4744897959183674),
          ('MQ', 15.818181818181818, 9.935828877005347),
          ('OO', 10.902374670184695, 10.08575197889182),
          ('UA', 10.935406698564593, -1.3337320574162679),
          ('US', 1.369281045751634, -1.4101307189542485),
          ('VX', 3.841666666666667, -2.4166666666666665),
          ('WN', 7.3715753424657535, 0.348458904109589),
          ('YV', 0.32, 0.78),
      ],
      '2012-12-25': [
          ('AA', 23.551581843191197, 35.62585969738652),
          ('AS', 3.4816326530612245, 0.27346938775510204),
          ('B6', 9.10590631364562, 3.989816700610998),
          ('DL', 2.2863795110593714, -3.668218859138533),
          ('EV', 17.35576923076923, 16.414835164835164),
          ('F9', 19.38, 21.786666666666665),
          ('FL', 1.3823529411764706, 0.9205882352941176),
          ('HA', -4.725806451612903, -3.9946236559139785),
          ('MQ', 32.527716186252775, 44.148558758314856),
          ('OO', 15.788595271210012, 16.617524339360223),
          ('UA', 16.663145539906104, 10.772300469483568),
          ('US', 2.7953216374269005, 0.2236842105263158),
          ('VX', 23.62878787878788, 23.636363636363637),
          ('WN', 14.423791821561338, 10.142193308550183),
          ('YV', 11.256302521008404, 11.659663865546218),
      ],
  }

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.outdir = (
        self.test_pipeline.get_option('temp_location') + '/flight_delays_it-' +
        str(uuid.uuid4()))
    self.output_path = os.path.join(self.outdir, 'output.csv')

  def tearDown(self):
    FileSystems.delete([self.outdir + '/'])

  @pytest.mark.examples_postcommit
  @pytest.mark.it_postcommit
  def test_flight_delays(self):
    flight_delays.run_flight_delay_pipeline(
        self.test_pipeline,
        start_date='2012-12-23',
        end_date='2012-12-25',
        output=self.output_path)

    def read_csv(path):
      with FileSystems.open(path) as fp:
        return pd.read_csv(fp)

    # Parse result file and compare.
    for date, expectation in self.EXPECTED.items():
      result_df = pd.concat(
          read_csv(metadata.path) for metadata in FileSystems.match(
              [f'{self.output_path}-{date}*'])[0].metadata_list)
      result_df = result_df.sort_values('airline').reset_index(drop=True)

      expected_df = pd.DataFrame(
          expectation, columns=['airline', 'departure_delay', 'arrival_delay'])
      expected_df = expected_df.sort_values('airline').reset_index(drop=True)

      try:
        pd.testing.assert_frame_equal(result_df, expected_df)
      except AssertionError as e:
        raise AssertionError(
            f"date={date!r} result DataFrame:\n\n"
            f"{result_df}\n\n"
            "Differs from Expectation:\n\n"
            f"{expected_df}") from e


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
