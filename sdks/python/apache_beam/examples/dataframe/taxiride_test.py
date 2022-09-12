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

"""Unit tests for the taxiride example pipelines."""

# pytype: skip-file

from __future__ import absolute_import

import glob
import logging
import os
import re
import tempfile
import unittest

import pandas as pd

import apache_beam as beam
from apache_beam.examples.dataframe import taxiride
from apache_beam.testing.util import open_shards


class TaxiRideExampleTest(unittest.TestCase):

  # First 10 lines from gs://apache-beam-samples/nyc_taxi/misc/sample.csv
  # pylint: disable=line-too-long
  SAMPLE_RIDES = """VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge
  1,2019-01-01 00:46:40,2019-01-01 00:53:20,1,1.50,1,N,151,239,1,7,0.5,0.5,1.65,0,0.3,9.95,
  1,2019-01-01 00:59:47,2019-01-01 01:18:59,1,2.60,1,N,239,246,1,14,0.5,0.5,1,0,0.3,16.3,
  2,2018-12-21 13:48:30,2018-12-21 13:52:40,3,.00,1,N,236,236,1,4.5,0.5,0.5,0,0,0.3,5.8,
  2,2018-11-28 15:52:25,2018-11-28 15:55:45,5,.00,1,N,193,193,2,3.5,0.5,0.5,0,0,0.3,7.55,
  2,2018-11-28 15:56:57,2018-11-28 15:58:33,5,.00,2,N,193,193,2,52,0,0.5,0,0,0.3,55.55,
  2,2018-11-28 16:25:49,2018-11-28 16:28:26,5,.00,1,N,193,193,2,3.5,0.5,0.5,0,5.76,0.3,13.31,
  2,2018-11-28 16:29:37,2018-11-28 16:33:43,5,.00,2,N,193,193,2,52,0,0.5,0,0,0.3,55.55,
  1,2019-01-01 00:21:28,2019-01-01 00:28:37,1,1.30,1,N,163,229,1,6.5,0.5,0.5,1.25,0,0.3,9.05,
  1,2019-01-01 00:32:01,2019-01-01 00:45:39,1,3.70,1,N,229,7,1,13.5,0.5,0.5,3.7,0,0.3,18.5
  """
  # pylint: enable=line-too-long

  SAMPLE_ZONE_LOOKUP = """"LocationID","Borough","Zone","service_zone"
  7,"Queens","Astoria","Boro Zone"
  193,"Queens","Queensbridge/Ravenswood","Boro Zone"
  229,"Manhattan","Sutton Place/Turtle Bay North","Yellow Zone"
  236,"Manhattan","Upper East Side North","Yellow Zone"
  239,"Manhattan","Upper West Side South","Yellow Zone"
  246,"Manhattan","West Chelsea/Hudson Yards","Yellow Zone"
  """

  def setUp(self):
    self.tmpdir = tempfile.TemporaryDirectory()
    self.input_path = os.path.join(self.tmpdir.name, 'rides*.csv')
    self.lookup_path = os.path.join(self.tmpdir.name, 'lookup.csv')
    self.output_path = os.path.join(self.tmpdir.name, 'output.csv')

    # Duplicate sample data in 100 different files to replicate multi-file read
    for i in range(100):
      with open(os.path.join(self.tmpdir.name, f'rides{i}.csv'), 'w') as fp:
        fp.write(self.SAMPLE_RIDES)

    with open(self.lookup_path, 'w') as fp:
      fp.write(self.SAMPLE_ZONE_LOOKUP)

  def tearDown(self):
    self.tmpdir.cleanup()

  def test_aggregation(self):
    # Compute expected result
    rides = pd.concat(pd.read_csv(path) for path in glob.glob(self.input_path))
    expected_counts = rides.groupby('DOLocationID').passenger_count.sum()

    taxiride.run_aggregation_pipeline(
        beam.Pipeline(), self.input_path, self.output_path)

    # Parse result file and compare.
    # TODO(https://github.com/apache/beam/issues/20926): taxiride examples
    # should produce int sums, not floats
    results = []
    with open_shards(f'{self.output_path}-*') as result_file:
      for line in result_file:
        match = re.search(r'(\S+),([0-9\.]+)', line)
        if match is not None:
          results.append((int(match.group(1)), int(float(match.group(2)))))
        elif line.strip():
          self.assertEqual(line.strip(), 'DOLocationID,passenger_count')
    self.assertEqual(sorted(results), sorted(expected_counts.items()))

  def test_enrich(self):
    # Compute expected result
    rides = pd.concat(pd.read_csv(path) for path in glob.glob(self.input_path))
    zones = pd.read_csv(self.lookup_path)
    rides = rides.merge(
        zones.set_index('LocationID').Borough,
        right_index=True,
        left_on='DOLocationID',
        how='left')
    expected_counts = rides.groupby('Borough').passenger_count.sum()

    taxiride.run_enrich_pipeline(
        beam.Pipeline(), self.input_path, self.output_path, self.lookup_path)

    # Parse result file and compare.
    # TODO(BEAM-XXXX): taxiride examples should produce int sums, not floats
    results = []
    with open_shards(f'{self.output_path}-*') as result_file:
      for line in result_file:
        match = re.search(r'(\S+),([0-9\.]+)', line)
        if match is not None:
          results.append((match.group(1), int(float(match.group(2)))))
        elif line.strip():
          self.assertEqual(line.strip(), 'Borough,passenger_count')
    self.assertEqual(sorted(results), sorted(expected_counts.items()))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
