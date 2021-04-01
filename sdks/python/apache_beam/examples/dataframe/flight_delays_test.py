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

"""Test for the wordcount example."""

# pytype: skip-file

from __future__ import absolute_import

import collections
import logging
import re
import tempfile
import unittest

from apache_beam.examples.dataframe import flight_delays
from apache_beam.testing.util import open_shards


class FlightDelaysTest(unittest.TestCase):

  EXPECTED_20121206 = [
      ('EV', 1.634, -4.667),
      ('HA', -2.877, -1.791),
      ('VX', 0.233,-6.519),
      ('WN', 3.379, -3.502),
      ('YV', -4.859, -7.296),
  ]

  def test_single_day(self):
    with tempfile.TemporaryDirectory() as out_dir:
      flight_delays.run(
          ['--start_date=2012-12-06', '--end_date=2012-12-06', f'--output={out_dir}/result.csv'])
      # Parse result file and compare.
      results = []
      with open_shards(f'{out_dir}/result.csv-2012-12-06T00:00:00-2012-12-07T00:00:00*') as result_file:
        for line in result_file:
          match = re.search(r'(\S+),([0-9.]+),([0-9.]+)', line)
          if match is not None:
            results.append((match.group(1), float(match.group(2)), float(match.group(3))))
          elif line.strip():
            self.assertEqual(line.strip(), 'airline,departure_delay,arrival_delay')
      self.assertEqual(sorted(results), sorted(EXPECTED_20121206))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
