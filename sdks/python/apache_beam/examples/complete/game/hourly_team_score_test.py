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

"""Test for the user_score example."""

# pytype: skip-file

import logging
import unittest

import apache_beam as beam
from apache_beam.examples.complete.game import hourly_team_score
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class HourlyTeamScoreTest(unittest.TestCase):

  SAMPLE_DATA = [
      'user1_team1,team1,18,1447686663000,2015-11-16 15:11:03.921',
      'user1_team1,team1,18,1447690263000,2015-11-16 16:11:03.921',
      'user2_team2,team2,2,1447690263000,2015-11-16 16:11:03.955',
      'user3_team3,team3,8,1447690263000,2015-11-16 16:11:03.955',
      'user4_team3,team3,5,1447690263000,2015-11-16 16:11:03.959',
      'user1_team1,team1,14,1447697463000,2015-11-16 18:11:03.955',
  ]

  def test_hourly_team_score(self):
    with TestPipeline() as p:
      result = (
          p
          | beam.Create(HourlyTeamScoreTest.SAMPLE_DATA)
          | hourly_team_score.HourlyTeamScore(
              start_min='2015-11-16-15-20',
              stop_min='2015-11-16-17-20',
              window_duration=60))
      assert_that(
          result, equal_to([('team1', 18), ('team2', 2), ('team3', 13)]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
