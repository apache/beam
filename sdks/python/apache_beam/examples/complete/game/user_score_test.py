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

from __future__ import absolute_import

import logging
import unittest

import apache_beam as beam
from apache_beam.examples.complete.game import user_score
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class UserScoreTest(unittest.TestCase):

  SAMPLE_DATA = [
      'user1_team1,team1,18,1447686663000,2015-11-16 15:11:03.921',
      'user1_team1,team1,18,1447690263000,2015-11-16 16:11:03.921',
      'user2_team2,team2,2,1447690263000,2015-11-16 16:11:03.955',
      'user3_team3,team3,8,1447690263000,2015-11-16 16:11:03.955',
      'user4_team3,team3,5,1447690263000,2015-11-16 16:11:03.959',
      'user1_team1,team1,14,1447697463000,2015-11-16 18:11:03.955',
  ]

  def test_user_score(self):
    with TestPipeline() as p:
      result = (
          p | beam.Create(UserScoreTest.SAMPLE_DATA) | user_score.UserScore())
      assert_that(result, equal_to([
          ('user1_team1', 50), ('user2_team2', 2), ('user3_team3', 8),
          ('user4_team3', 5)]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
