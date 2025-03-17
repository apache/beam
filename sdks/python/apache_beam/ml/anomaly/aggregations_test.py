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

from apache_beam.ml.anomaly import aggregations
from apache_beam.ml.anomaly.base import AnomalyPrediction


class LabelAggTestWithMissingOrError(unittest.TestCase):
  def test_default(self):
    normal = AnomalyPrediction(label=0)
    outlier = AnomalyPrediction(label=1)
    missing = AnomalyPrediction(label=-2)
    error = AnomalyPrediction(label=None)

    vote = aggregations.MajorityVote().apply

    # missing and error labels are ignored if there is any normal/outlier
    self.assertEqual(vote([normal, missing, error]), normal)
    self.assertEqual(vote([outlier, missing, error]), outlier)

    # if there is any missing among errors, return missing
    self.assertEqual(vote([error, missing, error]), missing)

    # return error only when all are errors
    self.assertEqual(vote([error, error, error]), error)


class MajorityVoteTest(unittest.TestCase):
  def test_default(self):
    normal = AnomalyPrediction(label=0)
    outlier = AnomalyPrediction(label=1)
    vote = aggregations.MajorityVote().apply

    self.assertEqual(vote([]), AnomalyPrediction())

    self.assertEqual(vote([normal]), normal)

    self.assertEqual(vote([outlier]), outlier)

    self.assertEqual(vote([outlier, normal, normal]), normal)

    self.assertEqual(vote([outlier, normal, outlier]), outlier)

    # use normal to break ties by default
    self.assertEqual(vote([outlier, normal]), normal)

  def test_tie_breaker(self):
    normal = AnomalyPrediction(label=0)
    outlier = AnomalyPrediction(label=1)
    vote = aggregations.MajorityVote(tie_breaker=1).apply

    self.assertEqual(vote([outlier, normal]), outlier)


class AllVoteTest(unittest.TestCase):
  def test_default(self):
    normal = AnomalyPrediction(label=0)
    outlier = AnomalyPrediction(label=1)
    vote = aggregations.AllVote().apply

    self.assertEqual(vote([]), AnomalyPrediction())

    self.assertEqual(vote([normal]), normal)

    self.assertEqual(vote([outlier]), outlier)

    # outlier is only labeled when everyone is outlier
    self.assertEqual(vote([normal, normal, normal]), normal)
    self.assertEqual(vote([outlier, normal, normal]), normal)
    self.assertEqual(vote([outlier, normal, outlier]), normal)
    self.assertEqual(vote([outlier, outlier, outlier]), outlier)


class AnyVoteTest(unittest.TestCase):
  def test_default(self):
    normal = AnomalyPrediction(label=0)
    outlier = AnomalyPrediction(label=1)
    vote = aggregations.AnyVote().apply

    self.assertEqual(vote([]), AnomalyPrediction())

    self.assertEqual(vote([normal]), normal)

    self.assertEqual(vote([outlier]), outlier)

    # outlier is labeled when at least one is outlier
    self.assertEqual(vote([normal, normal, normal]), normal)
    self.assertEqual(vote([outlier, normal, normal]), outlier)
    self.assertEqual(vote([outlier, normal, outlier]), outlier)
    self.assertEqual(vote([outlier, outlier, outlier]), outlier)


class ScoreAggTestWithMissingOrError(unittest.TestCase):
  def test_default(self):
    normal = AnomalyPrediction(score=1.0)
    missing = AnomalyPrediction(score=float("NaN"))
    error = AnomalyPrediction(score=None)

    avg = aggregations.AverageScore().apply

    # missing and error scores are ignored if there is any normal/outlier
    self.assertEqual(avg([normal, missing, error]), normal)

    # if there is any missing among errors, return missing.
    # note that NaN != NaN, so we cannot use `assertEqual` here.
    self.assertTrue(avg([error, missing, error]).score)

    # return error only when all are errors
    self.assertEqual(avg([error, error, error]), error)


class AverageScoreTest(unittest.TestCase):
  def test_default(self):
    avg = aggregations.AverageScore().apply

    self.assertEqual(avg([]), AnomalyPrediction())

    self.assertEqual(
        avg([AnomalyPrediction(score=1)]), AnomalyPrediction(score=1))

    self.assertEqual(
        avg([AnomalyPrediction(score=1), AnomalyPrediction(score=2)]),
        AnomalyPrediction(score=1.5))


class MaxScoreTest(unittest.TestCase):
  def test_default(self):
    avg = aggregations.MaxScore().apply

    self.assertEqual(avg([]), AnomalyPrediction())

    self.assertEqual(
        avg([AnomalyPrediction(score=1)]), AnomalyPrediction(score=1))

    self.assertEqual(
        avg([AnomalyPrediction(score=1), AnomalyPrediction(score=2)]),
        AnomalyPrediction(score=2))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
