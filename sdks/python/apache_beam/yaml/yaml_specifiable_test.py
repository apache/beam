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
from typing import Callable

import apache_beam as beam
from apache_beam.ml.anomaly.base import AnomalyDetector
from apache_beam.ml.anomaly.specifiable import specifiable
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils import python_callable
from apache_beam.yaml.yaml_transform import YamlTransform

TEST_PROVIDERS = {
    'PyMap': lambda fn: beam.Map(python_callable.PythonCallableWithSource(fn)),
}


@specifiable
class FakeDetector(AnomalyDetector):  # pylint: disable=unused-variable
  def __init__(self, fn: Callable):
    super().__init__()
    self._fn = fn

  def learn_one(self, x: beam.Row) -> None:
    pass

  def score_one(self, x: beam.Row) -> float:
    v = next(iter(x))
    return self._fn(v)


class YamlSpecifiableTransformTest(unittest.TestCase):
  def test_specifiable_transform(self):
    TRAIN_DATA = [
        (0, beam.Row(x=1)),
        (0, beam.Row(x=2)),
        (0, beam.Row(x=2)),
        (0, beam.Row(x=4)),
        (0, beam.Row(x=9)),
    ]
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | beam.Create(TRAIN_DATA) | YamlTransform(
          '''
          type: chain
          transforms:
            - type: AnomalyDetection
              config:
                detector:
                  type: 'ZScore'
                  config:
                    sub_stat_tracker:
                      type: 'IncSlidingMeanTracker'
                      config:
                        window_size: 5
                    stdev_tracker:
                      type: 'IncSlidingStdevTracker'
                      config:
                        window_size: 5
            - type: PyMap
              config:
                  fn: "lambda x: (x[1].predictions[0].label)"
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([-2, -2, 0, 1, 1]))

  def test_specifiable_transform_with_callable(self):
    TRAIN_DATA = [
        (0, beam.Row(x=1)),
        (0, beam.Row(x=2)),
        (0, beam.Row(x=2)),
        (0, beam.Row(x=4)),
        (0, beam.Row(x=9)),
    ]
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | beam.Create(TRAIN_DATA) | YamlTransform(
          '''
          type: chain
          transforms:
            - type: AnomalyDetection
              config:
                detector:
                  type: 'FakeDetector'
                  config:
                    fn:
                      callable: "lambda x: x * 10.0"
            - type: PyMap
              config:
                  fn: "lambda x: (x[1].predictions[0].score)"
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([10.0, 20.0, 20.0, 40.0, 90.0]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
