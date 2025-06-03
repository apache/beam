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
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils import python_callable
from apache_beam.yaml.yaml_transform import YamlTransform

TEST_PROVIDERS = {
    'PyMap': lambda fn: beam.Map(python_callable.PythonCallableWithSource(fn)),
}


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
                      type: 'IncLandmarkMeanTracker'
                      config: {}
                    stdev_tracker:
                      type: 'IncLandmarkStdevTracker'
                      config: {}
            - type: PyMap
              config:
                  fn: "lambda x: (x[1].predictions[0].label)"
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([-2, -2, 0, 1, 1]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
