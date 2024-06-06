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
import tempfile
import unittest

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml.yaml_transform import YamlTransform

try:
  # pylint: disable=wrong-import-order, wrong-import-position, unused-import
  from apache_beam.ml.transforms import tft
except ImportError:
  raise unittest.SkipTest('tensorflow_transform is not installed.')

TRAIN_DATA = [
    beam.Row(num=0, text='And God said, Let there be light,'),
    beam.Row(num=2, text='And there was light'),
    beam.Row(num=8, text='And God saw the light, that it was good'),
]

TEST_DATA = [
    beam.Row(num=6, text='And God divided the light from the darkness.'),
]


class MLTransformTest(unittest.TestCase):
  def test_ml_transform(self):
    ml_opts = beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle', yaml_experimental_features=['ML'])
    with tempfile.TemporaryDirectory() as tempdir:
      with beam.Pipeline(options=ml_opts) as p:
        elements = p | beam.Create(TRAIN_DATA)
        result = elements | YamlTransform(
            f'''
            type: MLTransform
            config:
              write_artifact_location: {tempdir}
              transforms:
                - type: ScaleTo01
                  config:
                    columns: [num]
                - type: ComputeAndApplyVocabulary
                  config:
                    columns: [text]
                    split_string_by_delimiter: ' ,.'
            ''')
        assert_that(
            # Why is this an array, not a scalar?
            result | beam.Map(lambda x: x.num[0]),
            equal_to([0, .25, 1]))
        assert_that(
            result | beam.Map(lambda x: set(x.text))
            | beam.CombineGlobally(lambda xs: set.union(*xs)),
            equal_to([set(range(13))]),
            label='CheckVocab')

      with beam.Pipeline(options=ml_opts) as p:
        elements = p | beam.Create(TEST_DATA)
        result = elements | YamlTransform(
            f'''
            type: MLTransform
            config:
              read_artifact_location: {tempdir}
            ''')
        assert_that(result | beam.Map(lambda x: x.num[0]), equal_to([.75]))
        assert_that(
            result | beam.Map(lambda x: len(set(x.text))),
            equal_to([5]),
            label='CheckVocab')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
