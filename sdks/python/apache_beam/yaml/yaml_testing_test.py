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
import os
import tempfile
import unittest

# isort is fighting with yapf here.
# isort: off
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.yaml import yaml_testing

# Note that executing this pipeline will actually fail if the untested
# transforms are not in fact elided.
SIMPLE_PIPELINE = '''
pipeline:
  type: chain
  transforms:
    - type: ReadFromText
      name: MyRead
      config:
        path: 'does_not_exist.txt'
    - type: MapToFields
      name: MyMapToFields
      config:
        append: True
        language: python
        fields:
          square: element**2
    - type: MapToFields
      name: ToBeExcluded
      config:
        language: python
        fields:
          bad:
            callable: |
              def non_shall_pass(row):
                raise ValueError(row)
'''


class YamlTestingTest(unittest.TestCase):
  def test_expected_outputs(self):
    yaml_testing.run_test(
        SIMPLE_PIPELINE,
        {
            'mock_inputs': [{
                'name': 'MyMapToFields',
                'elements': [1, 2, 3],
            }],
            'expected_outputs': [{
                'name': 'MyMapToFields',
                'elements': [
                    {
                        'element': 1, 'square': 1
                    },
                    {
                        'element': 2, 'square': 4
                    },
                    {
                        'element': 3, 'square': 9
                    },
                ]
            }]
        })

  def test_expected_outputs_fails_correctly(self):
    with self.assertRaisesRegex(Exception, 'unexpected.*9'):
      yaml_testing.run_test(
          SIMPLE_PIPELINE,
          {
              'mock_inputs': [{
                  'name': 'MyMapToFields',
                  'elements': [1, 2, 3],
              }],
              'expected_outputs': [{
                  'name': 'MyMapToFields',
                  'elements': [
                      {
                          'element': 1, 'square': 1
                      },
                      {
                          'element': 2, 'square': 4
                      },
                  ]
              }]
          })

  def test_expected_inputs(self):
    yaml_testing.run_test(
        SIMPLE_PIPELINE,
        {
            'mock_outputs': [{
                'name': 'MyRead',
                'elements': [1, 2, 3],
            }],
            'expected_inputs': [{
                'name': 'ToBeExcluded',
                'elements': [
                    {
                        'element': 1, 'square': 1
                    },
                    {
                        'element': 2, 'square': 4
                    },
                    {
                        'element': 3, 'square': 9
                    },
                ]
            }]
        })

  def test_expected_inputs_fails_correctly(self):
    with self.assertRaisesRegex(Exception, 'unexpected.*9'):
      yaml_testing.run_test(
          SIMPLE_PIPELINE,
          {
              'mock_outputs': [{
                  'name': 'MyRead',
                  'elements': [1, 2, 3],
              }],
              'expected_inputs': [{
                  'name': 'ToBeExcluded',
                  'elements': [
                      {
                          'element': 1, 'square': 1
                      },
                      {
                          'element': 2, 'square': 4
                      },
                  ]
              }]
          })

  def test_unmocked_inputs(self):
    with self.assertRaisesRegex(Exception, 'Non-mocked source MyRead'):
      yaml_testing.run_test(
          SIMPLE_PIPELINE,
          {
              'expected_inputs': [{
                  'name': 'ToBeExcluded',
                  'elements': [
                      {
                          'element': 1, 'square': 1
                      },
                      {
                          'element': 2, 'square': 4
                      },
                      {
                          'element': 3, 'square': 9
                      },
                  ]
              }]
          })

  def test_fixes(self):
    fixes = yaml_testing.run_test(
        SIMPLE_PIPELINE,
        {
            'mock_outputs': [{
                'name': 'MyRead',
                'elements': [1, 2, 3],
            }],
            'expected_inputs': [{
                'name': 'ToBeExcluded',
                'elements': [
                    {
                        'element': 1, 'square': 1
                    },
                    {
                        'element': 2, 'square': 4
                    },
                ]
            }]
        },
        fix_failures=True)
    self.assertEqual(
        fixes,
        {('expected_inputs', 'ToBeExcluded'): [
             dict(element=1, square=1),
             dict(element=2, square=4),
             dict(element=3, square=9),
         ]})

  def test_create(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      input_path = os.path.join(tmpdir, 'input.csv')
      with open(input_path, 'w') as fout:
        fout.write('a,b,c\n')
        for ix in range(1000):
          fout.write(f'{ix % 5},{ix},Ccc\n')
      pipeline = f'''
      pipeline:
        type: chain
        transforms:
          - type: ReadFromCsv
            config:
              path: {input_path}
          - type: Filter
            config:
              language: python
              keep: a == 1
          - type: WriteToSink
      '''
      test_spec = yaml_testing.create_test(
          pipeline, max_num_inputs=100, min_num_outputs=5)

    self.assertEqual(len(test_spec['mock_outputs']), 1)
    self.assertEqual(len(test_spec['expected_inputs']), 1)
    self.assertGreaterEqual(len(test_spec['expected_inputs'][0]['elements']), 5)
    yaml_testing.run_test(pipeline, test_spec)

  @unittest.skipIf(
      TestPipeline().get_pipeline_options().view_as(StandardOptions).runner
      is None,
      'Do not run this test on precommit suites.')
  def test_join_transform_serialization(self):
    """Test that Join transforms work with YAML testing framework
    and cloudpickle.
    
    This test validates the fix for the grpc channel serialization issue
    that was causing TypeError: no default __reduce__ due to non-trivial
    __cinit__ when using Join transforms with the YAML testing framework.
    """
    join_pipeline = '''
      pipeline:
        transforms:
        - type: Create
          name: Create1
          config:
            elements:
            - ride_id: "1"
              pickup_location: "downtown"
            - ride_id: "2"
              pickup_location: "airport"

        - type: Create
          name: Create2
          config:
            elements:
            - ride_id: "1"
              dropoff_location: "mall"
            - ride_id: "2"
              dropoff_location: "hotel"

        - type: Join
          name: JoinRides
          input:
            pickup: Create1
            dropoff: Create2
          config:
            equalities: ride_id
            type: inner
            fields:
              pickup: [ride_id, pickup_location]
              dropoff: [dropoff_location]

        - type: LogForTesting
          name: LogResult
          input: JoinRides
      '''

    # Test with expected_inputs to validate the Join transform output
    yaml_testing.run_test(
        join_pipeline,
        {
            'expected_inputs': [{
                'name': 'LogResult',
                'elements': [{
                    'ride_id': '1',
                    'pickup_location': 'downtown',
                    'dropoff_location': 'mall'
                },
                             {
                                 'ride_id': '2',
                                 'pickup_location': 'airport',
                                 'dropoff_location': 'hotel'
                             }]
            }]
        })

    # Test with mock_outputs to validate Join transform can handle mocked inputs
    yaml_testing.run_test(
        join_pipeline,
        {
            'mock_outputs': [{
                'name': 'Create1',
                'elements': [{
                    'ride_id': '3', 'pickup_location': 'station'
                }]
            },
                             {
                                 'name': 'Create2',
                                 'elements': [{
                                     'ride_id': '3',
                                     'dropoff_location': 'office'
                                 }]
                             }],
            'expected_inputs': [{
                'name': 'LogResult',
                'elements': [{
                    'ride_id': '3',
                    'pickup_location': 'station',
                    'dropoff_location': 'office'
                }]
            }]
        })


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
