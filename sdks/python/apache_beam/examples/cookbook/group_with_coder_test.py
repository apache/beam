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

"""Test for the custom coders example."""

# pytype: skip-file

import logging
import unittest
import uuid

import pytest

from apache_beam.examples.cookbook import group_with_coder
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import read_files_from_pattern

# Protect against environments where gcsio library is not available.
try:
  from apache_beam.io.gcp import gcsio
except ImportError:
  gcsio = None

# Patch group_with_coder.PlayerCoder.decode(). To test that the PlayerCoder was
# used, we do not strip the prepended 'x:' string when decoding a Player object.
group_with_coder.PlayerCoder.decode = lambda self, s: group_with_coder.Player(  # type: ignore[assignment]
    s.decode('utf-8'))


def create_content_input_file(path, records):
  logging.info('Creating file: %s', path)
  gcs = gcsio.GcsIO()
  with gcs.open(path, 'w') as f:
    for record in records:
      f.write(b'%s\n' % record.encode('utf-8'))
  return path


@unittest.skipIf(gcsio is None, 'GCP dependencies are not installed')
@pytest.mark.examples_postcommit
class GroupWithCoderTest(unittest.TestCase):

  SAMPLE_RECORDS = [
      'joe,10',
      'fred,3',
      'mary,7',
      'joe,20',
      'fred,6',
      'ann,5',
      'joe,30',
      'ann,10',
      'mary,1'
  ]

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    # Setup the file with expected content.
    self.temp_location = self.test_pipeline.get_option('temp_location')
    self.input_file = create_content_input_file(
        '/'.join([self.temp_location, str(uuid.uuid4()), 'input.txt']),
        self.SAMPLE_RECORDS)

  #TODO(https://github.com/apache/beam/issues/23608) Fix and enable
  @pytest.mark.sickbay_dataflow
  def test_basics_with_type_check(self):
    # Run the workflow with pipeline_type_check option. This will make sure
    # the typehints associated with all transforms will have non-default values
    # and therefore any custom coders will be used. In our case we want to make
    # sure the coder for the Player class will be used.
    output = '/'.join([self.temp_location, str(uuid.uuid4()), 'result'])
    extra_opts = {'input': self.input_file, 'output': output}
    group_with_coder.run(
        self.test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    # Parse result file and compare.
    results = []
    lines = read_files_from_pattern('%s*' % output).splitlines()
    for line in lines:
      name, points = line.split(',')
      results.append((name, int(points)))
      logging.info('result: %s', results)
    self.assertEqual(
        sorted(results),
        sorted([('x:ann', 15), ('x:fred', 9), ('x:joe', 60), ('x:mary', 8)]))

  def test_basics_without_type_check(self):
    # Run the workflow without pipeline_type_check option. This will make sure
    # the typehints associated with all transforms will have default values and
    # therefore any custom coders will not be used. The default coder (pickler)
    # will be used instead.
    output = '/'.join([self.temp_location, str(uuid.uuid4()), 'result'])
    extra_opts = {'input': self.input_file, 'output': output}
    with self.assertRaises(Exception) as context:
      # yapf: disable
      group_with_coder.run(
          self.test_pipeline.get_full_options_as_args(**extra_opts) +
          ['--no_pipeline_type_check'],
          save_main_session=False)
    self.assertIn('Unable to deterministically encode', str(context.exception))
    self.assertIn('CombinePerKey(sum)/GroupByKey', str(context.exception))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
