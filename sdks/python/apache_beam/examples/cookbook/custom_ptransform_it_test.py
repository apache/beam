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

"""End-to-end test for Custom PTransform example."""
# pytype: skip-file

import logging
import unittest
import uuid

import pytest

from apache_beam.examples.cookbook import custom_ptransform
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import create_file
from apache_beam.testing.test_utils import read_files_from_pattern


def format_result(result_string):
  def format_tuple(result_elem_list):
    [country, counter] = result_elem_list
    return country, int(counter.strip())

  result_list = list(
      map(
          lambda result_elem: format_tuple(result_elem.split(',')),
          result_string.replace('\'', '').replace('[',
                                                  '').replace(']', '').replace(
                                                      '\"', '').split('\n')))
  return result_list


class CustomPTransformIT(unittest.TestCase):
  WORDS = ['CAT', 'DOG', 'CAT', 'CAT', 'DOG']
  EXPECTED_RESULT = "('CAT DOG CAT CAT DOG', 2)"

  @pytest.mark.examples_postcommit
  def test_custom_ptransform_output_files_on_small_input(self):
    test_pipeline = TestPipeline(is_integration_test=True)

    # Setup the files with expected content.
    temp_location = test_pipeline.get_option('temp_location')
    input = '/'.join([temp_location, str(uuid.uuid4()), 'input.txt'])
    output = '/'.join([temp_location, str(uuid.uuid4()), 'result'])
    create_file(input, ' '.join(self.WORDS))
    extra_opts = {'input': input, 'output': output}
    custom_ptransform.run(test_pipeline.get_full_options_as_args(**extra_opts))

    # Load result file and compare.
    result = read_files_from_pattern('%s*' % output).strip()
    self.assertEqual(result, self.EXPECTED_RESULT)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
