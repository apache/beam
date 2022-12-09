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

"""Test for the multiple_output_pardo example."""

# pytype: skip-file

import logging
import re
import unittest
import uuid

import pytest

from apache_beam.examples.cookbook import multiple_output_pardo
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import create_file
from apache_beam.testing.test_utils import read_files_from_pattern


class MultipleOutputParDo(unittest.TestCase):

  SAMPLE_TEXT = 'A whole new world\nA new fantastic point of view'
  EXPECTED_SHORT_WORDS = [('A', 2), ('new', 2), ('of', 1)]
  EXPECTED_WORDS = [('whole', 1), ('world', 1), ('fantastic', 1), ('point', 1),
                    ('view', 1)]

  def get_wordcount_results(self, result_path):
    results = []
    lines = read_files_from_pattern(result_path).splitlines()
    for line in lines:
      match = re.search(r'([A-Za-z]+): ([0-9]+)', line)
      if match is not None:
        results.append((match.group(1), int(match.group(2))))
    return results

  @pytest.mark.examples_postcommit
  @pytest.mark.sickbay_flink
  def test_multiple_output_pardo(self):
    test_pipeline = TestPipeline(is_integration_test=True)

    # Setup the files with expected content.
    temp_location = test_pipeline.get_option('temp_location')
    input_folder = '/'.join([temp_location, str(uuid.uuid4())])
    input = create_file('/'.join([input_folder, 'input.txt']), self.SAMPLE_TEXT)
    result_prefix = '/'.join([temp_location, str(uuid.uuid4()), 'result'])

    extra_opts = {'input': input, 'output': result_prefix}
    multiple_output_pardo.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)

    expected_char_count = len(''.join(self.SAMPLE_TEXT.split('\n')))
    contents = read_files_from_pattern(result_prefix + '-chars*')
    self.assertEqual(expected_char_count, int(contents))

    short_words = self.get_wordcount_results(result_prefix + '-short-words*')
    self.assertEqual(sorted(short_words), sorted(self.EXPECTED_SHORT_WORDS))

    words = self.get_wordcount_results(result_prefix + '-words*')
    self.assertEqual(sorted(words), sorted(self.EXPECTED_WORDS))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
