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

"""Test for the TF-IDF example."""

# pytype: skip-file

import logging
import os
import re
import tempfile
import unittest

import pytest

import apache_beam as beam
from apache_beam.examples.complete import tfidf
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import open_shards

EXPECTED_RESULTS = set([
    ('ghi', '1.txt', 0.3662040962227032), ('abc', '1.txt', 0.0),
    ('abc', '3.txt', 0.0), ('abc', '2.txt', 0.0),
    ('def', '1.txt', 0.13515503603605478), ('def', '2.txt', 0.2027325540540822)
])

EXPECTED_LINE_RE = r'\(u?\'([a-z]*)\', \(\'.*([0-9]\.txt)\', (.*)\)\)'


@pytest.mark.examples_postcommit
class TfIdfTest(unittest.TestCase):
  def create_file(self, path, contents):
    logging.info('Creating temp file: %s', path)
    with open(path, 'wb') as f:
      f.write(contents.encode('utf-8'))

  def test_tfidf_transform(self):
    with TestPipeline() as p:

      def re_key(word_uri_tfidf):
        (word, (uri, tfidf)) = word_uri_tfidf
        return (word, uri, tfidf)

      uri_to_line = p | 'create sample' >> beam.Create(
          [('1.txt', 'abc def ghi'), ('2.txt', 'abc def'), ('3.txt', 'abc')])
      result = (uri_to_line | tfidf.TfIdf() | beam.Map(re_key))
      assert_that(result, equal_to(EXPECTED_RESULTS))
      # Run the pipeline. Note that the assert_that above adds to the pipeline
      # a check that the result PCollection contains expected values.
      # To actually trigger the check the pipeline must be run (e.g. by
      # exiting the with context).

  def test_basics(self):
    # Setup the files with expected content.
    temp_folder = tempfile.mkdtemp()
    self.create_file(os.path.join(temp_folder, '1.txt'), 'abc def ghi')
    self.create_file(os.path.join(temp_folder, '2.txt'), 'abc def')
    self.create_file(os.path.join(temp_folder, '3.txt'), 'abc')
    tfidf.run([
        '--uris=%s/*' % temp_folder,
        '--output',
        os.path.join(temp_folder, 'result')
    ],
              save_main_session=False)
    # Parse result file and compare.
    results = []
    with open_shards(os.path.join(temp_folder, 'result-*-of-*')) as result_file:
      for line in result_file:
        match = re.search(EXPECTED_LINE_RE, line)
        logging.info('Result line: %s', line)
        if match is not None:
          results.append(
              (match.group(1), match.group(2), float(match.group(3))))
    logging.info('Computed results: %s', set(results))
    self.assertEqual(set(results), EXPECTED_RESULTS)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
