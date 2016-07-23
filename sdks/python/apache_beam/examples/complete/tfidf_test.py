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

import logging
import os
import re
import tempfile
import unittest

import apache_beam as beam
from apache_beam.examples.complete import tfidf


EXPECTED_RESULTS = set([
    ('ghi', '1.txt', 0.3662040962227032),
    ('abc', '1.txt', 0.0),
    ('abc', '3.txt', 0.0),
    ('abc', '2.txt', 0.0),
    ('def', '1.txt', 0.13515503603605478),
    ('def', '2.txt', 0.2027325540540822)])


EXPECTED_LINE_RE = r'\(u\'([a-z]*)\', \(\'.*([0-9]\.txt)\', (.*)\)\)'


class TfIdfTest(unittest.TestCase):

  def create_file(self, path, contents):
    logging.info('Creating temp file: %s', path)
    with open(path, 'w') as f:
      f.write(contents)

  def test_tfidf_transform(self):
    p = beam.Pipeline('DirectPipelineRunner')
    uri_to_line = p | beam.Create(
        'create sample',
        [('1.txt', 'abc def ghi'),
         ('2.txt', 'abc def'),
         ('3.txt', 'abc')])
    result = (
        uri_to_line
        | tfidf.TfIdf()
        | beam.Map(lambda (word, (uri, tfidf)): (word, uri, tfidf)))
    beam.assert_that(result, beam.equal_to(EXPECTED_RESULTS))
    # Run the pipeline. Note that the assert_that above adds to the pipeline
    # a check that the result PCollection contains expected values. To actually
    # trigger the check the pipeline must be run.
    p.run()

  def test_basics(self):
    # Setup the files with expected content.
    temp_folder = tempfile.mkdtemp()
    self.create_file(os.path.join(temp_folder, '1.txt'), 'abc def ghi')
    self.create_file(os.path.join(temp_folder, '2.txt'), 'abc def')
    self.create_file(os.path.join(temp_folder, '3.txt'), 'abc')
    tfidf.run([
        '--uris=%s/*' % temp_folder,
        '--output', os.path.join(temp_folder, 'result')])
    # Parse result file and compare.
    results = []
    with open(os.path.join(temp_folder,
                           'result-00000-of-00001')) as result_file:
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
