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

"""Tests for the various custom Count implementation examples."""

import logging
import tempfile
import unittest

from apache_beam.examples.cookbook import custom_ptransform
from apache_beam.utils.options import PipelineOptions


class CustomCountTest(unittest.TestCase):

  def test_count1(self):
    self.run_pipeline(custom_ptransform.run_count1)

  def test_count2(self):
    self.run_pipeline(custom_ptransform.run_count2)

  def test_count3(self):
    self.run_pipeline(custom_ptransform.run_count3, factor=2)

  def run_pipeline(self, count_implementation, factor=1):
    input_path = self.create_temp_file('CAT\nDOG\nCAT\nCAT\nDOG\n')
    output_path = input_path + '.result'

    known_args, pipeline_args = custom_ptransform.get_args([
        '--input=%s*' % input_path, '--output=%s' % output_path
    ])

    count_implementation(known_args, PipelineOptions(pipeline_args))
    self.assertEqual(["(u'CAT', %d)" % (3 * factor),
                      "(u'DOG', %d)" % (2 * factor)],
                     self.get_output(output_path + '-00000-of-00001'))

  def create_temp_file(self, contents=''):
    with tempfile.NamedTemporaryFile(delete=False) as f:
      f.write(contents)
      return f.name

  def get_output(self, path):
    logging.info('Reading output from "%s"', path)
    lines = []
    with open(path) as f:
      lines = f.readlines()
    return sorted(s.rstrip('\n') for s in lines)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
