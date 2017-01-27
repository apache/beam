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

"""Test for the estimate_pi example."""

import json
import logging
import tempfile
import unittest

from apache_beam.examples.complete import estimate_pi


class EstimatePiTest(unittest.TestCase):

  def create_temp_file(self, contents):
    with tempfile.NamedTemporaryFile(delete=False) as f:
      f.write(contents)
      return f.name

  def test_basics(self):
    temp_path = self.create_temp_file('result')
    estimate_pi.run([
        '--output=%s' % temp_path])
    # Parse result file and compare.
    with open(temp_path + '-00000-of-00001') as result_file:
      estimated_pi = json.loads(result_file.readline())[2]
      # Note: Probabilistically speaking this test can fail with a probability
      # that is very small (VERY) given that we run at least 10 million trials.
      self.assertTrue(estimated_pi > 3.13 and estimated_pi < 3.15)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
