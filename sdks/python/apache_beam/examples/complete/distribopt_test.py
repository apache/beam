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

"""Test for the distrib_optimization example."""

from __future__ import absolute_import

import logging
import os
import tempfile
import unittest
from ast import literal_eval as make_tuple

import numpy as np
from mock import MagicMock
from mock import patch

from apache_beam.testing.util import open_shards

FILE_CONTENTS = 'OP01,8,12,0,12\n' \
                'OP02,30,14,3,12\n' \
                'OP03,25,7,3,14\n' \
                'OP04,87,7,2,2\n' \
                'OP05,19,1,7,10'

EXPECTED_MAPPING = {
    'OP01': 'A',
    'OP02': 'B',
    'OP03': 'B',
    'OP04': 'C',
    'OP05': 'A'
}


class DistribOptimizationTest(unittest.TestCase):

  def create_file(self, path, contents):
    logging.info('Creating temp file: %s', path)
    with open(path, 'w') as f:
      f.write(contents)

  def test_basics(self):
    # Setup the files with expected content.
    temp_folder = tempfile.mkdtemp()
    self.create_file(os.path.join(temp_folder, 'input.txt'), FILE_CONTENTS)

    # Run pipeline
    # Avoid dependency on SciPy
    scipy_mock = MagicMock()
    result_mock = MagicMock(x=np.ones(3))
    scipy_mock.optimize.minimize = MagicMock(return_value=result_mock)
    modules = {
        'scipy': scipy_mock,
        'scipy.optimize': scipy_mock.optimize
    }

    with patch.dict('sys.modules', modules):
      from apache_beam.examples.complete import distribopt
      distribopt.run([
          '--input=%s/input.txt' % temp_folder,
          '--output', os.path.join(temp_folder, 'result')])

    # Load result file and compare.
    with open_shards(os.path.join(temp_folder, 'result-*-of-*')) as result_file:
      lines = result_file.readlines()

    # Only 1 result
    self.assertEqual(len(lines), 1)

    # parse result line and verify optimum
    optimum = make_tuple(lines[0])
    self.assertAlmostEqual(optimum['cost'], 454.39597, places=3)
    self.assertDictEqual(optimum['mapping'], EXPECTED_MAPPING)
    production = optimum['production']
    for plant in ['A', 'B', 'C']:
      np.testing.assert_almost_equal(production[plant], np.ones(3))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
