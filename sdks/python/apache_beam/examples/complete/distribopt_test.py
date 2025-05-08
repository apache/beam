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

# pytype: skip-file

import logging
import unittest
import uuid
from ast import literal_eval as make_tuple

import numpy as np
import pytest
from mock import MagicMock
from mock import patch

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import create_file
from apache_beam.testing.test_utils import read_files_from_pattern

FILE_CONTENTS = 'OP01,8,12,0,12\n' \
                'OP02,30,14,3,12\n' \
                'OP03,25,7,3,14\n' \
                'OP04,87,7,2,2\n' \
                'OP05,19,1,7,10'

EXPECTED_MAPPING = {
    'OP01': 'A', 'OP02': 'B', 'OP03': 'B', 'OP04': 'C', 'OP05': 'A'
}


class DistribOptimizationTest(unittest.TestCase):
  #TODO(https://github.com/apache/beam/issues/23606) Fix and enable
  @pytest.mark.sickbay_dataflow
  @pytest.mark.examples_postcommit
  def test_basics(self):
    test_pipeline = TestPipeline(is_integration_test=True)

    # Setup the files with expected content.
    temp_location = test_pipeline.get_option('temp_location')
    input = '/'.join([temp_location, str(uuid.uuid4()), 'input.txt'])
    output = '/'.join([temp_location, str(uuid.uuid4()), 'result'])
    create_file(input, FILE_CONTENTS)
    extra_opts = {'input': input, 'output': output}

    # Run pipeline
    # Avoid dependency on SciPy
    scipy_mock = MagicMock()
    result_mock = MagicMock(x=np.ones(3))
    scipy_mock.optimize.minimize = MagicMock(return_value=result_mock)
    modules = {'scipy': scipy_mock, 'scipy.optimize': scipy_mock.optimize}

    with patch.dict('sys.modules', modules):
      from apache_beam.examples.complete import distribopt
      distribopt.run(
          test_pipeline.get_full_options_as_args(**extra_opts),
          save_main_session=False)

    # Load result file and compare.
    lines = read_files_from_pattern('%s*' % output).splitlines()

    # Only 1 result
    self.assertEqual(len(lines), 1)

    # Handle NumPy string representation before parsing
    cleaned_line = lines[0].replace("np.str_('", "'").replace("')", "'")

    # parse result line and verify optimum
    optimum = make_tuple(cleaned_line)
    self.assertAlmostEqual(optimum['cost'], 454.39597, places=3)
    self.assertDictEqual(optimum['mapping'], EXPECTED_MAPPING)
    production = optimum['production']
    for plant in ['A', 'B', 'C']:
      np.testing.assert_almost_equal(production[plant], np.ones(3))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
