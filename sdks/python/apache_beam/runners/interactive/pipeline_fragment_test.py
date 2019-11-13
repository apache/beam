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

"""Tests for apache_beam.runners.interactive.pipeline_fragment."""
from __future__ import absolute_import

import unittest

import apache_beam as beam
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner as ir
from apache_beam.runners.interactive import pipeline_fragment as pf
from apache_beam.runners.interactive.testing.mock_ipython import mock_get_ipython
from apache_beam.runners.interactive.testing.pipeline_assertion import assert_pipeline_equal

# TODO(BEAM-8288): clean up the work-around of nose tests using Python2 without
# unittest.mock module.
try:
  from unittest.mock import patch
except ImportError:
  from mock import patch


@unittest.skipIf(not ie.current_env().is_interactive_ready,
                 '[interactive] dependency is not installed.')
class PipelineFragmentTest(unittest.TestCase):

  def setUp(self):
    # Assume a notebook frontend is connected to the mocked ipython kernel.
    ie.current_env()._is_in_ipython = True
    ie.current_env()._is_in_notebook = True

  @patch('IPython.get_ipython', mock_get_ipython)
  def test_build_pipeline_fragment(self):
    with mock_get_ipython():  # Cell 1
      p = beam.Pipeline(ir.InteractiveRunner())
      p_expected = beam.Pipeline(ir.InteractiveRunner())
      # Watch local scope now to allow interactive beam to track the pipelines.
      ib.watch(locals())

    with mock_get_ipython():  # Cell 2
      # pylint: disable=range-builtin-not-iterating
      init = p | 'Init' >> beam.Create(range(10))
      init_expected = p_expected | 'Init' >> beam.Create(range(10))

    with mock_get_ipython():  # Cell 3
      square = init | 'Square' >> beam.Map(lambda x: x * x)
      _ = init | 'Cube' >> beam.Map(lambda x: x ** 3)
      _ = init_expected | 'Square' >> beam.Map(lambda x: x * x)

    # Watch every PCollections have been defined so far in local scope.
    ib.watch(locals())
    fragment = pf.PipelineFragment([square]).deduce_fragment()
    assert_pipeline_equal(self, p_expected, fragment)


  if __name__ == '__main__':
    unittest.main()
