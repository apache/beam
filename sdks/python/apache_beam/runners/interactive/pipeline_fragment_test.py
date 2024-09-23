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
import unittest
from unittest.mock import patch

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner as ir
from apache_beam.runners.interactive import pipeline_fragment as pf
from apache_beam.runners.interactive.testing.mock_ipython import mock_get_ipython
from apache_beam.runners.interactive.testing.pipeline_assertion import assert_pipeline_equal
from apache_beam.runners.interactive.testing.pipeline_assertion import assert_pipeline_proto_equal
from apache_beam.testing.test_stream import TestStream


@unittest.skipIf(
    not ie.current_env().is_interactive_ready,
    '[interactive] dependency is not installed.')
class PipelineFragmentTest(unittest.TestCase):
  def setUp(self):
    ie.new_env()
    # Assume a notebook frontend is connected to the mocked ipython kernel.
    ie.current_env()._is_in_ipython = True
    ie.current_env()._is_in_notebook = True

  @patch('IPython.get_ipython', new_callable=mock_get_ipython)
  def test_build_pipeline_fragment(self, cell):
    with cell:  # Cell 1
      p = beam.Pipeline(ir.InteractiveRunner())
      p_expected = beam.Pipeline(ir.InteractiveRunner())
      # Watch local scope now to allow interactive beam to track the pipelines.
      ib.watch(locals())

    with cell:  # Cell 2
      # pylint: disable=bad-option-value
      init = p | 'Init' >> beam.Create(range(10))
      init_expected = p_expected | 'Init' >> beam.Create(range(10))

    with cell:  # Cell 3
      square = init | 'Square' >> beam.Map(lambda x: x * x)
      _ = init | 'Cube' >> beam.Map(lambda x: x**3)
      _ = init_expected | 'Square' >> beam.Map(lambda x: x * x)

    # Watch every PCollection has been defined so far in local scope.
    ib.watch(locals())
    fragment = pf.PipelineFragment([square]).deduce_fragment()
    assert_pipeline_equal(self, p_expected, fragment)

  @patch('IPython.get_ipython', new_callable=mock_get_ipython)
  def test_user_pipeline_intact_after_deducing_pipeline_fragment(self, cell):
    with cell:  # Cell 1
      p = beam.Pipeline(ir.InteractiveRunner())
      # Watch the pipeline `p` immediately without calling locals().
      ib.watch({'p': p})

    with cell:  # Cell 2
      # pylint: disable=bad-option-value
      init = p | 'Init' >> beam.Create(range(10))

    with cell:  # Cell 3
      square = init | 'Square' >> beam.Map(lambda x: x * x)

    with cell:  # Cell 4
      cube = init | 'Cube' >> beam.Map(lambda x: x**3)

    # Watch every PCollection has been defined so far in local scope without
    # calling locals().
    ib.watch({'init': init, 'square': square, 'cube': cube})
    user_pipeline_proto_before_deducing_fragment = p.to_runner_api(
        return_context=False)
    _ = pf.PipelineFragment([square]).deduce_fragment()
    user_pipeline_proto_after_deducing_fragment = p.to_runner_api(
        return_context=False)
    assert_pipeline_proto_equal(
        self,
        user_pipeline_proto_before_deducing_fragment,
        user_pipeline_proto_after_deducing_fragment)

  @patch('IPython.get_ipython', new_callable=mock_get_ipython)
  def test_pipeline_fragment_produces_correct_data(self, cell):
    with cell:  # Cell 1
      p = beam.Pipeline(ir.InteractiveRunner())
      ib.watch({'p': p})

    with cell:  # Cell 2
      # pylint: disable=bad-option-value
      init = p | 'Init' >> beam.Create(range(5))

    with cell:  # Cell 3
      square = init | 'Square' >> beam.Map(lambda x: x * x)
      _ = init | 'Cube' >> beam.Map(lambda x: x**3)

    ib.watch(locals())
    result = pf.PipelineFragment([square]).run()
    self.assertEqual([0, 1, 4, 9, 16], list(result.get(square)))

  def test_fragment_does_not_prune_teststream(self):
    """Tests that the fragment does not prune the TestStream composite parts.
    """
    options = StandardOptions(streaming=True)
    p = beam.Pipeline(ir.InteractiveRunner(), options)

    test_stream = p | TestStream(output_tags=['a', 'b'])

    # pylint: disable=unused-variable
    a = test_stream['a'] | 'a' >> beam.Map(lambda _: _)
    b = test_stream['b'] | 'b' >> beam.Map(lambda _: _)

    fragment = pf.PipelineFragment([b]).deduce_fragment()

    # If the fragment does prune the TestStreawm composite parts, then the
    # resulting graph is invalid and the following call will raise an exception.
    fragment.to_runner_api()

  @patch('IPython.get_ipython', new_callable=mock_get_ipython)
  def test_pipeline_composites(self, cell):
    """Tests that composites are supported.
    """
    with cell:  # Cell 1
      p = beam.Pipeline(ir.InteractiveRunner())
      ib.watch({'p': p})

    with cell:  # Cell 2
      # pylint: disable=bad-option-value
      init = p | 'Init' >> beam.Create(range(5))

    with cell:  # Cell 3
      # Have a composite within a composite to test that all transforms under a
      # composite are added.

      @beam.ptransform_fn
      def Bar(pcoll):
        return pcoll | beam.Map(lambda n: 2 * n)

      @beam.ptransform_fn
      def Foo(pcoll):
        p1 = pcoll | beam.Map(lambda n: 3 * n)
        p2 = pcoll | beam.Map(str)
        bar = p1 | Bar()
        return {'pc1': p1, 'pc2': p2, 'bar': bar}

      res = init | Foo()
      ib.watch(res)

    pc = res['bar']

    result = pf.PipelineFragment([pc]).run()
    self.assertEqual([0, 6, 12, 18, 24], list(result.get(pc)))


if __name__ == '__main__':
  unittest.main()
