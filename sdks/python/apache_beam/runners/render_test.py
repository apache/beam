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
# pytype: skip-file

import os
import argparse
import logging
import subprocess
import unittest
import tempfile
import pytest

import apache_beam as beam
from apache_beam.runners import render

default_options = render.RenderOptions._add_argparse_args(
    argparse.ArgumentParser()).parse_args([])


class RenderRunnerTest(unittest.TestCase):
  def test_basic_graph(self):
    p = beam.Pipeline()
    _ = (
        p | beam.Impulse() | beam.Map(lambda _: 2)
        | 'CustomName' >> beam.Map(lambda x: x * x))
    dot = render.PipelineRenderer(p.to_runner_api(), default_options).to_dot()
    self.assertIn('digraph', dot)
    self.assertIn('CustomName', dot)
    self.assertEqual(dot.count('->'), 2)

  def test_render_config_validation(self):
    p = beam.Pipeline()
    _ = (
        p | beam.Impulse() | beam.Map(lambda _: 2)
        | 'CustomName' >> beam.Map(lambda x: x * x))
    pipeline_proto = p.to_runner_api()
    with pytest.raises(ValueError):
      render.RenderRunner().run_portable_pipeline(
          pipeline_proto, render.RenderOptions())

  def test_side_input(self):
    p = beam.Pipeline()
    pcoll = p | beam.Impulse() | beam.FlatMap(lambda x: [1, 2, 3])
    dot = render.PipelineRenderer(p.to_runner_api(), default_options).to_dot()
    self.assertEqual(dot.count('->'), 1)
    self.assertNotIn('dashed', dot)

    _ = pcoll | beam.Map(
        lambda x, side: x * side, side=beam.pvalue.AsList(pcoll))
    dot = render.PipelineRenderer(p.to_runner_api(), default_options).to_dot()
    self.assertEqual(dot.count('->'), 3)
    self.assertIn('dashed', dot)

  def test_composite_collapse(self):
    p = beam.Pipeline()
    _ = p | beam.Create([1, 2, 3]) | beam.Map(lambda x: x * x)
    pipeline_proto = p.to_runner_api()
    renderer = render.PipelineRenderer(pipeline_proto, default_options)
    self.assertEqual(renderer.to_dot().count('->'), 8)
    create_transform_id, = [
        id
        for (id, transform) in pipeline_proto.components.transforms.items()
        if transform.unique_name == 'Create']
    renderer.update(toggle=[create_transform_id])
    self.assertEqual(renderer.to_dot().count('->'), 1)


class DotRequiringRenderingTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    try:
      subprocess.run(['dot', '-V'], capture_output=True, check=True)
    except FileNotFoundError:
      cls._dot_installed = False
    else:
      cls._dot_installed = True

  def setUp(self) -> None:
    if not self._dot_installed:  # type: ignore[attr-defined]
      self.skipTest('dot executable not installed')

  def test_run_portable_pipeline(self):
    p = beam.Pipeline()
    _ = (
        p | beam.Impulse() | beam.Map(lambda _: 2)
        | 'CustomName' >> beam.Map(lambda x: x * x))
    pipeline_proto = p.to_runner_api()

    with tempfile.TemporaryDirectory() as tmpdir:
      svg_path = os.path.join(tmpdir, "my_output.svg")
      render.RenderRunner().run_portable_pipeline(
          pipeline_proto, render.RenderOptions(render_output=[svg_path]))
      assert os.path.exists(svg_path)

  def test_dot_well_formed(self):
    p = beam.Pipeline()
    _ = p | beam.Create([1, 2, 3]) | beam.Map(lambda x: x * x)
    pipeline_proto = p.to_runner_api()
    renderer = render.PipelineRenderer(pipeline_proto, default_options)
    # Doesn't actually look at the output, but ensures dot executes correctly.
    renderer.render_data()
    create_transform_id, = [
        id
        for (id, transform) in pipeline_proto.components.transforms.items()
        if transform.unique_name == 'Create']
    renderer.update(toggle=[create_transform_id])
    renderer.render_data()

  def test_leaf_composite_filter(self):
    p = beam.Pipeline()
    _ = p | beam.Create([1, 2, 3]) | beam.Map(lambda x: x * x)
    dot = render.PipelineRenderer(
        p.to_runner_api(),
        render.RenderOptions(['--render_leaf_composite_nodes=Create'
                              ])).to_dot()
    self.assertEqual(dot.count('->'), 1)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
