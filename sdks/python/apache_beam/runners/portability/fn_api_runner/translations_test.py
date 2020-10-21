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

from __future__ import absolute_import

import logging
import unittest

import apache_beam as beam
from apache_beam import runners
from apache_beam.options import pipeline_options
from apache_beam.portability import common_urns
from apache_beam.runners.portability.fn_api_runner import translations
from apache_beam.transforms import combiners
from apache_beam.transforms import core
from apache_beam.transforms import environments
from apache_beam.transforms.core import Create


class TranslationsTest(unittest.TestCase):
  def test_eliminate_common_key_with_void(self):
    pipeline = beam.Pipeline()
    pcoll = pipeline | 'Start' >> beam.Create([1, 2, 3])
    _ = pcoll | 'TestKeyWithNoneA' >> beam.ParDo(core._KeyWithNone())
    _ = pcoll | 'TestKeyWithNoneB' >> beam.ParDo(core._KeyWithNone())

    pipeline_proto = pipeline.to_runner_api()
    _, stages = translations.create_and_optimize_stages(
        pipeline_proto, [translations.eliminate_common_key_with_none],
        known_runner_urns=frozenset())
    key_with_none_stages = [
        stage for stage in stages if 'TestKeyWithNone' in stage.name
    ]
    self.assertEqual(len(key_with_none_stages), 1)

  def test_pack_combiners(self):
    pipeline = beam.Pipeline()
    vals = [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]
    pcoll = pipeline | 'start-perkey' >> Create([('a', x) for x in vals])
    _ = pcoll | 'mean-perkey' >> combiners.Mean.PerKey()
    _ = pcoll | 'count-perkey' >> combiners.Count.PerKey()

    environment = environments.DockerEnvironment.from_options(
        pipeline_options.PortableOptions(sdk_location='container'))
    pipeline_proto = pipeline.to_runner_api(default_environment=environment)
    _, stages = translations.create_and_optimize_stages(
        pipeline_proto, [translations.pack_combiners],
        known_runner_urns=frozenset())
    combine_per_key_stages = []
    for stage in stages:
      for transform in stage.transforms:
        if transform.spec.urn == common_urns.composites.COMBINE_PER_KEY.urn:
          combine_per_key_stages.append(stage)
    self.assertEqual(len(combine_per_key_stages), 1)
    self.assertIn('/Pack', combine_per_key_stages[0].name)

  def test_pack_combiners_with_missing_environment_capability(self):
    pipeline = beam.Pipeline()
    vals = [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]
    pcoll = pipeline | 'start-perkey' >> Create([('a', x) for x in vals])
    _ = pcoll | 'mean-perkey' >> combiners.Mean.PerKey()
    _ = pcoll | 'count-perkey' >> combiners.Count.PerKey()

    environment = environments.DockerEnvironment(capabilities=())
    pipeline_proto = pipeline.to_runner_api(default_environment=environment)
    _, stages = translations.create_and_optimize_stages(
        pipeline_proto, [translations.pack_combiners],
        known_runner_urns=frozenset())
    combine_per_key_stages = []
    for stage in stages:
      for transform in stage.transforms:
        if transform.spec.urn == common_urns.composites.COMBINE_PER_KEY.urn:
          combine_per_key_stages.append(stage)
    # Combiner packing should be skipped because the environment is missing
    # the beam:combinefn:packed_python:v1 capability.
    self.assertEqual(len(combine_per_key_stages), 2)
    self.assertNotIn('/Pack', combine_per_key_stages[0].name)

  def test_pack_global_combiners(self):
    pipeline = beam.Pipeline()
    vals = [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]
    pcoll = pipeline | 'start' >> Create(vals)
    _ = pcoll | 'mean' >> combiners.Mean.Globally()
    _ = pcoll | 'count' >> combiners.Count.Globally()

    environment = environments.DockerEnvironment.from_options(
        pipeline_options.PortableOptions(sdk_location='container'))
    pipeline_proto = pipeline.to_runner_api(default_environment=environment)
    _, stages = translations.create_and_optimize_stages(
        pipeline_proto, [
            translations.eliminate_common_key_with_none,
            translations.pack_combiners,
        ],
        known_runner_urns=frozenset())
    key_with_void_stages = [
        stage for stage in stages if 'KeyWithVoid' in stage.name
    ]
    self.assertEqual(len(key_with_void_stages), 1)

    combine_per_key_stages = []
    for stage in stages:
      for transform in stage.transforms:
        if transform.spec.urn == common_urns.composites.COMBINE_PER_KEY.urn:
          combine_per_key_stages.append(stage)
    self.assertEqual(len(combine_per_key_stages), 1)
    self.assertIn('/Pack', combine_per_key_stages[0].name)

  def test_optimize_empty_pipeline(self):
    pipeline = beam.Pipeline()
    pipeline_proto = pipeline.to_runner_api()
    optimized_pipeline_proto = translations.optimize_pipeline(
        pipeline_proto, [], known_runner_urns=frozenset(), partial=True)
    runner = runners.DirectRunner()
    beam.Pipeline.from_runner_api(
        optimized_pipeline_proto, runner, pipeline_options.PipelineOptions())


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
