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
    class MultipleKeyWithNone(beam.PTransform):
      def expand(self, pcoll):
        _ = pcoll | 'key-with-none-a' >> beam.ParDo(core._KeyWithNone())
        _ = pcoll | 'key-with-none-b' >> beam.ParDo(core._KeyWithNone())
        _ = pcoll | 'key-with-none-c' >> beam.ParDo(core._KeyWithNone())

    pipeline = beam.Pipeline()
    _ = pipeline | beam.Create(
        [1, 2, 3]) | 'multiple-key-with-none' >> MultipleKeyWithNone()
    pipeline_proto = pipeline.to_runner_api()
    _, stages = translations.create_and_optimize_stages(
        pipeline_proto, [translations.eliminate_common_key_with_none],
        known_runner_urns=frozenset())
    key_with_none_stages = [
        stage for stage in stages if 'key-with-none' in stage.name
    ]
    self.assertEqual(len(key_with_none_stages), 1)
    self.assertIn('multiple-key-with-none', key_with_none_stages[0].parent)

  def test_pack_combiners(self):
    class MultipleCombines(beam.PTransform):
      def expand(self, pcoll):
        _ = pcoll | 'mean-perkey' >> combiners.Mean.PerKey()
        _ = pcoll | 'count-perkey' >> combiners.Count.PerKey()
        _ = pcoll | 'largest-perkey' >> core.CombinePerKey(combiners.Largest(1))

    pipeline = beam.Pipeline()
    vals = [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]
    _ = pipeline | Create([('a', x) for x in vals
                           ]) | 'multiple-combines' >> MultipleCombines()
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
    self.assertIn('Packed', combine_per_key_stages[0].name)
    self.assertIn('Packed', combine_per_key_stages[0].transforms[0].unique_name)
    self.assertIn('multiple-combines', combine_per_key_stages[0].parent)
    self.assertNotIn('-perkey', combine_per_key_stages[0].parent)

  def test_pack_combiners_with_missing_environment_capability(self):
    class MultipleCombines(beam.PTransform):
      def expand(self, pcoll):
        _ = pcoll | 'mean-perkey' >> combiners.Mean.PerKey()
        _ = pcoll | 'count-perkey' >> combiners.Count.PerKey()
        _ = pcoll | 'largest-perkey' >> core.CombinePerKey(combiners.Largest(1))

    pipeline = beam.Pipeline()
    vals = [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]
    _ = pipeline | Create([('a', x) for x in vals]) | MultipleCombines()
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
    self.assertEqual(len(combine_per_key_stages), 3)
    for combine_per_key_stage in combine_per_key_stages:
      self.assertNotIn('Packed', combine_per_key_stage.name)
      self.assertNotIn(
          'Packed', combine_per_key_stage.transforms[0].unique_name)

  def test_pack_global_combiners(self):
    class MultipleCombines(beam.PTransform):
      def expand(self, pcoll):
        _ = pcoll | 'mean-globally' >> combiners.Mean.Globally()
        _ = pcoll | 'count-globally' >> combiners.Count.Globally()
        _ = pcoll | 'largest-globally' >> core.CombineGlobally(
            combiners.Largest(1))

    pipeline = beam.Pipeline()
    vals = [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]
    _ = pipeline | Create(vals) | 'multiple-combines' >> MultipleCombines()
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
    self.assertIn('multiple-combines', key_with_void_stages[0].parent)
    self.assertNotIn('-globally', key_with_void_stages[0].parent)

    combine_per_key_stages = []
    for stage in stages:
      for transform in stage.transforms:
        if transform.spec.urn == common_urns.composites.COMBINE_PER_KEY.urn:
          combine_per_key_stages.append(stage)
    self.assertEqual(len(combine_per_key_stages), 1)
    self.assertIn('Packed', combine_per_key_stages[0].name)
    self.assertIn('Packed', combine_per_key_stages[0].transforms[0].unique_name)
    self.assertIn('multiple-combines', combine_per_key_stages[0].parent)
    self.assertNotIn('-globally', combine_per_key_stages[0].parent)

  def test_optimize_empty_pipeline(self):
    pipeline = beam.Pipeline()
    pipeline_proto = pipeline.to_runner_api()
    optimized_pipeline_proto = translations.optimize_pipeline(
        pipeline_proto, [], known_runner_urns=frozenset(), partial=True)
    # Tests that Pipeline.from_runner_api() does not throw an exception.
    runner = runners.DirectRunner()
    beam.Pipeline.from_runner_api(
        optimized_pipeline_proto, runner, pipeline_options.PipelineOptions())

  def test_optimize_single_combine_globally(self):
    pipeline = beam.Pipeline()
    vals = [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]
    _ = pipeline | Create(vals) | combiners.Count.Globally()
    pipeline_proto = pipeline.to_runner_api()
    optimized_pipeline_proto = translations.optimize_pipeline(
        pipeline_proto,
        [
            translations.eliminate_common_key_with_none,
            translations.pack_combiners,
        ],
        known_runner_urns=frozenset(),
        partial=True)
    # Tests that Pipeline.from_runner_api() does not throw an exception.
    runner = runners.DirectRunner()
    beam.Pipeline.from_runner_api(
        optimized_pipeline_proto, runner, pipeline_options.PipelineOptions())

  def test_optimize_multiple_combine_globally(self):
    pipeline = beam.Pipeline()
    vals = [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]
    pcoll = pipeline | Create(vals)
    _ = pcoll | 'mean-globally' >> combiners.Mean.Globally()
    _ = pcoll | 'count-globally' >> combiners.Count.Globally()
    _ = pcoll | 'largest-globally' >> core.CombineGlobally(combiners.Largest(1))
    pipeline_proto = pipeline.to_runner_api()
    optimized_pipeline_proto = translations.optimize_pipeline(
        pipeline_proto,
        [
            translations.eliminate_common_key_with_none,
            translations.pack_combiners,
        ],
        known_runner_urns=frozenset(),
        partial=True)
    # Tests that Pipeline.from_runner_api() does not throw an exception.
    runner = runners.DirectRunner()
    beam.Pipeline.from_runner_api(
        optimized_pipeline_proto, runner, pipeline_options.PipelineOptions())

  def test_pipeline_from_sorted_stages_is_toplogically_ordered(self):
    pipeline = beam.Pipeline()
    side = pipeline | 'side' >> Create([3, 4])

    class CreateAndMultiplyBySide(beam.PTransform):
      def expand(self, pcoll):
        return (
            pcoll | 'main' >> Create([1, 2]) | 'compute' >> beam.FlatMap(
                lambda x, s: [x * y for y in s], beam.pvalue.AsIter(side)))

    _ = pipeline | 'create-and-multiply-by-side' >> CreateAndMultiplyBySide()
    pipeline_proto = pipeline.to_runner_api()
    optimized_pipeline_proto = translations.optimize_pipeline(
        pipeline_proto, [
            (lambda stages, _: reversed(list(stages))),
            translations.sort_stages,
        ],
        known_runner_urns=frozenset(),
        partial=True)

    def assert_is_topologically_sorted(transform_id, visited_pcolls):
      transform = optimized_pipeline_proto.components.transforms[transform_id]
      self.assertTrue(set(transform.inputs.values()).issubset(visited_pcolls))
      visited_pcolls.update(transform.outputs.values())
      for subtransform in transform.subtransforms:
        assert_is_topologically_sorted(subtransform, visited_pcolls)

    self.assertEqual(len(optimized_pipeline_proto.root_transform_ids), 1)
    assert_is_topologically_sorted(
        optimized_pipeline_proto.root_transform_ids[0], set())


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
