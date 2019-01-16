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

"""Tests for apache_beam.runners.interactive.pipeline_ananlyzer.

This module is experimental. No backwards-compatibility guarantees.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import apache_beam as beam
from apache_beam.runners.direct import direct_runner
from apache_beam.runners.interactive import cache_manager as cache
from apache_beam.runners.interactive import pipeline_analyzer


def to_stable_runner_api(p):
  """The extra round trip ensures a stable pipeline proto.
  """
  return (beam.pipeline.Pipeline.from_runner_api(
      p.to_runner_api(use_fake_coders=True),
      p.runner,
      p._options).to_runner_api(use_fake_coders=True))


class PipelineAnalyzerTest(unittest.TestCase):

  def setUp(self):
    self.runner = direct_runner.DirectRunner()
    self.cache_manager = cache.FileBasedCacheManager()

  def tearDown(self):
    self.cache_manager.cleanup()

  def assertPipelineEqual(self, pipeline_proto1, pipeline_proto2):
    """A naive check for Pipeline proto equality.
    """
    components1 = pipeline_proto1.components
    components2 = pipeline_proto2.components

    self.assertEqual(len(components1.transforms), len(components2.transforms))
    self.assertEqual(len(components1.pcollections),
                     len(components2.pcollections))

    # GreatEqual instead of Equal because the pipeline_proto_to_execute could
    # include more windowing_stratagies and coders than necessary.
    self.assertGreaterEqual(len(components1.windowing_strategies),
                            len(components2.windowing_strategies))
    self.assertGreaterEqual(len(components1.coders), len(components2.coders))
    self.assertTransformEqual(pipeline_proto1,
                              pipeline_proto1.root_transform_ids[0],
                              pipeline_proto2,
                              pipeline_proto2.root_transform_ids[0])

  def assertTransformEqual(self, pipeline_proto1, transform_id1,
                           pipeline_proto2, transform_id2):
    """A naive check for Transform proto equality.
    """
    transform_proto1 = pipeline_proto1.components.transforms[transform_id1]
    transform_proto2 = pipeline_proto2.components.transforms[transform_id2]
    self.assertEqual(transform_proto1.spec.urn, transform_proto2.spec.urn)
    # Skipping payload checking because PTransforms of the same functionality
    # could generate different payloads.
    self.assertEqual(len(transform_proto1.subtransforms),
                     len(transform_proto2.subtransforms))
    self.assertSetEqual(set(transform_proto1.inputs),
                        set(transform_proto2.inputs))
    self.assertSetEqual(set(transform_proto1.outputs),
                        set(transform_proto2.outputs))

  def test_basic(self):
    p = beam.Pipeline(runner=self.runner)

    # The cold run.
    pcoll = (p
             | 'Create' >> beam.Create([1, 2, 3])
             | 'Double' >> beam.Map(lambda x: x*2)
             | 'Square' >> beam.Map(lambda x: x**2))
    analyzer = pipeline_analyzer.PipelineAnalyzer(self.cache_manager,
                                                  to_stable_runner_api(p),
                                                  self.runner)
    pipeline_to_execute = beam.pipeline.Pipeline.from_runner_api(
        analyzer.pipeline_proto_to_execute(),
        p.runner,
        p._options
    )
    pipeline_to_execute.run().wait_until_finish()
    self.assertEqual(
        len(analyzer.tl_required_trans_ids()),
        7  # Create, Double, Square, CacheSample * 3, CacheFull
    )
    self.assertEqual(len(analyzer.tl_referenced_pcoll_ids()), 3)
    self.assertEqual(len(analyzer.read_cache_ids()), 0)
    self.assertEqual(len(analyzer.write_cache_ids()), 4)

    # The second run.
    _ = (pcoll
         | 'Triple' >> beam.Map(lambda x: x*3)
         | 'Cube' >> beam.Map(lambda x: x**3))
    analyzer = pipeline_analyzer.PipelineAnalyzer(self.cache_manager,
                                                  to_stable_runner_api(p),
                                                  self.runner)
    pipeline_to_execute = beam.pipeline.Pipeline.from_runner_api(
        analyzer.pipeline_proto_to_execute(),
        p.runner,
        p._options
    )
    self.assertEqual(
        len(analyzer.tl_required_trans_ids()),
        6  # Read, Triple, Cube, CacheSample * 2, CacheFull
    )
    self.assertEqual(len(analyzer.tl_referenced_pcoll_ids()), 3)
    self.assertEqual(len(analyzer.read_cache_ids()), 1)
    self.assertEqual(len(analyzer.write_cache_ids()), 3)

    # No need to actually execute the second run.

  def test_word_count(self):
    p = beam.Pipeline(runner=self.runner)

    class WordExtractingDoFn(beam.DoFn):

      def process(self, element):
        text_line = element.strip()
        words = text_line.split()
        return words

    # Count the occurrences of each word.
    pcoll1 = p | beam.Create(['to be or not to be that is the question'])
    pcoll2 = pcoll1 | 'Split' >> beam.ParDo(WordExtractingDoFn())
    pcoll3 = pcoll2 | 'Pair with One' >> beam.Map(lambda x: (x, 1))
    pcoll4 = pcoll3 | 'Group' >> beam.GroupByKey()
    pcoll5 = pcoll4 | 'Count' >> beam.Map(lambda item: (item[0], sum(item[1])))

    analyzer = pipeline_analyzer.PipelineAnalyzer(self.cache_manager,
                                                  to_stable_runner_api(p),
                                                  self.runner)

    cache_label1 = 'PColl-1111111'
    cache_label2 = 'PColl-2222222'
    cache_label3 = 'PColl-3333333'
    cache_label4 = 'PColl-4444444'
    cache_label5 = 'PColl-5555555'

    # pylint: disable=expression-not-assigned
    pcoll1 | 'CacheSample%s' % cache_label1 >> cache.WriteCache(
        self.cache_manager, cache_label1, sample=True, sample_size=10)
    pcoll2 | 'CacheSample%s' % cache_label2 >> cache.WriteCache(
        self.cache_manager, cache_label2, sample=True, sample_size=10)
    pcoll3 | 'CacheSample%s' % cache_label3 >> cache.WriteCache(
        self.cache_manager, cache_label3, sample=True, sample_size=10)
    pcoll4 | 'CacheSample%s' % cache_label4 >> cache.WriteCache(
        self.cache_manager, cache_label3, sample=True, sample_size=10)
    pcoll5 | 'CacheSample%s' % cache_label5 >> cache.WriteCache(
        self.cache_manager, cache_label3, sample=True, sample_size=10)
    pcoll5 | 'CacheFull%s' % cache_label5 >> cache.WriteCache(
        self.cache_manager, cache_label3)
    expected_pipeline_proto = to_stable_runner_api(p)

    self.assertPipelineEqual(analyzer.pipeline_proto_to_execute(),
                             expected_pipeline_proto)

    pipeline_to_execute = beam.pipeline.Pipeline.from_runner_api(
        analyzer.pipeline_proto_to_execute(),
        p.runner,
        p._options
    )
    pipeline_to_execute.run().wait_until_finish()

  def test_write_cache_expansion(self):
    p = beam.Pipeline(runner=self.runner)

    pcoll1 = p | 'Create' >> beam.Create([1, 2, 3])
    pcoll2 = pcoll1 | 'Double' >> beam.Map(lambda x: x*2)
    pcoll3 = pcoll2 | 'Square' >> beam.Map(lambda x: x**2)
    analyzer = pipeline_analyzer.PipelineAnalyzer(self.cache_manager,
                                                  to_stable_runner_api(p),
                                                  self.runner)

    cache_label1 = 'PColl-1234567'
    cache_label2 = 'PColl-7654321'
    cache_label3 = 'PColl-3141593'

    # pylint: disable=expression-not-assigned
    pcoll1 | 'CacheSample%s' % cache_label1 >> cache.WriteCache(
        self.cache_manager, cache_label1, sample=True, sample_size=10)
    pcoll2 | 'CacheSample%s' % cache_label2 >> cache.WriteCache(
        self.cache_manager, cache_label2, sample=True, sample_size=10)
    pcoll3 | 'CacheSample%s' % cache_label3 >> cache.WriteCache(
        self.cache_manager, cache_label3, sample=True, sample_size=10)
    pcoll3 | 'CacheFull%s' % cache_label3 >> cache.WriteCache(
        self.cache_manager, cache_label3)
    expected_pipeline_proto = to_stable_runner_api(p)

    # Since WriteCache expansion leads to more than 50 PTransform protos in the
    # pipeline, a simple check of proto map size is enough.
    self.assertPipelineEqual(analyzer.pipeline_proto_to_execute(),
                             expected_pipeline_proto)

  def test_read_cache_expansion(self):
    p = beam.Pipeline(runner=self.runner)

    # The cold run.
    pcoll = (p
             | 'Create' >> beam.Create([1, 2, 3])
             | 'Double' >> beam.Map(lambda x: x*2)
             | 'Square' >> beam.Map(lambda x: x**2))
    pipeline_proto = to_stable_runner_api(p)

    pipeline_info = pipeline_analyzer.PipelineInfo(pipeline_proto.components)
    pcoll_id = 'ref_PCollection_PCollection_3'  # Output PCollection of Square
    cache_label1 = pipeline_info.cache_label(pcoll_id)

    analyzer = pipeline_analyzer.PipelineAnalyzer(self.cache_manager,
                                                  pipeline_proto,
                                                  self.runner)
    pipeline_to_execute = beam.pipeline.Pipeline.from_runner_api(
        analyzer.pipeline_proto_to_execute(),
        p.runner,
        p._options
    )
    pipeline_to_execute.run().wait_until_finish()

    # The second run.
    _ = (pcoll
         | 'Triple' >> beam.Map(lambda x: x*3)
         | 'Cube' >> beam.Map(lambda x: x**3))
    analyzer = pipeline_analyzer.PipelineAnalyzer(self.cache_manager,
                                                  to_stable_runner_api(p),
                                                  self.runner)

    expected_pipeline = beam.Pipeline(runner=self.runner)
    pcoll1 = (expected_pipeline
              | 'Load%s' % cache_label1 >> cache.ReadCache(
                  self.cache_manager, cache_label1))
    pcoll2 = pcoll1 | 'Triple' >> beam.Map(lambda x: x*3)
    pcoll3 = pcoll2 | 'Cube' >> beam.Map(lambda x: x**3)

    cache_label2 = 'PColl-7654321'
    cache_label3 = 'PColl-3141593'

    # pylint: disable=expression-not-assigned
    pcoll2 | 'CacheSample%s' % cache_label2 >> cache.WriteCache(
        self.cache_manager, cache_label2, sample=True, sample_size=10)
    pcoll3 | 'CacheSample%s' % cache_label3 >> cache.WriteCache(
        self.cache_manager, cache_label3, sample=True, sample_size=10)
    pcoll3 | 'CacheFull%s' % cache_label3 >> cache.WriteCache(
        self.cache_manager, cache_label3)

    # Since ReadCache & WriteCache expansion leads to more than 50 PTransform
    # protos in the pipeline, a simple check of proto map size is enough.
    self.assertPipelineEqual(analyzer.pipeline_proto_to_execute(),
                             to_stable_runner_api(expected_pipeline))


if __name__ == '__main__':
  unittest.main()
