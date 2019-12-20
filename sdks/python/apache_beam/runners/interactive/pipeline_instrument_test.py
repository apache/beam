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

"""Tests for apache_beam.runners.interactive.pipeline_instrument."""
from __future__ import absolute_import

import tempfile
import time
import unittest

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import filesystems
from apache_beam.pipeline import PipelineVisitor
from apache_beam.runners.interactive import cache_manager as cache
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import pipeline_instrument as instr
from apache_beam.runners.interactive import interactive_runner
from apache_beam.runners.interactive.testing.pipeline_assertion import assert_pipeline_equal
from apache_beam.runners.interactive.testing.pipeline_assertion import assert_pipeline_proto_equal

# Work around nose tests using Python2 without unittest.mock module.
try:
  from unittest.mock import MagicMock
except ImportError:
  from mock import MagicMock


class PipelineInstrumentTest(unittest.TestCase):

  def setUp(self):
    ie.new_env(cache_manager=cache.FileBasedCacheManager())

  def test_pcolls_to_pcoll_id(self):
    p = beam.Pipeline(interactive_runner.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    init_pcoll = p | 'Init Create' >> beam.Impulse()
    _, ctx = p.to_runner_api(use_fake_coders=True, return_context=True)
    self.assertEqual(instr.pcolls_to_pcoll_id(p, ctx), {
        str(init_pcoll): 'ref_PCollection_PCollection_1'})

  def test_cacheable_key_without_version_map(self):
    p = beam.Pipeline(interactive_runner.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    init_pcoll = p | 'Init Create' >> beam.Create(range(10))
    _, ctx = p.to_runner_api(use_fake_coders=True, return_context=True)
    self.assertEqual(
        instr.cacheable_key(init_pcoll, instr.pcolls_to_pcoll_id(p, ctx)),
        str(id(init_pcoll)) + '_ref_PCollection_PCollection_10')

  def test_cacheable_key_with_version_map(self):
    p = beam.Pipeline(interactive_runner.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    init_pcoll = p | 'Init Create' >> beam.Create(range(10))

    # It's normal that when executing, the pipeline object is a different
    # but equivalent instance from what user has built. The pipeline instrument
    # should be able to identify if the original instance has changed in an
    # interactive env while mutating the other instance for execution. The
    # version map can be used to figure out what the PCollection instances are
    # in the original instance and if the evaluation has changed since last
    # execution.
    p2 = beam.Pipeline(interactive_runner.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    init_pcoll_2 = p2 | 'Init Create' >> beam.Create(range(10))
    _, ctx = p2.to_runner_api(use_fake_coders=True, return_context=True)

    # The cacheable_key should use id(init_pcoll) as prefix even when
    # init_pcoll_2 is supplied as long as the version map is given.
    self.assertEqual(
        instr.cacheable_key(init_pcoll_2, instr.pcolls_to_pcoll_id(p2, ctx), {
            'ref_PCollection_PCollection_10': str(id(init_pcoll))}),
        str(id(init_pcoll)) + '_ref_PCollection_PCollection_10')

  def test_cache_key(self):
    p = beam.Pipeline(interactive_runner.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    init_pcoll = p | 'Init Create' >> beam.Create(range(10))
    squares = init_pcoll | 'Square' >> beam.Map(lambda x: x * x)
    cubes = init_pcoll | 'Cube' >> beam.Map(lambda x: x ** 3)
    # Watch the local variables, i.e., the Beam pipeline defined.
    ib.watch(locals())

    pin = instr.pin(p)
    self.assertEqual(pin.cache_key(init_pcoll), 'init_pcoll_' + str(
        id(init_pcoll)) + '_' + str(id(init_pcoll.producer)))
    self.assertEqual(pin.cache_key(squares), 'squares_' + str(
        id(squares)) + '_' + str(id(squares.producer)))
    self.assertEqual(pin.cache_key(cubes), 'cubes_' + str(
        id(cubes)) + '_' + str(id(cubes.producer)))

  def test_cacheables(self):
    p = beam.Pipeline(interactive_runner.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    init_pcoll = p | 'Init Create' >> beam.Create(range(10))
    squares = init_pcoll | 'Square' >> beam.Map(lambda x: x * x)
    cubes = init_pcoll | 'Cube' >> beam.Map(lambda x: x ** 3)
    ib.watch(locals())

    pin = instr.pin(p)
    self.assertEqual(pin.cacheables, {
        pin._cacheable_key(init_pcoll): {
            'var': 'init_pcoll',
            'version': str(id(init_pcoll)),
            'pcoll_id': 'ref_PCollection_PCollection_10',
            'producer_version': str(id(init_pcoll.producer)),
            'pcoll': init_pcoll
        },
        pin._cacheable_key(squares): {
            'var': 'squares',
            'version': str(id(squares)),
            'pcoll_id': 'ref_PCollection_PCollection_11',
            'producer_version': str(id(squares.producer)),
            'pcoll': squares
        },
        pin._cacheable_key(cubes): {
            'var': 'cubes',
            'version': str(id(cubes)),
            'pcoll_id': 'ref_PCollection_PCollection_12',
            'producer_version': str(id(cubes.producer)),
            'pcoll': cubes
        }
    })

  def test_has_unbounded_source(self):
    p = beam.Pipeline(interactive_runner.InteractiveRunner())
    _ = p | 'ReadUnboundedSource' >> beam.io.ReadFromPubSub(
        subscription='projects/fake-project/subscriptions/fake_sub')
    self.assertTrue(instr.has_unbounded_sources(p))

  def test_not_has_unbounded_source(self):
    p = beam.Pipeline(interactive_runner.InteractiveRunner())
    with tempfile.NamedTemporaryFile(delete=False) as f:
      f.write(b'test')
    _ = p | 'ReadBoundedSource' >> beam.io.ReadFromText(f.name)
    self.assertFalse(instr.has_unbounded_sources(p))

  def test_background_caching_pipeline_proto(self):
    p = beam.Pipeline(interactive_runner.InteractiveRunner())

    # Test that the two ReadFromPubSub are correctly cut out.
    a = p | 'ReadUnboundedSourceA' >> beam.io.ReadFromPubSub(
        subscription='projects/fake-project/subscriptions/fake_sub')
    b = p | 'ReadUnboundedSourceB' >> beam.io.ReadFromPubSub(
        subscription='projects/fake-project/subscriptions/fake_sub')

    # Add some extra PTransform afterwards to make sure that only the unbounded
    # sources remain.
    c = (a, b) | beam.CoGroupByKey()
    _ = c | beam.Map(lambda x: x)

    ib.watch(locals())
    instrumenter = instr.pin(p)
    actual_pipeline = instrumenter.background_caching_pipeline_proto()

    # Now recreate the expected pipeline, which should only have the unbounded
    # sources.
    p = beam.Pipeline(interactive_runner.InteractiveRunner())
    a = p | 'ReadUnboundedSourceA' >> beam.io.ReadFromPubSub(
        subscription='projects/fake-project/subscriptions/fake_sub')
    _ = a | 'a' >> cache.WriteCache(ie.current_env().cache_manager(), '')

    b = p | 'ReadUnboundedSourceB' >> beam.io.ReadFromPubSub(
        subscription='projects/fake-project/subscriptions/fake_sub')
    _ = b | 'b' >> cache.WriteCache(ie.current_env().cache_manager(), '')

    expected_pipeline = p.to_runner_api(return_context=False,
                                        use_fake_coders=True)

    assert_pipeline_proto_equal(self, expected_pipeline, actual_pipeline)

  def _example_pipeline(self, watch=True):
    p = beam.Pipeline(interactive_runner.InteractiveRunner())
    # pylint: disable=range-builtin-not-iterating
    init_pcoll = p | 'Init Create' >> beam.Create(range(10))
    second_pcoll = init_pcoll | 'Second' >> beam.Map(lambda x: x * x)
    if watch:
      ib.watch(locals())
    return (p, init_pcoll, second_pcoll)

  def _mock_write_cache(self, pcoll, cache_key):
    """Cache the PCollection where cache.WriteCache would write to."""
    cache_path = filesystems.FileSystems.join(
        ie.current_env().cache_manager()._cache_dir, 'full')
    if not filesystems.FileSystems.exists(cache_path):
      filesystems.FileSystems.mkdirs(cache_path)

    # Pause for 0.1 sec, because the Jenkins test runs so fast that the file
    # writes happen at the same timestamp.
    time.sleep(0.1)

    cache_file = cache_key + '-1-of-2'
    labels = ['full', cache_key]

    # Usually, the pcoder will be inferred from `pcoll.element_type`
    pcoder = coders.registry.get_coder(object)
    ie.current_env().cache_manager().save_pcoder(pcoder, *labels)
    sink = ie.current_env().cache_manager().sink(*labels)

    with open(ie.current_env().cache_manager()._path('full', cache_file),
              'wb') as f:
      sink.write_record(f, pcoll)

  def test_instrument_example_pipeline_to_write_cache(self):
    # Original instance defined by user code has all variables handlers.
    p_origin, init_pcoll, second_pcoll = self._example_pipeline()
    # Copied instance when execution has no user defined variables.
    p_copy, _, _ = self._example_pipeline(False)
    # Instrument the copied pipeline.
    pin = instr.pin(p_copy)
    # Manually instrument original pipeline with expected pipeline transforms.
    init_pcoll_cache_key = pin.cache_key(init_pcoll)
    _ = init_pcoll | (
        ('_WriteCache_' + init_pcoll_cache_key) >> cache.WriteCache(
            ie.current_env().cache_manager(), init_pcoll_cache_key))
    second_pcoll_cache_key = pin.cache_key(second_pcoll)
    _ = second_pcoll | (
        ('_WriteCache_' + second_pcoll_cache_key) >> cache.WriteCache(
            ie.current_env().cache_manager(), second_pcoll_cache_key))
    # The 2 pipelines should be the same now.
    assert_pipeline_equal(self, p_copy, p_origin)

  def test_instrument_example_pipeline_to_read_cache(self):
    p_origin, init_pcoll, second_pcoll = self._example_pipeline()
    p_copy, _, _ = self._example_pipeline(False)

    # Mock as if cacheable PCollections are cached.
    init_pcoll_cache_key = 'init_pcoll_' + str(
        id(init_pcoll)) + '_' + str(id(init_pcoll.producer))
    self._mock_write_cache(init_pcoll, init_pcoll_cache_key)
    second_pcoll_cache_key = 'second_pcoll_' + str(
        id(second_pcoll)) + '_' + str(id(second_pcoll.producer))
    self._mock_write_cache(second_pcoll, second_pcoll_cache_key)
    ie.current_env().cache_manager().exists = MagicMock(return_value=True)
    instr.pin(p_copy)

    cached_init_pcoll = p_origin | (
        '_ReadCache_' + init_pcoll_cache_key) >> cache.ReadCache(
            ie.current_env().cache_manager(), init_pcoll_cache_key)

    # second_pcoll is never used as input and there is no need to read cache.

    class TestReadCacheWireVisitor(PipelineVisitor):
      """Replace init_pcoll with cached_init_pcoll for all occuring inputs."""

      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        if transform_node.inputs:
          input_list = list(transform_node.inputs)
          for i in range(len(input_list)):
            if input_list[i] == init_pcoll:
              input_list[i] = cached_init_pcoll
          transform_node.inputs = tuple(input_list)

    v = TestReadCacheWireVisitor()
    p_origin.visit(v)
    assert_pipeline_equal(self, p_origin, p_copy)

  def test_find_out_correct_user_pipeline(self):
    # This is the user pipeline instance we care in the watched scope.
    user_pipeline, _, _ = self._example_pipeline()
    # This is a new runner pipeline instance with the same pipeline graph to
    # what the user_pipeline represents.
    runner_pipeline = beam.pipeline.Pipeline.from_runner_api(
        user_pipeline.to_runner_api(use_fake_coders=True),
        user_pipeline.runner,
        options=None)
    # This is a totally irrelevant user pipeline in the watched scope.
    irrelevant_user_pipeline = beam.Pipeline(
        interactive_runner.InteractiveRunner())
    ib.watch({'irrelevant_user_pipeline': irrelevant_user_pipeline})
    # Build instrument from the runner pipeline.
    pipeline_instrument = instr.pin(runner_pipeline)
    self.assertTrue(pipeline_instrument.user_pipeline is user_pipeline)


if __name__ == '__main__':
  unittest.main()
