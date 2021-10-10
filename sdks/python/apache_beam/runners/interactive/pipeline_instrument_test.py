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
# pytype: skip-file

import unittest

import apache_beam as beam
from apache_beam import coders
from apache_beam.pipeline import PipelineVisitor
from apache_beam.runners.interactive import cache_manager as cache
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import pipeline_instrument as instr
from apache_beam.runners.interactive import interactive_runner
from apache_beam.runners.interactive import utils
from apache_beam.runners.interactive.caching.cacheable import Cacheable
from apache_beam.runners.interactive.caching.cacheable import CacheKey
from apache_beam.runners.interactive.caching.streaming_cache import StreamingCache
from apache_beam.runners.interactive.testing.pipeline_assertion import assert_pipeline_equal
from apache_beam.runners.interactive.testing.pipeline_assertion import assert_pipeline_proto_contain_top_level_transform
from apache_beam.runners.interactive.testing.pipeline_assertion import assert_pipeline_proto_equal
from apache_beam.runners.interactive.testing.pipeline_assertion import \
    assert_pipeline_proto_not_contain_top_level_transform
from apache_beam.runners.interactive.testing.test_cache_manager import InMemoryCache
from apache_beam.testing.test_stream import TestStream


class PipelineInstrumentTest(unittest.TestCase):
  def setUp(self):
    ie.new_env()

  def cache_key_of(self, name, pcoll):
    return CacheKey.from_pcoll(name, pcoll).to_str()

  def test_pcoll_to_pcoll_id(self):
    p = beam.Pipeline(interactive_runner.InteractiveRunner())
    ie.current_env().set_cache_manager(InMemoryCache(), p)
    # pylint: disable=bad-option-value
    init_pcoll = p | 'Init Create' >> beam.Impulse()
    _, ctx = p.to_runner_api(return_context=True)
    self.assertEqual(
        instr.pcoll_to_pcoll_id(p, ctx),
        {str(init_pcoll): 'ref_PCollection_PCollection_1'})

  def test_pcoll_id_with_user_pipeline(self):
    p_id_user = beam.Pipeline(interactive_runner.InteractiveRunner())
    ie.current_env().set_cache_manager(InMemoryCache(), p_id_user)
    init_pcoll = p_id_user | 'Init Create' >> beam.Create([1, 2, 3])
    instrumentation = instr.build_pipeline_instrument(p_id_user)
    self.assertEqual(
        instrumentation.pcoll_id(init_pcoll), 'ref_PCollection_PCollection_8')

  def test_pcoll_id_with_runner_pipeline(self):
    p_id_runner = beam.Pipeline(interactive_runner.InteractiveRunner())
    ie.current_env().set_cache_manager(InMemoryCache(), p_id_runner)
    # pylint: disable=possibly-unused-variable
    init_pcoll = p_id_runner | 'Init Create' >> beam.Create([1, 2, 3])
    ib.watch(locals())

    # It's normal that when executing, the pipeline object is a different
    # but equivalent instance from what user has built. The pipeline instrument
    # should be able to identify if the original instance has changed in an
    # interactive env while mutating the other instance for execution. The
    # version map can be used to figure out what the PCollection instances are
    # in the original instance and if the evaluation has changed since last
    # execution.
    p2_id_runner = beam.Pipeline(interactive_runner.InteractiveRunner())
    # pylint: disable=bad-option-value
    init_pcoll_2 = p2_id_runner | 'Init Create' >> beam.Create(range(10))
    ie.current_env().add_derived_pipeline(p_id_runner, p2_id_runner)

    instrumentation = instr.build_pipeline_instrument(p2_id_runner)
    # The cache_key should use id(init_pcoll) as prefix even when
    # init_pcoll_2 is supplied as long as the version map is given.
    self.assertEqual(
        instrumentation.pcoll_id(init_pcoll_2), 'ref_PCollection_PCollection_8')

  def test_cache_key(self):
    p = beam.Pipeline(interactive_runner.InteractiveRunner())
    ie.current_env().set_cache_manager(InMemoryCache(), p)
    # pylint: disable=bad-option-value
    init_pcoll = p | 'Init Create' >> beam.Create(range(10))
    squares = init_pcoll | 'Square' >> beam.Map(lambda x: x * x)
    cubes = init_pcoll | 'Cube' >> beam.Map(lambda x: x**3)
    # Watch the local variables, i.e., the Beam pipeline defined.
    ib.watch(locals())

    pipeline_instrument = instr.build_pipeline_instrument(p)
    self.assertEqual(
        pipeline_instrument.cache_key(init_pcoll),
        self.cache_key_of('init_pcoll', init_pcoll))
    self.assertEqual(
        pipeline_instrument.cache_key(squares),
        self.cache_key_of('squares', squares))
    self.assertEqual(
        pipeline_instrument.cache_key(cubes), self.cache_key_of('cubes', cubes))

  def test_cacheables(self):
    p_cacheables = beam.Pipeline(interactive_runner.InteractiveRunner())
    ie.current_env().set_cache_manager(InMemoryCache(), p_cacheables)
    # pylint: disable=bad-option-value
    init_pcoll = p_cacheables | 'Init Create' >> beam.Create(range(10))
    squares = init_pcoll | 'Square' >> beam.Map(lambda x: x * x)
    cubes = init_pcoll | 'Cube' >> beam.Map(lambda x: x**3)
    ib.watch(locals())

    pipeline_instrument = instr.build_pipeline_instrument(p_cacheables)

    self.assertEqual(
        pipeline_instrument._cacheables,
        {
            pipeline_instrument.pcoll_id(init_pcoll): Cacheable(
                var='init_pcoll',
                version=str(id(init_pcoll)),
                producer_version=str(id(init_pcoll.producer)),
                pcoll=init_pcoll),
            pipeline_instrument.pcoll_id(squares): Cacheable(
                var='squares',
                version=str(id(squares)),
                producer_version=str(id(squares.producer)),
                pcoll=squares),
            pipeline_instrument.pcoll_id(cubes): Cacheable(
                var='cubes',
                version=str(id(cubes)),
                producer_version=str(id(cubes.producer)),
                pcoll=cubes)
        })

  def test_background_caching_pipeline_proto(self):
    p = beam.Pipeline(interactive_runner.InteractiveRunner())
    ie.current_env().set_cache_manager(StreamingCache(cache_dir=None), p)

    # Test that the two ReadFromPubSub are correctly cut out.
    a = p | 'ReadUnboundedSourceA' >> beam.io.ReadFromPubSub(
        subscription='projects/fake-project/subscriptions/fake_sub')
    b = p | 'ReadUnboundedSourceB' >> beam.io.ReadFromPubSub(
        subscription='projects/fake-project/subscriptions/fake_sub')

    # Add some extra PTransform afterwards to make sure that only the unbounded
    # sources remain.
    c = (a, b) | beam.Flatten()
    _ = c | beam.Map(lambda x: x)

    ib.watch(locals())
    instrumenter = instr.build_pipeline_instrument(p)
    actual_pipeline = instrumenter.background_caching_pipeline_proto()

    # Now recreate the expected pipeline, which should only have the unbounded
    # sources.
    p = beam.Pipeline(interactive_runner.InteractiveRunner())
    ie.current_env().set_cache_manager(StreamingCache(cache_dir=None), p)
    a = p | 'ReadUnboundedSourceA' >> beam.io.ReadFromPubSub(
        subscription='projects/fake-project/subscriptions/fake_sub')
    _ = (
        a
        | 'reify a' >> beam.Map(lambda _: _)
        | 'a' >> cache.WriteCache(ie.current_env().get_cache_manager(p), ''))

    b = p | 'ReadUnboundedSourceB' >> beam.io.ReadFromPubSub(
        subscription='projects/fake-project/subscriptions/fake_sub')
    _ = (
        b
        | 'reify b' >> beam.Map(lambda _: _)
        | 'b' >> cache.WriteCache(ie.current_env().get_cache_manager(p), ''))

    expected_pipeline = p.to_runner_api(return_context=False)
    assert_pipeline_proto_equal(self, expected_pipeline, actual_pipeline)

  def _example_pipeline(self, watch=True, bounded=True):
    p_example = beam.Pipeline(interactive_runner.InteractiveRunner())
    ie.current_env().set_cache_manager(InMemoryCache(), p_example)
    # pylint: disable=bad-option-value
    if bounded:
      source = beam.Create(range(10))
    else:
      source = beam.io.ReadFromPubSub(
          subscription='projects/fake-project/subscriptions/fake_sub')

    init_pcoll = p_example | 'Init Source' >> source
    second_pcoll = init_pcoll | 'Second' >> beam.Map(lambda x: x * x)
    if watch:
      ib.watch(locals())
    return (p_example, init_pcoll, second_pcoll)

  def _mock_write_cache(self, pipeline, values, cache_key):
    """Cache the PCollection where cache.WriteCache would write to."""
    labels = ['full', cache_key]

    # Usually, the pcoder will be inferred from `pcoll.element_type`
    pcoder = coders.registry.get_coder(object)
    cache_manager = ie.current_env().get_cache_manager(pipeline)
    cache_manager.save_pcoder(pcoder, *labels)
    cache_manager.write(values, *labels)

  def test_instrument_example_pipeline_to_write_cache(self):
    # Original instance defined by user code has all variables handlers.
    p_origin, init_pcoll, second_pcoll = self._example_pipeline()
    # Copied instance when execution has no user defined variables.
    p_copy, _, _ = self._example_pipeline(watch=False)
    ie.current_env().add_derived_pipeline(p_origin, p_copy)
    # Instrument the copied pipeline.
    pipeline_instrument = instr.build_pipeline_instrument(p_copy)
    # Manually instrument original pipeline with expected pipeline transforms.
    init_pcoll_cache_key = pipeline_instrument.cache_key(init_pcoll)
    _ = (
        init_pcoll
        | 'reify init' >> beam.Map(lambda _: _)
        | '_WriteCache_' + init_pcoll_cache_key >> cache.WriteCache(
            ie.current_env().get_cache_manager(p_origin), init_pcoll_cache_key))
    second_pcoll_cache_key = pipeline_instrument.cache_key(second_pcoll)
    _ = (
        second_pcoll
        | 'reify second' >> beam.Map(lambda _: _)
        | '_WriteCache_' + second_pcoll_cache_key >> cache.WriteCache(
            ie.current_env().get_cache_manager(p_origin),
            second_pcoll_cache_key))
    # The 2 pipelines should be the same now.
    assert_pipeline_equal(self, p_copy, p_origin)

  def test_instrument_example_pipeline_to_read_cache(self):
    p_origin, init_pcoll, second_pcoll = self._example_pipeline()
    p_copy, _, _ = self._example_pipeline(False)

    # Mock as if cacheable PCollections are cached.
    init_pcoll_cache_key = self.cache_key_of('init_pcoll', init_pcoll)
    self._mock_write_cache(p_origin, [b'1', b'2', b'3'], init_pcoll_cache_key)
    second_pcoll_cache_key = self.cache_key_of('second_pcoll', second_pcoll)
    self._mock_write_cache(p_origin, [b'1', b'4', b'9'], second_pcoll_cache_key)
    # Mark the completeness of PCollections from the original(user) pipeline.
    ie.current_env().mark_pcollection_computed((init_pcoll, second_pcoll))
    ie.current_env().add_derived_pipeline(p_origin, p_copy)
    instr.build_pipeline_instrument(p_copy)

    cached_init_pcoll = (
        p_origin
        | '_ReadCache_' + init_pcoll_cache_key >> cache.ReadCache(
            ie.current_env().get_cache_manager(p_origin), init_pcoll_cache_key)
        | 'unreify' >> beam.Map(lambda _: _))

    # second_pcoll is never used as input and there is no need to read cache.

    class TestReadCacheWireVisitor(PipelineVisitor):
      """Replace init_pcoll with cached_init_pcoll for all occuring inputs."""
      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        if transform_node.inputs:
          main_inputs = dict(transform_node.main_inputs)
          for tag, main_input in main_inputs.items():
            if main_input == init_pcoll:
              main_inputs[tag] = cached_init_pcoll
          transform_node.main_inputs = main_inputs

    v = TestReadCacheWireVisitor()
    p_origin.visit(v)
    assert_pipeline_equal(self, p_origin, p_copy)

  def test_find_out_correct_user_pipeline(self):
    # This is the user pipeline instance we care in the watched scope.
    user_pipeline, _, _ = self._example_pipeline()
    # This is a new runner pipeline instance with the same pipeline graph to
    # what the user_pipeline represents.
    runner_pipeline = beam.pipeline.Pipeline.from_runner_api(
        user_pipeline.to_runner_api(), user_pipeline.runner, options=None)
    ie.current_env().add_derived_pipeline(user_pipeline, runner_pipeline)
    # This is a totally irrelevant user pipeline in the watched scope.
    irrelevant_user_pipeline = beam.Pipeline(
        interactive_runner.InteractiveRunner())
    ib.watch({'irrelevant_user_pipeline': irrelevant_user_pipeline})
    # Build instrument from the runner pipeline.
    pipeline_instrument = instr.build_pipeline_instrument(runner_pipeline)
    self.assertIs(pipeline_instrument.user_pipeline, user_pipeline)

  def test_instrument_example_unbounded_pipeline_to_read_cache(self):
    """Tests that the instrumenter works for a single unbounded source.
    """
    # Create the pipeline that will be instrumented.
    p_original = beam.Pipeline(interactive_runner.InteractiveRunner())
    ie.current_env().set_cache_manager(
        StreamingCache(cache_dir=None), p_original)
    source_1 = p_original | 'source1' >> beam.io.ReadFromPubSub(
        subscription='projects/fake-project/subscriptions/fake_sub')
    # pylint: disable=possibly-unused-variable
    pcoll_1 = source_1 | 'square1' >> beam.Map(lambda x: x * x)

    # Mock as if cacheable PCollections are cached.
    ib.watch(locals())
    # This should be noop.
    utils.watch_sources(p_original)
    for name, pcoll in locals().items():
      if not isinstance(pcoll, beam.pvalue.PCollection):
        continue
      cache_key = self.cache_key_of(name, pcoll)
      self._mock_write_cache(p_original, [], cache_key)

    # Instrument the original pipeline to create the pipeline the user will see.
    instrumenter = instr.build_pipeline_instrument(p_original)
    actual_pipeline = beam.Pipeline.from_runner_api(
        proto=instrumenter.instrumented_pipeline_proto(),
        runner=interactive_runner.InteractiveRunner(),
        options=None)

    # Now, build the expected pipeline which replaces the unbounded source with
    # a TestStream.
    source_1_cache_key = self.cache_key_of('source_1', source_1)
    p_expected = beam.Pipeline()
    test_stream = (p_expected | TestStream(output_tags=[source_1_cache_key]))
    # pylint: disable=expression-not-assigned
    test_stream[source_1_cache_key] | 'square1' >> beam.Map(lambda x: x * x)

    # Test that the TestStream is outputting to the correct PCollection.
    class TestStreamVisitor(PipelineVisitor):
      def __init__(self):
        self.output_tags = set()

      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        transform = transform_node.transform
        if isinstance(transform, TestStream):
          self.output_tags = transform.output_tags

    v = TestStreamVisitor()
    actual_pipeline.visit(v)
    expected_output_tags = set([source_1_cache_key])
    actual_output_tags = v.output_tags
    self.assertSetEqual(expected_output_tags, actual_output_tags)

    # Test that the pipeline is as expected.
    assert_pipeline_proto_equal(
        self,
        p_expected.to_runner_api(),
        instrumenter.instrumented_pipeline_proto())

  def test_able_to_cache_intermediate_unbounded_source_pcollection(self):
    """Tests being able to cache an intermediate source PCollection.

    In the following pipeline, the source doesn't have a reference and so is
    not automatically cached in the watch() command. This tests that this case
    is taken care of.
    """
    # Create the pipeline that will be instrumented.
    from apache_beam.options.pipeline_options import StandardOptions
    options = StandardOptions(streaming=True)
    streaming_cache_manager = StreamingCache(cache_dir=None)
    p_original_cache_source = beam.Pipeline(
        interactive_runner.InteractiveRunner(), options)
    ie.current_env().set_cache_manager(
        streaming_cache_manager, p_original_cache_source)

    # pylint: disable=possibly-unused-variable
    source_1 = (
        p_original_cache_source
        | 'source1' >> beam.io.ReadFromPubSub(
            subscription='projects/fake-project/subscriptions/fake_sub')
        | beam.Map(lambda e: e))

    # Watch but do not cache the PCollections.
    ib.watch(locals())
    # Make sure that sources without a user reference are still cached.
    utils.watch_sources(p_original_cache_source)

    intermediate_source_pcoll = None
    for watching in ie.current_env().watching():
      watching = list(watching)
      for var, watchable in watching:
        if 'synthetic' in var:
          intermediate_source_pcoll = watchable
          break

    # Instrument the original pipeline to create the pipeline the user will see.
    p_copy = beam.Pipeline.from_runner_api(
        p_original_cache_source.to_runner_api(),
        runner=interactive_runner.InteractiveRunner(),
        options=options)
    ie.current_env().add_derived_pipeline(p_original_cache_source, p_copy)
    instrumenter = instr.build_pipeline_instrument(p_copy)
    actual_pipeline = beam.Pipeline.from_runner_api(
        proto=instrumenter.instrumented_pipeline_proto(),
        runner=interactive_runner.InteractiveRunner(),
        options=options)
    ie.current_env().add_derived_pipeline(
        p_original_cache_source, actual_pipeline)

    # Now, build the expected pipeline which replaces the unbounded source with
    # a TestStream.
    intermediate_source_pcoll_cache_key = \
        self.cache_key_of('synthetic_var_' + str(id(intermediate_source_pcoll)),
                     intermediate_source_pcoll)
    p_expected = beam.Pipeline()
    ie.current_env().set_cache_manager(streaming_cache_manager, p_expected)
    test_stream = (
        p_expected
        | TestStream(output_tags=[intermediate_source_pcoll_cache_key]))
    # pylint: disable=expression-not-assigned
    (
        test_stream[intermediate_source_pcoll_cache_key]
        | 'square1' >> beam.Map(lambda e: e)
        | 'reify' >> beam.Map(lambda _: _)
        | cache.WriteCache(
            ie.current_env().get_cache_manager(p_expected), 'unused'))

    # Test that the TestStream is outputting to the correct PCollection.
    class TestStreamVisitor(PipelineVisitor):
      def __init__(self):
        self.output_tags = set()

      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        transform = transform_node.transform
        if isinstance(transform, TestStream):
          self.output_tags = transform.output_tags

    v = TestStreamVisitor()
    actual_pipeline.visit(v)
    expected_output_tags = set([intermediate_source_pcoll_cache_key])
    actual_output_tags = v.output_tags
    self.assertSetEqual(expected_output_tags, actual_output_tags)

    # Test that the pipeline is as expected.
    assert_pipeline_proto_equal(
        self,
        p_expected.to_runner_api(),
        instrumenter.instrumented_pipeline_proto())

  def test_instrument_mixed_streaming_batch(self):
    """Tests caching for both batch and streaming sources in the same pipeline.

    This ensures that cached bounded and unbounded sources are read from the
    TestStream.
    """
    # Create the pipeline that will be instrumented.
    from apache_beam.options.pipeline_options import StandardOptions
    options = StandardOptions(streaming=True)
    p_original = beam.Pipeline(interactive_runner.InteractiveRunner(), options)
    streaming_cache_manager = StreamingCache(cache_dir=None)
    ie.current_env().set_cache_manager(streaming_cache_manager, p_original)
    source_1 = p_original | 'source1' >> beam.io.ReadFromPubSub(
        subscription='projects/fake-project/subscriptions/fake_sub')
    source_2 = p_original | 'source2' >> beam.Create([1, 2, 3, 4, 5])

    # pylint: disable=possibly-unused-variable
    pcoll_1 = ((source_1, source_2)
               | beam.Flatten()
               | 'square1' >> beam.Map(lambda x: x * x))

    # Watch but do not cache the PCollections.
    ib.watch(locals())
    # This should be noop.
    utils.watch_sources(p_original)
    self._mock_write_cache(
        p_original, [], self.cache_key_of('source_2', source_2))
    ie.current_env().mark_pcollection_computed([source_2])

    # Instrument the original pipeline to create the pipeline the user will see.
    p_copy = beam.Pipeline.from_runner_api(
        p_original.to_runner_api(),
        runner=interactive_runner.InteractiveRunner(),
        options=options)
    ie.current_env().add_derived_pipeline(p_original, p_copy)
    instrumenter = instr.build_pipeline_instrument(p_copy)
    actual_pipeline = beam.Pipeline.from_runner_api(
        proto=instrumenter.instrumented_pipeline_proto(),
        runner=interactive_runner.InteractiveRunner(),
        options=options)

    # Now, build the expected pipeline which replaces the unbounded source with
    # a TestStream.
    source_1_cache_key = self.cache_key_of('source_1', source_1)
    source_2_cache_key = self.cache_key_of('source_2', source_2)
    p_expected = beam.Pipeline()
    ie.current_env().set_cache_manager(streaming_cache_manager, p_expected)
    test_stream = (
        p_expected
        | TestStream(output_tags=[source_1_cache_key, source_2_cache_key]))
    # pylint: disable=expression-not-assigned
    ((
        test_stream[self.cache_key_of('source_1', source_1)],
        test_stream[self.cache_key_of('source_2', source_2)])
     | beam.Flatten()
     | 'square1' >> beam.Map(lambda x: x * x)
     | 'reify' >> beam.Map(lambda _: _)
     | cache.WriteCache(
         ie.current_env().get_cache_manager(p_expected), 'unused'))

    # Test that the TestStream is outputting to the correct PCollection.
    class TestStreamVisitor(PipelineVisitor):
      def __init__(self):
        self.output_tags = set()

      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        transform = transform_node.transform
        if isinstance(transform, TestStream):
          self.output_tags = transform.output_tags

    v = TestStreamVisitor()
    actual_pipeline.visit(v)
    expected_output_tags = set([source_1_cache_key, source_2_cache_key])
    actual_output_tags = v.output_tags
    self.assertSetEqual(expected_output_tags, actual_output_tags)

    # Test that the pipeline is as expected.
    assert_pipeline_proto_equal(
        self,
        p_expected.to_runner_api(),
        instrumenter.instrumented_pipeline_proto())

  def test_instrument_example_unbounded_pipeline_direct_from_source(self):
    """Tests that the it caches PCollections from a source.
    """
    # Create the pipeline that will be instrumented.
    from apache_beam.options.pipeline_options import StandardOptions
    options = StandardOptions(streaming=True)
    p_original_direct_source = beam.Pipeline(
        interactive_runner.InteractiveRunner(), options)
    ie.current_env().set_cache_manager(
        StreamingCache(cache_dir=None), p_original_direct_source)
    source_1 = p_original_direct_source | 'source1' >> beam.io.ReadFromPubSub(
        subscription='projects/fake-project/subscriptions/fake_sub')
    # pylint: disable=possibly-unused-variable
    p_expected = beam.Pipeline()
    # pylint: disable=unused-variable
    test_stream = (
        p_expected
        | TestStream(output_tags=[self.cache_key_of('source_1', source_1)]))
    # Watch but do not cache the PCollections.
    ib.watch(locals())
    # This should be noop.
    utils.watch_sources(p_original_direct_source)
    # Instrument the original pipeline to create the pipeline the user will see.
    p_copy = beam.Pipeline.from_runner_api(
        p_original_direct_source.to_runner_api(),
        runner=interactive_runner.InteractiveRunner(),
        options=options)
    ie.current_env().add_derived_pipeline(p_original_direct_source, p_copy)
    instrumenter = instr.build_pipeline_instrument(p_copy)
    actual_pipeline = beam.Pipeline.from_runner_api(
        proto=instrumenter.instrumented_pipeline_proto(),
        runner=interactive_runner.InteractiveRunner(),
        options=options)
    ie.current_env().add_derived_pipeline(
        p_original_direct_source, actual_pipeline)

    # Now, build the expected pipeline which replaces the unbounded source with
    # a TestStream.
    source_1_cache_key = self.cache_key_of('source_1', source_1)

    # Test that the TestStream is outputting to the correct PCollection.
    class TestStreamVisitor(PipelineVisitor):
      def __init__(self):
        self.output_tags = set()

      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        transform = transform_node.transform
        if isinstance(transform, TestStream):
          self.output_tags = transform.output_tags

    v = TestStreamVisitor()
    actual_pipeline.visit(v)
    expected_output_tags = set([source_1_cache_key])
    actual_output_tags = v.output_tags
    self.assertSetEqual(expected_output_tags, actual_output_tags)

    # Test that the pipeline is as expected.
    assert_pipeline_proto_equal(
        self,
        p_expected.to_runner_api(),
        instrumenter.instrumented_pipeline_proto())

  def test_instrument_example_unbounded_pipeline_to_read_cache_not_cached(self):
    """Tests that the instrumenter works when the PCollection is not cached.
    """
    # Create the pipeline that will be instrumented.
    from apache_beam.options.pipeline_options import StandardOptions
    options = StandardOptions(streaming=True)
    p_original_read_cache = beam.Pipeline(
        interactive_runner.InteractiveRunner(), options)
    ie.current_env().set_cache_manager(
        StreamingCache(cache_dir=None), p_original_read_cache)
    source_1 = p_original_read_cache | 'source1' >> beam.io.ReadFromPubSub(
        subscription='projects/fake-project/subscriptions/fake_sub')
    # pylint: disable=possibly-unused-variable
    pcoll_1 = source_1 | 'square1' >> beam.Map(lambda x: x * x)

    # Watch but do not cache the PCollections.
    ib.watch(locals())
    # This should be noop.
    utils.watch_sources(p_original_read_cache)
    # Instrument the original pipeline to create the pipeline the user will see.
    p_copy = beam.Pipeline.from_runner_api(
        p_original_read_cache.to_runner_api(),
        runner=interactive_runner.InteractiveRunner(),
        options=options)
    ie.current_env().add_derived_pipeline(p_original_read_cache, p_copy)
    instrumenter = instr.build_pipeline_instrument(p_copy)
    actual_pipeline = beam.Pipeline.from_runner_api(
        proto=instrumenter.instrumented_pipeline_proto(),
        runner=interactive_runner.InteractiveRunner(),
        options=options)

    # Now, build the expected pipeline which replaces the unbounded source with
    # a TestStream.
    source_1_cache_key = self.cache_key_of('source_1', source_1)
    p_expected = beam.Pipeline()
    ie.current_env().set_cache_manager(
        StreamingCache(cache_dir=None), p_expected)
    test_stream = (p_expected | TestStream(output_tags=[source_1_cache_key]))
    # pylint: disable=expression-not-assigned
    (
        test_stream[source_1_cache_key]
        | 'square1' >> beam.Map(lambda x: x * x)
        | 'reify' >> beam.Map(lambda _: _)
        | cache.WriteCache(
            ie.current_env().get_cache_manager(p_expected), 'unused'))

    # Test that the TestStream is outputting to the correct PCollection.
    class TestStreamVisitor(PipelineVisitor):
      def __init__(self):
        self.output_tags = set()

      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        transform = transform_node.transform
        if isinstance(transform, TestStream):
          self.output_tags = transform.output_tags

    v = TestStreamVisitor()
    actual_pipeline.visit(v)
    expected_output_tags = set([source_1_cache_key])
    actual_output_tags = v.output_tags
    self.assertSetEqual(expected_output_tags, actual_output_tags)

    # Test that the pipeline is as expected.
    assert_pipeline_proto_equal(
        self,
        p_expected.to_runner_api(),
        instrumenter.instrumented_pipeline_proto())

  def test_instrument_example_unbounded_pipeline_to_multiple_read_cache(self):
    """Tests that the instrumenter works for multiple unbounded sources.
    """
    # Create the pipeline that will be instrumented.
    p_original = beam.Pipeline(interactive_runner.InteractiveRunner())
    ie.current_env().set_cache_manager(
        StreamingCache(cache_dir=None), p_original)
    source_1 = p_original | 'source1' >> beam.io.ReadFromPubSub(
        subscription='projects/fake-project/subscriptions/fake_sub')
    source_2 = p_original | 'source2' >> beam.io.ReadFromPubSub(
        subscription='projects/fake-project/subscriptions/fake_sub')
    # pylint: disable=possibly-unused-variable
    pcoll_1 = source_1 | 'square1' >> beam.Map(lambda x: x * x)
    # pylint: disable=possibly-unused-variable
    pcoll_2 = source_2 | 'square2' >> beam.Map(lambda x: x * x)

    # Mock as if cacheable PCollections are cached.
    ib.watch(locals())
    # This should be noop.
    utils.watch_sources(p_original)
    for name, pcoll in locals().items():
      if not isinstance(pcoll, beam.pvalue.PCollection):
        continue
      cache_key = self.cache_key_of(name, pcoll)
      self._mock_write_cache(p_original, [], cache_key)

    # Instrument the original pipeline to create the pipeline the user will see.
    instrumenter = instr.build_pipeline_instrument(p_original)
    actual_pipeline = beam.Pipeline.from_runner_api(
        proto=instrumenter.instrumented_pipeline_proto(),
        runner=interactive_runner.InteractiveRunner(),
        options=None)

    # Now, build the expected pipeline which replaces the unbounded source with
    # a TestStream.
    source_1_cache_key = self.cache_key_of('source_1', source_1)
    source_2_cache_key = self.cache_key_of('source_2', source_2)
    p_expected = beam.Pipeline()
    test_stream = (
        p_expected
        | TestStream(
            output_tags=[
                self.cache_key_of('source_1', source_1),
                self.cache_key_of('source_2', source_2)
            ]))
    # pylint: disable=expression-not-assigned
    test_stream[source_1_cache_key] | 'square1' >> beam.Map(lambda x: x * x)
    # pylint: disable=expression-not-assigned
    test_stream[source_2_cache_key] | 'square2' >> beam.Map(lambda x: x * x)

    # Test that the TestStream is outputting to the correct PCollection.
    class TestStreamVisitor(PipelineVisitor):
      def __init__(self):
        self.output_tags = set()

      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        transform = transform_node.transform
        if isinstance(transform, TestStream):
          self.output_tags = transform.output_tags

    v = TestStreamVisitor()
    actual_pipeline.visit(v)
    expected_output_tags = set([source_1_cache_key, source_2_cache_key])
    actual_output_tags = v.output_tags
    self.assertSetEqual(expected_output_tags, actual_output_tags)

    # Test that the pipeline is as expected.
    assert_pipeline_proto_equal(
        self,
        p_expected.to_runner_api(),
        instrumenter.instrumented_pipeline_proto())

  def test_pipeline_pruned_when_input_pcoll_is_cached(self):
    user_pipeline, init_pcoll, _ = self._example_pipeline()
    runner_pipeline = beam.Pipeline.from_runner_api(
        user_pipeline.to_runner_api(), user_pipeline.runner, None)
    ie.current_env().add_derived_pipeline(user_pipeline, runner_pipeline)

    # Mock as if init_pcoll is cached.
    init_pcoll_cache_key = self.cache_key_of('init_pcoll', init_pcoll)
    self._mock_write_cache(
        user_pipeline, [b'1', b'2', b'3'], init_pcoll_cache_key)
    ie.current_env().mark_pcollection_computed([init_pcoll])
    # Build an instrument from the runner pipeline.
    pipeline_instrument = instr.build_pipeline_instrument(runner_pipeline)

    pruned_proto = pipeline_instrument.instrumented_pipeline_proto()
    # Skip the prune step for comparison, it should contain the sub-graph that
    # produces init_pcoll but not useful anymore.
    full_proto = pipeline_instrument._pipeline.to_runner_api()
    self.assertEqual(
        len(
            pruned_proto.components.transforms[
                'ref_AppliedPTransform_AppliedPTransform_1'].subtransforms),
        5)
    assert_pipeline_proto_not_contain_top_level_transform(
        self, pruned_proto, 'Init Source')
    self.assertEqual(
        len(
            full_proto.components.transforms[
                'ref_AppliedPTransform_AppliedPTransform_1'].subtransforms),
        6)
    assert_pipeline_proto_contain_top_level_transform(
        self, full_proto, 'Init-Source')

  def test_side_effect_pcoll_is_included(self):
    pipeline_with_side_effect = beam.Pipeline(
        interactive_runner.InteractiveRunner())
    ie.current_env().set_cache_manager(
        InMemoryCache(), pipeline_with_side_effect)
    # Deliberately not assign the result to a variable to make it a
    # "side effect" transform. Note we never watch anything from
    # the pipeline defined locally either.
    # pylint: disable=bad-option-value,expression-not-assigned
    pipeline_with_side_effect | 'Init Create' >> beam.Create(range(10))
    pipeline_instrument = instr.build_pipeline_instrument(
        pipeline_with_side_effect)
    self.assertTrue(pipeline_instrument._extended_targets)


if __name__ == '__main__':
  unittest.main()
