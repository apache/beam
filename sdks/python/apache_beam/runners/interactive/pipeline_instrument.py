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

"""Module to instrument interactivity to the given pipeline.

For internal use only; no backwards-compatibility guarantees.
This module accesses current interactive environment and analyzes given pipeline
to transform original pipeline into a one-shot pipeline with interactivity.
"""
# pytype: skip-file

import logging
from typing import Dict

import apache_beam as beam
from apache_beam.pipeline import PipelineVisitor
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import pipeline_fragment as pf
from apache_beam.runners.interactive import background_caching_job
from apache_beam.runners.interactive import utils
from apache_beam.runners.interactive.caching.cacheable import Cacheable
from apache_beam.runners.interactive.caching.cacheable import CacheKey
from apache_beam.runners.interactive.caching.reify import WRITE_CACHE
from apache_beam.runners.interactive.caching.reify import reify_to_cache
from apache_beam.runners.interactive.caching.reify import unreify_from_cache
from apache_beam.testing import test_stream

_LOGGER = logging.getLogger(__name__)


class PipelineInstrument(object):
  """A pipeline instrument for pipeline to be executed by interactive runner.

  This module should never depend on underlying runner that interactive runner
  delegates. It instruments the original instance of pipeline directly by
  appending or replacing transforms with help of cache. It provides
  interfaces to recover states of original pipeline. It's the interactive
  runner's responsibility to coordinate supported underlying runners to run
  the pipeline instrumented and recover the original pipeline states if needed.
  """
  def __init__(self, pipeline, options=None):
    self._pipeline = pipeline

    self._user_pipeline = ie.current_env().user_pipeline(pipeline)
    if not self._user_pipeline:
      self._user_pipeline = pipeline
    self._cache_manager = ie.current_env().get_cache_manager(
        self._user_pipeline, create_if_absent=True)
    # Check if the user defined pipeline contains any source to cache.
    # If so, during the check, the cache manager is converted into a
    # streaming cache manager, thus re-assign.
    if background_caching_job.has_source_to_cache(self._user_pipeline):
      self._cache_manager = ie.current_env().get_cache_manager(
          self._user_pipeline)

    self._background_caching_pipeline = beam.pipeline.Pipeline.from_runner_api(
        pipeline.to_runner_api(), pipeline.runner, options)
    ie.current_env().add_derived_pipeline(
        self._pipeline, self._background_caching_pipeline)

    # Snapshot of original pipeline information.
    (self._original_pipeline_proto,
     context) = self._pipeline.to_runner_api(return_context=True)

    # All compute-once-against-original-pipeline fields.
    self._unbounded_sources = utils.unbounded_sources(
        self._background_caching_pipeline)
    self._pcoll_to_pcoll_id = pcoll_to_pcoll_id(self._pipeline, context)

    # A Dict[str, Cacheable] from a PCollection id to a Cacheable that belongs
    # to the analyzed pipeline.
    self._cacheables = self.find_cacheables()

    # A dict from cache key to PCollection that is read from cache.
    # If exists, caller should reuse the PCollection read. If not, caller
    # should create new transform and track the PCollection read from cache.
    # (Dict[str, AppliedPTransform]).
    self._cached_pcoll_read = {}

    # A dict from PCollections in the runner pipeline instance to their
    # corresponding PCollections in the user pipeline instance. Populated
    # after preprocess().
    self._runner_pcoll_to_user_pcoll = {}
    self._pruned_pipeline_proto = None

    # Refers target pcolls output by instrumented write cache transforms, used
    # by pruning logic as supplemental targets to build pipeline fragment up
    # from.
    self._extended_targets = set()

    # Refers pcolls used as inputs but got replaced by outputs of read cache
    # transforms instrumented, used by pruning logic as targets no longer need
    # to be produced during pipeline runs.
    self._ignored_targets = set()

    # Set of PCollections that are written to cache.
    self.cached_pcolls = set()

  def instrumented_pipeline_proto(self):
    """Always returns a new instance of portable instrumented proto."""
    targets = set(self._runner_pcoll_to_user_pcoll.keys())
    targets.update(self._extended_targets)
    targets = targets.difference(self._ignored_targets)
    if len(targets) > 0:
      # Prunes upstream transforms that don't contribute to the targets the
      # instrumented pipeline run cares.
      return pf.PipelineFragment(
          list(targets)).deduce_fragment().to_runner_api()
    return self._pipeline.to_runner_api()

  def _required_components(
      self,
      pipeline_proto,
      required_transforms_ids,
      visited,
      follow_outputs=False,
      follow_inputs=False):
    """Returns the components and subcomponents of the given transforms.

    This method returns required components such as transforms and PCollections
    related to the given transforms and to all of their subtransforms. This
    method accomplishes this recursively.
    """
    if not required_transforms_ids:
      return ({}, {})

    transforms = pipeline_proto.components.transforms
    pcollections = pipeline_proto.components.pcollections

    # Cache the transforms that will be copied into the new pipeline proto.
    required_transforms = {k: transforms[k] for k in required_transforms_ids}

    # Cache all the output PCollections of the transforms.
    pcollection_ids = [
        pc for t in required_transforms.values() for pc in t.outputs.values()
    ]
    required_pcollections = {
        pc_id: pcollections[pc_id]
        for pc_id in pcollection_ids
    }

    subtransforms = {}
    subpcollections = {}

    # Recursively go through all the subtransforms and add their components.
    for transform_id, transform in required_transforms.items():
      if transform_id in pipeline_proto.root_transform_ids:
        continue
      (t, pc) = self._required_components(
          pipeline_proto,
          transform.subtransforms,
          visited,
          follow_outputs=False,
          follow_inputs=False)
      subtransforms.update(t)
      subpcollections.update(pc)

    if follow_outputs:
      outputs = [
          pc_id for t in required_transforms.values()
          for pc_id in t.outputs.values()
      ]
      visited_copy = visited.copy()
      consuming_transforms = {
          t_id: t
          for t_id,
          t in transforms.items()
          if set(outputs).intersection(set(t.inputs.values()))
      }
      consuming_transforms = set(consuming_transforms.keys())
      visited.update(consuming_transforms)
      consuming_transforms = consuming_transforms - visited_copy
      (t, pc) = self._required_components(
          pipeline_proto,
          list(consuming_transforms),
          visited,
          follow_outputs,
          follow_inputs)
      subtransforms.update(t)
      subpcollections.update(pc)

    if follow_inputs:
      inputs = [
          pc_id for t in required_transforms.values()
          for pc_id in t.inputs.values()
      ]
      producing_transforms = {
          t_id: t
          for t_id,
          t in transforms.items()
          if set(inputs).intersection(set(t.outputs.values()))
      }
      (t, pc) = self._required_components(
          pipeline_proto,
          list(producing_transforms.keys()),
          visited,
          follow_outputs,
          follow_inputs)
      subtransforms.update(t)
      subpcollections.update(pc)

    # Now we got all the components and their subcomponents, so return the
    # complete collection.
    required_transforms.update(subtransforms)
    required_pcollections.update(subpcollections)

    return (required_transforms, required_pcollections)

  def prune_subgraph_for(self, pipeline, required_transform_ids):
    # Create the pipeline_proto to read all the components from. It will later
    # create a new pipeline proto from the cut out components.
    pipeline_proto, context = pipeline.to_runner_api(return_context=True)

    # Get all the root transforms. The caching transforms will be subtransforms
    # of one of these roots.
    roots = [root for root in pipeline_proto.root_transform_ids]

    (t, p) = self._required_components(
        pipeline_proto,
        roots + required_transform_ids,
        set(),
        follow_outputs=True,
        follow_inputs=True)

    def set_proto_map(proto_map, new_value):
      proto_map.clear()
      for key, value in new_value.items():
        proto_map[key].CopyFrom(value)

    # Copy the transforms into the new pipeline.
    pipeline_to_execute = beam_runner_api_pb2.Pipeline()
    pipeline_to_execute.root_transform_ids[:] = roots
    set_proto_map(pipeline_to_execute.components.transforms, t)
    set_proto_map(pipeline_to_execute.components.pcollections, p)
    set_proto_map(
        pipeline_to_execute.components.coders, context.to_runner_api().coders)
    set_proto_map(
        pipeline_to_execute.components.windowing_strategies,
        context.to_runner_api().windowing_strategies)

    # Cut out all subtransforms in the root that aren't the required transforms.
    for root_id in roots:
      root = pipeline_to_execute.components.transforms[root_id]
      root.subtransforms[:] = [
          transform_id for transform_id in root.subtransforms
          if transform_id in pipeline_to_execute.components.transforms
      ]

    return pipeline_to_execute

  def background_caching_pipeline_proto(self):
    """Returns the background caching pipeline.

    This method creates a background caching pipeline by: adding writes to cache
    from each unbounded source (done in the instrument method), and cutting out
    all components (transform, PCollections, coders, windowing strategies) that
    are not the unbounded sources or writes to cache (or subtransforms thereof).
    """
    # Create the pipeline_proto to read all the components from. It will later
    # create a new pipeline proto from the cut out components.
    pipeline_proto, context = self._background_caching_pipeline.to_runner_api(
        return_context=True)

    # Get all the sources we want to cache.
    sources = utils.unbounded_sources(self._background_caching_pipeline)

    # Get all the root transforms. The caching transforms will be subtransforms
    # of one of these roots.
    roots = [root for root in pipeline_proto.root_transform_ids]

    # Get the transform IDs of the caching transforms. These caching operations
    # are added the the _background_caching_pipeline in the instrument() method.
    # It's added there so that multiple calls to this method won't add multiple
    # caching operations (idempotent).
    transforms = pipeline_proto.components.transforms
    caching_transform_ids = [
        t_id for root in roots for t_id in transforms[root].subtransforms
        if WRITE_CACHE in t_id
    ]

    # Get the IDs of the unbounded sources.
    required_transform_labels = [src.full_label for src in sources]
    unbounded_source_ids = [
        k for k,
        v in transforms.items() if v.unique_name in required_transform_labels
    ]

    # The required transforms are the tranforms that we want to cut out of
    # the pipeline_proto and insert into a new pipeline to return.
    required_transform_ids = (
        roots + caching_transform_ids + unbounded_source_ids)
    (t, p) = self._required_components(
        pipeline_proto, required_transform_ids, set())

    def set_proto_map(proto_map, new_value):
      proto_map.clear()
      for key, value in new_value.items():
        proto_map[key].CopyFrom(value)

    # Copy the transforms into the new pipeline.
    pipeline_to_execute = beam_runner_api_pb2.Pipeline()
    pipeline_to_execute.root_transform_ids[:] = roots
    set_proto_map(pipeline_to_execute.components.transforms, t)
    set_proto_map(pipeline_to_execute.components.pcollections, p)
    set_proto_map(
        pipeline_to_execute.components.coders, context.to_runner_api().coders)
    set_proto_map(
        pipeline_to_execute.components.windowing_strategies,
        context.to_runner_api().windowing_strategies)

    # Cut out all subtransforms in the root that aren't the required transforms.
    for root_id in roots:
      root = pipeline_to_execute.components.transforms[root_id]
      root.subtransforms[:] = [
          transform_id for transform_id in root.subtransforms
          if transform_id in pipeline_to_execute.components.transforms
      ]

    return pipeline_to_execute

  @property
  def cacheables(self) -> Dict[str, Cacheable]:
    """Returns the Cacheables by PCollection ids.

    If you're already working with user defined pipelines and PCollections,
    do not build a PipelineInstrument just to get the cacheables. Instead,
    use apache_beam.runners.interactive.utils.cacheables.
    """
    return self._cacheables

  @property
  def has_unbounded_sources(self):
    """Returns whether the pipeline has any recordable sources.
    """
    return len(self._unbounded_sources) > 0

  @property
  def original_pipeline_proto(self):
    """Returns a snapshot of the pipeline proto before instrumentation."""
    return self._original_pipeline_proto

  @property
  def user_pipeline(self):
    """Returns a reference to the pipeline instance defined by the user. If a
    pipeline has no cacheable PCollection and the user pipeline cannot be
    found, return None indicating there is nothing to be cached in the user
    pipeline.

    The pipeline given for instrumenting and mutated in this class is not
    necessarily the pipeline instance defined by the user. From the watched
    scopes, this class figures out what the user pipeline instance is.
    This metadata can be used for tracking pipeline results.
    """
    return self._user_pipeline

  @property
  def runner_pcoll_to_user_pcoll(self):
    """Returns cacheable PCollections correlated from instances in the runner
    pipeline to instances in the user pipeline."""
    return self._runner_pcoll_to_user_pcoll

  def find_cacheables(self) -> Dict[str, Cacheable]:
    """Finds PCollections that need to be cached for analyzed pipeline.

    There might be multiple pipelines defined and watched, this will only find
    cacheables belong to the analyzed pipeline.
    """
    result = {}
    cacheables = utils.cacheables()
    for _, cacheable in cacheables.items():
      if cacheable.pcoll.pipeline is not self._user_pipeline:
        # Ignore all cacheables from other pipelines.
        continue
      pcoll_id = self.pcoll_id(cacheable.pcoll)
      if not pcoll_id:
        _LOGGER.debug(
            'Unable to retrieve PCollection id for %s. Ignored.',
            cacheable.pcoll)
        continue
      result[self.pcoll_id(cacheable.pcoll)] = cacheable
    return result

  def instrument(self):
    """Instruments original pipeline with cache.

    For cacheable output PCollection, if cache for the key doesn't exist, do
    _write_cache(); for cacheable input PCollection, if cache for the key
    exists, do _read_cache(). No instrument in any other situation.

    Modifies:
      self._pipeline
    """
    cacheable_inputs = set()
    all_inputs = set()
    all_outputs = set()
    unbounded_source_pcolls = set()

    class InstrumentVisitor(PipelineVisitor):
      """Visitor utilizes cache to instrument the pipeline."""
      def __init__(self, pin):
        self._pin = pin

      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        if isinstance(transform_node.transform,
                      tuple(ie.current_env().options.recordable_sources)):
          unbounded_source_pcolls.update(transform_node.outputs.values())
        cacheable_inputs.update(self._pin._cacheable_inputs(transform_node))
        ins, outs = self._pin._all_inputs_outputs(transform_node)
        all_inputs.update(ins)
        all_outputs.update(outs)

    v = InstrumentVisitor(self)
    self._pipeline.visit(v)
    # Every output PCollection that is never used as an input PCollection is
    # considered as a side effect of the pipeline run and should be included.
    self._extended_targets.update(all_outputs.difference(all_inputs))
    # Add the unbounded source PCollections to the cacheable inputs. This allows
    # for the caching of unbounded sources without a variable reference.
    cacheable_inputs.update(unbounded_source_pcolls)

    # Create ReadCache transforms.
    for cacheable_input in cacheable_inputs:
      self._read_cache(
          self._pipeline,
          cacheable_input,
          cacheable_input in unbounded_source_pcolls)
    # Replace/wire inputs w/ cached PCollections from ReadCache transforms.
    self._replace_with_cached_inputs(self._pipeline)

    # Write cache for all cacheables.
    for _, cacheable in self._cacheables.items():
      self._write_cache(
          self._pipeline, cacheable.pcoll, ignore_unbounded_reads=True)

    # Instrument the background caching pipeline if we can.
    if self.has_unbounded_sources:
      for source in self._unbounded_sources:
        self._write_cache(
            self._background_caching_pipeline,
            source.outputs[None],
            output_as_extended_target=False,
            is_capture=True)

      class TestStreamVisitor(PipelineVisitor):
        def __init__(self):
          self.test_stream = None

        def enter_composite_transform(self, transform_node):
          self.visit_transform(transform_node)

        def visit_transform(self, transform_node):
          if (self.test_stream is None and
              isinstance(transform_node.transform, test_stream.TestStream)):
            self.test_stream = transform_node.full_label

      v = TestStreamVisitor()
      self._pipeline.visit(v)
      pipeline_proto = self._pipeline.to_runner_api(return_context=False)
      test_stream_id = ''
      for t_id, t in pipeline_proto.components.transforms.items():
        if t.unique_name == v.test_stream:
          test_stream_id = t_id
          break
      self._pruned_pipeline_proto = self.prune_subgraph_for(
          self._pipeline, [test_stream_id])
      pruned_pipeline = beam.Pipeline.from_runner_api(
          proto=self._pruned_pipeline_proto,
          runner=self._pipeline.runner,
          options=self._pipeline._options)
      ie.current_env().add_derived_pipeline(self._pipeline, pruned_pipeline)
      self._pipeline = pruned_pipeline

  def preprocess(self):
    """Pre-processes the pipeline.

    Since the pipeline instance in the class might not be the same instance
    defined in the user code, the pre-process will figure out the relationship
    of cacheable PCollections between these 2 instances by replacing 'pcoll'
    fields in the cacheable dictionary with ones from the running instance.
    """
    class PreprocessVisitor(PipelineVisitor):
      def __init__(self, pin):
        self._pin = pin

      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        for in_pcoll in transform_node.inputs:
          self._process(in_pcoll)
        for out_pcoll in transform_node.outputs.values():
          self._process(out_pcoll)

      def _process(self, pcoll):
        pcoll_id = self._pin._pcoll_to_pcoll_id.get(str(pcoll), '')
        if pcoll_id in self._pin._cacheables:
          pcoll_id = self._pin.pcoll_id(pcoll)
          user_pcoll = self._pin._cacheables[pcoll_id].pcoll
          if (pcoll_id in self._pin._cacheables and user_pcoll != pcoll):
            self._pin._runner_pcoll_to_user_pcoll[pcoll] = user_pcoll
            self._pin._cacheables[pcoll_id].pcoll = pcoll

    v = PreprocessVisitor(self)
    self._pipeline.visit(v)

  def _write_cache(
      self,
      pipeline,
      pcoll,
      output_as_extended_target=True,
      ignore_unbounded_reads=False,
      is_capture=False):
    """Caches a cacheable PCollection.

    For the given PCollection, by appending sub transform part that materialize
    the PCollection through sink into cache implementation. The cache write is
    not immediate. It happens when the runner runs the transformed pipeline
    and thus not usable for this run as intended. This function always writes
    the cache for the given PCollection as long as the PCollection belongs to
    the pipeline being instrumented and the keyed cache is absent.

    Modifies:
      pipeline
    """
    # Makes sure the pcoll belongs to the pipeline being instrumented.
    if pcoll.pipeline is not pipeline:
      return

    # Ignore the unbounded reads from recordable sources as these will be pruned
    # out using the PipelineFragment later on.
    if ignore_unbounded_reads:
      ignore = False
      producer = pcoll.producer
      while producer:
        if isinstance(producer.transform,
                      tuple(ie.current_env().options.recordable_sources)):
          ignore = True
          break
        producer = producer.parent
      if ignore:
        self._ignored_targets.add(pcoll)
        return

    # The keyed cache is always valid within this instrumentation.
    key = self.cache_key(pcoll)
    # Only need to write when the cache with expected key doesn't exist.
    if not self._cache_manager.exists('full', key):
      self.cached_pcolls.add(self.runner_pcoll_to_user_pcoll.get(pcoll, pcoll))
      # Read the windowing information and cache it along with the element. This
      # caches the arguments to a WindowedValue object because Python has logic
      # that detects if a DoFn returns a WindowedValue. When it detecs one, it
      # puts the element into the correct window then emits the value to
      # downstream transforms.
      extended_target = reify_to_cache(
          pcoll=pcoll,
          cache_key=key,
          cache_manager=self._cache_manager,
          is_capture=is_capture)
      if output_as_extended_target:
        self._extended_targets.add(extended_target)

  def _read_cache(self, pipeline, pcoll, is_unbounded_source_output):
    """Reads a cached pvalue.

    A noop will cause the pipeline to execute the transform as
    it is and cache nothing from this transform for next run.

    Modifies:
      pipeline
    """
    # Makes sure the pcoll belongs to the pipeline being instrumented.
    if pcoll.pipeline is not pipeline:
      return
    # The keyed cache is always valid within this instrumentation.
    key = self.cache_key(pcoll)
    # Can only read from cache when the cache with expected key exists and its
    # computation has been completed.
    is_cached = self._cache_manager.exists('full', key)
    is_computed = (
        pcoll in self._runner_pcoll_to_user_pcoll and
        self._runner_pcoll_to_user_pcoll[pcoll] in
        ie.current_env().computed_pcollections)
    if ((is_cached and is_computed) or is_unbounded_source_output):
      if key not in self._cached_pcoll_read:
        # Mutates the pipeline with cache read transform attached
        # to root of the pipeline.

        # To put the cached value into the correct window, simply return a
        # WindowedValue constructed from the element.
        pcoll_from_cache = unreify_from_cache(
            pipeline=pipeline, cache_key=key, cache_manager=self._cache_manager)
        self._cached_pcoll_read[key] = pcoll_from_cache
    # else: NOOP when cache doesn't exist, just compute the original graph.

  def _replace_with_cached_inputs(self, pipeline):
    """Replace PCollection inputs in the pipeline with cache if possible.

    For any input PCollection, find out whether there is valid cache. If so,
    replace the input of the AppliedPTransform with output of the
    AppliedPtransform that sources pvalue from the cache. If there is no valid
    cache, noop.
    """

    # Find all cached unbounded PCollections.

    # If the pipeline has unbounded sources, then we want to force all cache
    # reads to go through the TestStream (even if they are bounded sources).
    if self.has_unbounded_sources:

      class CacheableUnboundedPCollectionVisitor(PipelineVisitor):
        def __init__(self, pin):
          self._pin = pin
          self.unbounded_pcolls = set()

        def enter_composite_transform(self, transform_node):
          self.visit_transform(transform_node)

        def visit_transform(self, transform_node):
          if transform_node.outputs:
            for output_pcoll in transform_node.outputs.values():
              key = self._pin.cache_key(output_pcoll)
              if key in self._pin._cached_pcoll_read:
                self.unbounded_pcolls.add(key)

          if transform_node.inputs:
            for input_pcoll in transform_node.inputs:
              key = self._pin.cache_key(input_pcoll)
              if key in self._pin._cached_pcoll_read:
                self.unbounded_pcolls.add(key)

      v = CacheableUnboundedPCollectionVisitor(self)
      pipeline.visit(v)

      # The set of keys from the cached unbounded PCollections will be used as
      # the output tags for the TestStream. This is to remember what cache-key
      # is associated with which PCollection.
      output_tags = v.unbounded_pcolls

      # Take the PCollections that will be read from the TestStream and insert
      # them back into the dictionary of cached PCollections. The next step will
      # replace the downstream consumer of the non-cached PCollections with
      # these PCollections.
      if output_tags:
        output_pcolls = pipeline | test_stream.TestStream(
            output_tags=output_tags, coder=self._cache_manager._default_pcoder)
        for tag, pcoll in output_pcolls.items():
          self._cached_pcoll_read[tag] = pcoll

    class ReadCacheWireVisitor(PipelineVisitor):
      """Visitor wires cache read as inputs to replace corresponding original
      input PCollections in pipeline.
      """
      def __init__(self, pin):
        """Initializes with a PipelineInstrument."""
        self._pin = pin

      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        if transform_node.inputs:
          main_inputs = dict(transform_node.main_inputs)
          for tag, input_pcoll in main_inputs.items():
            key = self._pin.cache_key(input_pcoll)

            # Replace the input pcollection with the cached pcollection (if it
            # has been cached).
            if key in self._pin._cached_pcoll_read:
              # Ignore this pcoll in the final pruned instrumented pipeline.
              self._pin._ignored_targets.add(input_pcoll)
              main_inputs[tag] = self._pin._cached_pcoll_read[key]
          # Update the transform with its new inputs.
          transform_node.main_inputs = main_inputs

    v = ReadCacheWireVisitor(self)
    pipeline.visit(v)

  def _cacheable_inputs(self, transform):
    inputs = set()
    for in_pcoll in transform.inputs:
      if self.pcoll_id(in_pcoll) in self._cacheables:
        inputs.add(in_pcoll)
    return inputs

  def _all_inputs_outputs(self, transform):
    inputs = set()
    outputs = set()
    for in_pcoll in transform.inputs:
      inputs.add(in_pcoll)
    for _, out_pcoll in transform.outputs.items():
      outputs.add(out_pcoll)
    return inputs, outputs

  def pcoll_id(self, pcoll):
    """Gets the PCollection id of the given pcoll.

    Returns '' if not found.
    """
    return self._pcoll_to_pcoll_id.get(str(pcoll), '')

  def cache_key(self, pcoll):
    """Gets the identifier of a cacheable PCollection in cache.

    If the pcoll is not a cacheable, return ''.
    This is only needed in pipeline instrument when the origin of given pcoll
    is unknown (whether it's from the user pipeline or a runner pipeline). If
    a pcoll is from the user pipeline, always use CacheKey.from_pcoll to build
    the key.
    The key is what the pcoll would use as identifier if it's materialized in
    cache. It doesn't mean that there would definitely be such cache already.
    Also, the pcoll can come from the original user defined pipeline object or
    an equivalent pcoll from a transformed copy of the original pipeline.
    """
    cacheable = self._cacheables.get(self.pcoll_id(pcoll), None)
    if cacheable:
      if cacheable.pcoll in self.runner_pcoll_to_user_pcoll:
        user_pcoll = self.runner_pcoll_to_user_pcoll[cacheable.pcoll]
      else:
        user_pcoll = cacheable.pcoll
      return CacheKey.from_pcoll(cacheable.var, user_pcoll).to_str()
    return ''


def build_pipeline_instrument(pipeline, options=None):
  """Creates PipelineInstrument for a pipeline and its options with cache.

  Throughout the process, the returned PipelineInstrument snapshots the given
  pipeline and then mutates the pipeline. It's invoked by interactive components
  such as the InteractiveRunner and the given pipeline should be implicitly
  created runner pipelines instead of pipeline instances defined by the user.

  This is the shorthand for doing 3 steps: 1) compute once for metadata of the
  given runner pipeline and everything watched from user pipelines; 2) associate
  info between the runner pipeline and its corresponding user pipeline,
  eliminate data from other user pipelines if there are any; 3) mutate the
  runner pipeline to apply interactivity.
  """
  pi = PipelineInstrument(pipeline, options)
  pi.preprocess()
  pi.instrument()  # Instruments the pipeline only once.
  return pi


def pcoll_to_pcoll_id(pipeline, original_context):
  """Returns a dict mapping PCollections string to PCollection IDs.

  Using a PipelineVisitor to iterate over every node in the pipeline,
  records the mapping from PCollections to PCollections IDs. This mapping
  will be used to query cached PCollections.

  Returns:
    (dict from str to str) a dict mapping str(pcoll) to pcoll_id.
  """
  class PCollVisitor(PipelineVisitor):
    """"A visitor that records input and output values to be replaced.

    Input and output values that should be updated are recorded in maps
    input_replacements and output_replacements respectively.

    We cannot update input and output values while visiting since that
    results in validation errors.
    """
    def __init__(self):
      self.pcoll_to_pcoll_id = {}

    def enter_composite_transform(self, transform_node):
      self.visit_transform(transform_node)

    def visit_transform(self, transform_node):
      for pcoll in transform_node.outputs.values():
        self.pcoll_to_pcoll_id[str(pcoll)] = (
            original_context.pcollections.get_id(pcoll))

  v = PCollVisitor()
  pipeline.visit(v)
  return v.pcoll_to_pcoll_id
