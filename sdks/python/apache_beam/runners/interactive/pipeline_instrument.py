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

from __future__ import absolute_import

import apache_beam as beam
from apache_beam.pipeline import PipelineVisitor
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.interactive import cache_manager as cache
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import pipeline_fragment as pf
from apache_beam.runners.interactive import background_caching_job
from apache_beam.testing import test_stream
from apache_beam.transforms.window import WindowedValue

READ_CACHE = "_ReadCache_"
WRITE_CACHE = "_WriteCache_"


# TODO: turn this into a dataclass object when we finally get off of Python2.
class Cacheable:
  def __init__(self, pcoll_id, var, version, pcoll, producer_version):
    self.pcoll_id = pcoll_id
    self.var = var
    self.version = version
    self.pcoll = pcoll
    self.producer_version = producer_version

  def __eq__(self, other):
    return (
        self.pcoll_id == other.pcoll_id and self.var == other.var and
        self.version == other.version and self.pcoll == other.pcoll and
        self.producer_version == other.producer_version)

  def __hash__(self):
    return hash((
        self.pcoll_id,
        self.var,
        self.version,
        self.pcoll,
        self.producer_version))


# TODO: turn this into a dataclass object when we finally get off of Python2.
class CacheKey:
  def __init__(self, var, version, producer_version, pipeline_id):
    self.var = var
    self.version = version
    self.producer_version = producer_version
    self.pipeline_id = pipeline_id

  @staticmethod
  def from_str(r):
    split = r.split('-')
    return CacheKey(split[0], split[1], split[2], split[3])

  def __repr__(self):
    return '-'.join(
        [self.var, self.version, self.producer_version, self.pipeline_id])


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
    # The cache manager per user-defined pipeline is lazily initiated the first
    # time accessed. It is owned by interactive_environment module. This
    # shortcut reference will be initialized when the user pipeline associated
    # to the given pipeline is identified.
    self._cache_manager = None

    # Invoke a round trip through the runner API. This makes sure the Pipeline
    # proto is stable. The snapshot of pipeline will not be mutated within this
    # module and can be used to recover original pipeline if needed.
    self._pipeline_snap = beam.pipeline.Pipeline.from_runner_api(
        pipeline.to_runner_api(use_fake_coders=True), pipeline.runner, options)

    self._background_caching_pipeline = beam.pipeline.Pipeline.from_runner_api(
        pipeline.to_runner_api(use_fake_coders=True), pipeline.runner, options)

    # Snapshot of original pipeline information.
    (self._original_pipeline_proto,
     self._original_context) = self._pipeline_snap.to_runner_api(
         return_context=True, use_fake_coders=True)

    # All compute-once-against-original-pipeline fields.
    self._unbounded_sources = unbounded_sources(
        self._background_caching_pipeline)
    # TODO(BEAM-7760): once cache scope changed, this is not needed to manage
    # relationships across pipelines, runners, and jobs.
    self._pcolls_to_pcoll_id = pcolls_to_pcoll_id(
        self._pipeline_snap, self._original_context)

    # A mapping from PCollection id to python id() value in user defined
    # pipeline instance.
    (
        self._pcoll_version_map,
        self._cacheables,
        # A dict from pcoll_id to variable name of the referenced PCollection.
        # (Dict[str, str])
        self._cacheable_var_by_pcoll_id) = cacheables(self.pcolls_to_pcoll_id)

    # A dict from cache key to PCollection that is read from cache.
    # If exists, caller should reuse the PCollection read. If not, caller
    # should create new transform and track the PCollection read from cache.
    # (Dict[str, AppliedPTransform]).
    self._cached_pcoll_read = {}

    # Reference to the user defined pipeline instance based on the given
    # pipeline. The class never mutates it.
    # Note: the original pipeline is not the user pipeline.
    self._user_pipeline = None

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

  def instrumented_pipeline_proto(self):
    """Always returns a new instance of portable instrumented proto."""
    targets = set(self._runner_pcoll_to_user_pcoll.keys())
    targets.update(self._extended_targets)
    targets = targets.difference(self._ignored_targets)
    if len(targets) > 0:
      # Prunes upstream transforms that don't contribute to the targets the
      # instrumented pipeline run cares.
      return pf.PipelineFragment(
          list(targets)).deduce_fragment().to_runner_api(use_fake_coders=True)
    return self._pipeline.to_runner_api(use_fake_coders=True)

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
    pipeline_proto, context = pipeline.to_runner_api(
        return_context=True, use_fake_coders=False)

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
        return_context=True, use_fake_coders=False)

    # Get all the sources we want to cache.
    sources = unbounded_sources(self._background_caching_pipeline)

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
  def has_unbounded_sources(self):
    """Returns whether the pipeline has any capturable sources.
    """
    return len(self._unbounded_sources) > 0

  @property
  def cacheables(self):
    """Finds cacheable PCollections from the pipeline.

    The function only treats the result as cacheables since there is no
    guarantee whether PCollections that need to be cached have been cached or
    not. A PCollection needs to be cached when it's bound to a user defined
    variable in the source code. Otherwise, the PCollection is not reusable
    nor introspectable which nullifies the need of cache.
    """
    return self._cacheables

  @property
  def pcolls_to_pcoll_id(self):
    """Returns a dict mapping str(PCollection)s to IDs."""
    return self._pcolls_to_pcoll_id

  @property
  def original_pipeline_proto(self):
    """Returns the portable proto representation of the pipeline before
    instrumentation."""
    return self._original_pipeline_proto

  @property
  def original_pipeline(self):
    """Returns a snapshot of the pipeline before instrumentation."""
    return self._pipeline_snap

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
                      tuple(ie.current_env().options.capturable_sources)):
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
    # Add the unbounded source pcollections to the cacheable inputs. This allows
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
    for _, cacheable in self.cacheables.items():
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
      pipeline_proto = self._pipeline.to_runner_api(
          return_context=False, use_fake_coders=True)
      test_stream_id = ''
      for t_id, t in pipeline_proto.components.transforms.items():
        if t.unique_name == v.test_stream:
          test_stream_id = t_id
          break
      self._pruned_pipeline_proto = self.prune_subgraph_for(
          self._pipeline, [test_stream_id])
      self._pipeline = beam.Pipeline.from_runner_api(
          proto=self._pruned_pipeline_proto,
          runner=self._pipeline.runner,
          options=self._pipeline._options)

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
        pcoll_id = self._pin.pcolls_to_pcoll_id.get(str(pcoll), '')
        if pcoll_id in self._pin._pcoll_version_map:
          cacheable_key = self._pin._cacheable_key(pcoll)
          user_pcoll = self._pin.cacheables[cacheable_key].pcoll
          if (cacheable_key in self._pin.cacheables and user_pcoll != pcoll):
            if not self._pin._user_pipeline:
              # Retrieve a reference to the user defined pipeline instance.
              self._pin._user_pipeline = user_pcoll.pipeline
              # Retrieve a reference to the cache manager for the user defined
              # pipeline instance.
              self._pin._cache_manager = ie.current_env().get_cache_manager(
                  self._pin._user_pipeline, create_if_absent=True)
              # Check if the user defined pipeline contains any source to cache.
              # If so, during the check, the cache manager is converted into a
              # streaming cache manager, thus re-assign the reference.
              if background_caching_job.has_source_to_cache(
                  self._pin._user_pipeline):
                self._pin._cache_manager = ie.current_env().get_cache_manager(
                    self._pin._user_pipeline)
            self._pin._runner_pcoll_to_user_pcoll[pcoll] = user_pcoll
            self._pin.cacheables[cacheable_key].pcoll = pcoll

    v = PreprocessVisitor(self)
    self._pipeline.visit(v)
    if not self._user_pipeline:
      self._user_pipeline = self._pipeline
      self._cache_manager = ie.current_env().get_cache_manager(
          self._user_pipeline, create_if_absent=True)

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

    # Ignore the unbounded reads from capturable sources as these will be pruned
    # out using the PipelineFragment later on.
    if ignore_unbounded_reads:
      ignore = False
      producer = pcoll.producer
      while producer:
        if isinstance(producer.transform,
                      tuple(ie.current_env().options.capturable_sources)):
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
      label = '{}{}'.format(WRITE_CACHE, key)

      # Read the windowing information and cache it along with the element. This
      # caches the arguments to a WindowedValue object because Python has logic
      # that detects if a DoFn returns a WindowedValue. When it detecs one, it
      # puts the element into the correct window then emits the value to
      # downstream transforms.
      class Reify(beam.DoFn):
        def process(
            self,
            e,
            w=beam.DoFn.WindowParam,
            p=beam.DoFn.PaneInfoParam,
            t=beam.DoFn.TimestampParam):
          yield test_stream.WindowedValueHolder(WindowedValue(e, t, [w], p))

      extended_target = (
          pcoll
          | label + 'reify' >> beam.ParDo(Reify())
          | label >> cache.WriteCache(
              self._cache_manager, key, is_capture=is_capture))
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
        class Unreify(beam.DoFn):
          def process(self, e):
            yield e.windowed_value

        pcoll_from_cache = (
            pipeline
            | '{}{}'.format(READ_CACHE, key) >> cache.ReadCache(
                self._cache_manager, key)
            | '{}{}unreify'.format(READ_CACHE, key) >> beam.ParDo(Unreify()))
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
          input_list = list(transform_node.inputs)
          for i, input_pcoll in enumerate(input_list):
            key = self._pin.cache_key(input_pcoll)

            # Replace the input pcollection with the cached pcollection (if it
            # has been cached).
            if key in self._pin._cached_pcoll_read:
              # Ignore this pcoll in the final pruned instrumented pipeline.
              self._pin._ignored_targets.add(input_pcoll)
              input_list[i] = self._pin._cached_pcoll_read[key]
          # Update the transform with its new inputs.
          transform_node.inputs = tuple(input_list)

    v = ReadCacheWireVisitor(self)
    pipeline.visit(v)

  def _cacheable_inputs(self, transform):
    inputs = set()
    for in_pcoll in transform.inputs:
      if self._cacheable_key(in_pcoll) in self.cacheables:
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

  def _cacheable_key(self, pcoll):
    """Gets the key a cacheable PCollection is tracked within the instrument."""
    return cacheable_key(
        pcoll, self.pcolls_to_pcoll_id, self._pcoll_version_map)

  def cache_key(self, pcoll):
    """Gets the identifier of a cacheable PCollection in cache.

    If the pcoll is not a cacheable, return ''.
    The key is what the pcoll would use as identifier if it's materialized in
    cache. It doesn't mean that there would definitely be such cache already.
    Also, the pcoll can come from the original user defined pipeline object or
    an equivalent pcoll from a transformed copy of the original pipeline.

    'pcoll_id' of cacheable is not stable for cache_key, thus not included in
    cache key. A combination of 'var', 'version' and 'producer_version' is
    sufficient to identify a cached PCollection.
    """
    cacheable = self.cacheables.get(self._cacheable_key(pcoll), None)
    if cacheable:
      if cacheable.pcoll in self.runner_pcoll_to_user_pcoll:
        user_pcoll = self.runner_pcoll_to_user_pcoll[cacheable.pcoll]
      else:
        user_pcoll = cacheable.pcoll

      return repr(
          CacheKey(
              cacheable.var,
              cacheable.version,
              cacheable.producer_version,
              str(id(user_pcoll.pipeline))))
    return ''

  def cacheable_var_by_pcoll_id(self, pcoll_id):
    """Retrieves the variable name of a PCollection.

    In source code, PCollection variables are defined in the user pipeline. When
    it's converted to the runner api representation, each PCollection referenced
    in the user pipeline is assigned a unique-within-pipeline pcoll_id. Given
    such pcoll_id, retrieves the str variable name defined in user pipeline for
    that referenced PCollection. If the PCollection is not watched, return None.
    """
    return self._cacheable_var_by_pcoll_id.get(pcoll_id, None)


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


def user_pipeline(pipeline):
  _, context = pipeline.to_runner_api(return_context=True)
  pcoll_ids = pcolls_to_pcoll_id(pipeline, context)

  for watching in ie.current_env().watching():
    for _, v in watching:
      # TODO(BEAM-8288): cleanup the attribute check when py2 is not supported.
      if hasattr(v, '__class__') and isinstance(v, beam.pvalue.PCollection):
        pcoll_id = pcoll_ids.get(str(v), None)
        if (pcoll_id in context.pcollections and
            context.pcollections[pcoll_id] != v):
          return v.pipeline
  return pipeline


def cacheables(pcolls_to_pcoll_id):
  """Finds PCollections that need to be cached for analyzed PCollections.

  The function only treats the result as cacheables since there is no guarantee
  whether PCollections that need to be cached have been cached or not. A
  PCollection needs to be cached when it's bound to a user defined variable in
  the source code. Otherwise, the PCollection is not reusable nor introspectable
  which nullifies the need of cache. There might be multiple pipelines defined
  and watched, this will only return for PCollections with pcolls_to_pcoll_id
  analyzed. The check is not strict because pcoll_id is not unique across
  multiple pipelines. Additional check needs to be done during instrument.
  """
  pcoll_version_map = {}
  cacheables = {}
  cacheable_var_by_pcoll_id = {}
  for watching in ie.current_env().watching():
    for key, val in watching:
      # TODO(BEAM-8288): cleanup the attribute check when py2 is not supported.
      if hasattr(val, '__class__') and isinstance(val, beam.pvalue.PCollection):
        cacheable = {}

        pcoll_id = pcolls_to_pcoll_id.get(str(val), None)
        # It's highly possible that PCollection str is not unique across
        # multiple pipelines, further check during instrument is needed.
        if not pcoll_id:
          continue

        cacheable = Cacheable(
            pcoll_id=pcoll_id,
            var=key,
            version=str(id(val)),
            pcoll=val,
            producer_version=str(id(val.producer)))
        pcoll_version_map[cacheable.pcoll_id] = cacheable.version
        cacheables[cacheable_key(val, pcolls_to_pcoll_id)] = cacheable
        cacheable_var_by_pcoll_id[cacheable.pcoll_id] = key

  return pcoll_version_map, cacheables, cacheable_var_by_pcoll_id


def cacheable_key(pcoll, pcolls_to_pcoll_id, pcoll_version_map=None):
  pcoll_version = str(id(pcoll))
  pcoll_id = pcolls_to_pcoll_id.get(str(pcoll), '')
  if pcoll_version_map:
    original_pipeline_pcoll_version = pcoll_version_map.get(pcoll_id, None)
    if original_pipeline_pcoll_version:
      pcoll_version = original_pipeline_pcoll_version
  return '_'.join((pcoll_version, pcoll_id))


def has_unbounded_sources(pipeline):
  """Checks if a given pipeline has capturable sources."""
  return len(unbounded_sources(pipeline)) > 0


def unbounded_sources(pipeline):
  """Returns a pipeline's capturable sources."""
  class CheckUnboundednessVisitor(PipelineVisitor):
    """Visitor checks if there are any unbounded read sources in the Pipeline.

    Visitor visits all nodes and checks if it is an instance of capturable
    sources.
    """
    def __init__(self):
      self.unbounded_sources = []

    def enter_composite_transform(self, transform_node):
      self.visit_transform(transform_node)

    def visit_transform(self, transform_node):
      if isinstance(transform_node.transform,
                    tuple(ie.current_env().options.capturable_sources)):
        self.unbounded_sources.append(transform_node)

  v = CheckUnboundednessVisitor()
  pipeline.visit(v)
  return v.unbounded_sources


def pcolls_to_pcoll_id(pipeline, original_context):
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
      self.pcolls_to_pcoll_id = {}

    def enter_composite_transform(self, transform_node):
      self.visit_transform(transform_node)

    def visit_transform(self, transform_node):
      for pcoll in transform_node.outputs.values():
        self.pcolls_to_pcoll_id[str(pcoll)] = (
            original_context.pcollections.get_id(pcoll))

  v = PCollVisitor()
  pipeline.visit(v)
  return v.pcolls_to_pcoll_id


def watch_sources(pipeline):
  """Watches the unbounded sources in the pipeline.

  Sources can output to a PCollection without a user variable reference. In
  this case the source is not cached. We still want to cache the data so we
  synthetically create a variable to the intermediate PCollection.
  """

  retrieved_user_pipeline = user_pipeline(pipeline)

  class CacheableUnboundedPCollectionVisitor(PipelineVisitor):
    def __init__(self):
      self.unbounded_pcolls = set()

    def enter_composite_transform(self, transform_node):
      self.visit_transform(transform_node)

    def visit_transform(self, transform_node):
      if isinstance(transform_node.transform,
                    tuple(ie.current_env().options.capturable_sources)):
        for pcoll in transform_node.outputs.values():
          ie.current_env().watch({'synthetic_var_' + str(id(pcoll)): pcoll})

  retrieved_user_pipeline.visit(CacheableUnboundedPCollectionVisitor())
