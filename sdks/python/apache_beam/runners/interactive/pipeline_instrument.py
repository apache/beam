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
from __future__ import absolute_import

import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.pipeline import PipelineVisitor
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.interactive import cache_manager as cache
from apache_beam.runners.interactive import interactive_environment as ie

READ_CACHE = "_ReadCache_"
WRITE_CACHE = "_WriteCache_"

# Use a tuple to define the list of unbounded sources. It is not always feasible
# to correctly find all the unbounded sources in the SDF world. This is
# because SDF allows the source to dynamically create sources at runtime.
REPLACEABLE_UNBOUNDED_SOURCES = (
    ReadFromPubSub,
)


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
    # The global cache manager is lazily initiated outside of this module by any
    # interactive runner so that its lifespan could cover multiple runs in
    # the interactive environment. Owned by interactive_environment module. Not
    # owned by this module.
    self._cache_manager = ie.current_env().cache_manager()

    # Invoke a round trip through the runner API. This makes sure the Pipeline
    # proto is stable. The snapshot of pipeline will not be mutated within this
    # module and can be used to recover original pipeline if needed.
    self._pipeline_snap = beam.pipeline.Pipeline.from_runner_api(
        pipeline.to_runner_api(use_fake_coders=True),
        pipeline.runner,
        options)

    self._background_caching_pipeline = beam.pipeline.Pipeline.from_runner_api(
        pipeline.to_runner_api(use_fake_coders=True),
        pipeline.runner,
        options)

    # Snapshot of original pipeline information.
    (self._original_pipeline_proto,
     self._original_context) = self._pipeline_snap.to_runner_api(
         return_context=True, use_fake_coders=True)

    # All compute-once-against-original-pipeline fields.
    self._unbounded_sources = unbounded_sources(
        self._background_caching_pipeline)
    # TODO(BEAM-7760): once cache scope changed, this is not needed to manage
    # relationships across pipelines, runners, and jobs.
    self._pcolls_to_pcoll_id = pcolls_to_pcoll_id(self._pipeline_snap,
                                                  self._original_context)

    # A mapping from PCollection id to python id() value in user defined
    # pipeline instance.
    (self._pcoll_version_map,
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

  def instrumented_pipeline_proto(self):
    """Always returns a new instance of portable instrumented proto."""
    return self._pipeline.to_runner_api(use_fake_coders=True)

  def _required_components(self, pipeline_proto, required_transforms_ids):
    """Returns the components and subcomponents of the given transforms.

    This method returns all the components (transforms, PCollections, coders,
    and windowing stratgies) related to the given transforms and to all of their
    subtransforms. This method accomplishes this recursively.
    """
    transforms = pipeline_proto.components.transforms
    pcollections = pipeline_proto.components.pcollections
    coders = pipeline_proto.components.coders
    windowing_strategies = pipeline_proto.components.windowing_strategies

    # Cache the transforms that will be copied into the new pipeline proto.
    required_transforms = {k: transforms[k] for k in required_transforms_ids}

    # Cache all the output PCollections of the transforms.
    pcollection_ids = [pc for t in required_transforms.values()
                       for pc in t.outputs.values()]
    required_pcollections = {pc_id: pcollections[pc_id]
                             for pc_id in pcollection_ids}

    # Cache all the PCollection coders.
    coder_ids = [pc.coder_id for pc in required_pcollections.values()]
    required_coders = {c_id: coders[c_id] for c_id in coder_ids}

    # Cache all the windowing strategy ids.
    windowing_strategies_ids = [pc.windowing_strategy_id
                                for pc in required_pcollections.values()]
    required_windowing_strategies = {ws_id: windowing_strategies[ws_id]
                                     for ws_id in windowing_strategies_ids}

    subtransforms = {}
    subpcollections = {}
    subcoders = {}
    subwindowing_strategies = {}

    # Recursively go through all the subtransforms and add their components.
    for transform_id, transform in required_transforms.items():
      if transform_id in pipeline_proto.root_transform_ids:
        continue
      (t, pc, c, ws) = self._required_components(pipeline_proto,
                                                 transform.subtransforms)
      subtransforms.update(t)
      subpcollections.update(pc)
      subcoders.update(c)
      subwindowing_strategies.update(ws)

    # Now we got all the components and their subcomponents, so return the
    # complete collection.
    required_transforms.update(subtransforms)
    required_pcollections.update(subpcollections)
    required_coders.update(subcoders)
    required_windowing_strategies.update(subwindowing_strategies)

    return (required_transforms, required_pcollections, required_coders,
            required_windowing_strategies)

  def background_caching_pipeline_proto(self):
    """Returns the background caching pipeline.

    This method creates a background caching pipeline by: adding writes to cache
    from each unbounded source (done in the instrument method), and cutting out
    all components (transform, PCollections, coders, windowing strategies) that
    are not the unbounded sources or writes to cache (or subtransforms thereof).
    """
    # Create the pipeline_proto to read all the components from. It will later
    # create a new pipeline proto from the cut out components.
    pipeline_proto = self._background_caching_pipeline.to_runner_api(
        return_context=False, use_fake_coders=True)

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
    caching_transform_ids = [t_id for root in roots
                             for t_id in transforms[root].subtransforms
                             if WRITE_CACHE in t_id]

    # Get the IDs of the unbounded sources.
    required_transform_labels = [src.full_label for src in sources]
    unbounded_source_ids = [k for k, v in transforms.items()
                            if v.unique_name in required_transform_labels]

    # The required transforms are the tranforms that we want to cut out of
    # the pipeline_proto and insert into a new pipeline to return.
    required_transform_ids = (roots + caching_transform_ids +
                              unbounded_source_ids)
    (t, p, c, w) = self._required_components(pipeline_proto,
                                             required_transform_ids)

    def set_proto_map(proto_map, new_value):
      proto_map.clear()
      for key, value in new_value.items():
        proto_map[key].CopyFrom(value)

    # Copy the transforms into the new pipeline.
    pipeline_to_execute = beam_runner_api_pb2.Pipeline()
    pipeline_to_execute.root_transform_ids[:] = roots
    set_proto_map(pipeline_to_execute.components.transforms, t)
    set_proto_map(pipeline_to_execute.components.pcollections, p)
    set_proto_map(pipeline_to_execute.components.coders, c)
    set_proto_map(pipeline_to_execute.components.windowing_strategies, w)

    # Cut out all subtransforms in the root that aren't the required transforms.
    for root_id in roots:
      root = pipeline_to_execute.components.transforms[root_id]
      root.subtransforms[:] = [
          transform_id for transform_id in root.subtransforms
          if transform_id in pipeline_to_execute.components.transforms]

    return pipeline_to_execute

  @property
  def has_unbounded_sources(self):
    """Returns whether the pipeline has any `REPLACEABLE_UNBOUNDED_SOURCES`.
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

  def instrument(self):
    """Instruments original pipeline with cache.

    For cacheable output PCollection, if cache for the key doesn't exist, do
    _write_cache(); for cacheable input PCollection, if cache for the key
    exists, do _read_cache(). No instrument in any other situation.

    Modifies:
      self._pipeline
    """
    cacheable_inputs = set()

    class InstrumentVisitor(PipelineVisitor):
      """Visitor utilizes cache to instrument the pipeline."""

      def __init__(self, pin):
        self._pin = pin

      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        cacheable_inputs.update(self._pin._cacheable_inputs(transform_node))

    v = InstrumentVisitor(self)
    self._pipeline.visit(v)
    # Create ReadCache transforms.
    for cacheable_input in cacheable_inputs:
      self._read_cache(self._pipeline, cacheable_input)
    # Replace/wire inputs w/ cached PCollections from ReadCache transforms.
    self._replace_with_cached_inputs(self._pipeline)
    # Write cache for all cacheables.
    for _, cacheable in self.cacheables.items():
      self._write_cache(self._pipeline, cacheable['pcoll'])

    # Instrument the background caching pipeline if we can.
    if self.has_unbounded_sources:
      for source in self._unbounded_sources:
        self._write_cache(self._background_caching_pipeline,
                          source.outputs[None])

    # TODO(BEAM-7760): prune sub graphs that doesn't need to be executed.

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
          if (cacheable_key in self._pin.cacheables and
              self._pin.cacheables[cacheable_key]['pcoll'] != pcoll):
            if not self._pin._user_pipeline:
              # Retrieve a reference to the user defined pipeline instance.
              self._pin._user_pipeline = self._pin.cacheables[cacheable_key][
                  'pcoll'].pipeline
            self._pin.cacheables[cacheable_key]['pcoll'] = pcoll

    v = PreprocessVisitor(self)
    self._pipeline.visit(v)

  def _write_cache(self, pipeline, pcoll):
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
    # The keyed cache is always valid within this instrumentation.
    key = self.cache_key(pcoll)
    # Only need to write when the cache with expected key doesn't exist.
    if not self._cache_manager.exists('full', key):
      label = '{}{}'.format(WRITE_CACHE, key)
      _ = pcoll | label >> cache.WriteCache(self._cache_manager, key)

  def _read_cache(self, pipeline, pcoll):
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
    # Can only read from cache when the cache with expected key exists.
    if self._cache_manager.exists('full', key):
      if key not in self._cached_pcoll_read:
        # Mutates the pipeline with cache read transform attached
        # to root of the pipeline.
        pcoll_from_cache = (
            pipeline
            | '{}{}'.format(READ_CACHE, key) >> cache.ReadCache(
                self._cache_manager, key))
        self._cached_pcoll_read[key] = pcoll_from_cache
    # else: NOOP when cache doesn't exist, just compute the original graph.

  def _replace_with_cached_inputs(self, pipeline):
    """Replace PCollection inputs in the pipeline with cache if possible.

    For any input PCollection, find out whether there is valid cache. If so,
    replace the input of the AppliedPTransform with output of the
    AppliedPtransform that sources pvalue from the cache. If there is no valid
    cache, noop.
    """

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
          for i in range(len(input_list)):
            key = self._pin.cache_key(input_list[i])
            if key in self._pin._cached_pcoll_read:
              input_list[i] = self._pin._cached_pcoll_read[key]
          transform_node.inputs = tuple(input_list)

    v = ReadCacheWireVisitor(self)
    pipeline.visit(v)

  def _cacheable_inputs(self, transform):
    inputs = set()
    for in_pcoll in transform.inputs:
      if self._cacheable_key(in_pcoll) in self.cacheables:
        inputs.add(in_pcoll)
    return inputs

  def _cacheable_key(self, pcoll):
    """Gets the key a cacheable PCollection is tracked within the instrument."""
    return cacheable_key(pcoll, self.pcolls_to_pcoll_id,
                         self._pcoll_version_map)

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
      return '_'.join((cacheable['var'],
                       cacheable['version'],
                       cacheable['producer_version']))
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


def pin(pipeline, options=None):
  """Creates PipelineInstrument for a pipeline and its options with cache.

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
        cacheable['pcoll_id'] = pcolls_to_pcoll_id.get(str(val), None)
        # It's highly possible that PCollection str is not unique across
        # multiple pipelines, further check during instrument is needed.
        if not cacheable['pcoll_id']:
          continue
        cacheable['var'] = key
        cacheable['version'] = str(id(val))
        cacheable['pcoll'] = val
        cacheable['producer_version'] = str(id(val.producer))
        pcoll_version_map[cacheable['pcoll_id']] = cacheable['version']
        cacheables[cacheable_key(val, pcolls_to_pcoll_id)] = cacheable
        cacheable_var_by_pcoll_id[cacheable['pcoll_id']] = key

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
  """Checks if a given pipeline has replaceable unbounded sources."""
  return len(unbounded_sources(pipeline)) > 0


def unbounded_sources(pipeline):
  """Returns a pipeline's replaceable unbounded sources."""

  class CheckUnboundednessVisitor(PipelineVisitor):
    """Visitor checks if there are any unbounded read sources in the Pipeline.

    Visitor visits all nodes and checks if it is an instance of
    `REPLACEABLE_UNBOUNDED_SOURCES`.
    """

    def __init__(self):
      self.unbounded_sources = []

    def enter_composite_transform(self, transform_node):
      self.visit_transform(transform_node)

    def visit_transform(self, transform_node):
      if isinstance(transform_node.transform, REPLACEABLE_UNBOUNDED_SOURCES):
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
