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

"""Analyzes and modifies the pipeline that utilize the PCollection cache.

This module is experimental. No backwards-compatibility guarantees.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections

import apache_beam as beam
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.interactive import cache_manager as cache


class PipelineAnalyzer(object):
  def __init__(self, cache_manager, pipeline_proto, underlying_runner,
               options=None, desired_cache_labels=None):
    """Constructor of PipelineAnanlyzer.

    Args:
      cache_manager: (CacheManager)
      pipeline_proto: (Pipeline proto)
      underlying_runner: (PipelineRunner)
      options: (PipelineOptions)
      desired_cache_labels: (Set[str]) a set of labels of the PCollection
        queried by the user.
    """
    self._cache_manager = cache_manager
    self._pipeline_proto = pipeline_proto
    self._desired_cache_labels = desired_cache_labels or []

    self._pipeline = beam.pipeline.Pipeline.from_runner_api(
        self._pipeline_proto,
        runner=underlying_runner,
        options=options)
    # context returned from to_runner_api is more informative than that returned
    # from from_runner_api.
    _, self._context = self._pipeline.to_runner_api(
        return_context=True, use_fake_coders=True)
    self._pipeline_info = PipelineInfo(self._pipeline_proto.components)

    # Result of the analysis that can be queried by the user.
    self._pipeline_proto_to_execute = None
    self._top_level_referenced_pcoll_ids = None
    self._top_level_required_transforms = None

    self._caches_used = set()
    self._read_cache_ids = set()
    self._write_cache_ids = set()

    # used for _insert_producing_transforms()
    self._analyzed_pcoll_ids = set()

    self._analyze_pipeline()

  def _analyze_pipeline(self):
    """Analyzes the pipeline and sets the variables that can be queried.

    This function construct Pipeline proto to execute by
      1. Start from target PCollections and recursively insert the producing
         PTransforms of those PCollections, where the producing PTransforms are
         either ReadCache or PTransforms in the original pipeline.
      2. Append WriteCache PTransforsm in the pipeline.

    After running this function, the following variables will be set:
      self._pipeline_proto_to_execute
      self._top_level_referenced_pcoll_ids
      self._top_level_required_transforms
      self._caches_used
      self._read_cache_ids
      self._write_cache_ids
    """
    # We filter PTransforms to be executed bottom-up from these PCollections.
    desired_pcollections = self._desired_pcollections(self._pipeline_info)

    required_transforms = collections.OrderedDict()
    top_level_required_transforms = collections.OrderedDict()

    for pcoll_id in desired_pcollections:
      # TODO(qinyeli): Collections consumed by no-output transforms.
      self._insert_producing_transforms(pcoll_id,
                                        required_transforms,
                                        top_level_required_transforms)

    top_level_referenced_pcoll_ids = self._referenced_pcoll_ids(
        top_level_required_transforms)

    for pcoll_id in self._pipeline_info.all_pcollections():
      if not pcoll_id in top_level_referenced_pcoll_ids:
        continue

      if (pcoll_id in desired_pcollections
          and not pcoll_id in self._caches_used):
        self._insert_caching_transforms(pcoll_id,
                                        required_transforms,
                                        top_level_required_transforms)

      if not self._cache_manager.exists(
          'sample', self._pipeline_info.cache_label(pcoll_id)):
        self._insert_caching_transforms(pcoll_id,
                                        required_transforms,
                                        top_level_required_transforms,
                                        sample=True)

    required_transforms['_root'] = beam_runner_api_pb2.PTransform(
        subtransforms=list(top_level_required_transforms))

    referenced_pcoll_ids = self._referenced_pcoll_ids(
        required_transforms)
    referenced_pcollections = {}
    for pcoll_id in referenced_pcoll_ids:
      obj = self._context.pcollections.get_by_id(pcoll_id)
      proto = self._context.pcollections.get_proto(obj)
      referenced_pcollections[pcoll_id] = proto

    pipeline_to_execute = beam_runner_api_pb2.Pipeline()
    pipeline_to_execute.root_transform_ids[:] = ['_root']
    set_proto_map(pipeline_to_execute.components.transforms,
                  required_transforms)
    set_proto_map(pipeline_to_execute.components.pcollections,
                  referenced_pcollections)
    set_proto_map(pipeline_to_execute.components.coders,
                  self._context.to_runner_api().coders)
    set_proto_map(pipeline_to_execute.components.windowing_strategies,
                  self._context.to_runner_api().windowing_strategies)

    self._pipeline_proto_to_execute = pipeline_to_execute
    self._top_level_referenced_pcoll_ids = top_level_referenced_pcoll_ids
    self._top_level_required_transforms = top_level_required_transforms

  # -------------------------------------------------------------------------- #
  # Getters
  # -------------------------------------------------------------------------- #

  def pipeline_info(self):
    """Return PipelineInfo of the original pipeline.
    """
    return self._pipeline_info

  def pipeline_proto_to_execute(self):
    """Returns Pipeline proto to be executed.
    """
    return self._pipeline_proto_to_execute

  def tl_referenced_pcoll_ids(self):
    """Returns a set of PCollection IDs referenced by top level PTransforms.
    """
    return self._top_level_referenced_pcoll_ids

  def tl_required_trans_ids(self):
    """Returns a set of required top level PTransform IDs.
    """
    return list(self._top_level_required_transforms)

  def caches_used(self):
    """Returns a set of PCollection IDs to read from cache.
    """
    return self._caches_used

  def read_cache_ids(self):
    """Return a set of ReadCache PTransform IDs inserted.
    """
    return self._read_cache_ids

  def write_cache_ids(self):
    """Return a set of WriteCache PTransform IDs inserted.
    """
    return self._write_cache_ids

  # -------------------------------------------------------------------------- #
  # Helper methods for _analyze_pipeline()
  # -------------------------------------------------------------------------- #

  def _insert_producing_transforms(self,
                                   pcoll_id,
                                   required_transforms,
                                   top_level_required_transforms,
                                   leaf=False):
    """Inserts PTransforms producing the given PCollection into the dicts.

    Args:
      pcoll_id: (str)
      required_transforms: (Dict[str, PTransform proto])
      top_level_required_transforms: (Dict[str, PTransform proto])
      leaf: (bool) whether the PCollection should be read from cache if the
        cache exists.

    Modifies:
      required_transforms
      top_level_required_transforms
      self._read_cache_ids
    """
    if pcoll_id in self._analyzed_pcoll_ids:
      return
    else:
      self._analyzed_pcoll_ids.add(pcoll_id)

    cache_label = self._pipeline_info.cache_label(pcoll_id)
    if self._cache_manager.exists('full', cache_label) and not leaf:
      self._caches_used.add(pcoll_id)

      cache_label = self._pipeline_info.cache_label(pcoll_id)
      dummy_pcoll = (self._pipeline
                     | 'Load%s' % cache_label >> cache.ReadCache(
                         self._cache_manager, cache_label))

      read_cache = self._top_level_producer(dummy_pcoll)
      read_cache_id = self._context.transforms.get_id(read_cache)
      read_cache_proto = read_cache.to_runner_api(self._context)
      read_cache_proto.outputs['None'] = pcoll_id
      top_level_required_transforms[read_cache_id] = read_cache_proto
      self._read_cache_ids.add(read_cache_id)

      for transform in self._include_subtransforms(read_cache):
        transform_id = self._context.transforms.get_id(transform)
        transform_proto = transform.to_runner_api(self._context)
        if dummy_pcoll in transform.outputs.values():
          transform_proto.outputs['None'] = pcoll_id
        required_transforms[transform_id] = transform_proto

    else:
      pcoll = self._context.pcollections.get_by_id(pcoll_id)

      top_level_transform = self._top_level_producer(pcoll)
      for transform in self._include_subtransforms(top_level_transform):
        transform_id = self._context.transforms.get_id(transform)
        transform_proto = self._context.transforms.get_proto(transform)

        # Inserting ancestor PTransforms.
        for input_id in transform_proto.inputs.values():
          self._insert_producing_transforms(input_id,
                                            required_transforms,
                                            top_level_required_transforms)
        required_transforms[transform_id] = transform_proto

      # Must be inserted after inserting ancestor PTransforms.
      top_level_id = self._context.transforms.get_id(top_level_transform)
      top_level_proto = self._context.transforms.get_proto(top_level_transform)
      top_level_required_transforms[top_level_id] = top_level_proto

  def _insert_caching_transforms(self,
                                 pcoll_id,
                                 required_transforms,
                                 top_level_required_transforms,
                                 sample=False):
    """Inserts PTransforms caching the given PCollection into the dicts.

    Args:
      pcoll_id: (str)
      required_transforms: (Dict[str, PTransform proto])
      top_level_required_transforms: (Dict[str, PTransform proto])
      sample: (bool) whether to cache sample or cache full.

    Modifies:
      required_transforms
      top_level_required_transforms
      self._write_cache_ids
    """
    cache_label = self._pipeline_info.cache_label(pcoll_id)
    pcoll = self._context.pcollections.get_by_id(pcoll_id)

    if not sample:
      pdone = pcoll | 'CacheFull%s' % cache_label >> cache.WriteCache(
          self._cache_manager, cache_label)
    else:
      pdone = pcoll | 'CacheSample%s' % cache_label >> cache.WriteCache(
          self._cache_manager, cache_label, sample=True,
          sample_size=10)

    write_cache = self._top_level_producer(pdone)
    write_cache_id = self._context.transforms.get_id(write_cache)
    write_cache_proto = write_cache.to_runner_api(self._context)
    top_level_required_transforms[write_cache_id] = write_cache_proto
    self._write_cache_ids.add(write_cache_id)

    for transform in self._include_subtransforms(write_cache):
      transform_id = self._context.transforms.get_id(transform)
      transform_proto = transform.to_runner_api(self._context)
      required_transforms[transform_id] = transform_proto

  def _desired_pcollections(self, pipeline_info):
    """Returns IDs of desired (queried or leaf) PCollections.

    Args:
      pipeline_info: (PipelineInfo) info of the original pipeline.

    Returns:
      (Set[str]) a set of PCollections IDs of either leaf PCollections or
      PCollections referenced by the user. These PCollections should be cached
      at the end of pipeline execution.
    """
    desired_pcollections = set(pipeline_info.leaf_pcollections())
    for pcoll_id in pipeline_info.all_pcollections():
      cache_label = pipeline_info.cache_label(pcoll_id)

      if cache_label in self._desired_cache_labels:
        desired_pcollections.add(pcoll_id)
    return desired_pcollections

  def _referenced_pcoll_ids(self, required_transforms):
    """Returns PCollection IDs referenced in the given transforms.

    Args:
      transforms: (Dict[str, PTransform proto]) mapping ID to protos.

    Returns:
      (Set[str]) PCollection IDs referenced as either input or output in the
        given transforms.
    """
    referenced_pcoll_ids = set()
    for transform_proto in required_transforms.values():
      for pcoll_id in transform_proto.inputs.values():
        referenced_pcoll_ids.add(pcoll_id)

      for pcoll_id in transform_proto.outputs.values():
        referenced_pcoll_ids.add(pcoll_id)

    return referenced_pcoll_ids

  def _top_level_producer(self, pcoll):
    """Given a PCollection, returns the top level producing PTransform.

    Args:
      pcoll: (PCollection)

    Returns:
      (PTransform) top level producing PTransform of pcoll.
    """
    top_level_transform = pcoll.producer
    while top_level_transform.parent.parent:
      top_level_transform = top_level_transform.parent
    return top_level_transform

  def _include_subtransforms(self, transform):
    """Depth-first yield the PTransform itself and its sub transforms.

    Args:
      transform: (PTransform)

    Yields:
      The input PTransform itself and all its sub transforms.
    """
    yield transform
    for subtransform in transform.parts[::-1]:
      for yielded in self._include_subtransforms(subtransform):
        yield yielded


class PipelineInfo(object):
  """Provides access to pipeline metadata."""

  def __init__(self, proto):
    self._proto = proto
    self._producers = {}
    self._consumers = collections.defaultdict(list)
    for transform_id, transform_proto in self._proto.transforms.items():
      if transform_proto.subtransforms:
        continue
      for tag, pcoll_id in transform_proto.outputs.items():
        self._producers[pcoll_id] = transform_id, tag
      for pcoll_id in transform_proto.inputs.values():
        self._consumers[pcoll_id].append(transform_id)
    self._derivations = {}

  def all_pcollections(self):
    return self._proto.pcollections.keys()

  def leaf_pcollections(self):
    for pcoll_id in self._proto.pcollections:
      if not self._consumers[pcoll_id]:
        yield pcoll_id

  def producer(self, pcoll_id):
    return self._producers[pcoll_id]

  def cache_label(self, pcoll_id):
    """Returns the cache label given the PCollection ID."""
    return self._derivation(pcoll_id).cache_label()

  def _derivation(self, pcoll_id):
    if pcoll_id not in self._derivations:
      transform_id, output_tag = self._producers[pcoll_id]
      transform_proto = self._proto.transforms[transform_id]
      self._derivations[pcoll_id] = self.Derivation({
          input_tag: self._derivation(input_id)
          for input_tag, input_id in transform_proto.inputs.items()
      }, transform_proto, output_tag)
    return self._derivations[pcoll_id]

  class Derivation(object):
    """Records derivation info of a PCollection. Helper for PipelineInfo."""

    def __init__(self, inputs, transform_proto, output_tag):
      """Constructor of Derivation.

      Args:
        inputs: (Dict[str, Derivation]) maps PCollection names to Derivations.
        transform_proto: (Transform proto) the producing PTransform.
        output_tag: (str) local name of the PCollection in analysis.
      """
      self._inputs = inputs
      self._transform_info = {
          # TODO(qinyeli): remove name field when collision is resolved.
          'name': transform_proto.unique_name,
          'urn': transform_proto.spec.urn,
          'payload': transform_proto.spec.payload.decode('latin1')
      }
      self._output_tag = output_tag
      self._hash = None

    def __eq__(self, other):
      if isinstance(other, self.Derivation):
        # pylint: disable=protected-access
        return (self._inputs == other._inputs and
                self._transform_info == other._transform_info)

    def __ne__(self, other):
      # TODO(BEAM-5949): Needed for Python 2 compatibility.
      return not self == other

    def __hash__(self):
      if self._hash is None:
        self._hash = (hash(tuple(sorted(self._transform_info.items())))
                      + sum(hash(tag) * hash(input)
                            for tag, input in self._inputs.items())
                      + hash(self._output_tag))
      return self._hash

    def cache_label(self):
      # TODO(qinyeli): Collision resistance?
      return 'Pcoll-%x' % abs(hash(self))

    def json(self):
      return {
          'inputs': self._inputs,
          'transform': self._transform_info,
          'output_tag': self._output_tag
      }

    def __repr__(self):
      return str(self.json())


# TODO(qinyeli) move to proto_utils
def set_proto_map(proto_map, new_value):
  proto_map.clear()
  for key, value in new_value.items():
    proto_map[key].CopyFrom(value)
