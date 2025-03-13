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

"""Module to write cache for PCollections being computed.

For internal use only; no backward-compatibility guarantees.
"""
# pytype: skip-file

from typing import Tuple

import apache_beam as beam
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.interactive import cache_manager as cache
from apache_beam.runners.interactive.caching.cacheable import Cacheable
from apache_beam.runners.interactive.caching.reify import reify_to_cache
from apache_beam.runners.pipeline_context import PipelineContext
from apache_beam.transforms.ptransform import PTransform


class WriteCache:
  """Class that facilitates writing cache for PCollections being computed.
  """
  def __init__(
      self,
      pipeline: beam_runner_api_pb2.Pipeline,
      context: PipelineContext,
      cache_manager: cache.CacheManager,
      cacheable: Cacheable):
    self._pipeline = pipeline
    self._context = context
    self._cache_manager = cache_manager
    self._cacheable = cacheable
    self._key = cacheable.to_key().to_str()

  def write_cache(self) -> None:
    """Writes cache for the cacheable PCollection that is being computed.

    First, it creates a temporary pipeline instance on top of the existing
    component_id_map from self._pipeline's context so that both pipelines
    share the context and have no conflict component ids.
    Second, it creates a _PCollectionPlaceHolder in the temporary pipeline that
    mimics the attributes of the cacheable PCollection to be written into cache.
    It also marks all components in the current temporary pipeline as
    ignorable when later copying components to self._pipeline.
    Third, it instantiates a _WriteCacheTransform that uses the
    _PCollectionPlaceHolder as the input. This adds a subgraph under top level
    transforms that writes the _PCollectionPlaceHolder into cache.
    Fourth, it copies components of the subgraph from the temporary pipeline to
    self._pipeline, skipping components that are ignored in the temporary
    pipeline and components that are not in the temporary pipeline but presents
    in the component_id_map of self._pipeline.
    Last, it replaces inputs of all transforms that consume the
    _PCollectionPlaceHolder with the cacheable PCollection to be written to
    cache.
    """
    template, write_input_placeholder = self._build_runner_api_template()
    input_placeholder_id = self._context.pcollections.get_id(
        write_input_placeholder.placeholder_pcoll)
    input_id = self._context.pcollections.get_id(self._cacheable.pcoll)

    # Copy cache writing subgraph from the template to the pipeline proto.
    for pcoll_id in template.components.pcollections:
      if (pcoll_id in self._pipeline.components.pcollections or
          pcoll_id in write_input_placeholder.ignorable_components.pcollections
          ):
        continue
      self._pipeline.components.pcollections[pcoll_id].CopyFrom(
          template.components.pcollections[pcoll_id])
    for coder_id in template.components.coders:
      if (coder_id in self._pipeline.components.coders or
          coder_id in write_input_placeholder.ignorable_components.coders):
        continue
      self._pipeline.components.coders[coder_id].CopyFrom(
          template.components.coders[coder_id])
    for windowing_strategy_id in template.components.windowing_strategies:
      if (windowing_strategy_id in
          self._pipeline.components.windowing_strategies or
          windowing_strategy_id in
          write_input_placeholder.ignorable_components.windowing_strategies):
        continue
      self._pipeline.components.windowing_strategies[
          windowing_strategy_id].CopyFrom(
              template.components.windowing_strategies[windowing_strategy_id])
    template_root_transform_id = template.root_transform_ids[0]
    root_transform_id = self._pipeline.root_transform_ids[0]
    for transform_id in template.components.transforms:
      if (transform_id in self._pipeline.components.transforms or transform_id
          in write_input_placeholder.ignorable_components.transforms):
        continue
      self._pipeline.components.transforms[transform_id].CopyFrom(
          template.components.transforms[transform_id])
    for top_level_transform in template.components.transforms[
        template_root_transform_id].subtransforms:
      if (top_level_transform in
          write_input_placeholder.ignorable_components.transforms):
        continue
      self._pipeline.components.transforms[
          root_transform_id].subtransforms.append(top_level_transform)

    # Replace all the input pcoll of input_placeholder_id from cache writing
    # with cacheable pcoll of input_id.
    for transform in self._pipeline.components.transforms.values():
      inputs = transform.inputs
      if input_placeholder_id in inputs.values():
        keys_need_replacement = set()
        for key in inputs:
          if inputs[key] == input_placeholder_id:
            keys_need_replacement.add(key)
        for key in keys_need_replacement:
          inputs[key] = input_id

  def _build_runner_api_template(
      self) -> Tuple[beam_runner_api_pb2.Pipeline, '_PCollectionPlaceHolder']:
    pph = _PCollectionPlaceHolder(self._cacheable.pcoll, self._context)
    transform = _WriteCacheTransform(self._cache_manager, self._key)
    _ = pph.placeholder_pcoll | 'sink_cache_' + self._key >> transform
    return pph.placeholder_pcoll.pipeline.to_runner_api(), pph


class _WriteCacheTransform(PTransform):
  """A composite transform encapsulates writing cache for PCollections.
  """
  def __init__(self, cache_manager: cache.CacheManager, key: str):
    self._cache_manager = cache_manager
    self._key = key

  def expand(self, pcoll: beam.pvalue.PCollection) -> beam.pvalue.PValue:
    return reify_to_cache(
        pcoll=pcoll, cache_key=self._key, cache_manager=self._cache_manager)


class _PCollectionPlaceHolder:
  """A placeholder as an input to the cache writing transform.
  """
  def __init__(self, pcoll: beam.pvalue.PCollection, context: PipelineContext):
    tmp_pipeline = beam.Pipeline()
    tmp_pipeline.component_id_map = context.component_id_map
    self._input_placeholder = tmp_pipeline | 'CreatePInput' >> beam.Create(
        [], reshuffle=False)
    self._input_placeholder.tag = pcoll.tag
    self._input_placeholder.element_type = pcoll.element_type
    self._input_placeholder.is_bounded = pcoll.is_bounded
    self._input_placeholder._windowing = pcoll.windowing
    self._ignorable_components = tmp_pipeline.to_runner_api().components

  @property
  def placeholder_pcoll(self) -> beam.pvalue.PCollection:
    return self._input_placeholder

  @property
  def ignorable_components(self) -> beam_runner_api_pb2.Components:
    """Subgraph generated by the placeholder that can be ignored in the final
    pipeline proto.
    """
    return self._ignorable_components
