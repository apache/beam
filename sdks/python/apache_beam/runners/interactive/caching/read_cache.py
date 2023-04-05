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

"""Module to read cache of computed PCollections.

For internal use only; no backward-compatibility guarantees.
"""
# pytype: skip-file

from typing import Tuple

import apache_beam as beam
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.interactive import cache_manager as cache
from apache_beam.runners.interactive.caching.cacheable import Cacheable
from apache_beam.runners.interactive.caching.reify import unreify_from_cache
from apache_beam.runners.pipeline_context import PipelineContext
from apache_beam.transforms.ptransform import PTransform


class ReadCache:
  """Class that facilitates reading cache of computed PCollections.
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
    self._key = repr(cacheable.to_key())

  def read_cache(self) -> Tuple[str, str]:
    """Reads cache of the cacheable PCollection and wires the cache into the
    pipeline proto. Returns the pipeline-scoped ids of the cacheable PCollection
    and the cache reading output PCollection that replaces it.

    First, it creates a temporary pipeline instance on top of the existing
    component_id_map from the self._pipeline's context so that both pipelines
    share the context and have no conflict component ids.
    Second, it instantiates a _ReadCacheTransform to build the temporary
    pipeline with a subgraph under top level transforms that reads the cache of
    a cacheable PCollection.
    Third, it copies components of the subgraph from the temporary pipeline to
    self._pipeline, skipping components that are not in the temporary pipeline
    but presents in the component_id_map of self._pipeline. Since to_runner_api
    generates components for all entries in the component_id_map, those
    component ids from the context shared by self._pipeline need to be ignored.
    Last, it replaces inputs of all transforms that consume the cacheable
    PCollection with the output PCollection of the _ReadCacheTransform so that
    the whole pipeline computes with data from the cache. The pipeline
    fragment of reading the cacheable PCollection is now disconnected from the
    rest of the pipeline and can be pruned later.
    """
    template, read_output = self._build_runner_api_template()
    output_id = self._context.pcollections.get_id(read_output)
    source_id = self._context.pcollections.get_id(self._cacheable.pcoll)
    # Copy cache reading subgraph from the template to the pipeline proto.
    for pcoll_id in template.components.pcollections:
      if pcoll_id in self._pipeline.components.pcollections:
        continue
      self._pipeline.components.pcollections[pcoll_id].CopyFrom(
          template.components.pcollections[pcoll_id])
    for coder_id in template.components.coders:
      if coder_id in self._pipeline.components.coders:
        continue
      self._pipeline.components.coders[coder_id].CopyFrom(
          template.components.coders[coder_id])
    for windowing_strategy_id in template.components.windowing_strategies:
      if (windowing_strategy_id in
          self._pipeline.components.windowing_strategies):
        continue
      self._pipeline.components.windowing_strategies[
          windowing_strategy_id].CopyFrom(
              template.components.windowing_strategies[windowing_strategy_id])
    template_root_transform_id = template.root_transform_ids[0]
    root_transform_id = self._pipeline.root_transform_ids[0]
    for transform_id in template.components.transforms:
      if (transform_id == template_root_transform_id or
          transform_id in self._pipeline.components.transforms):
        continue
      self._pipeline.components.transforms[transform_id].CopyFrom(
          template.components.transforms[transform_id])
    self._pipeline.components.transforms[
        root_transform_id].subtransforms.extend(
            template.components.transforms[template_root_transform_id].
            subtransforms)

    # Replace all the input pcoll of source_id with output pcoll of output_id
    # from cache reading.
    for transform in self._pipeline.components.transforms.values():
      inputs = transform.inputs
      if source_id in inputs.values():
        keys_need_replacement = set()
        for key in inputs:
          if inputs[key] == source_id:
            keys_need_replacement.add(key)
        for key in keys_need_replacement:
          inputs[key] = output_id

    return source_id, output_id

  def _build_runner_api_template(
      self) -> Tuple[beam_runner_api_pb2.Pipeline, beam.pvalue.PCollection]:
    transform = _ReadCacheTransform(self._cache_manager, self._key)
    tmp_pipeline = beam.Pipeline()
    tmp_pipeline.component_id_map = self._context.component_id_map
    read_output = tmp_pipeline | 'source_cache_' >> transform
    return tmp_pipeline.to_runner_api(), read_output


class _ReadCacheTransform(PTransform):
  """A composite transform encapsulates reading cache of PCollections.
  """
  def __init__(self, cache_manager: cache.CacheManager, key: str):
    self._cache_manager = cache_manager
    self._key = key

  def expand(self, pcoll: beam.pvalue.PCollection) -> beam.pvalue.PCollection:
    return unreify_from_cache(
        pipeline=pcoll.pipeline,
        cache_key=self._key,
        cache_manager=self._cache_manager)
