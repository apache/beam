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

"""Module to augment interactive flavor into the given pipeline.

For internal use only; no backward-compatibility guarantees.
"""
# pytype: skip-file

import copy
from typing import Dict
from typing import Optional
from typing import Set

import apache_beam as beam
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import background_caching_job
from apache_beam.runners.interactive.caching.cacheable import Cacheable
from apache_beam.runners.interactive.caching.read_cache import ReadCache
from apache_beam.runners.interactive.caching.write_cache import WriteCache


class AugmentedPipeline:
  """A pipeline with augmented interactive flavor that caches intermediate
  PCollections defined by the user, reads computed PCollections as source and
  prunes unnecessary pipeline parts for fast computation.
  """
  def __init__(
      self,
      user_pipeline: beam.Pipeline,
      pcolls: Optional[Set[beam.pvalue.PCollection]] = None):
    """
    Initializes a pipelilne for augmenting interactive flavor.

    Args:
      user_pipeline: a beam.Pipeline instance defined by the user.
      pcolls: cacheable pcolls to be computed/retrieved. If the set is
        empty, all intermediate pcolls assigned to variables are applicable.
    """
    assert not pcolls or all(pcoll.pipeline is user_pipeline for pcoll in
      pcolls), 'All %s need to belong to %s' % (pcolls, user_pipeline)
    self._user_pipeline = user_pipeline
    self._pcolls = pcolls
    self._cache_manager = ie.current_env().get_cache_manager(
        self._user_pipeline, create_if_absent=True)
    if background_caching_job.has_source_to_cache(self._user_pipeline):
      self._cache_manager = ie.current_env().get_cache_manager(
          self._user_pipeline)
    _, self._context = self._user_pipeline.to_runner_api(return_context=True)
    self._context.component_id_map = copy.copy(
        self._user_pipeline.component_id_map)
    self._cacheables = self.cacheables()

  @property
  def augmented_pipeline(self) -> beam_runner_api_pb2.Pipeline:
    return self.augment()

  # TODO(BEAM-10708): Support generating a background recording job that
  # contains unbound source recording transforms only.
  @property
  def background_recording_pipeline(self) -> beam_runner_api_pb2.Pipeline:
    raise NotImplementedError

  def cacheables(self) -> Dict[beam.pvalue.PCollection, Cacheable]:
    """Finds all the cacheable intermediate PCollections in the pipeline with
    their metadata.
    """
    c = {}
    for watching in ie.current_env().watching():
      for key, val in watching:
        if (isinstance(val, beam.pvalue.PCollection) and
            val.pipeline is self._user_pipeline and
            (not self._pcolls or val in self._pcolls)):
          c[val] = Cacheable(
              var=key,
              pcoll=val,
              version=str(id(val)),
              producer_version=str(id(val.producer)))
    return c

  def augment(self) -> beam_runner_api_pb2.Pipeline:
    """Augments the pipeline with cache. Always calculates a new result.

    For a cacheable PCollection, if cache exists, read cache; else, write cache.
    """
    pipeline = self._user_pipeline.to_runner_api()

    # Find pcolls eligible for reading or writing cache.
    readcache_pcolls = set()
    for pcoll, cacheable in self._cacheables.items():
      key = repr(cacheable.to_key())
      if (self._cache_manager.exists('full', key) and
          pcoll in ie.current_env().computed_pcollections):
        readcache_pcolls.add(pcoll)
    writecache_pcolls = set(
        self._cacheables.keys()).difference(readcache_pcolls)

    # Wire in additional transforms to read cache and write cache.
    for readcache_pcoll in readcache_pcolls:
      ReadCache(
          pipeline,
          self._context,
          self._cache_manager,
          self._cacheables[readcache_pcoll]).read_cache()
    for writecache_pcoll in writecache_pcolls:
      WriteCache(
          pipeline,
          self._context,
          self._cache_manager,
          self._cacheables[writecache_pcoll]).write_cache()
    # TODO(BEAM-10708): Support streaming, add pruning logic, and integrate
    # pipeline fragment logic.
    return pipeline
