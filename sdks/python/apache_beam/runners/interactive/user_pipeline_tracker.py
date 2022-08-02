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

"""Class that tracks derived/pipeline fragments from user pipelines.

For internal use only; no backwards-compatibility guarantees.
In the InteractiveRunner the design is to keep the user pipeline unchanged,
create a copy of the user pipeline, and modify the copy. When the derived
pipeline runs, there should only be per-user pipeline state. This makes sure
that derived pipelines can link back to the parent user pipeline.
"""

import shutil
from typing import Iterator
from typing import Optional

import apache_beam as beam  # type: ignore


class UserPipelineTracker:
  """Tracks user pipelines from derived pipelines.

  This data structure is similar to a disjoint set data structure. A derived
  pipeline can only have one parent user pipeline. A user pipeline can have many
  derived pipelines.
  """
  def __init__(self):
    self._user_pipelines: dict[beam.Pipeline, list[beam.Pipeline]] = {}
    self._derived_pipelines: dict[beam.Pipeline] = {}
    self._pid_to_pipelines: dict[beam.Pipeline] = {}

  def __iter__(self) -> Iterator[beam.Pipeline]:
    """Iterates through all the user pipelines."""
    for p in self._user_pipelines:
      yield p

  def _key(self, pipeline: beam.Pipeline) -> str:
    return str(id(pipeline))

  def evict(self, pipeline: beam.Pipeline) -> None:
    """Evicts the pipeline.

    Removes the given pipeline and derived pipelines if a user pipeline.
    Otherwise, removes the given derived pipeline.
    """
    user_pipeline = self.get_user_pipeline(pipeline)
    if user_pipeline:
      for d in self._user_pipelines[user_pipeline]:
        del self._derived_pipelines[d]
      del self._user_pipelines[user_pipeline]
    elif pipeline in self._derived_pipelines:
      del self._derived_pipelines[pipeline]

  def clear(self) -> None:
    """Clears the tracker of all user and derived pipelines."""
    # Remove all local_tempdir of created pipelines.
    for p in self._pid_to_pipelines.values():
      shutil.rmtree(p.local_tempdir, ignore_errors=True)

    self._user_pipelines.clear()
    self._derived_pipelines.clear()
    self._pid_to_pipelines.clear()

  def get_pipeline(self, pid: str) -> Optional[beam.Pipeline]:
    """Returns the pipeline corresponding to the given pipeline id."""
    return self._pid_to_pipelines.get(pid, None)

  def add_user_pipeline(self, p: beam.Pipeline) -> beam.Pipeline:
    """Adds a user pipeline with an empty set of derived pipelines."""
    self._memoize_pipieline(p)

    # Create a new node for the user pipeline if it doesn't exist already.
    user_pipeline = self.get_user_pipeline(p)
    if not user_pipeline:
      user_pipeline = p
      self._user_pipelines[p] = []

    return user_pipeline

  def _memoize_pipieline(self, p: beam.Pipeline) -> None:
    """Memoizes the pid of the pipeline to the pipeline object."""
    pid = self._key(p)
    if pid not in self._pid_to_pipelines:
      self._pid_to_pipelines[pid] = p

  def add_derived_pipeline(
      self, maybe_user_pipeline: beam.Pipeline,
      derived_pipeline: beam.Pipeline) -> None:
    """Adds a derived pipeline with the user pipeline.

    If the `maybe_user_pipeline` is a user pipeline, then the derived pipeline
    will be added to its set. Otherwise, the derived pipeline will be added to
    the user pipeline of the `maybe_user_pipeline`.

    By doing the above one can do:
    p = beam.Pipeline()

    derived1 = beam.Pipeline()
    derived2 = beam.Pipeline()

    ut = UserPipelineTracker()
    ut.add_derived_pipeline(p, derived1)
    ut.add_derived_pipeline(derived1, derived2)

    # Returns p.
    ut.get_user_pipeline(derived2)
    """
    self._memoize_pipieline(maybe_user_pipeline)
    self._memoize_pipieline(derived_pipeline)

    # Cannot add a derived pipeline twice.
    assert derived_pipeline not in self._derived_pipelines

    # Get the "true" user pipeline. This allows for the user to derive a
    # pipeline from another derived pipeline, use both as arguments, and this
    # method will still get the correct user pipeline.
    user = self.add_user_pipeline(maybe_user_pipeline)

    # Map the derived pipeline to the user pipeline.
    self._derived_pipelines[derived_pipeline] = user
    self._user_pipelines[user].append(derived_pipeline)

  def get_user_pipeline(self, p: beam.Pipeline) -> Optional[beam.Pipeline]:
    """Returns the user pipeline of the given pipeline.

    If the given pipeline has no user pipeline, i.e. not added to this tracker,
    then this returns None. If the given pipeline is a user pipeline then this
    returns the same pipeline. If the given pipeline is a derived pipeline then
    this returns the user pipeline.
    """

    # If `p` is a user pipeline then return it.
    if p in self._user_pipelines:
      return p

    # If `p` exists then return its user pipeline.
    if p in self._derived_pipelines:
      return self._derived_pipelines[p]

    # Otherwise, `p` is not in this tracker.
    return None
