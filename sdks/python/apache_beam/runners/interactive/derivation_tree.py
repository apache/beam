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
# pytype: skip-file


class DerivationTree:
  """Tracks which pipelines are derived from user pipelines.

  This data structure is similar to a disjoint set data structure. A derived
  pipeline can only have one parent user pipeline. A user pipeline can have many
  derived pipelines.
  """
  class Node:
    """Object to track the relationship between user to derived pipelines."""
    def __init__(self):
      self.user_pipeline = None
      self.derived_pipelines = set()

  def __init__(self):
    self._tree = {}
    self._pid_to_pipelines = {}

  def __iter__(self):
    """Iterates through all the user pipelines."""
    for n in self._tree.values():
      yield n.user_pipeline

  def _key(self, pipeline):
    return str(id(pipeline))

  def clear(self):
    """Clears the tree of all user and derived pipelines."""
    self._tree.clear()

  def get_pipeline(self, pid):
    """Returns the pipeline corresponding to the given pipeline id."""
    if pid in self._pid_to_pipelines:
      return self._pid_to_pipelines[pid]
    return None

  def add_user_pipeline(self, p):
    """Adds a user pipeline with an empty set of derived pipelines."""
    self._memoize_pipieline(p)

    # Create a new node for the user pipeline if it doesn't exist already.
    user_pipeline = self.get_user_pipeline(p)
    if not user_pipeline:
      user_pipeline = p
      node = DerivationTree.Node()
      node.user_pipeline = p
      self._tree[self._key(p)] = node

    return user_pipeline

  def _memoize_pipieline(self, p):
    """Memoizes the pid of the pipeline to the pipeline object."""
    pid = self._key(p)
    if pid not in self._pid_to_pipelines:
      self._pid_to_pipelines[pid] = p

  def add_derived_pipeline(self, maybe_user_pipeline, derived_pipeline):
    """Adds a derived pipeline to the tree.

    If the `maybe_user_pipeline` is a user pipeline, then the derived pipeline
    will be added to its set. Otherwise, the derived pipeline will be added to
    the user pipeline of the `maybe_user_pipeline`.

    By doing the above one can do:
    p = beam.Pipeline()

    derived1 = beam.Pipeline()
    derived2 = beam.Pipeline()

    dt = DerivationTree()
    dt.add_derived_pipeline(p, derived1)
    dt.add_derived_pipeline(derived1, derived2)

    # Returns p.
    dt.get_user_pipeline(derived2)
    """
    self._memoize_pipieline(maybe_user_pipeline)
    self._memoize_pipieline(derived_pipeline)

    # Get the "true" user pipeline. This allows for the user to derive a
    # pipeline from another derived pipeline, use both as arguments, and this
    # method will still get the correct user pipeline.
    user = self.add_user_pipeline(maybe_user_pipeline)

    # Add the derived pipeline to the user pipeline.
    node = self._tree[self._key(user)]
    node.derived_pipelines.add(derived_pipeline)

  def get_user_pipeline(self, p):
    """Returns the user pipeline of the given pipeline.

    If the given pipeline has no user pipeline, i.e. not added to this tree,
    then this returns None. If the given pipeline is a user pipeline then this
    returns the same pipeline. If the given pipeline is a derived pipeline then
    this returns the user pipeline.
    """

    # If `p` is a user pipeline then return it.
    pid = self._key(p)
    if pid in self._tree:
      return p

    # If `p` is in the tree then return its user pipeline.
    for n in self._tree.values():
      if p in n.derived_pipelines:
        return n.user_pipeline

    # Otherwise, `p` is not in the tree.
    return None
