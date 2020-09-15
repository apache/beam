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

from __future__ import absolute_import

from typing import Any
from typing import Iterable
from typing import Tuple
from typing import TypeVar

import pandas as pd

Frame = TypeVar('Frame', bound=pd.core.generic.NDFrame)


class Partitioning(object):
  """A class representing a (consistent) partitioning of dataframe objects.
  """
  def is_subpartitioning_of(self, other):
    # type: (Partitioning) -> bool

    """Returns whether self is a sub-partition of other.

    Specifically, returns whether something partitioned by self is necissarily
    also partitioned by other.
    """
    raise NotImplementedError

  def partition_fn(self, df):
    # type: (Frame) -> Iterable[Tuple[Any, Frame]]

    """A callable that actually performs the partitioning of a Frame df.

    This will be invoked via a FlatMap in conjunction with a GroupKey to
    achieve the desired partitioning.
    """
    raise NotImplementedError


class Index(Partitioning):
  """A partitioning by index (either fully or partially).

  If the set of "levels" of the index to consider is not specified, the entire
  index is used.

  These form a partial order, given by

      Nothing() < Index([i]) < Index([i, j]) < ... < Index() < Singleton()

  The ordering is implemented via the is_subpartitioning_of method, where the
  examples on the right are subpartitionings of the examples on the left above.
  """

  _INDEX_PARTITIONS = 10

  def __init__(self, levels=None):
    self._levels = levels

  def __eq__(self, other):
    return type(self) == type(other) and self._levels == other._levels

  def __ne__(self, other):
    return not self == other

  def __hash__(self):
    if self._levels:
      return hash(tuple(sorted(self._levels)))
    else:
      return hash(type(self))

  def is_subpartitioning_of(self, other):
    if isinstance(other, Nothing):
      return True
    elif isinstance(other, Index):
      if self._levels is None:
        return True
      elif other._levels is None:
        return False
      else:
        return all(level in other._levels for level in self._levels)
    else:
      return False

  def partition_fn(self, df):
    if self._levels is None:
      levels = list(range(df.index.nlevels))
    else:
      levels = self._levels
    hashes = sum(
        pd.util.hash_array(df.index.get_level_values(level))
        for level in levels)
    for key in range(self._INDEX_PARTITIONS):
      yield key, df[hashes % self._INDEX_PARTITIONS == key]


class Singleton(Partitioning):
  """A partitioning of all the data into a single partition.
  """
  def __eq__(self, other):
    return type(self) == type(other)

  def __ne__(self, other):
    return not self == other

  def __hash__(self):
    return hash(type(self))

  def is_subpartitioning_of(self, other):
    return True

  def partition_fn(self, df):
    yield None, df


class Nothing(Partitioning):
  """A partitioning imposing no constraints on the actual partitioning.
  """
  def __eq__(self, other):
    return type(self) == type(other)

  def __ne__(self, other):
    return not self == other

  def __hash__(self):
    return hash(type(self))

  def is_subpartitioning_of(self, other):
    return isinstance(other, Nothing)
