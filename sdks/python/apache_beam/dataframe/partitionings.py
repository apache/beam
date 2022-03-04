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

import random
from typing import Any
from typing import Iterable
from typing import Tuple
from typing import TypeVar

import numpy as np
import pandas as pd

Frame = TypeVar('Frame', bound=pd.core.generic.NDFrame)


class Partitioning(object):
  """A class representing a (consistent) partitioning of dataframe objects.
  """
  def __repr__(self):
    return self.__class__.__name__

  def is_subpartitioning_of(self, other):
    # type: (Partitioning) -> bool

    """Returns whether self is a sub-partition of other.

    Specifically, returns whether something partitioned by self is necissarily
    also partitioned by other.
    """
    raise NotImplementedError

  def __lt__(self, other):
    return self != other and self <= other

  def __le__(self, other):
    return not self.is_subpartitioning_of(other)

  def partition_fn(self, df, num_partitions):
    # type: (Frame, int) -> Iterable[Tuple[Any, Frame]]

    """A callable that actually performs the partitioning of a Frame df.

    This will be invoked via a FlatMap in conjunction with a GroupKey to
    achieve the desired partitioning.
    """
    raise NotImplementedError

  def test_partition_fn(self, df):
    return self.partition_fn(df, 5)


class Index(Partitioning):
  """A partitioning by index (either fully or partially).

  If the set of "levels" of the index to consider is not specified, the entire
  index is used.

  These form a partial order, given by

      Singleton() < Index([i]) < Index([i, j]) < ... < Index() < Arbitrary()

  The ordering is implemented via the is_subpartitioning_of method, where the
  examples on the right are subpartitionings of the examples on the left above.
  """
  def __init__(self, levels=None):
    self._levels = levels

  def __repr__(self):
    if self._levels:
      return 'Index%s' % self._levels
    else:
      return 'Index'

  def __eq__(self, other):
    return type(self) == type(other) and self._levels == other._levels

  def __hash__(self):
    if self._levels:
      return hash(tuple(sorted(self._levels)))
    else:
      return hash(type(self))

  def is_subpartitioning_of(self, other):
    if isinstance(other, Singleton):
      return True
    elif isinstance(other, Index):
      if self._levels is None:
        return True
      elif other._levels is None:
        return False
      else:
        return all(level in self._levels for level in other._levels)
    elif isinstance(other, (Arbitrary, JoinIndex)):
      return False
    else:
      raise ValueError(f"Encountered unknown type {other!r}")

  def _hash_index(self, df):
    if self._levels is None:
      levels = list(range(df.index.nlevels))
    else:
      levels = self._levels
    return sum(
        pd.util.hash_array(np.asarray(df.index.get_level_values(level)))
        for level in levels)

  def partition_fn(self, df, num_partitions):
    hashes = self._hash_index(df)
    for key in range(num_partitions):
      yield key, df[hashes % num_partitions == key]

  def check(self, dfs):
    # Drop empty DataFrames
    dfs = [df for df in dfs if len(df)]

    if not len(dfs):
      return True

    def apply_consistent_order(dfs):
      # Apply consistent order between dataframes by using sum of the index's
      # hash.
      # Apply consistent order within dataframe with sort_index()
      # Also drops any empty dataframes.
      return sorted((df.sort_index() for df in dfs if len(df)),
                    key=lambda df: sum(self._hash_index(df)))

    dfs = apply_consistent_order(dfs)
    repartitioned_dfs = apply_consistent_order(
        df for _, df in self.test_partition_fn(pd.concat(dfs)))

    # Assert that each index is identical
    for df, repartitioned_df in zip(dfs, repartitioned_dfs):
      if not df.index.equals(repartitioned_df.index):
        return False

    return True


class Singleton(Partitioning):
  """A partitioning of all the data into a single partition.
  """
  def __init__(self, reason=None):
    self._reason = reason

  @property
  def reason(self):
    return self._reason

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))

  def is_subpartitioning_of(self, other):
    return isinstance(other, Singleton)

  def partition_fn(self, df, num_partitions):
    yield None, df

  def check(self, dfs):
    return len(dfs) <= 1


class JoinIndex(Partitioning):
  """A partitioning that lets two frames be joined.
  This can either be a hash partitioning on the full index, or a common
  ancestor with no intervening re-indexing/re-partitioning.

  It fits into the partial ordering as

      Index() < JoinIndex(x) < JoinIndex() < Arbitrary()

  with

      JoinIndex(x) and JoinIndex(y)

  being incomparable for nontrivial x != y.

  Expressions desiring to make use of this index should simply declare a
  requirement of JoinIndex().
  """
  def __init__(self, ancestor=None):
    self._ancestor = ancestor

  def __repr__(self):
    if self._ancestor:
      return 'JoinIndex[%s]' % self._ancestor
    else:
      return 'JoinIndex'

  def __eq__(self, other):
    if type(self) != type(other):
      return False
    elif self._ancestor is None:
      return other._ancestor is None
    elif other._ancestor is None:
      return False
    else:
      return self._ancestor == other._ancestor

  def __hash__(self):
    return hash((type(self), self._ancestor))

  def is_subpartitioning_of(self, other):
    if isinstance(other, Arbitrary):
      return False
    elif isinstance(other, JoinIndex):
      return self._ancestor is None or self == other
    else:
      return True

  def test_partition_fn(self, df):
    return Index().test_partition_fn(df)

  def check(self, dfs):
    return True


class Arbitrary(Partitioning):
  """A partitioning imposing no constraints on the actual partitioning.
  """
  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))

  def is_subpartitioning_of(self, other):
    return True

  def test_partition_fn(self, df):
    num_partitions = 10

    def shuffled(seq):
      seq = list(seq)
      random.shuffle(seq)
      return seq

    part = pd.Series(shuffled(range(len(df))), index=df.index) % num_partitions
    for k in range(num_partitions):
      yield k, df[part == k]

  def check(self, dfs):
    return True
