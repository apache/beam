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

from typing import *

import apache_beam as beam
from apache_beam.dataframe import convert
from apache_beam.dataframe import expressions


class ExpressionCache(object):
  """Utility class for caching deferred DataFrames expressions.

  This is cache is currently a light-weight wrapper around the
  TO_PCOLLECTION_CACHE in the beam.dataframes.convert module and the
  computed_pcollections in the interactive module.

  Example::

    df : beam.dataframe.DeferredDataFrame = ...
    ...
    cache = ExpressionCache()
    cache.replace_with_cached(df._expr)

  This will automatically link the instance to the existing caches. After it is
  created, the cache can then be used to modify an existing deferred dataframe
  expression tree to replace nodes with computed PCollections.

  This object can be created and destroyed whenever. This class holds no state
  and the only side-effect is modifying the given expression.
  """
  def __init__(self, pcollection_cache=None, computed_cache=None):
    from apache_beam.runners.interactive import interactive_environment as ie

    self._pcollection_cache = (
        convert.TO_PCOLLECTION_CACHE
        if pcollection_cache is None else pcollection_cache)
    self._computed_cache = (
        ie.current_env().computed_pcollections
        if computed_cache is None else computed_cache)

  def replace_with_cached(
      self, expr: expressions.Expression) -> Dict[str, expressions.Expression]:
    """Replaces any previously computed expressions with PlaceholderExpressions.

    This is used to correctly read any expressions that were cached in previous
    runs. This enables the InteractiveRunner to prune off old calculations from
    the expression tree.
    """

    replaced_inputs: Dict[str, expressions.Expression] = {}
    self._replace_with_cached_recur(expr, replaced_inputs)
    return replaced_inputs

  def _replace_with_cached_recur(
      self,
      expr: expressions.Expression,
      replaced_inputs: Dict[str, expressions.Expression]) -> None:
    """Recursive call for `replace_with_cached`.

    Recurses through the expression tree and replaces any cached inputs with
    `PlaceholderExpression`s.
    """

    final_inputs = []

    for input in expr.args():
      pc = self._get_cached(input)

      # Only read from cache when there is the PCollection has been fully
      # computed. This is so that no partial results are used.
      if self._is_computed(pc):

        # Reuse previously seen cached expressions. This is so that the same
        # value isn't cached multiple times.
        if input._id in replaced_inputs:
          cached = replaced_inputs[input._id]
        else:
          cached = expressions.PlaceholderExpression(
              input.proxy(), self._pcollection_cache[input._id])

          replaced_inputs[input._id] = cached
        final_inputs.append(cached)
      else:
        final_inputs.append(input)
        self._replace_with_cached_recur(input, replaced_inputs)
    expr._args = tuple(final_inputs)

  def _get_cached(self,
                  expr: expressions.Expression) -> Optional[beam.PCollection]:
    """Returns the PCollection associated with the expression."""
    return self._pcollection_cache.get(expr._id, None)

  def _is_computed(self, pc: beam.PCollection) -> bool:
    """Returns True if the PCollection has been run and computed."""
    return pc is not None and pc in self._computed_cache
