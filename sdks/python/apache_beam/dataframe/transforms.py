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

import pandas as pd

import apache_beam as beam
from apache_beam import transforms
from apache_beam.dataframe import expressions
from apache_beam.dataframe import frame_base
from apache_beam.dataframe import frames  # pylint: disable=unused-import


class DataframeTransform(transforms.PTransform):
  """A PTransform for applying function that takes and returns dataframes
  to one or more PCollections.

  For example, if pcoll is a PCollection of dataframes, one could write::

      pcoll | DataframeTransform(lambda df: df.group_by('key').sum(), proxy=...)

  To pass multiple PCollections, pass a tuple of PCollections wich will be
  passed to the callable as positional arguments, or a dictionary of
  PCollections, in which case they will be passed as keyword arguments.
  """
  def __init__(self, func, proxy):
    self._func = func
    self._proxy = proxy

  def expand(self, input_pcolls):
    def wrap_as_dict(values):
      if isinstance(values, dict):
        return values
      elif isinstance(values, tuple):
        return dict(enumerate(values))
      else:
        return {None: values}

    # TODO: Infer the proxy from the input schema.
    def proxy(key):
      if key is None:
        return self._proxy
      else:
        return self._proxy[key]

    # The input can be a dictionary, tuple, or plain PCollection.
    # Wrap as a dict for homogeneity.
    # TODO: Possibly inject batching here.
    input_dict = wrap_as_dict(input_pcolls)
    placeholders = {
        key: frame_base.DeferredFrame.wrap(
            expressions.PlaceholderExpression(proxy(key)))
        for key in input_dict.keys()
    }

    # The calling convention of the user-supplied func varies according to the
    # type of the input.
    if isinstance(input_pcolls, dict):
      result_frames = self._func(**placeholders)
    elif isinstance(input_pcolls, tuple):
      result_frames = self._func(
          *(value for _, value in sorted(placeholders.items())))
    else:
      result_frames = self._func(placeholders[None])

    # Likewise the output may be a dict, tuple, or raw (deferred) Dataframe.
    result_dict = wrap_as_dict(result_frames)

    result_pcolls = self._apply_deferred_ops(
        {placeholders[key]._expr: pcoll
         for key, pcoll in input_dict.items()},
        {key: df._expr
         for key, df in result_dict.items()})

    # Convert the result back into a set of PCollections.
    if isinstance(result_frames, dict):
      return result_pcolls
    elif isinstance(result_frames, tuple):
      return tuple((value for _, value in sorted(result_pcolls.items())))
    else:
      return result_pcolls[None]

  def _apply_deferred_ops(
      self,
      inputs,  # type: Dict[PlaceholderExpr, PCollection]
      outputs,  # type: Dict[Any, Expression]
      ):  # -> Dict[Any, PCollection]
    """Construct a Beam graph that evaluates a set of expressions on a set of
    input PCollections.

    :param inputs: A mapping of placeholder expressions to PCollections.
    :param outputs: A mapping of keys to expressions defined in terms of the
        placeholders of inputs.

    Returns a dictionary whose keys are those of outputs, and whose values are
    PCollections corresponding to the values of outputs evaluated at the
    values of inputs.

    Logically, `_apply_deferred_ops({x: a, y: b}, {f: F(x, y), g: G(x, y)})`
    returns `{f: F(a, b), g: G(a, b)}`.
    """
    class ComputeStage(beam.PTransform):
      """A helper transform that computes a single stage of operations.
      """
      def __init__(self, stage):
        self.stage = stage

      def default_label(self):
        return '%s:%s' % (self.stage.ops, id(self))

      def expand(self, pcolls):
        if self.stage.is_grouping:
          # Arrange such that partitioned_pcoll is properly partitioned.
          input_pcolls = {
              tag: pcoll | 'Flat%s' % tag >> beam.FlatMap(partition_by_index)
              for (tag, pcoll) in pcolls.items()
          }
          partitioned_pcoll = input_pcolls | beam.CoGroupByKey(
          ) | beam.MapTuple(
              lambda _,
              inputs: {tag: pd.concat(vs)
                       for tag, vs in inputs.items()})
        else:
          # Already partitioned, or no partitioning needed.
          (k, pcoll), = pcolls.items()
          partitioned_pcoll = pcoll | beam.Map(lambda df: {k: df})

        # Actually evaluate the expressions.
        def evaluate(partition, stage=self.stage):
          session = expressions.Session(
              {expr: partition[expr._id]
               for expr in stage.inputs})
          for expr in stage.outputs:
            yield beam.pvalue.TaggedOutput(expr._id, expr.evaluate_at(session))

        return partitioned_pcoll | beam.FlatMap(evaluate).with_outputs()

    class Stage(object):
      """Used to build up a set of operations that can be fused together.
      """
      def __init__(self, inputs, is_grouping):
        self.inputs = set(inputs)
        self.is_grouping = is_grouping or len(self.inputs) > 1
        self.ops = []
        self.outputs = set()

    # First define some helper functions.
    def output_is_partitioned_by_index(expr, stage):
      if expr in stage.inputs:
        return stage.is_grouping
      elif expr.preserves_partition_by_index():
        if expr.requires_partition_by_index():
          return True
        else:
          return all(
              output_is_partitioned_by_index(arg, stage) for arg in expr.args())
      else:
        return False

    def partition_by_index(df, levels=None, parts=10):
      if levels is None:
        levels = list(range(df.index.nlevels))
      elif isinstance(levels, (int, str)):
        levels = [levels]
      hashes = sum(
          pd.util.hash_array(df.index.get_level_values(level))
          for level in levels)
      for key in range(parts):
        yield key, df[hashes % parts == key]

    def common_stages(stage_lists):
      # Set intersection, with a preference for earlier items in the list.
      if stage_lists:
        for stage in stage_lists[0]:
          if all(stage in other for other in stage_lists[1:]):
            yield stage

    @memoize
    def expr_to_stages(expr):
      assert expr not in inputs
      # First attempt to compute this expression as part of an existing stage,
      # if possible.
      #
      # If expr does not require partitioning, just grab any stage, else grab
      # the first stage where all of expr's inputs are partitioned as required.
      # In either case, use the first such stage because earlier stages are
      # closer to the inputs (have fewer intermediate stages).
      for stage in common_stages([expr_to_stages(arg) for arg in expr.args()
                                  if arg not in inputs]):
        if (not expr.requires_partition_by_index() or
            all(output_is_partitioned_by_index(arg, stage)
                for arg in expr.args())):
          break
      else:
        # Otherwise, compute this expression as part of a new stage.
        stage = Stage(expr.args(), expr.requires_partition_by_index())
        for arg in expr.args():
          if arg not in inputs:
            # For each non-input argument, declare that it is also available in
            # this new stage.
            expr_to_stages(arg).append(stage)
            # It also must be declared as an output of the producing stage.
            expr_to_stage(arg).outputs.add(arg)
      stage.ops.append(expr)
      # This is a list as given expression may be available in many stages.
      return [stage]

    def expr_to_stage(expr):
      # Any will do; the first requires the fewest intermediate stages.
      return expr_to_stages(expr)[0]

    # Ensure each output is computed.
    for expr in outputs.values():
      if expr not in inputs:
        expr_to_stage(expr).outputs.add(expr)

    @memoize
    def stage_to_result(stage):
      return {expr._id: expr_to_pcoll(expr)
              for expr in stage.inputs} | ComputeStage(stage)

    @memoize
    def expr_to_pcoll(expr):
      if expr in inputs:
        return inputs[expr]
      else:
        return stage_to_result(expr_to_stage(expr))[expr._id]

    # Now we can compute and return the result.
    return {k: expr_to_pcoll(expr) for k, expr in outputs.items()}


def memoize(f):
  cache = {}

  def wrapper(*args):
    if args not in cache:
      cache[args] = f(*args)
    return cache[args]

  return wrapper
