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

from typing import TYPE_CHECKING
from typing import Any
from typing import Dict
from typing import List
from typing import Mapping
from typing import Tuple
from typing import TypeVar
from typing import Union

import pandas as pd

import apache_beam as beam
from apache_beam import transforms
from apache_beam.dataframe import expressions
from apache_beam.dataframe import frames  # pylint: disable=unused-import
from apache_beam.dataframe import partitionings

if TYPE_CHECKING:
  # pylint: disable=ungrouped-imports
  from apache_beam.pvalue import PCollection

T = TypeVar('T')

TARGET_PARTITION_SIZE = 1 << 23  # 8M
MAX_PARTITIONS = 1000
DEFAULT_PARTITIONS = 100
MIN_PARTITIONS = 10


class DataframeTransform(transforms.PTransform):
  """A PTransform for applying function that takes and returns dataframes
  to one or more PCollections.

  DataframeTransform will accept a PCollection with a schema and batch it
  into dataframes if necessary. In this case the proxy can be omitted:

      (pcoll | beam.Row(key=..., foo=..., bar=...)
             | DataframeTransform(lambda df: df.group_by('key').sum()))

  It is also possible to process a PCollection of dataframes directly, in this
  case a proxy must be provided. For example, if pcoll is a PCollection of
  dataframes, one could write::

      pcoll | DataframeTransform(lambda df: df.group_by('key').sum(), proxy=...)

  To pass multiple PCollections, pass a tuple of PCollections wich will be
  passed to the callable as positional arguments, or a dictionary of
  PCollections, in which case they will be passed as keyword arguments.

  Args:
    yield_elements: (optional, default: "schemas") If set to "pandas", return
        PCollections containing the raw Pandas objects (DataFrames or Series),
        if set to "schemas", return an element-wise PCollection, where DataFrame
        and Series instances are expanded to one element per row. DataFrames are
        converted to schema-aware PCollections, where column values can be
        accessed by attribute.
    include_indexes: (optional, default: False) When yield_elements="schemas",
        if include_indexes=True, attempt to include index columns in the output
        schema for expanded DataFrames. Raises an error if any of the index
        levels are unnamed (name=None), or if any of the names are not unique
        among all column and index names.
  """
  def __init__(
      self, func, proxy=None, yield_elements="schemas", include_indexes=False):
    self._func = func
    self._proxy = proxy
    self._yield_elements = yield_elements
    self._include_indexes = include_indexes

  def expand(self, input_pcolls):
    # Avoid circular import.
    from apache_beam.dataframe import convert

    # Convert inputs to a flat dict.
    input_dict = _flatten(input_pcolls)  # type: Dict[Any, PCollection]
    proxies = _flatten(self._proxy) if self._proxy is not None else {
        tag: None
        for tag in input_dict.keys()
    }
    input_frames = {
        k: convert.to_dataframe(pc, proxies[k])
        for k, pc in input_dict.items()
    }  # type: Dict[Any, DeferredFrame]

    # Apply the function.
    frames_input = _substitute(input_pcolls, input_frames)
    if isinstance(frames_input, dict):
      result_frames = self._func(**frames_input)
    elif isinstance(frames_input, tuple):
      result_frames = self._func(*frames_input)
    else:
      result_frames = self._func(frames_input)

    # Compute results as a tuple.
    result_frames_dict = _flatten(result_frames)
    keys = list(result_frames_dict.keys())
    result_frames_tuple = tuple(result_frames_dict[key] for key in keys)
    result_pcolls_tuple = convert.to_pcollection(
        *result_frames_tuple,
        label='Eval',
        always_return_tuple=True,
        yield_elements=self._yield_elements,
        include_indexes=self._include_indexes)

    # Convert back to the structure returned by self._func.
    result_pcolls_dict = dict(zip(keys, result_pcolls_tuple))
    return _substitute(result_frames, result_pcolls_dict)


class _DataframeExpressionsTransform(transforms.PTransform):
  def __init__(self, outputs):
    self._outputs = outputs

  def expand(self, inputs):
    return self._apply_deferred_ops(inputs, self._outputs)

  def _apply_deferred_ops(
      self,
      inputs,  # type: Dict[expressions.Expression, PCollection]
      outputs,  # type: Dict[Any, expressions.Expression]
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

        scalar_inputs = [expr for expr in self.stage.inputs if is_scalar(expr)]
        tabular_inputs = [
            expr for expr in self.stage.inputs if not is_scalar(expr)
        ]

        if len(tabular_inputs) == 0:
          partitioned_pcoll = next(pcolls.values()).pipeline | beam.Create([{}])

        elif self.stage.partitioning != partitionings.Nothing():
          # Partitioning required for these operations.
          # Compute the number of partitions to use for the inputs based on
          # the estimated size of the inputs.
          if self.stage.partitioning == partitionings.Singleton():
            # Always a single partition, don't waste time computing sizes.
            num_partitions = 1
          else:
            # Estimate the sizes from the outputs of a *previous* stage such
            # that using these estimates will not cause a fusion break.
            input_sizes = [
                estimate_size(input, same_stage_ok=False)
                for input in tabular_inputs
            ]
            if None in input_sizes:
              # We were unable to (cheaply) compute the size of one or more
              # inputs.
              num_partitions = DEFAULT_PARTITIONS
            else:
              num_partitions = beam.pvalue.AsSingleton(
                  input_sizes
                  | 'FlattenSizes' >> beam.Flatten()
                  | 'SumSizes' >> beam.CombineGlobally(sum)
                  | 'NumPartitions' >> beam.Map(
                      lambda size: max(
                          MIN_PARTITIONS,
                          min(MAX_PARTITIONS, size // TARGET_PARTITION_SIZE))))

          # Arrange such that partitioned_pcoll is properly partitioned.
          main_pcolls = {
              expr._id: pcolls[expr._id] | 'Partition_%s_%s' %
              (self.stage.partitioning, expr._id) >> beam.FlatMap(
                  self.stage.partitioning.partition_fn, num_partitions)
              for expr in tabular_inputs
          } | beam.CoGroupByKey()
          partitioned_pcoll = main_pcolls | beam.MapTuple(
              lambda _,
              inputs: {tag: pd.concat(vs)
                       for tag, vs in inputs.items()})

        else:
          # Already partitioned, or no partitioning needed.
          assert len(tabular_inputs) == 1
          tag = tabular_inputs[0]._id
          partitioned_pcoll = pcolls[tag] | beam.Map(lambda df: {tag: df})

        side_pcolls = {
            expr._id: beam.pvalue.AsSingleton(pcolls[expr._id])
            for expr in scalar_inputs
        }

        # Actually evaluate the expressions.
        def evaluate(partition, stage=self.stage, **side_inputs):
          session = expressions.Session(
              dict([(expr, partition[expr._id]) for expr in tabular_inputs] +
                   [(expr, side_inputs[expr._id]) for expr in scalar_inputs]))
          for expr in stage.outputs:
            yield beam.pvalue.TaggedOutput(expr._id, expr.evaluate_at(session))

        return partitioned_pcoll | beam.FlatMap(evaluate, **
                                                side_pcolls).with_outputs()

    class Stage(object):
      """Used to build up a set of operations that can be fused together.

      Note that these Dataframe "stages" contain a CoGBK and hence are often
      split across multiple "executable" stages.
      """
      def __init__(self, inputs, partitioning):
        self.inputs = set(inputs)
        if len(self.inputs) > 1 and partitioning == partitionings.Nothing():
          # We have to shuffle to co-locate, might as well partition.
          self.partitioning = partitionings.Index()
        else:
          self.partitioning = partitioning
        self.ops = []
        self.outputs = set()

      def __repr__(self, indent=0):
        if indent:
          sep = '\n' + ' ' * indent
        else:
          sep = ''
        return (
            "Stage[%sinputs=%s, %spartitioning=%s, %sops=%s, %soutputs=%s]" % (
                sep,
                self.inputs,
                sep,
                self.partitioning,
                sep,
                self.ops,
                sep,
                self.outputs))

    # First define some helper functions.
    def output_is_partitioned_by(expr, stage, partitioning):
      if partitioning == partitionings.Nothing():
        # Always satisfied.
        return True
      elif stage.partitioning == partitionings.Singleton():
        # Within a stage, the singleton partitioning is trivially preserved.
        return True
      elif expr in stage.inputs:
        # Inputs are all partitioned by stage.partitioning.
        return stage.partitioning.is_subpartitioning_of(partitioning)
      elif expr.preserves_partition_by().is_subpartitioning_of(partitioning):
        # Here expr preserves at least the requested partitioning; its outputs
        # will also have this partitioning iff its inputs do.
        if expr.requires_partition_by().is_subpartitioning_of(partitioning):
          # If expr requires at least this partitioning, we will arrange such
          # that its inputs satisfy this.
          return True
        else:
          # Otherwise, recursively check all the inputs.
          return all(
              output_is_partitioned_by(arg, stage, partitioning)
              for arg in expr.args())
      else:
        return False

    def common_stages(stage_lists):
      # Set intersection, with a preference for earlier items in the list.
      if stage_lists:
        for stage in stage_lists[0]:
          if all(stage in other for other in stage_lists[1:]):
            yield stage

    @memoize
    def is_scalar(expr):
      return not isinstance(expr.proxy(), pd.core.generic.NDFrame)

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
      required_partitioning = expr.requires_partition_by()
      for stage in common_stages([expr_to_stages(arg) for arg in expr.args()
                                  if arg not in inputs]):
        if all(output_is_partitioned_by(arg, stage, required_partitioning)
               for arg in expr.args() if not is_scalar(arg)):
          break
      else:
        # Otherwise, compute this expression as part of a new stage.
        stage = Stage(expr.args(), required_partitioning)
        for arg in expr.args():
          if arg not in inputs:
            # For each non-input argument, declare that it is also available in
            # this new stage.
            expr_to_stages(arg).append(stage)
            # It also must be declared as an output of the producing stage.
            expr_to_stage(arg).outputs.add(arg)
      stage.ops.append(expr)
      # Ensure that any inputs for the overall transform are added
      # in downstream stages.
      for arg in expr.args():
        if arg in inputs:
          stage.inputs.add(arg)
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

    @memoize
    def estimate_size(expr, same_stage_ok):
      # Returns a pcollection of ints whose sum is the estimated size of the
      # given expression.
      pipeline = next(iter(inputs.values())).pipeline
      label = 'Size[%s, %s]' % (expr._id, same_stage_ok)
      if is_scalar(expr):
        return pipeline | label >> beam.Create([0])
      elif same_stage_ok:
        return expr_to_pcoll(expr) | label >> beam.Map(_total_memory_usage)
      elif expr in inputs:
        return None
      else:
        # This is the stage to avoid.
        expr_stage = expr_to_stage(expr)
        # If the stage doesn't start with a shuffle, it's not safe to fuse
        # the computation into its parent either.
        has_shuffle = expr_stage.partitioning != partitionings.Nothing()
        # We assume the size of an expression is the sum of the size of its
        # inputs, which may be off by quite a bit, but the goal is to get
        # within an order of magnitude or two.
        arg_sizes = []
        for arg in expr.args():
          if is_scalar(arg):
            continue
          elif arg in inputs:
            return None
          arg_size = estimate_size(
              arg,
              same_stage_ok=has_shuffle and expr_to_stage(arg) != expr_stage)
          if arg_size is None:
            return None
          arg_sizes.append(arg_size)
        return arg_sizes | label >> beam.Flatten(pipeline=pipeline)

    # Now we can compute and return the result.
    return {k: expr_to_pcoll(expr) for k, expr in outputs.items()}


def _total_memory_usage(frame):
  assert isinstance(frame, (pd.core.generic.NDFrame, pd.Index))
  try:
    size = frame.memory_usage()
    if not isinstance(size, int):
      size = size.sum()
    return size
  except AttributeError:
    # Don't know, assume it's really big.
    float('inf')


def memoize(f):
  cache = {}

  def wrapper(*args, **kwargs):
    key = args, tuple(sorted(kwargs.items()))
    if key not in cache:
      cache[key] = f(*args, **kwargs)
    return cache[key]

  return wrapper


def _dict_union(dicts):
  result = {}
  for d in dicts:
    result.update(d)
  return result


def _flatten(
    valueish,  # type: Union[T, List[T], Tuple[T], Dict[Any, T]]
    root=(),  # type: Tuple[Any, ...]
    ):
  # type: (...) -> Mapping[Tuple[Any, ...], T]

  """Given a nested structure of dicts, tuples, and lists, return a flat
  dictionary where the values are the leafs and the keys are the "paths" to
  these leaves.

  For example `{a: x, b: (y, z)}` becomes `{(a,): x, (b, 0): y, (b, 1): c}`.
  """
  if isinstance(valueish, dict):
    return _dict_union(_flatten(v, root + (k, )) for k, v in valueish.items())
  elif isinstance(valueish, (tuple, list)):
    return _dict_union(
        _flatten(v, root + (ix, )) for ix, v in enumerate(valueish))
  else:
    return {root: valueish}


def _substitute(valueish, replacements, root=()):
  """Substitutes the values in valueish with those in replacements where the
  keys are as in _flatten.

  For example,

  ```
  _substitute(
      {a: x, b: (y, z)},
      {(a,): X, (b, 0): Y, (b, 1): Z})
  ```

  returns `{a: X, b: (Y, Z)}`.
  """
  if isinstance(valueish, dict):
    return type(valueish)({
        k: _substitute(v, replacements, root + (k, ))
        for (k, v) in valueish.items()
    })
  elif isinstance(valueish, (tuple, list)):
    return type(valueish)((
        _substitute(v, replacements, root + (ix, ))
        for (ix, v) in enumerate(valueish)))
  else:
    return replacements[root]
