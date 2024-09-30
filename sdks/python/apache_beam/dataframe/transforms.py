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

import collections
import logging
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
from apache_beam.utils import windowed_value

__all__ = [
    'DataframeTransform',
]

if TYPE_CHECKING:
  # pylint: disable=ungrouped-imports
  from apache_beam.pvalue import PCollection

T = TypeVar('T')

TARGET_PARTITION_SIZE = 1 << 23  # 8M
MIN_PARTITION_SIZE = 1 << 19  # 0.5M
MAX_PARTITIONS = 1000
DEFAULT_PARTITIONS = 100
MIN_PARTITIONS = 10
PER_COL_OVERHEAD = 1000


class DataframeTransform(transforms.PTransform):
  """A PTransform for applying function that takes and returns dataframes
  to one or more PCollections.

  :class:`DataframeTransform` will accept a PCollection with a `schema`_ and
  batch it into :class:`~pandas.DataFrame` instances if necessary::

      (pcoll | beam.Select(key=..., foo=..., bar=...)
             | DataframeTransform(lambda df: df.group_by('key').sum()))

  It is also possible to process a PCollection of :class:`~pandas.DataFrame`
  instances directly, in this case a "proxy" must be provided. For example, if
  ``pcoll`` is a PCollection of DataFrames, one could write::

      pcoll | DataframeTransform(lambda df: df.group_by('key').sum(), proxy=...)

  To pass multiple PCollections, pass a tuple of PCollections wich will be
  passed to the callable as positional arguments, or a dictionary of
  PCollections, in which case they will be passed as keyword arguments.

  Args:
    yield_elements: (optional, default: "schemas") If set to ``"pandas"``,
        return PCollection(s) containing the raw Pandas objects
        (:class:`~pandas.DataFrame` or :class:`~pandas.Series` as appropriate).
        If set to ``"schemas"``, return an element-wise PCollection, where
        DataFrame and Series instances are expanded to one element per row.
        DataFrames are converted to `schema-aware`_ PCollections, where column
        values can be accessed by attribute.
    include_indexes: (optional, default: False) When
       ``yield_elements="schemas"``, if ``include_indexes=True``, attempt to
       include index columns in the output schema for expanded DataFrames.
       Raises an error if any of the index levels are unnamed (name=None), or if
       any of the names are not unique among all column and index names.
    proxy: (optional) An empty :class:`~pandas.DataFrame` or
        :class:`~pandas.Series` instance with the same ``dtype`` and ``name``
        as the elements of the input PCollection. Required when input
        PCollection :class:`~pandas.DataFrame` or :class:`~pandas.Series`
        elements. Ignored when input PCollection has a `schema`_.

  .. _schema:
    https://beam.apache.org/documentation/programming-guide/#what-is-a-schema
  .. _schema-aware:
    https://beam.apache.org/documentation/programming-guide/#what-is-a-schema
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
        for tag in input_dict
    }
    input_frames = {
        k: convert.to_dataframe(pc, proxies[k])
        for k, pc in input_dict.items()
    }  # type: Dict[Any, DeferredFrame] # noqa: F821

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
        logging.info('Computing dataframe stage %s for %s', self, self.stage)
        scalar_inputs = [expr for expr in self.stage.inputs if is_scalar(expr)]
        tabular_inputs = [
            expr for expr in self.stage.inputs if not is_scalar(expr)
        ]

        if len(tabular_inputs) == 0:
          partitioned_pcoll = next(iter(
              pcolls.values())).pipeline | beam.Create([{}])

        elif self.stage.partitioning != partitionings.Arbitrary():
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

          partition_fn = self.stage.partitioning.partition_fn

          class Partition(beam.PTransform):
            def expand(self, pcoll):
              return (
                  pcoll
                  # Attempt to create batches of reasonable size.
                  | beam.ParDo(_PreBatch())
                  # Actually partition.
                  | beam.FlatMap(partition_fn, num_partitions)
                  # Don't bother shuffling empty partitions.
                  | beam.Filter(lambda k_df: len(k_df[1])))

          # Arrange such that partitioned_pcoll is properly partitioned.
          main_pcolls = {
              expr._id: pcolls[expr._id] | 'Partition_%s_%s' %
              (self.stage.partitioning, expr._id) >> Partition()
              for expr in tabular_inputs
          } | beam.CoGroupByKey()
          partitioned_pcoll = main_pcolls | beam.ParDo(_ReBatch())

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
          def lookup(expr):
            # Use proxy if there's no data in this partition
            return expr.proxy(
            ).iloc[:0] if partition[expr._id] is None else partition[expr._id]

          session = expressions.Session(
              dict([(expr, lookup(expr)) for expr in tabular_inputs] +
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
        if (len(self.inputs) > 1 and
            partitioning.is_subpartitioning_of(partitionings.Index())):
          # We have to shuffle to co-locate, might as well partition.
          self.partitioning = partitionings.Index()
        elif isinstance(partitioning, partitionings.JoinIndex):
          # Not an actionable partitioning, use index.
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
    @_memoize
    def output_partitioning_in_stage(expr, stage):
      """Return the output partitioning of expr when computed in stage,
      or returns None if the expression cannot be computed in this stage.
      """
      def maybe_upgrade_to_join_index(partitioning):
        if partitioning.is_subpartitioning_of(partitionings.JoinIndex()):
          return partitionings.JoinIndex(expr)
        else:
          return partitioning

      if expr in stage.inputs or expr in inputs:
        # Inputs are all partitioned by stage.partitioning.
        return maybe_upgrade_to_join_index(stage.partitioning)

      # Anything that's not an input must have arguments
      assert len(expr.args())

      arg_partitionings = set(
          output_partitioning_in_stage(arg, stage) for arg in expr.args()
          if not is_scalar(arg))

      if len(arg_partitionings) == 0:
        # All inputs are scalars, output partitioning isn't dependent on the
        # input.
        return maybe_upgrade_to_join_index(expr.preserves_partition_by())

      if len(arg_partitionings) > 1:
        # Arguments must be identically partitioned, can't compute this
        # expression here.
        return None

      arg_partitioning = arg_partitionings.pop()

      if not expr.requires_partition_by().is_subpartitioning_of(
          arg_partitioning):
        # Arguments aren't partitioned sufficiently for this expression
        return None

      return maybe_upgrade_to_join_index(
          expressions.output_partitioning(expr, arg_partitioning))

    def is_computable_in_stage(expr, stage):
      return output_partitioning_in_stage(expr, stage) is not None

    def common_stages(stage_lists):
      # Set intersection, with a preference for earlier items in the list.
      if stage_lists:
        for stage in stage_lists[0]:
          if all(stage in other for other in stage_lists[1:]):
            yield stage

    @_memoize
    def is_scalar(expr):
      return not isinstance(expr.proxy(), pd.core.generic.NDFrame)

    @_memoize
    def expr_to_stages(expr):
      if expr in inputs:
        # Don't create a stage for each input, but it is still useful to record
        # what which stages inputs are available from.
        return []

      # First attempt to compute this expression as part of an existing stage,
      # if possible.
      if all(arg in inputs for arg in expr.args()):
        # All input arguments;  try to pick a stage that already has as many
        # of the inputs, correctly partitioned, as possible.
        inputs_by_stage = collections.defaultdict(int)
        for arg in expr.args():
          for stage in expr_to_stages(arg):
            if is_computable_in_stage(expr, stage):
              inputs_by_stage[stage] += 1 + 100 * (
                  expr.requires_partition_by() == stage.partitioning)
        if inputs_by_stage:
          # Take the stage with the largest count.
          stage = max(inputs_by_stage.items(), key=lambda kv: kv[1])[0]
        else:
          stage = None
      else:
        # Try to pick a stage that has all the available non-input expressions.
        # TODO(robertwb): Baring any that have all of them, we could try and
        # pick one that has the most, but we need to ensure it is not a
        # predecessor of any of the missing argument's stages.
        for stage in common_stages([expr_to_stages(arg) for arg in expr.args()
                                    if arg not in inputs]):
          if is_computable_in_stage(expr, stage):
            break
        else:
          stage = None

      if stage is None:
        # No stage available, compute this expression as part of a new stage.
        stage = Stage([arg for arg in expr.args() if arg in inputs],
                      expr.requires_partition_by())
        for arg in expr.args():
          # For each argument, declare that it is also available in
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

    @_memoize
    def stage_to_result(stage):
      # print({expr._id: expr_to_pcoll(expr) for expr in stage.inputs})
      return {expr._id: expr_to_pcoll(expr)
              for expr in stage.inputs} | ComputeStage(stage)

    @_memoize
    def expr_to_pcoll(expr):
      if expr in inputs:
        return inputs[expr]
      else:
        return stage_to_result(expr_to_stage(expr))[expr._id]

    @_memoize
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
        has_shuffle = expr_stage.partitioning != partitionings.Arbitrary()
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
      size = size.sum() + PER_COL_OVERHEAD * len(size)
    else:
      size += PER_COL_OVERHEAD
    return size
  except AttributeError:
    # Don't know, assume it's really big.
    float('inf')


class _PreBatch(beam.DoFn):
  def __init__(
      self, target_size=TARGET_PARTITION_SIZE, min_size=MIN_PARTITION_SIZE):
    self._target_size = target_size
    self._min_size = min_size

  def start_bundle(self):
    self._parts = collections.defaultdict(list)
    self._running_size = 0

  def process(
      self,
      part,
      window=beam.DoFn.WindowParam,
      timestamp=beam.DoFn.TimestampParam):
    part_size = _total_memory_usage(part)
    if part_size >= self._min_size:
      yield part
    else:
      self._running_size += part_size
      self._parts[window, timestamp].append(part)
      if self._running_size >= self._target_size:
        yield from self.finish_bundle()

  def finish_bundle(self):
    for (window, timestamp), parts in self._parts.items():
      yield windowed_value.WindowedValue(_concat(parts), timestamp, (window, ))
    self.start_bundle()


class _ReBatch(beam.DoFn):
  """Groups all the parts from various workers into the same dataframe.

  Also groups across partitions, up to a given data size, to recover some
  efficiency in the face of over-partitioning.
  """
  def __init__(
      self, target_size=TARGET_PARTITION_SIZE, min_size=MIN_PARTITION_SIZE):
    self._target_size = target_size
    self._min_size = min_size

  def start_bundle(self):
    self._parts = collections.defaultdict(lambda: collections.defaultdict(list))
    self._running_size = 0

  def process(
      self,
      element,
      window=beam.DoFn.WindowParam,
      timestamp=beam.DoFn.TimestampParam):
    _, tagged_parts = element
    for tag, parts in tagged_parts.items():
      for part in parts:
        self._running_size += _total_memory_usage(part)
      self._parts[window, timestamp][tag].extend(parts)
    if self._running_size >= self._target_size:
      yield from self.finish_bundle()

  def finish_bundle(self):
    for (window, timestamp), tagged_parts in self._parts.items():
      yield windowed_value.WindowedValue(  # yapf break
      {
          tag: _concat(parts) if parts else None
          for (tag, parts) in tagged_parts.items()
      },
      timestamp, (window, ))
    self.start_bundle()


def _memoize(f):
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


def _concat(parts):
  if len(parts) == 1:
    return parts[0]
  else:
    return pd.concat(parts)


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
