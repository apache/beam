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

"""Simple utility PTransforms.
"""

# pytype: skip-file

import collections
import contextlib
import random
import re
import threading
import time
import uuid
from typing import TYPE_CHECKING
from typing import Any
from typing import Iterable
from typing import List
from typing import Tuple
from typing import TypeVar
from typing import Union

from apache_beam import coders
from apache_beam import typehints
from apache_beam.metrics import Metrics
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.pvalue import AsSideInput
from apache_beam.transforms import window
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.transforms.core import CombinePerKey
from apache_beam.transforms.core import Create
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.core import FlatMap
from apache_beam.transforms.core import Flatten
from apache_beam.transforms.core import GroupByKey
from apache_beam.transforms.core import Map
from apache_beam.transforms.core import MapTuple
from apache_beam.transforms.core import ParDo
from apache_beam.transforms.core import Windowing
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.ptransform import ptransform_fn
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.trigger import Always
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import CombiningValueStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer
from apache_beam.transforms.window import NonMergingWindowFn
from apache_beam.transforms.window import TimestampCombiner
from apache_beam.transforms.window import TimestampedValue
from apache_beam.typehints import trivial_inference
from apache_beam.typehints.decorators import get_signature
from apache_beam.typehints.sharded_key_type import ShardedKeyType
from apache_beam.utils import windowed_value
from apache_beam.utils.annotations import deprecated
from apache_beam.utils.annotations import experimental
from apache_beam.utils.sharded_key import ShardedKey

if TYPE_CHECKING:
  from apache_beam import pvalue
  from apache_beam.runners.pipeline_context import PipelineContext

__all__ = [
    'BatchElements',
    'CoGroupByKey',
    'Distinct',
    'Keys',
    'KvSwap',
    'Regex',
    'Reify',
    'RemoveDuplicates',
    'Reshuffle',
    'ToString',
    'Values',
    'WithKeys',
    'GroupIntoBatches'
]

K = TypeVar('K')
V = TypeVar('V')
T = TypeVar('T')


class CoGroupByKey(PTransform):
  """Groups results across several PCollections by key.

  Given an input dict of serializable keys (called "tags") to 0 or more
  PCollections of (key, value) tuples, it creates a single output PCollection
  of (key, value) tuples whose keys are the unique input keys from all inputs,
  and whose values are dicts mapping each tag to an iterable of whatever values
  were under the key in the corresponding PCollection, in this manner::

      ('some key', {'tag1': ['value 1 under "some key" in pcoll1',
                             'value 2 under "some key" in pcoll1',
                             ...],
                    'tag2': ... ,
                    ... })

  where `[]` refers to an iterable, not a list.

  For example, given::

      {'tag1': pc1, 'tag2': pc2, 333: pc3}

  where::

      pc1 = beam.Create([(k1, v1)]))
      pc2 = beam.Create([])
      pc3 = beam.Create([(k1, v31), (k1, v32), (k2, v33)])

  The output PCollection would consist of items::

      [(k1, {'tag1': [v1], 'tag2': [], 333: [v31, v32]}),
       (k2, {'tag1': [], 'tag2': [], 333: [v33]})]

  where `[]` refers to an iterable, not a list.

  CoGroupByKey also works for tuples, lists, or other flat iterables of
  PCollections, in which case the values of the resulting PCollections
  will be tuples whose nth value is the iterable of values from the nth
  PCollection---conceptually, the "tags" are the indices into the input.
  Thus, for this input::

     (pc1, pc2, pc3)

  the output would be::

      [(k1, ([v1], [], [v31, v32]),
       (k2, ([], [], [v33]))]

  where, again, `[]` refers to an iterable, not a list.

  Attributes:
    **kwargs: Accepts a single named argument "pipeline", which specifies the
      pipeline that "owns" this PTransform. Ordinarily CoGroupByKey can obtain
      this information from one of the input PCollections, but if there are none
      (or if there's a chance there may be none), this argument is the only way
      to provide pipeline information, and should be considered mandatory.
  """
  def __init__(self, *, pipeline=None):
    self.pipeline = pipeline

  def _extract_input_pvalues(self, pvalueish):
    try:
      # If this works, it's a dict.
      return pvalueish, tuple(pvalueish.values())
    except AttributeError:
      # Cast iterables a tuple so we can do re-iteration.
      pcolls = tuple(pvalueish)
      return pcolls, pcolls

  def expand(self, pcolls):
    if not pcolls:
      pcolls = (self.pipeline | Create([]), )
    if isinstance(pcolls, dict):
      tags = list(pcolls.keys())
      if all(isinstance(tag, str) and len(tag) < 10 for tag in tags):
        # Small, string tags. Pass them as data.
        pcolls_dict = pcolls
        restore_tags = None
      else:
        # Pass the tags in the restore_tags closure.
        tags = list(pcolls.keys())
        pcolls_dict = {str(ix): pcolls[tag] for (ix, tag) in enumerate(tags)}
        restore_tags = lambda vs: {
            tag: vs[str(ix)]
            for (ix, tag) in enumerate(tags)
        }
    else:
      # Tags are tuple indices.
      tags = [str(ix) for ix in range(len(pcolls))]
      pcolls_dict = dict(zip(tags, pcolls))
      restore_tags = lambda vs: tuple(vs[tag] for tag in tags)

    input_key_types = []
    input_value_types = []
    for pcoll in pcolls_dict.values():
      key_type, value_type = typehints.trivial_inference.key_value_types(
          pcoll.element_type)
      input_key_types.append(key_type)
      input_value_types.append(value_type)
    output_key_type = typehints.Union[tuple(input_key_types)]
    iterable_input_value_types = tuple(
        typehints.Iterable[t] for t in input_value_types)

    output_value_type = typehints.Dict[
        str, typehints.Union[iterable_input_value_types or [typehints.Any]]]
    result = (
        pcolls_dict
        | 'CoGroupByKeyImpl' >>
        _CoGBKImpl(pipeline=self.pipeline).with_output_types(
            typehints.Tuple[output_key_type, output_value_type]))

    if restore_tags:
      if isinstance(pcolls, dict):
        dict_key_type = typehints.Union[tuple(
            trivial_inference.instance_to_type(tag) for tag in tags)]
        output_value_type = typehints.Dict[
            dict_key_type, typehints.Union[iterable_input_value_types]]
      else:
        output_value_type = typehints.Tuple[iterable_input_value_types]
      result |= 'RestoreTags' >> MapTuple(
          lambda k, vs: (k, restore_tags(vs))).with_output_types(
              typehints.Tuple[output_key_type, output_value_type])

    return result


class _CoGBKImpl(PTransform):
  def __init__(self, *, pipeline=None):
    self.pipeline = pipeline

  def expand(self, pcolls):
    # Check input PCollections for PCollection-ness, and that they all belong
    # to the same pipeline.
    for pcoll in pcolls.values():
      self._check_pcollection(pcoll)
      if self.pipeline:
        assert pcoll.pipeline == self.pipeline

    tags = list(pcolls.keys())

    def add_tag(tag):
      return lambda k, v: (k, (tag, v))

    def collect_values(key, tagged_values):
      grouped_values = {tag: [] for tag in tags}
      for tag, value in tagged_values:
        grouped_values[tag].append(value)
      return key, grouped_values

    return ([
        pcoll
        | 'Tag[%s]' % tag >> MapTuple(add_tag(tag))
        for (tag, pcoll) in pcolls.items()
    ]
            | Flatten(pipeline=self.pipeline)
            | GroupByKey()
            | MapTuple(collect_values))


@ptransform_fn
@typehints.with_input_types(Tuple[K, V])
@typehints.with_output_types(K)
def Keys(pcoll, label='Keys'):  # pylint: disable=invalid-name
  """Produces a PCollection of first elements of 2-tuples in a PCollection."""
  return pcoll | label >> MapTuple(lambda k, _: k)


@ptransform_fn
@typehints.with_input_types(Tuple[K, V])
@typehints.with_output_types(V)
def Values(pcoll, label='Values'):  # pylint: disable=invalid-name
  """Produces a PCollection of second elements of 2-tuples in a PCollection."""
  return pcoll | label >> MapTuple(lambda _, v: v)


@ptransform_fn
@typehints.with_input_types(Tuple[K, V])
@typehints.with_output_types(Tuple[V, K])
def KvSwap(pcoll, label='KvSwap'):  # pylint: disable=invalid-name
  """Produces a PCollection reversing 2-tuples in a PCollection."""
  return pcoll | label >> MapTuple(lambda k, v: (v, k))


@ptransform_fn
@typehints.with_input_types(T)
@typehints.with_output_types(T)
def Distinct(pcoll):  # pylint: disable=invalid-name
  """Produces a PCollection containing distinct elements of a PCollection."""
  return (
      pcoll
      | 'ToPairs' >> Map(lambda v: (v, None))
      | 'Group' >> CombinePerKey(lambda vs: None)
      | 'Distinct' >> Keys())


@deprecated(since='2.12', current='Distinct')
@ptransform_fn
@typehints.with_input_types(T)
@typehints.with_output_types(T)
def RemoveDuplicates(pcoll):
  """Produces a PCollection containing distinct elements of a PCollection."""
  return pcoll | 'RemoveDuplicates' >> Distinct()


class _BatchSizeEstimator(object):
  """Estimates the best size for batches given historical timing.
  """

  _MAX_DATA_POINTS = 100
  _MAX_GROWTH_FACTOR = 2

  def __init__(
      self,
      min_batch_size=1,
      max_batch_size=10000,
      target_batch_overhead=.05,
      target_batch_duration_secs=1,
      variance=0.25,
      clock=time.time,
      ignore_first_n_seen_per_batch_size=0):
    if min_batch_size > max_batch_size:
      raise ValueError(
          "Minimum (%s) must not be greater than maximum (%s)" %
          (min_batch_size, max_batch_size))
    if target_batch_overhead and not 0 < target_batch_overhead <= 1:
      raise ValueError(
          "target_batch_overhead (%s) must be between 0 and 1" %
          (target_batch_overhead))
    if target_batch_duration_secs and target_batch_duration_secs <= 0:
      raise ValueError(
          "target_batch_duration_secs (%s) must be positive" %
          (target_batch_duration_secs))
    if not (target_batch_overhead or target_batch_duration_secs):
      raise ValueError(
          "At least one of target_batch_overhead or "
          "target_batch_duration_secs must be positive.")
    if ignore_first_n_seen_per_batch_size < 0:
      raise ValueError(
          'ignore_first_n_seen_per_batch_size (%s) must be non '
          'negative' % (ignore_first_n_seen_per_batch_size))
    self._min_batch_size = min_batch_size
    self._max_batch_size = max_batch_size
    self._target_batch_overhead = target_batch_overhead
    self._target_batch_duration_secs = target_batch_duration_secs
    self._variance = variance
    self._clock = clock
    self._data = []
    self._ignore_next_timing = False
    self._ignore_first_n_seen_per_batch_size = (
        ignore_first_n_seen_per_batch_size)
    self._batch_size_num_seen = {}
    self._replay_last_batch_size = None

    self._size_distribution = Metrics.distribution(
        'BatchElements', 'batch_size')
    self._time_distribution = Metrics.distribution(
        'BatchElements', 'msec_per_batch')
    # Beam distributions only accept integer values, so we use this to
    # accumulate under-reported values until they add up to whole milliseconds.
    # (Milliseconds are chosen because that's conventionally used elsewhere in
    # profiling-style counters.)
    self._remainder_msecs = 0

  def ignore_next_timing(self):
    """Call to indicate the next timing should be ignored.

    For example, the first emit of a ParDo operation is known to be anomalous
    due to setup that may occur.
    """
    self._ignore_next_timing = True

  @contextlib.contextmanager
  def record_time(self, batch_size):
    start = self._clock()
    yield
    elapsed = self._clock() - start
    elapsed_msec = 1e3 * elapsed + self._remainder_msecs
    self._size_distribution.update(batch_size)
    self._time_distribution.update(int(elapsed_msec))
    self._remainder_msecs = elapsed_msec - int(elapsed_msec)
    # If we ignore the next timing, replay the batch size to get accurate
    # timing.
    if self._ignore_next_timing:
      self._ignore_next_timing = False
      self._replay_last_batch_size = min(batch_size, self._max_batch_size)
    else:
      self._data.append((batch_size, elapsed))
      if len(self._data) >= self._MAX_DATA_POINTS:
        self._thin_data()

  def _thin_data(self):
    # Make sure we don't change the parity of len(self._data)
    # As it's used below to alternate jitter.
    self._data.pop(random.randrange(len(self._data) // 4))
    self._data.pop(random.randrange(len(self._data) // 2))

  @staticmethod
  def linear_regression_no_numpy(xs, ys):
    # Least squares fit for y = a + bx over all points.
    n = float(len(xs))
    xbar = sum(xs) / n
    ybar = sum(ys) / n
    if xbar == 0:
      return ybar, 0
    if all(xs[0] == x for x in xs):
      # Simply use the mean if all values in xs are same.
      return 0, ybar / xbar
    b = (
        sum([(x - xbar) * (y - ybar)
             for x, y in zip(xs, ys)]) / sum([(x - xbar)**2 for x in xs]))
    a = ybar - b * xbar
    return a, b

  @staticmethod
  def linear_regression_numpy(xs, ys):
    # pylint: disable=wrong-import-order, wrong-import-position
    import numpy as np
    from numpy import sum
    n = len(xs)
    if all(xs[0] == x for x in xs):
      # If all values of xs are same then fallback to linear_regression_no_numpy
      return _BatchSizeEstimator.linear_regression_no_numpy(xs, ys)
    xs = np.asarray(xs, dtype=float)
    ys = np.asarray(ys, dtype=float)

    # First do a simple least squares fit for y = a + bx over all points.
    b, a = np.polyfit(xs, ys, 1)

    if n < 10:
      return a, b
    else:
      # Refine this by throwing out outliers, according to Cook's distance.
      # https://en.wikipedia.org/wiki/Cook%27s_distance
      sum_x = sum(xs)
      sum_x2 = sum(xs**2)
      errs = a + b * xs - ys
      s2 = sum(errs**2) / (n - 2)
      if s2 == 0:
        # It's an exact fit!
        return a, b
      h = (sum_x2 - 2 * sum_x * xs + n * xs**2) / (n * sum_x2 - sum_x**2)
      cook_ds = 0.5 / s2 * errs**2 * (h / (1 - h)**2)

      # Re-compute the regression, excluding those points with Cook's distance
      # greater than 0.5, and weighting by the inverse of x to give a more
      # stable y-intercept (as small batches have relatively more information
      # about the fixed overhead).
      weight = (cook_ds <= 0.5) / xs
      b, a = np.polyfit(xs, ys, 1, w=weight)
      return a, b

  try:
    # pylint: disable=wrong-import-order, wrong-import-position
    import numpy as np
    linear_regression = linear_regression_numpy
  except ImportError:
    linear_regression = linear_regression_no_numpy

  def _calculate_next_batch_size(self):
    if self._min_batch_size == self._max_batch_size:
      return self._min_batch_size
    elif len(self._data) < 1:
      return self._min_batch_size
    elif len(self._data) < 2:
      # Force some variety so we have distinct batch sizes on which to do
      # linear regression below.
      return int(
          max(
              min(
                  self._max_batch_size,
                  self._min_batch_size * self._MAX_GROWTH_FACTOR),
              self._min_batch_size + 1))

    # There tends to be a lot of noise in the top quantile, which also
    # has outsided influence in the regression.  If we have enough data,
    # Simply declare the top 20% to be outliers.
    trimmed_data = sorted(self._data)[:max(20, len(self._data) * 4 // 5)]

    # Linear regression for y = a + bx, where x is batch size and y is time.
    xs, ys = zip(*trimmed_data)
    a, b = self.linear_regression(xs, ys)

    # Avoid nonsensical or division-by-zero errors below due to noise.
    a = max(a, 1e-10)
    b = max(b, 1e-20)

    last_batch_size = self._data[-1][0]
    cap = min(last_batch_size * self._MAX_GROWTH_FACTOR, self._max_batch_size)

    target = self._max_batch_size

    if self._target_batch_duration_secs:
      # Solution to a + b*x = self._target_batch_duration_secs.
      target = min(target, (self._target_batch_duration_secs - a) / b)

    if self._target_batch_overhead:
      # Solution to a / (a + b*x) = self._target_batch_overhead.
      target = min(target, (a / b) * (1 / self._target_batch_overhead - 1))

    # Avoid getting stuck at a single batch size (especially the minimal
    # batch size) which would not allow us to extrapolate to other batch
    # sizes.
    # Jitter alternates between 0 and 1.
    jitter = len(self._data) % 2
    # Smear our samples across a range centered at the target.
    if len(self._data) > 10:
      target += int(target * self._variance * 2 * (random.random() - .5))

    return int(max(self._min_batch_size + jitter, min(target, cap)))

  def next_batch_size(self):
    # Check if we should replay a previous batch size due to it not being
    # recorded.
    if self._replay_last_batch_size:
      result = self._replay_last_batch_size
      self._replay_last_batch_size = None
    else:
      result = self._calculate_next_batch_size()

    seen_count = self._batch_size_num_seen.get(result, 0) + 1
    if seen_count <= self._ignore_first_n_seen_per_batch_size:
      self.ignore_next_timing()
    self._batch_size_num_seen[result] = seen_count
    return result


class _GlobalWindowsBatchingDoFn(DoFn):
  def __init__(self, batch_size_estimator, element_size_fn):
    self._batch_size_estimator = batch_size_estimator
    self._element_size_fn = element_size_fn

  def start_bundle(self):
    self._batch = []
    self._running_batch_size = 0
    self._target_batch_size = self._batch_size_estimator.next_batch_size()
    # The first emit often involves non-trivial setup.
    self._batch_size_estimator.ignore_next_timing()

  def process(self, element):
    self._batch.append(element)
    self._running_batch_size += self._element_size_fn(element)
    if self._running_batch_size >= self._target_batch_size:
      with self._batch_size_estimator.record_time(self._running_batch_size):
        yield window.GlobalWindows.windowed_value_at_end_of_window(self._batch)
      self._batch = []
      self._running_batch_size = 0
      self._target_batch_size = self._batch_size_estimator.next_batch_size()

  def finish_bundle(self):
    if self._batch:
      with self._batch_size_estimator.record_time(self._running_batch_size):
        yield window.GlobalWindows.windowed_value_at_end_of_window(self._batch)
      self._batch = None
      self._running_batch_size = 0
    self._target_batch_size = self._batch_size_estimator.next_batch_size()


class _SizedBatch():
  def __init__(self):
    self.elements = []
    self.size = 0


class _WindowAwareBatchingDoFn(DoFn):

  _MAX_LIVE_WINDOWS = 10

  def __init__(self, batch_size_estimator, element_size_fn):
    self._batch_size_estimator = batch_size_estimator
    self._element_size_fn = element_size_fn

  def start_bundle(self):
    self._batches = collections.defaultdict(_SizedBatch)
    self._target_batch_size = self._batch_size_estimator.next_batch_size()
    # The first emit often involves non-trivial setup.
    self._batch_size_estimator.ignore_next_timing()

  def process(self, element, window=DoFn.WindowParam):
    batch = self._batches[window]
    batch.elements.append(element)
    batch.size += self._element_size_fn(element)
    if batch.size >= self._target_batch_size:
      with self._batch_size_estimator.record_time(batch.size):
        yield windowed_value.WindowedValue(
            batch.elements, window.max_timestamp(), (window, ))
      del self._batches[window]
      self._target_batch_size = self._batch_size_estimator.next_batch_size()
    elif len(self._batches) > self._MAX_LIVE_WINDOWS:
      window, batch = max(
          self._batches.items(),
          key=lambda window_batch: window_batch[1].size)
      with self._batch_size_estimator.record_time(batch.size):
        yield windowed_value.WindowedValue(
            batch.elements, window.max_timestamp(), (window, ))
      del self._batches[window]
      self._target_batch_size = self._batch_size_estimator.next_batch_size()

  def finish_bundle(self):
    for window, batch in self._batches.items():
      if batch:
        with self._batch_size_estimator.record_time(batch.size):
          yield windowed_value.WindowedValue(
              batch.elements, window.max_timestamp(), (window, ))
    self._batches = None
    self._target_batch_size = self._batch_size_estimator.next_batch_size()


@typehints.with_input_types(T)
@typehints.with_output_types(List[T])
class BatchElements(PTransform):
  """A Transform that batches elements for amortized processing.

  This transform is designed to precede operations whose processing cost
  is of the form

      time = fixed_cost + num_elements * per_element_cost

  where the per element cost is (often significantly) smaller than the fixed
  cost and could be amortized over multiple elements.  It consumes a PCollection
  of element type T and produces a PCollection of element type List[T].

  This transform attempts to find the best batch size between the minimim
  and maximum parameters by profiling the time taken by (fused) downstream
  operations. For a fixed batch size, set the min and max to be equal.

  Elements are batched per-window and batches emitted in the window
  corresponding to its contents.

  Args:
    min_batch_size: (optional) the smallest size of a batch
    max_batch_size: (optional) the largest size of a batch
    target_batch_overhead: (optional) a target for fixed_cost / time,
        as used in the formula above
    target_batch_duration_secs: (optional) a target for total time per bundle,
        in seconds
    element_size_fn: (optional) A mapping of an element to its contribution to
        batch size, defaulting to every element having size 1.  When provided,
        attempts to provide batches of optimal total size which may consist of
        a varying number of elements.
    variance: (optional) the permitted (relative) amount of deviation from the
        (estimated) ideal batch size used to produce a wider base for
        linear interpolation
    clock: (optional) an alternative to time.time for measuring the cost of
        donwstream operations (mostly for testing)
  """
  def __init__(
      self,
      min_batch_size=1,
      max_batch_size=10000,
      target_batch_overhead=.05,
      target_batch_duration_secs=1,
      *,
      element_size_fn=lambda x: 1,
      variance=0.25,
      clock=time.time):
    self._batch_size_estimator = _BatchSizeEstimator(
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        target_batch_overhead=target_batch_overhead,
        target_batch_duration_secs=target_batch_duration_secs,
        variance=variance,
        clock=clock)
    self._element_size_fn = element_size_fn

  def expand(self, pcoll):
    if getattr(pcoll.pipeline.runner, 'is_streaming', False):
      raise NotImplementedError("Requires stateful processing (BEAM-2687)")
    elif pcoll.windowing.is_default():
      # This is the same logic as _GlobalWindowsBatchingDoFn, but optimized
      # for that simpler case.
      return pcoll | ParDo(
          _GlobalWindowsBatchingDoFn(
              self._batch_size_estimator, self._element_size_fn))
    else:
      return pcoll | ParDo(
          _WindowAwareBatchingDoFn(
              self._batch_size_estimator, self._element_size_fn))


class _IdentityWindowFn(NonMergingWindowFn):
  """Windowing function that preserves existing windows.

  To be used internally with the Reshuffle transform.
  Will raise an exception when used after DoFns that return TimestampedValue
  elements.
  """
  def __init__(self, window_coder):
    """Create a new WindowFn with compatible coder.
    To be applied to PCollections with windows that are compatible with the
    given coder.

    Arguments:
      window_coder: coders.Coder object to be used on windows.
    """
    super().__init__()
    if window_coder is None:
      raise ValueError('window_coder should not be None')
    self._window_coder = window_coder

  def assign(self, assign_context):
    if assign_context.window is None:
      raise ValueError(
          'assign_context.window should not be None. '
          'This might be due to a DoFn returning a TimestampedValue.')
    return [assign_context.window]

  def get_window_coder(self):
    return self._window_coder


@typehints.with_input_types(Tuple[K, V])
@typehints.with_output_types(Tuple[K, V])
class ReshufflePerKey(PTransform):
  """PTransform that returns a PCollection equivalent to its input,
  but operationally provides some of the side effects of a GroupByKey,
  in particular checkpointing, and preventing fusion of the surrounding
  transforms.

  ReshufflePerKey is experimental. No backwards compatibility guarantees.
  """
  def expand(self, pcoll):
    windowing_saved = pcoll.windowing
    if windowing_saved.is_default():
      # In this (common) case we can use a trivial trigger driver
      # and avoid the (expensive) window param.
      globally_windowed = window.GlobalWindows.windowed_value(None)
      MIN_TIMESTAMP = window.MIN_TIMESTAMP

      def reify_timestamps(element, timestamp=DoFn.TimestampParam):
        key, value = element
        if timestamp == MIN_TIMESTAMP:
          timestamp = None
        return key, (value, timestamp)

      def restore_timestamps(element):
        key, values = element
        return [
            globally_windowed.with_value((key, value)) if timestamp is None else
            window.GlobalWindows.windowed_value((key, value), timestamp)
            for (value, timestamp) in values
        ]
    else:

      # typing: All conditional function variants must have identical signatures
      def reify_timestamps(  # type: ignore[misc]
          element, timestamp=DoFn.TimestampParam, window=DoFn.WindowParam):
        key, value = element
        # Transport the window as part of the value and restore it later.
        return key, windowed_value.WindowedValue(value, timestamp, [window])

      def restore_timestamps(element):
        key, windowed_values = element
        return [wv.with_value((key, wv.value)) for wv in windowed_values]

    ungrouped = pcoll | Map(reify_timestamps).with_output_types(Any)

    # TODO(https://github.com/apache/beam/issues/19785) Using global window as
    # one of the standard window. This is to mitigate the Dataflow Java Runner
    # Harness limitation to accept only standard coders.
    ungrouped._windowing = Windowing(
        window.GlobalWindows(),
        triggerfn=Always(),
        accumulation_mode=AccumulationMode.DISCARDING,
        timestamp_combiner=TimestampCombiner.OUTPUT_AT_EARLIEST)
    result = (
        ungrouped
        | GroupByKey()
        | FlatMap(restore_timestamps).with_output_types(Any))
    result._windowing = windowing_saved
    return result


@typehints.with_input_types(T)
@typehints.with_output_types(T)
class Reshuffle(PTransform):
  """PTransform that returns a PCollection equivalent to its input,
  but operationally provides some of the side effects of a GroupByKey,
  in particular checkpointing, and preventing fusion of the surrounding
  transforms.

  Reshuffle adds a temporary random key to each element, performs a
  ReshufflePerKey, and finally removes the temporary key.

  Reshuffle is experimental. No backwards compatibility guarantees.
  """

  # We use 32-bit integer as the default number of buckets.
  _DEFAULT_NUM_BUCKETS = 1 << 32

  def __init__(self, num_buckets=None):
    """
    :param num_buckets: If set, specifies the maximum random keys that would be
      generated.
    """
    self.num_buckets = num_buckets if num_buckets else self._DEFAULT_NUM_BUCKETS

    valid_buckets = isinstance(num_buckets, int) and num_buckets > 0
    if not (num_buckets is None or valid_buckets):
      raise ValueError(
          'If `num_buckets` is set, it has to be an '
          'integer greater than 0, got %s' % num_buckets)

  def expand(self, pcoll):
    # type: (pvalue.PValue) -> pvalue.PCollection
    return (
        pcoll | 'AddRandomKeys' >>
        Map(lambda t: (random.randrange(0, self.num_buckets), t)
            ).with_input_types(T).with_output_types(Tuple[int, T])
        | ReshufflePerKey()
        | 'RemoveRandomKeys' >> Map(lambda t: t[1]).with_input_types(
            Tuple[int, T]).with_output_types(T))

  def to_runner_api_parameter(self, unused_context):
    # type: (PipelineContext) -> Tuple[str, None]
    return common_urns.composites.RESHUFFLE.urn, None

  @staticmethod
  @PTransform.register_urn(common_urns.composites.RESHUFFLE.urn, None)
  def from_runner_api_parameter(
      unused_ptransform, unused_parameter, unused_context):
    return Reshuffle()


def fn_takes_side_inputs(fn):
  try:
    signature = get_signature(fn)
  except TypeError:
    # We can't tell; maybe it does.
    return True

  return (
      len(signature.parameters) > 1 or any(
          p.kind == p.VAR_POSITIONAL or p.kind == p.VAR_KEYWORD
          for p in signature.parameters.values()))


@ptransform_fn
def WithKeys(pcoll, k, *args, **kwargs):
  """PTransform that takes a PCollection, and either a constant key or a
  callable, and returns a PCollection of (K, V), where each of the values in
  the input PCollection has been paired with either the constant key or a key
  computed from the value.  The callable may optionally accept positional or
  keyword arguments, which should be passed to WithKeys directly.  These may
  be either SideInputs or static (non-PCollection) values, such as ints.
  """
  if callable(k):
    if fn_takes_side_inputs(k):
      if all(isinstance(arg, AsSideInput)
             for arg in args) and all(isinstance(kwarg, AsSideInput)
                                      for kwarg in kwargs.values()):
        return pcoll | Map(
            lambda v,
            *args,
            **kwargs: (k(v, *args, **kwargs), v),
            *args,
            **kwargs)
      return pcoll | Map(lambda v: (k(v, *args, **kwargs), v))
    return pcoll | Map(lambda v: (k(v), v))
  return pcoll | Map(lambda v: (k, v))


@experimental()
@typehints.with_input_types(Tuple[K, V])
@typehints.with_output_types(Tuple[K, Iterable[V]])
class GroupIntoBatches(PTransform):
  """PTransform that batches the input into desired batch size. Elements are
  buffered until they are equal to batch size provided in the argument at which
  point they are output to the output Pcollection.

  Windows are preserved (batches will contain elements from the same window)

  GroupIntoBatches is experimental. Its use case will depend on the runner if
  it has support of States and Timers.
  """
  def __init__(
      self, batch_size, max_buffering_duration_secs=None, clock=time.time):
    """Create a new GroupIntoBatches.

    Arguments:
      batch_size: (required) How many elements should be in a batch
      max_buffering_duration_secs: (optional) How long in seconds at most an
        incomplete batch of elements is allowed to be buffered in the states.
        The duration must be a positive second duration and should be given as
        an int or float. Setting this parameter to zero effectively means no
        buffering limit.
      clock: (optional) an alternative to time.time (mostly for testing)
    """
    self.params = _GroupIntoBatchesParams(
        batch_size, max_buffering_duration_secs)
    self.clock = clock

  def expand(self, pcoll):
    input_coder = coders.registry.get_coder(pcoll)
    return pcoll | ParDo(
        _pardo_group_into_batches(
            input_coder,
            self.params.batch_size,
            self.params.max_buffering_duration_secs,
            self.clock))

  def to_runner_api_parameter(
      self,
      unused_context  # type: PipelineContext
  ):  # type: (...) -> Tuple[str, beam_runner_api_pb2.GroupIntoBatchesPayload]
    return (
        common_urns.group_into_batches_components.GROUP_INTO_BATCHES.urn,
        self.params.get_payload())

  @staticmethod
  @PTransform.register_urn(
      common_urns.group_into_batches_components.GROUP_INTO_BATCHES.urn,
      beam_runner_api_pb2.GroupIntoBatchesPayload)
  def from_runner_api_parameter(unused_ptransform, proto, unused_context):
    return GroupIntoBatches(*_GroupIntoBatchesParams.parse_payload(proto))

  @typehints.with_input_types(Tuple[K, V])
  @typehints.with_output_types(
      typehints.Tuple[
          ShardedKeyType[typehints.TypeVariable(K)],  # type: ignore[misc]
          typehints.Iterable[typehints.TypeVariable(V)]])
  class WithShardedKey(PTransform):
    """A GroupIntoBatches transform that outputs batched elements associated
    with sharded input keys.

    By default, keys are sharded to such that the input elements with the same
    key are spread to all available threads executing the transform. Runners may
    override the default sharding to do a better load balancing during the
    execution time.
    """
    def __init__(
        self, batch_size, max_buffering_duration_secs=None, clock=time.time):
      """Create a new GroupIntoBatches with sharded output.
      See ``GroupIntoBatches`` transform for a description of input parameters.
      """
      self.params = _GroupIntoBatchesParams(
          batch_size, max_buffering_duration_secs)
      self.clock = clock

    _shard_id_prefix = uuid.uuid4().bytes

    def expand(self, pcoll):
      key_type, value_type = pcoll.element_type.tuple_types
      sharded_pcoll = pcoll | Map(
          lambda key_value: (
              ShardedKey(
                  key_value[0],
                  # Use [uuid, thread id] as the shard id.
                  GroupIntoBatches.WithShardedKey._shard_id_prefix + bytes(
                      threading.get_ident().to_bytes(8, 'big'))),
              key_value[1])).with_output_types(
                  typehints.Tuple[
                      ShardedKeyType[key_type],  # type: ignore[misc]
                      value_type])
      return (
          sharded_pcoll
          | GroupIntoBatches(
              self.params.batch_size,
              self.params.max_buffering_duration_secs,
              self.clock))

    def to_runner_api_parameter(
        self,
        unused_context  # type: PipelineContext
    ):  # type: (...) -> Tuple[str, beam_runner_api_pb2.GroupIntoBatchesPayload]
      return (
          common_urns.composites.GROUP_INTO_BATCHES_WITH_SHARDED_KEY.urn,
          self.params.get_payload())

    @staticmethod
    @PTransform.register_urn(
        common_urns.composites.GROUP_INTO_BATCHES_WITH_SHARDED_KEY.urn,
        beam_runner_api_pb2.GroupIntoBatchesPayload)
    def from_runner_api_parameter(unused_ptransform, proto, unused_context):
      return GroupIntoBatches.WithShardedKey(
          *_GroupIntoBatchesParams.parse_payload(proto))


class _GroupIntoBatchesParams:
  """This class represents the parameters for
  :class:`apache_beam.utils.GroupIntoBatches` transform, used to define how
  elements should be batched.
  """
  def __init__(self, batch_size, max_buffering_duration_secs):
    self.batch_size = batch_size
    self.max_buffering_duration_secs = (
        0
        if max_buffering_duration_secs is None else max_buffering_duration_secs)
    self._validate()

  def __eq__(self, other):
    if other is None or not isinstance(other, _GroupIntoBatchesParams):
      return False
    return (
        self.batch_size == other.batch_size and
        self.max_buffering_duration_secs == other.max_buffering_duration_secs)

  def _validate(self):
    assert self.batch_size is not None and self.batch_size > 0, (
        'batch_size must be a positive value')
    assert (
        self.max_buffering_duration_secs is not None and
        self.max_buffering_duration_secs >= 0), (
            'max_buffering_duration must be a non-negative value')

  def get_payload(self):
    return beam_runner_api_pb2.GroupIntoBatchesPayload(
        batch_size=self.batch_size,
        max_buffering_duration_millis=int(
            self.max_buffering_duration_secs * 1000))

  @staticmethod
  def parse_payload(
      proto  # type: beam_runner_api_pb2.GroupIntoBatchesPayload
  ):
    return proto.batch_size, proto.max_buffering_duration_millis / 1000


def _pardo_group_into_batches(
    input_coder, batch_size, max_buffering_duration_secs, clock=time.time):
  ELEMENT_STATE = BagStateSpec('values', input_coder)
  COUNT_STATE = CombiningValueStateSpec('count', input_coder, CountCombineFn())
  WINDOW_TIMER = TimerSpec('window_end', TimeDomain.WATERMARK)
  BUFFERING_TIMER = TimerSpec('buffering_end', TimeDomain.REAL_TIME)

  class _GroupIntoBatchesDoFn(DoFn):
    def process(
        self,
        element,
        window=DoFn.WindowParam,
        element_state=DoFn.StateParam(ELEMENT_STATE),
        count_state=DoFn.StateParam(COUNT_STATE),
        window_timer=DoFn.TimerParam(WINDOW_TIMER),
        buffering_timer=DoFn.TimerParam(BUFFERING_TIMER)):
      # Allowed lateness not supported in Python SDK
      # https://beam.apache.org/documentation/programming-guide/#watermarks-and-late-data
      window_timer.set(window.end)
      element_state.add(element)
      count_state.add(1)
      count = count_state.read()
      if count == 1 and max_buffering_duration_secs > 0:
        # This is the first element in batch. Start counting buffering time if a
        # limit was set.
        # pylint: disable=deprecated-method
        buffering_timer.set(clock() + max_buffering_duration_secs)
      if count >= batch_size:
        return self.flush_batch(element_state, count_state, buffering_timer)

    @on_timer(WINDOW_TIMER)
    def on_window_timer(
        self,
        element_state=DoFn.StateParam(ELEMENT_STATE),
        count_state=DoFn.StateParam(COUNT_STATE),
        buffering_timer=DoFn.TimerParam(BUFFERING_TIMER)):
      return self.flush_batch(element_state, count_state, buffering_timer)

    @on_timer(BUFFERING_TIMER)
    def on_buffering_timer(
        self,
        element_state=DoFn.StateParam(ELEMENT_STATE),
        count_state=DoFn.StateParam(COUNT_STATE),
        buffering_timer=DoFn.TimerParam(BUFFERING_TIMER)):
      return self.flush_batch(element_state, count_state, buffering_timer)

    def flush_batch(self, element_state, count_state, buffering_timer):
      batch = [element for element in element_state.read()]
      if not batch:
        return
      key, _ = batch[0]
      batch_values = [v for (k, v) in batch]
      element_state.clear()
      count_state.clear()
      buffering_timer.clear()
      yield key, batch_values

  return _GroupIntoBatchesDoFn()


class ToString(object):
  """
  PTransform for converting a PCollection element, KV or PCollection Iterable
  to string.
  """

  # pylint: disable=invalid-name
  @staticmethod
  def Element():
    """
    Transforms each element of the PCollection to a string.
    """
    return 'ElementToString' >> Map(str)

  @staticmethod
  def Iterables(delimiter=None):
    """
    Transforms each item in the iterable of the input of PCollection to a
    string. There is no trailing delimiter.
    """
    if delimiter is None:
      delimiter = ','
    return (
        'IterablesToString' >>
        Map(lambda xs: delimiter.join(str(x) for x in xs)).with_input_types(
            Iterable[Any]).with_output_types(str))

  # An alias for Iterables.
  Kvs = Iterables


class Reify(object):
  """PTransforms for converting between explicit and implicit form of various
  Beam values."""
  @typehints.with_input_types(T)
  @typehints.with_output_types(T)
  class Timestamp(PTransform):
    """PTransform to wrap a value in a TimestampedValue with it's
    associated timestamp."""
    @staticmethod
    def add_timestamp_info(element, timestamp=DoFn.TimestampParam):
      yield TimestampedValue(element, timestamp)

    def expand(self, pcoll):
      return pcoll | ParDo(self.add_timestamp_info)

  @typehints.with_input_types(T)
  @typehints.with_output_types(T)
  class Window(PTransform):
    """PTransform to convert an element in a PCollection into a tuple of
    (element, timestamp, window), wrapped in a TimestampedValue with it's
    associated timestamp."""
    @staticmethod
    def add_window_info(
        element, timestamp=DoFn.TimestampParam, window=DoFn.WindowParam):
      yield TimestampedValue((element, timestamp, window), timestamp)

    def expand(self, pcoll):
      return pcoll | ParDo(self.add_window_info)

  @typehints.with_input_types(Tuple[K, V])
  @typehints.with_output_types(Tuple[K, V])
  class TimestampInValue(PTransform):
    """PTransform to wrap the Value in a KV pair in a TimestampedValue with
    the element's associated timestamp."""
    @staticmethod
    def add_timestamp_info(element, timestamp=DoFn.TimestampParam):
      key, value = element
      yield (key, TimestampedValue(value, timestamp))

    def expand(self, pcoll):
      return pcoll | ParDo(self.add_timestamp_info)

  @typehints.with_input_types(Tuple[K, V])
  @typehints.with_output_types(Tuple[K, V])
  class WindowInValue(PTransform):
    """PTransform to convert the Value in a KV pair into a tuple of
    (value, timestamp, window), with the whole element being wrapped inside a
    TimestampedValue."""
    @staticmethod
    def add_window_info(
        element, timestamp=DoFn.TimestampParam, window=DoFn.WindowParam):
      key, value = element
      yield TimestampedValue((key, (value, timestamp, window)), timestamp)

    def expand(self, pcoll):
      return pcoll | ParDo(self.add_window_info)


class Regex(object):
  """
  PTransform  to use Regular Expression to process the elements in a
  PCollection.
  """

  ALL = "__regex_all_groups"

  @staticmethod
  def _regex_compile(regex):
    """Return re.compile if the regex has a string value"""
    if isinstance(regex, str):
      regex = re.compile(regex)
    return regex

  @staticmethod
  @typehints.with_input_types(str)
  @typehints.with_output_types(str)
  @ptransform_fn
  def matches(pcoll, regex, group=0):
    """
    Returns the matches (group 0 by default) if zero or more characters at the
    beginning of string match the regular expression. To match the entire
    string, add "$" sign at the end of regex expression.

    Group can be integer value or a string value.

    Args:
      regex: the regular expression string or (re.compile) pattern.
      group: (optional) name/number of the group, it can be integer or a string
        value. Defaults to 0, meaning the entire matched string will be
        returned.
    """
    regex = Regex._regex_compile(regex)

    def _process(element):
      m = regex.match(element)
      if m:
        yield m.group(group)

    return pcoll | FlatMap(_process)

  @staticmethod
  @typehints.with_input_types(str)
  @typehints.with_output_types(List[str])
  @ptransform_fn
  def all_matches(pcoll, regex):
    """
    Returns all matches (groups) if zero or more characters at the beginning
    of string match the regular expression.

    Args:
      regex: the regular expression string or (re.compile) pattern.
    """
    regex = Regex._regex_compile(regex)

    def _process(element):
      m = regex.match(element)
      if m:
        yield [m.group(ix) for ix in range(m.lastindex + 1)]

    return pcoll | FlatMap(_process)

  @staticmethod
  @typehints.with_input_types(str)
  @typehints.with_output_types(Tuple[str, str])
  @ptransform_fn
  def matches_kv(pcoll, regex, keyGroup, valueGroup=0):
    """
    Returns the KV pairs if the string matches the regular expression, deriving
    the key & value from the specified group of the regular expression.

    Args:
      regex: the regular expression string or (re.compile) pattern.
      keyGroup: The Regex group to use as the key. Can be int or str.
      valueGroup: (optional) Regex group to use the value. Can be int or str.
        The default value "0" returns entire matched string.
    """
    regex = Regex._regex_compile(regex)

    def _process(element):
      match = regex.match(element)
      if match:
        yield (match.group(keyGroup), match.group(valueGroup))

    return pcoll | FlatMap(_process)

  @staticmethod
  @typehints.with_input_types(str)
  @typehints.with_output_types(str)
  @ptransform_fn
  def find(pcoll, regex, group=0):
    """
    Returns the matches if a portion of the line matches the Regex. Returns
    the entire group (group 0 by default). Group can be integer value or a
    string value.

    Args:
      regex: the regular expression string or (re.compile) pattern.
      group: (optional) name of the group, it can be integer or a string value.
    """
    regex = Regex._regex_compile(regex)

    def _process(element):
      r = regex.search(element)
      if r:
        yield r.group(group)

    return pcoll | FlatMap(_process)

  @staticmethod
  @typehints.with_input_types(str)
  @typehints.with_output_types(Union[List[str], List[Tuple[str, str]]])
  @ptransform_fn
  def find_all(pcoll, regex, group=0, outputEmpty=True):
    """
    Returns the matches if a portion of the line matches the Regex. By default,
    list of group 0 will return with empty items. To get all groups, pass the
    `Regex.ALL` flag in the `group` parameter which returns all the groups in
    the tuple format.

    Args:
      regex: the regular expression string or (re.compile) pattern.
      group: (optional) name of the group, it can be integer or a string value.
      outputEmpty: (optional) Should empty be output. True to output empties
        and false if not.
    """
    regex = Regex._regex_compile(regex)

    def _process(element):
      matches = regex.finditer(element)
      if group == Regex.ALL:
        yield [(m.group(), m.groups()[0]) for m in matches
               if outputEmpty or m.groups()[0]]
      else:
        yield [m.group(group) for m in matches if outputEmpty or m.group(group)]

    return pcoll | FlatMap(_process)

  @staticmethod
  @typehints.with_input_types(str)
  @typehints.with_output_types(Tuple[str, str])
  @ptransform_fn
  def find_kv(pcoll, regex, keyGroup, valueGroup=0):
    """
    Returns the matches if a portion of the line matches the Regex. Returns the
    specified groups as the key and value pair.

    Args:
      regex: the regular expression string or (re.compile) pattern.
      keyGroup: The Regex group to use as the key. Can be int or str.
      valueGroup: (optional) Regex group to use the value. Can be int or str.
        The default value "0" returns entire matched string.
    """
    regex = Regex._regex_compile(regex)

    def _process(element):
      matches = regex.finditer(element)
      if matches:
        for match in matches:
          yield (match.group(keyGroup), match.group(valueGroup))

    return pcoll | FlatMap(_process)

  @staticmethod
  @typehints.with_input_types(str)
  @typehints.with_output_types(str)
  @ptransform_fn
  def replace_all(pcoll, regex, replacement):
    """
    Returns the matches if a portion of the line  matches the regex and
    replaces all matches with the replacement string.

    Args:
      regex: the regular expression string or (re.compile) pattern.
      replacement: the string to be substituted for each match.
    """
    regex = Regex._regex_compile(regex)
    return pcoll | Map(lambda elem: regex.sub(replacement, elem))

  @staticmethod
  @typehints.with_input_types(str)
  @typehints.with_output_types(str)
  @ptransform_fn
  def replace_first(pcoll, regex, replacement):
    """
    Returns the matches if a portion of the line matches the regex and replaces
    the first match with the replacement string.

    Args:
      regex: the regular expression string or (re.compile) pattern.
      replacement: the string to be substituted for each match.
    """
    regex = Regex._regex_compile(regex)
    return pcoll | Map(lambda elem: regex.sub(replacement, elem, 1))

  @staticmethod
  @typehints.with_input_types(str)
  @typehints.with_output_types(List[str])
  @ptransform_fn
  def split(pcoll, regex, outputEmpty=False):
    """
    Returns the list string which was splitted on the basis of regular
    expression. It will not output empty items (by defaults).

    Args:
      regex: the regular expression string or (re.compile) pattern.
      outputEmpty: (optional) Should empty be output. True to output empties
          and false if not.
    """
    regex = Regex._regex_compile(regex)
    outputEmpty = bool(outputEmpty)

    def _process(element):
      r = regex.split(element)
      if r and not outputEmpty:
        r = list(filter(None, r))
      yield r

    return pcoll | FlatMap(_process)
