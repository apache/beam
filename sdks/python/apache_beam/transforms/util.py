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

from __future__ import absolute_import
from __future__ import division

import collections
import contextlib
import random
import time
from builtins import object
from builtins import range
from builtins import zip

from future.utils import itervalues

from apache_beam import typehints
from apache_beam.metrics import Metrics
from apache_beam.portability import common_urns
from apache_beam.transforms import window
from apache_beam.transforms.core import CombinePerKey
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.core import FlatMap
from apache_beam.transforms.core import Flatten
from apache_beam.transforms.core import GroupByKey
from apache_beam.transforms.core import Map
from apache_beam.transforms.core import ParDo
from apache_beam.transforms.core import Windowing
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.ptransform import ptransform_fn
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.trigger import AfterCount
from apache_beam.transforms.window import NonMergingWindowFn
from apache_beam.transforms.window import TimestampCombiner
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils import windowed_value
from apache_beam.utils.annotations import deprecated

__all__ = [
    'BatchElements',
    'CoGroupByKey',
    'Distinct',
    'Keys',
    'KvSwap',
    'RemoveDuplicates',
    'Reshuffle',
    'Values',
    ]

K = typehints.TypeVariable('K')
V = typehints.TypeVariable('V')
T = typehints.TypeVariable('T')


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

  For example, given:

      {'tag1': pc1, 'tag2': pc2, 333: pc3}

  where:
      pc1 = [(k1, v1)]
      pc2 = []
      pc3 = [(k1, v31), (k1, v32), (k2, v33)]

  The output PCollection would be:

      [(k1, {'tag1': [v1], 'tag2': [], 333: [v31, v32]}),
       (k2, {'tag1': [], 'tag2': [], 333: [v33]})]

  CoGroupByKey also works for tuples, lists, or other flat iterables of
  PCollections, in which case the values of the resulting PCollections
  will be tuples whose nth value is the list of values from the nth
  PCollection---conceptually, the "tags" are the indices into the input.
  Thus, for this input::

     (pc1, pc2, pc3)

  the output would be::

      [(k1, ([v1], [], [v31, v32]),
       (k2, ([], [], [v33]))]

  Attributes:
    **kwargs: Accepts a single named argument "pipeline", which specifies the
      pipeline that "owns" this PTransform. Ordinarily CoGroupByKey can obtain
      this information from one of the input PCollections, but if there are none
      (or if there's a chance there may be none), this argument is the only way
      to provide pipeline information, and should be considered mandatory.
  """

  def __init__(self, **kwargs):
    super(CoGroupByKey, self).__init__()
    self.pipeline = kwargs.pop('pipeline', None)
    if kwargs:
      raise ValueError('Unexpected keyword arguments: %s' % list(kwargs.keys()))

  def _extract_input_pvalues(self, pvalueish):
    try:
      # If this works, it's a dict.
      return pvalueish, tuple(itervalues(pvalueish))
    except AttributeError:
      pcolls = tuple(pvalueish)
      return pcolls, pcolls

  def expand(self, pcolls):
    """Performs CoGroupByKey on argument pcolls; see class docstring."""
    # For associating values in K-V pairs with the PCollections they came from.
    def _pair_tag_with_value(key_value, tag):
      (key, value) = key_value
      return (key, (tag, value))

    # Creates the key, value pairs for the output PCollection. Values are either
    # lists or dicts (per the class docstring), initialized by the result of
    # result_ctor(result_ctor_arg).
    def _merge_tagged_vals_under_key(key_grouped, result_ctor,
                                     result_ctor_arg):
      (key, grouped) = key_grouped
      result_value = result_ctor(result_ctor_arg)
      for tag, value in grouped:
        result_value[tag].append(value)
      return (key, result_value)

    try:
      # If pcolls is a dict, we turn it into (tag, pcoll) pairs for use in the
      # general-purpose code below. The result value constructor creates dicts
      # whose keys are the tags.
      result_ctor_arg = list(pcolls)
      result_ctor = lambda tags: dict((tag, []) for tag in tags)
      pcolls = pcolls.items()
    except AttributeError:
      # Otherwise, pcolls is a list/tuple, so we turn it into (index, pcoll)
      # pairs. The result value constructor makes tuples with len(pcolls) slots.
      pcolls = list(enumerate(pcolls))
      result_ctor_arg = len(pcolls)
      result_ctor = lambda size: tuple([] for _ in range(size))

    # Check input PCollections for PCollection-ness, and that they all belong
    # to the same pipeline.
    for _, pcoll in pcolls:
      self._check_pcollection(pcoll)
      if self.pipeline:
        assert pcoll.pipeline == self.pipeline

    return ([pcoll | 'pair_with_%s' % tag >> Map(_pair_tag_with_value, tag)
             for tag, pcoll in pcolls]
            | Flatten(pipeline=self.pipeline)
            | GroupByKey()
            | Map(_merge_tagged_vals_under_key, result_ctor, result_ctor_arg))


def Keys(label='Keys'):  # pylint: disable=invalid-name
  """Produces a PCollection of first elements of 2-tuples in a PCollection."""
  return label >> Map(lambda k_v: k_v[0])


def Values(label='Values'):  # pylint: disable=invalid-name
  """Produces a PCollection of second elements of 2-tuples in a PCollection."""
  return label >> Map(lambda k_v1: k_v1[1])


def KvSwap(label='KvSwap'):  # pylint: disable=invalid-name
  """Produces a PCollection reversing 2-tuples in a PCollection."""
  return label >> Map(lambda k_v2: (k_v2[1], k_v2[0]))


@ptransform_fn
def Distinct(pcoll):  # pylint: disable=invalid-name
  """Produces a PCollection containing distinct elements of a PCollection."""
  return (pcoll
          | 'ToPairs' >> Map(lambda v: (v, None))
          | 'Group' >> CombinePerKey(lambda vs: None)
          | 'Distinct' >> Keys())


@deprecated(since='2.12', current='Distinct')
@ptransform_fn
def RemoveDuplicates(pcoll):
  """Produces a PCollection containing distinct elements of a PCollection."""
  return pcoll | 'RemoveDuplicates' >> Distinct()


class _BatchSizeEstimator(object):
  """Estimates the best size for batches given historical timing.
  """

  _MAX_DATA_POINTS = 100
  _MAX_GROWTH_FACTOR = 2

  def __init__(self,
               min_batch_size=1,
               max_batch_size=1000,
               target_batch_overhead=.1,
               target_batch_duration_secs=1,
               variance=0.25,
               clock=time.time):
    if min_batch_size > max_batch_size:
      raise ValueError("Minimum (%s) must not be greater than maximum (%s)" % (
          min_batch_size, max_batch_size))
    if target_batch_overhead and not 0 < target_batch_overhead <= 1:
      raise ValueError("target_batch_overhead (%s) must be between 0 and 1" % (
          target_batch_overhead))
    if target_batch_duration_secs and target_batch_duration_secs <= 0:
      raise ValueError("target_batch_duration_secs (%s) must be positive" % (
          target_batch_duration_secs))
    if not (target_batch_overhead or target_batch_duration_secs):
      raise ValueError("At least one of target_batch_overhead or "
                       "target_batch_duration_secs must be positive.")
    self._min_batch_size = min_batch_size
    self._max_batch_size = max_batch_size
    self._target_batch_overhead = target_batch_overhead
    self._target_batch_duration_secs = target_batch_duration_secs
    self._variance = variance
    self._clock = clock
    self._data = []
    self._ignore_next_timing = False

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
    self._ignore_next_timing = False

  @contextlib.contextmanager
  def record_time(self, batch_size):
    start = self._clock()
    yield
    elapsed = self._clock() - start
    elapsed_msec = 1e3 * elapsed + self._remainder_msecs
    self._size_distribution.update(batch_size)
    self._time_distribution.update(int(elapsed_msec))
    self._remainder_msecs = elapsed_msec - int(elapsed_msec)
    if self._ignore_next_timing:
      self._ignore_next_timing = False
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
    b = (sum([(x - xbar) * (y - ybar) for x, y in zip(xs, ys)])
         / sum([(x - xbar)**2 for x in xs]))
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

  def next_batch_size(self):
    if self._min_batch_size == self._max_batch_size:
      return self._min_batch_size
    elif len(self._data) < 1:
      return self._min_batch_size
    elif len(self._data) < 2:
      # Force some variety so we have distinct batch sizes on which to do
      # linear regression below.
      return int(max(
          min(self._max_batch_size,
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


class _GlobalWindowsBatchingDoFn(DoFn):
  def __init__(self, batch_size_estimator):
    self._batch_size_estimator = batch_size_estimator

  def start_bundle(self):
    self._batch = []
    self._batch_size = self._batch_size_estimator.next_batch_size()
    # The first emit often involves non-trivial setup.
    self._batch_size_estimator.ignore_next_timing()

  def process(self, element):
    self._batch.append(element)
    if len(self._batch) >= self._batch_size:
      with self._batch_size_estimator.record_time(self._batch_size):
        yield self._batch
      self._batch = []
      self._batch_size = self._batch_size_estimator.next_batch_size()

  def finish_bundle(self):
    if self._batch:
      with self._batch_size_estimator.record_time(self._batch_size):
        yield window.GlobalWindows.windowed_value(self._batch)
      self._batch = None
      self._batch_size = self._batch_size_estimator.next_batch_size()


class _WindowAwareBatchingDoFn(DoFn):

  _MAX_LIVE_WINDOWS = 10

  def __init__(self, batch_size_estimator):
    self._batch_size_estimator = batch_size_estimator

  def start_bundle(self):
    self._batches = collections.defaultdict(list)
    self._batch_size = self._batch_size_estimator.next_batch_size()
    # The first emit often involves non-trivial setup.
    self._batch_size_estimator.ignore_next_timing()

  def process(self, element, window=DoFn.WindowParam):
    self._batches[window].append(element)
    if len(self._batches[window]) >= self._batch_size:
      with self._batch_size_estimator.record_time(self._batch_size):
        yield windowed_value.WindowedValue(
            self._batches[window], window.max_timestamp(), (window,))
      del self._batches[window]
      self._batch_size = self._batch_size_estimator.next_batch_size()
    elif len(self._batches) > self._MAX_LIVE_WINDOWS:
      window, _ = sorted(
          self._batches.items(),
          key=lambda window_batch: len(window_batch[1]),
          reverse=True)[0]
      with self._batch_size_estimator.record_time(self._batch_size):
        yield windowed_value.WindowedValue(
            self._batches[window], window.max_timestamp(), (window,))
      del self._batches[window]
      self._batch_size = self._batch_size_estimator.next_batch_size()

  def finish_bundle(self):
    for window, batch in self._batches.items():
      if batch:
        with self._batch_size_estimator.record_time(self._batch_size):
          yield windowed_value.WindowedValue(
              batch, window.max_timestamp(), (window,))
    self._batches = None
    self._batch_size = self._batch_size_estimator.next_batch_size()


@typehints.with_input_types(T)
@typehints.with_output_types(typehints.List[T])
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
    min_batch_size: (optional) the smallest number of elements per batch
    max_batch_size: (optional) the largest number of elements per batch
    target_batch_overhead: (optional) a target for fixed_cost / time,
        as used in the formula above
    target_batch_duration_secs: (optional) a target for total time per bundle,
        in seconds
    variance: (optional) the permitted (relative) amount of deviation from the
        (estimated) ideal batch size used to produce a wider base for
        linear interpolation
    clock: (optional) an alternative to time.time for measuring the cost of
        donwstream operations (mostly for testing)
  """

  def __init__(self,
               min_batch_size=1,
               max_batch_size=10000,
               target_batch_overhead=.05,
               target_batch_duration_secs=1,
               variance=0.25,
               clock=time.time):
    self._batch_size_estimator = _BatchSizeEstimator(
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        target_batch_overhead=target_batch_overhead,
        target_batch_duration_secs=target_batch_duration_secs,
        variance=variance,
        clock=clock)

  def expand(self, pcoll):
    if getattr(pcoll.pipeline.runner, 'is_streaming', False):
      raise NotImplementedError("Requires stateful processing (BEAM-2687)")
    elif pcoll.windowing.is_default():
      # This is the same logic as _GlobalWindowsBatchingDoFn, but optimized
      # for that simpler case.
      return pcoll | ParDo(_GlobalWindowsBatchingDoFn(
          self._batch_size_estimator))
    else:
      return pcoll | ParDo(_WindowAwareBatchingDoFn(self._batch_size_estimator))


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
    super(_IdentityWindowFn, self).__init__()
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


@typehints.with_input_types(typehints.KV[K, V])
@typehints.with_output_types(typehints.KV[K, V])
class ReshufflePerKey(PTransform):
  """PTransform that returns a PCollection equivalent to its input,
  but operationally provides some of the side effects of a GroupByKey,
  in particular preventing fusion of the surrounding transforms,
  checkpointing, and deduplication by id.

  ReshufflePerKey is experimental. No backwards compatibility guarantees.
  """

  def expand(self, pcoll):
    windowing_saved = pcoll.windowing
    if windowing_saved.is_default():
      # In this (common) case we can use a trivial trigger driver
      # and avoid the (expensive) window param.
      globally_windowed = window.GlobalWindows.windowed_value(None)
      window_fn = window.GlobalWindows()
      MIN_TIMESTAMP = window.MIN_TIMESTAMP

      def reify_timestamps(element, timestamp=DoFn.TimestampParam):
        key, value = element
        if timestamp == MIN_TIMESTAMP:
          timestamp = None
        return key, (value, timestamp)

      def restore_timestamps(element):
        key, values = element
        return [
            globally_windowed.with_value((key, value))
            if timestamp is None
            else window.GlobalWindows.windowed_value((key, value), timestamp)
            for (value, timestamp) in values]

    else:
      # The linter is confused.
      # hash(1) is used to force "runtime" selection of _IdentityWindowFn
      # pylint: disable=abstract-class-instantiated
      cls = hash(1) and _IdentityWindowFn
      window_fn = cls(
          windowing_saved.windowfn.get_window_coder())

      def reify_timestamps(element, timestamp=DoFn.TimestampParam):
        key, value = element
        return key, TimestampedValue(value, timestamp)

      def restore_timestamps(element, window=DoFn.WindowParam):
        # Pass the current window since _IdentityWindowFn wouldn't know how
        # to generate it.
        key, values = element
        return [
            windowed_value.WindowedValue(
                (key, value.value), value.timestamp, [window])
            for value in values]

    ungrouped = pcoll | Map(reify_timestamps)
    ungrouped._windowing = Windowing(
        window_fn,
        triggerfn=AfterCount(1),
        accumulation_mode=AccumulationMode.DISCARDING,
        timestamp_combiner=TimestampCombiner.OUTPUT_AT_EARLIEST)
    result = (ungrouped
              | GroupByKey()
              | FlatMap(restore_timestamps))
    result._windowing = windowing_saved
    return result


@typehints.with_input_types(T)
@typehints.with_output_types(T)
class Reshuffle(PTransform):
  """PTransform that returns a PCollection equivalent to its input,
  but operationally provides some of the side effects of a GroupByKey,
  in particular preventing fusion of the surrounding transforms,
  checkpointing, and deduplication by id.

  Reshuffle adds a temporary random key to each element, performs a
  ReshufflePerKey, and finally removes the temporary key.

  Reshuffle is experimental. No backwards compatibility guarantees.
  """

  def expand(self, pcoll):
    return (pcoll
            | 'AddRandomKeys' >> Map(lambda t: (random.getrandbits(32), t))
            | ReshufflePerKey()
            | 'RemoveRandomKeys' >> Map(lambda t: t[1]))

  def to_runner_api_parameter(self, unused_context):
    return common_urns.composites.RESHUFFLE.urn, None

  @PTransform.register_urn(common_urns.composites.RESHUFFLE.urn, None)
  def from_runner_api_parameter(unused_parameter, unused_context):
    return Reshuffle()
