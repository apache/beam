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

"""EvaluationContext tracks global state, triggers and watermarks."""

from __future__ import absolute_import

import collections
import threading

from apache_beam.transforms import sideinputs
from apache_beam.runners.direct.clock import Clock
from apache_beam.runners.direct.watermark_manager import WatermarkManager
from apache_beam.runners.direct.executor import TransformExecutor
from apache_beam.runners.direct.direct_metrics import DirectMetrics
from apache_beam.utils import counters


class _ExecutionContext(object):

  def __init__(self, watermarks, existing_state):
    self._watermarks = watermarks
    self._existing_state = existing_state

  @property
  def watermarks(self):
    return self._watermarks

  @property
  def existing_state(self):
    return self._existing_state


class _SideInputView(object):

  def __init__(self, view):
    self._view = view
    self.callable_queue = collections.deque()
    self.elements = []
    self.value = None
    self.has_result = False


class _SideInputsContainer(object):
  """An in-process container for side inputs.

  It provides methods for blocking until a side-input is available and writing
  to a side input.
  """

  def __init__(self, views):
    self._lock = threading.Lock()
    self._views = {}
    for view in views:
      self._views[view] = _SideInputView(view)

  def get_value_or_schedule_after_output(self, side_input, task):
    with self._lock:
      view = self._views[side_input]
      if not view.has_result:
        view.callable_queue.append(task)
        task.blocked = True
      return (view.has_result, view.value)

  def add_values(self, side_input, values):
    with self._lock:
      view = self._views[side_input]
      assert not view.has_result
      view.elements.extend(values)

  def finalize_value_and_get_tasks(self, side_input):
    with self._lock:
      view = self._views[side_input]
      assert not view.has_result
      assert view.value is None
      assert view.callable_queue is not None
      view.value = self._pvalue_to_value(side_input, view.elements)
      view.elements = None
      result = tuple(view.callable_queue)
      for task in result:
        task.blocked = False
      view.callable_queue = None
      view.has_result = True
      return result

  def _pvalue_to_value(self, view, values):
    """Given a side input view, returns the associated value in requested form.

    Args:
      view: SideInput for the requested side input.
      values: Iterable values associated with the side input.

    Returns:
      The side input in its requested form.

    Raises:
      ValueError: If values cannot be converted into the requested form.
    """
    return sideinputs.SideInputMap(type(view), view._view_options(), values)


class EvaluationContext(object):
  """Evaluation context with the global state information of the pipeline.

  The evaluation context for a specific pipeline being executed by the
  DirectRunner. Contains state shared within the execution across all
  transforms.

  EvaluationContext contains shared state for an execution of the
  DirectRunner that can be used while evaluating a PTransform. This
  consists of views into underlying state and watermark implementations, access
  to read and write side inputs, and constructing counter sets and
  execution contexts. This includes executing callbacks asynchronously when
  state changes to the appropriate point (e.g. when a side input is
  requested and known to be empty).

  EvaluationContext also handles results by committing finalizing
  bundles based on the current global state and updating the global state
  appropriately. This includes updating the per-(step,key) state, updating
  global watermarks, and executing any callbacks that can be executed.
  """

  def __init__(self, pipeline_options, bundle_factory, root_transforms,
               value_to_consumers, step_names, views):
    self.pipeline_options = pipeline_options
    self._bundle_factory = bundle_factory
    self._root_transforms = root_transforms
    self._value_to_consumers = value_to_consumers
    self._step_names = step_names
    self.views = views
    self._pcollection_to_views = collections.defaultdict(list)
    for view in views:
      self._pcollection_to_views[view.pvalue].append(view)

    # AppliedPTransform -> Evaluator specific state objects
    self._application_state_internals = {}
    self._watermark_manager = WatermarkManager(
        Clock(), root_transforms, value_to_consumers)
    self._side_inputs_container = _SideInputsContainer(views)
    self._pending_unblocked_tasks = []
    self._counter_factory = counters.CounterFactory()
    self._cache = None
    self._metrics = DirectMetrics()

    self._lock = threading.Lock()

  def use_pvalue_cache(self, cache):
    assert not self._cache
    self._cache = cache

  def metrics(self):
    # TODO. Should this be made a @property?
    return self._metrics

  @property
  def has_cache(self):
    return self._cache is not None

  def append_to_cache(self, applied_ptransform, tag, elements):
    with self._lock:
      assert self._cache
      self._cache.append(applied_ptransform, tag, elements)

  def is_root_transform(self, applied_ptransform):
    return applied_ptransform in self._root_transforms

  def handle_result(
      self, completed_bundle, completed_timers, result):
    """Handle the provided result produced after evaluating the input bundle.

    Handle the provided TransformResult, produced after evaluating
    the provided committed bundle (potentially None, if the result of a root
    PTransform).

    The result is the output of running the transform contained in the
    TransformResult on the contents of the provided bundle.

    Args:
      completed_bundle: the bundle that was processed to produce the result.
      completed_timers: the timers that were delivered to produce the
                        completed_bundle.
      result: the TransformResult of evaluating the input bundle

    Returns:
      the committed bundles contained within the handled result.
    """
    with self._lock:
      committed_bundles, unprocessed_bundle = self._commit_bundles(
          result.uncommitted_output_bundles,
          result.unprocessed_bundle)
      self._watermark_manager.update_watermarks(
          completed_bundle, unprocessed_bundle, result.transform, completed_timers,
          committed_bundles, result.watermark_hold)

      self._metrics.commit_logical(completed_bundle,
                                   result.logical_metric_updates)

      # If the result is for a view, update side inputs container.
      if (result.uncommitted_output_bundles
          and result.uncommitted_output_bundles[0].pcollection
          in self._pcollection_to_views):
        for view in self._pcollection_to_views[
            result.uncommitted_output_bundles[0].pcollection]:
          for committed_bundle in committed_bundles:
            # side_input must be materialized.
            self._side_inputs_container.add_values(
                view,
                committed_bundle.get_elements_iterable(make_copy=True))
          if (self.get_execution_context(result.transform)
              .watermarks.input_watermark
              == WatermarkManager.WATERMARK_POS_INF):
            self._pending_unblocked_tasks.extend(
                self._side_inputs_container.finalize_value_and_get_tasks(view))

      if result.counters:
        for counter in result.counters:
          merged_counter = self._counter_factory.get_counter(
              counter.name, counter.combine_fn)
          merged_counter.accumulator.merge([counter.accumulator])

      self._application_state_internals[result.transform] = result.state
      return committed_bundles

  def get_aggregator_values(self, aggregator_or_name):
    return self._counter_factory.get_aggregator_values(aggregator_or_name)

  def schedule_pending_unblocked_tasks(self, executor_service):
    if self._pending_unblocked_tasks:
      with self._lock:
        for task in self._pending_unblocked_tasks:
          executor_service.submit(task)
        self._pending_unblocked_tasks = []

  def _commit_bundles(self, uncommitted_output_bundles, unprocessed_bundle):
    """Commits bundles and returns a immutable set of committed bundles."""
    for in_progress_bundle in uncommitted_output_bundles:
      producing_applied_ptransform = in_progress_bundle.pcollection.producer
      watermarks = self._watermark_manager.get_watermarks(
          producing_applied_ptransform)
      in_progress_bundle.commit(watermarks.synchronized_processing_output_time)
    if unprocessed_bundle:
      unprocessed_bundle.commit(None)
    return tuple(uncommitted_output_bundles), unprocessed_bundle

  def get_execution_context(self, applied_ptransform):
    return _ExecutionContext(
        self._watermark_manager.get_watermarks(applied_ptransform),
        self._application_state_internals.get(applied_ptransform))

  def create_bundle(self, output_pcollection):
    """Create an uncommitted bundle for the specified PCollection."""
    return self._bundle_factory.create_bundle(output_pcollection)

  def create_empty_committed_bundle(self, output_pcollection):
    """Create empty bundle useful for triggering evaluation."""
    return self._bundle_factory.create_empty_committed_bundle(
        output_pcollection)

  def extract_fired_timers(self):
    return self._watermark_manager.extract_fired_timers()

  def is_done(self, transform=None):
    """Checks completion of a step or the pipeline.

    Args:
      transform: AppliedPTransform to check for completion.

    Returns:
      True if the step will not produce additional output. If transform is None
      returns true if all steps are done.
    """
    if transform:
      return self._is_transform_done(transform)

    for applied_ptransform in self._step_names:
      if not self._is_transform_done(applied_ptransform):
        return False
    return True

  def _is_transform_done(self, transform):
    tw = self._watermark_manager.get_watermarks(transform)
    return tw.output_watermark == WatermarkManager.WATERMARK_POS_INF

  def get_value_or_schedule_after_output(self, side_input, task):
    assert isinstance(task, TransformExecutor)
    return self._side_inputs_container.get_value_or_schedule_after_output(
        side_input, task)
