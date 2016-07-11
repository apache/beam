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

"""InProcessEvaluationContext tracks global state, triggers and watermarks."""

from __future__ import absolute_import

import collections
import threading

from apache_beam.pvalue import DictPCollectionView
from apache_beam.pvalue import EmptySideInput
from apache_beam.pvalue import IterablePCollectionView
from apache_beam.pvalue import ListPCollectionView
from apache_beam.pvalue import SingletonPCollectionView
from apache_beam.runners.inprocess.clock import Clock
from apache_beam.runners.inprocess.inprocess_watermark_manager import InProcessWatermarkManager
from apache_beam.runners.inprocess.inprocess_executor import TransformExecutor
from apache_beam.utils import counters


class _InProcessExecutionContext(object):

  def __init__(self, watermarks, existing_state):
    self._watermarks = watermarks
    self._existing_state = existing_state

  @property
  def watermarks(self):
    return self._watermarks

  @property
  def existing_state(self):
    return self._existing_state


class _InProcessSideInputView(object):

  def __init__(self, view):
    self._view = view
    self.callable_queue = collections.deque()
    self.value = None
    self.has_result = False


class _InProcessSideInputsContainer(object):
  """An in-process container for PCollectionViews.

  It provides methods for blocking until a side-input is available and writing
  to a side input.
  """

  def __init__(self, views):
    self._lock = threading.Lock()
    self._views = {}
    for view in views:
      self._views[view] = _InProcessSideInputView(view)

  def get_value_or_schedule_after_output(self, pcollection_view, task):
    with self._lock:
      view = self._views[pcollection_view]
      if not view.has_result:
        view.callable_queue.append(task)
        task.blocked = True
      return (view.has_result, view.value)

  def set_value_and_get_callables(self, pcollection_view, values):
    with self._lock:
      view = self._views[pcollection_view]
      assert not view.has_result
      assert view.value is None
      assert view.callable_queue is not None
      view.value = self._pvalue_to_value(pcollection_view, values)
      result = tuple(view.callable_queue)
      for task in result:
        task.blocked = False
      view.callable_queue = None
      view.has_result = True
      return result

  def _pvalue_to_value(self, view, values):
    """Given a PCollectionView, returns the associated value in requested form.

    Args:
      view: PCollectionView for the requested side input.
      values: Iterable values associated with the side input.

    Returns:
      The side input in its requested form.

    Raises:
      ValueError: If values cannot be converted into the requested form.
    """
    if isinstance(view, SingletonPCollectionView):
      has_default, default_value = view._view_options()  # pylint: disable=protected-access
      if len(values) == 0:
        if has_default:
          result = default_value
        else:
          result = EmptySideInput()
      elif len(values) == 1:
        result = values[0].value
      else:
        raise ValueError(
            ('PCollection with more than one element accessed as '
             'a singleton view: %s.') % view)
    elif isinstance(view, IterablePCollectionView):
      result = [v.value for v in values]
    elif isinstance(view, ListPCollectionView):
      result = [v.value for v in values]
    elif isinstance(view, DictPCollectionView):
      result = dict(v.value for v in values)
    else:
      raise NotImplementedError
    return result


class InProcessEvaluationContext(object):
  """Evaluation context with the global state information of the pipeline.

  The evaluation context for a specific pipeline being executed by the
  InProcessPipelineRunner. Contains state shared within the execution across all
  transforms.

  InProcessEvaluationContext contains shared state for an execution of the
  InProcessPipelineRunner that can be used while evaluating a PTransform. This
  consists of views into underlying state and watermark implementations, access
  to read and write PCollectionViews, and constructing counter sets and
  execution contexts. This includes executing callbacks asynchronously when
  state changes to the appropriate point (e.g. when a PCollectionView is
  requested and known to be empty).

  InProcessEvaluationContext also handles results by committing finalizing
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

    # AppliedPTransform -> Evaluator specific state objects
    self._application_state_interals = {}
    self._watermark_manager = InProcessWatermarkManager(
        Clock(), root_transforms, value_to_consumers)
    self._side_inputs_container = _InProcessSideInputsContainer(views)
    self._pending_unblocked_tasks = []
    self._counter_factory = counters.CounterFactory()
    self._cache = None

    self._lock = threading.Lock()

  def use_pvalue_cache(self, cache):
    assert not self._cache
    self._cache = cache

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

    Handle the provided InProcessTransformResult, produced after evaluating
    the provided committed bundle (potentially None, if the result of a root
    PTransform).

    The result is the output of running the transform contained in the
    InProcessTransformResult on the contents of the provided bundle.

    Args:
      completed_bundle: the bundle that was processed to produce the result.
      completed_timers: the timers that were delivered to produce the
                        completed_bundle.
      result: the InProcessTransformResult of evaluating the input bundle

    Returns:
      the committed bundles contained within the handled result.
    """
    with self._lock:
      committed_bundles = self._commit_bundles(result.output_bundles)
      self._watermark_manager.update_watermarks(
          completed_bundle, result.transform, completed_timers,
          committed_bundles, result.watermark_hold)

      # If the result is for a view, update side inputs container.
      if (result.output_bundles
          and result.output_bundles[0].pcollection in self.views):
        if committed_bundles:
          assert len(committed_bundles) == 1
          side_input_result = committed_bundles[0].elements
        else:
          side_input_result = []
        tasks = self._side_inputs_container.set_value_and_get_callables(
            result.output_bundles[0].pcollection, side_input_result)
        self._pending_unblocked_tasks.extend(tasks)

      if result.counters:
        for counter in result.counters:
          merged_counter = self._counter_factory.get_counter(
              counter.name, counter.combine_fn)
          merged_counter.accumulator.merge([counter.accumulator])

      self._application_state_interals[result.transform] = result.state
      return committed_bundles

  def get_aggregator_values(self, aggregator_or_name):
    return self._counter_factory.get_aggregator_values(aggregator_or_name)

  def schedule_pending_unblocked_tasks(self, executor_service):
    if self._pending_unblocked_tasks:
      with self._lock:
        for task in self._pending_unblocked_tasks:
          executor_service.submit(task)
        self._pending_unblocked_tasks = []

  def _commit_bundles(self, uncommitted_bundles):
    """Commits bundles and returns a immutable set of committed bundles."""
    for in_progress_bundle in uncommitted_bundles:
      producing_applied_ptransform = in_progress_bundle.pcollection.producer
      watermarks = self._watermark_manager.get_watermarks(
          producing_applied_ptransform)
      in_progress_bundle.commit(watermarks.synchronized_processing_output_time)
    return tuple(uncommitted_bundles)

  def get_execution_context(self, applied_ptransform):
    return _InProcessExecutionContext(
        self._watermark_manager.get_watermarks(applied_ptransform),
        self._application_state_interals.get(applied_ptransform))

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
    else:
      for applied_ptransform in self._step_names:
        if not self._is_transform_done(applied_ptransform):
          return False
      return True

  def _is_transform_done(self, transform):
    tw = self._watermark_manager.get_watermarks(transform)
    return tw.output_watermark == InProcessWatermarkManager.WATERMARK_POS_INF

  def get_value_or_schedule_after_output(self, pcollection_view, task):
    assert isinstance(task, TransformExecutor)
    return self._side_inputs_container.get_value_or_schedule_after_output(
        pcollection_view, task)
