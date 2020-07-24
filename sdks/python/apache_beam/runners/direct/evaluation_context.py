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

# pytype: skip-file

from __future__ import absolute_import

import collections
import threading
from builtins import object
from typing import TYPE_CHECKING
from typing import Any
from typing import DefaultDict
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from apache_beam.runners.direct.direct_metrics import DirectMetrics
from apache_beam.runners.direct.executor import TransformExecutor
from apache_beam.runners.direct.watermark_manager import WatermarkManager
from apache_beam.transforms import sideinputs
from apache_beam.transforms.trigger import InMemoryUnmergedState
from apache_beam.utils import counters

if TYPE_CHECKING:
  from apache_beam import pvalue
  from apache_beam.pipeline import AppliedPTransform
  from apache_beam.runners.direct.bundle_factory import BundleFactory, _Bundle
  from apache_beam.runners.direct.util import TimerFiring
  from apache_beam.runners.direct.util import TransformResult
  from apache_beam.runners.direct.watermark_manager import _TransformWatermarks
  from apache_beam.utils.timestamp import Timestamp


class _ExecutionContext(object):
  """Contains the context for the execution of a single PTransform.

  It holds the watermarks for that transform, as well as keyed states.
  """
  def __init__(
      self,
      watermarks,  # type: _TransformWatermarks
      keyed_states):
    self.watermarks = watermarks
    self.keyed_states = keyed_states

    self._step_context = None

  def get_step_context(self):
    if not self._step_context:
      self._step_context = DirectStepContext(self.keyed_states)
    return self._step_context

  def reset(self):
    self._step_context = None


class _SideInputView(object):
  def __init__(self, view):
    self._view = view
    self.blocked_tasks = collections.deque()
    self.elements = []
    self.value = None
    self.watermark = None

  def __repr__(self):
    elements_string = (
        ', '.join(str(elm) for elm in self.elements) if self.elements else '[]')
    return '_SideInputView(elements=%s)' % elements_string


class _SideInputsContainer(object):
  """An in-process container for side inputs.

  It provides methods for blocking until a side-input is available and writing
  to a side input.
  """
  def __init__(self, side_inputs):
    # type: (Iterable[pvalue.AsSideInput]) -> None
    self._lock = threading.Lock()
    self._views = {}  # type: Dict[pvalue.AsSideInput, _SideInputView]
    self._transform_to_side_inputs = collections.defaultdict(
        list
    )  # type: DefaultDict[Optional[AppliedPTransform], List[pvalue.AsSideInput]]
    # this appears unused:
    self._side_input_to_blocked_tasks = collections.defaultdict(list)  # type: ignore

    for side in side_inputs:
      self._views[side] = _SideInputView(side)
      self._transform_to_side_inputs[side.pvalue.producer].append(side)

  def __repr__(self):
    views_string = (
        ', '.join(str(elm)
                  for elm in self._views.values()) if self._views else '[]')
    return '_SideInputsContainer(_views=%s)' % views_string

  def get_value_or_block_until_ready(self,
                                     side_input,
                                     task,  # type: TransformExecutor
                                     block_until  # type: Timestamp
                                    ):
    # type: (...) -> Any

    """Returns the value of a view whose task is unblocked or blocks its task.

    It gets the value of a view whose watermark has been updated and
    surpasses a given value.

    Args:
      side_input: ``_UnpickledSideInput`` value.
      task: ``TransformExecutor`` task waiting on a side input.
      block_until: Timestamp after which the task gets unblocked.

    Returns:
      The ``SideInputMap`` value of a view when the tasks it blocks are
      unblocked. Otherwise, None.
    """
    with self._lock:
      view = self._views[side_input]
      if view.watermark and view.watermark.output_watermark >= block_until:
        view.value = self._pvalue_to_value(side_input, view.elements)
        return view.value
      else:
        view.blocked_tasks.append((task, block_until))
        task.blocked = True

  def add_values(self, side_input, values):
    with self._lock:
      view = self._views[side_input]
      view.elements.extend(values)

  def update_watermarks_for_transform_and_unblock_tasks(
      self, ptransform, watermark):
    # type: (...) -> List[Tuple[TransformExecutor, Timestamp]]

    """Updates _SideInputsContainer after a watermark update and unbloks tasks.

    It traverses the list of side inputs per PTransform and calls
    _update_watermarks_for_side_input_and_unblock_tasks to unblock tasks.

    Args:
      ptransform: Value of a PTransform.
      watermark: Value of the watermark after an update for a PTransform.

    Returns:
      Tasks that get unblocked as a result of the watermark advancing.
    """
    unblocked_tasks = []
    for side in self._transform_to_side_inputs[ptransform]:
      unblocked_tasks.extend(
          self._update_watermarks_for_side_input_and_unblock_tasks(
              side, watermark))
    return unblocked_tasks

  def _update_watermarks_for_side_input_and_unblock_tasks(
      self, side_input, watermark):
    # type: (...) -> List[Tuple[TransformExecutor, Timestamp]]

    """Helps update _SideInputsContainer after a watermark update.

    For each view of the side input, it updates the value of the watermark
    recorded when the watermark moved and unblocks tasks accordingly.

    Args:
      side_input: ``_UnpickledSideInput`` value.
      watermark: Value of the watermark after an update for a PTransform.

    Returns:
      Tasks that get unblocked as a result of the watermark advancing.
    """
    with self._lock:
      view = self._views[side_input]
      view.watermark = watermark

      unblocked_tasks = []
      tasks_just_unblocked = []
      for task, block_until in view.blocked_tasks:
        if watermark.output_watermark >= block_until:
          view.value = self._pvalue_to_value(side_input, view.elements)
          unblocked_tasks.append(task)
          tasks_just_unblocked.append((task, block_until))
          task.blocked = False
      for task in tasks_just_unblocked:
        view.blocked_tasks.remove(task)
      return unblocked_tasks

  def _pvalue_to_value(self, side_input, values):
    """Given a side input, returns the associated value in its requested form.

    Args:
      side_input: _UnpickledSideInput object.
      values: Iterable values associated with the side input.

    Returns:
      The side input in its requested form.

    Raises:
      ValueError: If values cannot be converted into the requested form.
    """
    return sideinputs.SideInputMap(
        type(side_input), side_input._view_options(), values)


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

  def __init__(self,
               pipeline_options,
               bundle_factory,  # type: BundleFactory
               root_transforms,
               value_to_consumers,
               step_names,
               views,  # type: Iterable[pvalue.AsSideInput]
               clock
              ):
    self.pipeline_options = pipeline_options
    self._bundle_factory = bundle_factory
    self._root_transforms = root_transforms
    self._value_to_consumers = value_to_consumers
    self._step_names = step_names
    self.views = views
    self._pcollection_to_views = collections.defaultdict(
        list)  # type: DefaultDict[pvalue.PValue, List[pvalue.AsSideInput]]
    for view in views:
      self._pcollection_to_views[view.pvalue].append(view)
    self._transform_keyed_states = self._initialize_keyed_states(
        root_transforms, value_to_consumers)
    self._side_inputs_container = _SideInputsContainer(views)
    self._watermark_manager = WatermarkManager(
        clock,
        root_transforms,
        value_to_consumers,
        self._transform_keyed_states)
    self._pending_unblocked_tasks = [
    ]  # type: List[Tuple[TransformExecutor, Timestamp]]
    self._counter_factory = counters.CounterFactory()
    self._metrics = DirectMetrics()

    self._lock = threading.Lock()
    self.shutdown_requested = False

  def _initialize_keyed_states(self, root_transforms, value_to_consumers):
    """Initialize user state dicts.

    These dicts track user state per-key, per-transform and per-window.
    """
    transform_keyed_states = {}
    for transform in root_transforms:
      transform_keyed_states[transform] = {}
    for consumers in value_to_consumers.values():
      for consumer in consumers:
        transform_keyed_states[consumer] = {}
    return transform_keyed_states

  def metrics(self):
    # TODO. Should this be made a @property?
    return self._metrics

  def is_root_transform(self, applied_ptransform):
    # type: (AppliedPTransform) -> bool
    return applied_ptransform in self._root_transforms

  def handle_result(self,
                    completed_bundle,  # type: _Bundle
                    completed_timers,
                    result  # type: TransformResult
                   ):
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
      result: the ``TransformResult`` of evaluating the input bundle

    Returns:
      the committed bundles contained within the handled result.
    """
    with self._lock:
      committed_bundles, unprocessed_bundles = self._commit_bundles(
          result.uncommitted_output_bundles,
          result.unprocessed_bundles)

      self._metrics.commit_logical(
          completed_bundle, result.logical_metric_updates)

      # If the result is for a view, update side inputs container.
      self._update_side_inputs_container(committed_bundles, result)

      # Tasks generated from unblocked side inputs as the watermark progresses.
      tasks = self._watermark_manager.update_watermarks(
          completed_bundle,
          result.transform,
          completed_timers,
          committed_bundles,
          unprocessed_bundles,
          result.keyed_watermark_holds,
          self._side_inputs_container)
      self._pending_unblocked_tasks.extend(tasks)

      if result.counters:
        for counter in result.counters:
          merged_counter = self._counter_factory.get_counter(
              counter.name, counter.combine_fn)
          merged_counter.accumulator.merge([counter.accumulator])

      # Commit partial GBK states
      existing_keyed_state = self._transform_keyed_states[result.transform]
      for k, v in result.partial_keyed_state.items():
        existing_keyed_state[k] = v
      return committed_bundles

  def _update_side_inputs_container(self,
                                    committed_bundles,  # type: Iterable[_Bundle]
                                    result  # type: TransformResult
                                   ):
    """Update the side inputs container if we are outputting into a side input.

    Look at the result, and if it's outputing into a PCollection that we have
    registered as a PCollectionView, we add the result to the PCollectionView.
    """
    if (result.uncommitted_output_bundles and
        result.uncommitted_output_bundles[0].pcollection in
        self._pcollection_to_views):
      for view in self._pcollection_to_views[
          result.uncommitted_output_bundles[0].pcollection]:
        for committed_bundle in committed_bundles:
          # side_input must be materialized.
          self._side_inputs_container.add_values(
              view, committed_bundle.get_elements_iterable(make_copy=True))

  def get_aggregator_values(self, aggregator_or_name):
    return self._counter_factory.get_aggregator_values(aggregator_or_name)

  def schedule_pending_unblocked_tasks(self, executor_service):
    if self._pending_unblocked_tasks:
      with self._lock:
        for task in self._pending_unblocked_tasks:
          executor_service.submit(task)
        self._pending_unblocked_tasks = []

  def _commit_bundles(self,
                      uncommitted_bundles,  # type: Iterable[_Bundle]
                      unprocessed_bundles  # type: Iterable[_Bundle]
                     ):
    # type: (...) -> Tuple[Tuple[_Bundle, ...], Tuple[_Bundle, ...]]

    """Commits bundles and returns a immutable set of committed bundles."""
    for in_progress_bundle in uncommitted_bundles:
      producing_applied_ptransform = in_progress_bundle.pcollection.producer
      watermarks = self._watermark_manager.get_watermarks(
          producing_applied_ptransform)
      in_progress_bundle.commit(watermarks.synchronized_processing_output_time)

    for unprocessed_bundle in unprocessed_bundles:
      unprocessed_bundle.commit(None)
    return tuple(uncommitted_bundles), tuple(unprocessed_bundles)

  def get_execution_context(self, applied_ptransform):
    # type: (AppliedPTransform) -> _ExecutionContext
    return _ExecutionContext(
        self._watermark_manager.get_watermarks(applied_ptransform),
        self._transform_keyed_states[applied_ptransform])

  def create_bundle(self, output_pcollection):
    # type: (Union[pvalue.PBegin, pvalue.PCollection]) -> _Bundle

    """Create an uncommitted bundle for the specified PCollection."""
    return self._bundle_factory.create_bundle(output_pcollection)

  def create_empty_committed_bundle(self, output_pcollection):
    # type: (pvalue.PCollection) -> _Bundle

    """Create empty bundle useful for triggering evaluation."""
    return self._bundle_factory.create_empty_committed_bundle(
        output_pcollection)

  def extract_all_timers(self):
    # type: () -> Tuple[List[Tuple[AppliedPTransform, List[TimerFiring]]], bool]
    return self._watermark_manager.extract_all_timers()

  def is_done(self, transform=None):
    # type: (Optional[AppliedPTransform]) -> bool

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
    # type: (AppliedPTransform) -> bool
    tw = self._watermark_manager.get_watermarks(transform)
    return tw.output_watermark == WatermarkManager.WATERMARK_POS_INF

  def get_value_or_block_until_ready(self, side_input, task, block_until):
    assert isinstance(task, TransformExecutor)
    return self._side_inputs_container.get_value_or_block_until_ready(
        side_input, task, block_until)

  def shutdown(self):
    print('Shutting down EvaluationContext')
    self.shutdown_requested = True


class DirectUnmergedState(InMemoryUnmergedState):
  """UnmergedState implementation for the DirectRunner."""
  def __init__(self):
    super(DirectUnmergedState, self).__init__(defensive_copy=False)


class DirectStepContext(object):
  """Context for the currently-executing step."""
  def __init__(self, existing_keyed_state):
    self.existing_keyed_state = existing_keyed_state
    # In order to avoid partial writes of a bundle, every time
    # existing_keyed_state is accessed, a copy of the state is made
    # to be transferred to the bundle state once the bundle is committed.
    self.partial_keyed_state = {}

  def get_keyed_state(self, key):
    if not self.existing_keyed_state.get(key):
      self.existing_keyed_state[key] = DirectUnmergedState()
    if not self.partial_keyed_state.get(key):
      self.partial_keyed_state[key] = self.existing_keyed_state[key].copy()
    return self.partial_keyed_state[key]
