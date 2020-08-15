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

"""An executor that schedules and executes applied ptransforms."""

# pytype: skip-file

from __future__ import absolute_import

import collections
import itertools
import logging
import sys
import threading
import traceback
from builtins import object
from builtins import range
from typing import TYPE_CHECKING
from typing import Any
from typing import Dict
from typing import FrozenSet
from typing import Optional
from typing import Set
from weakref import WeakValueDictionary

from future.moves import queue
from future.utils import raise_

from apache_beam.metrics.execution import MetricsContainer
from apache_beam.runners.worker import statesampler
from apache_beam.transforms import sideinputs
from apache_beam.utils import counters

if TYPE_CHECKING:
  from apache_beam import pvalue
  from apache_beam.runners.direct.bundle_factory import _Bundle
  from apache_beam.runners.direct.evaluation_context import EvaluationContext
  from apache_beam.runners.direct.transform_evaluator import TransformEvaluatorRegistry

_LOGGER = logging.getLogger(__name__)


class _ExecutorService(object):
  """Thread pool for executing tasks in parallel."""
  class CallableTask(object):
    def call(self, state_sampler):
      pass

    @property
    def name(self):
      return None

  class _ExecutorServiceWorker(threading.Thread):
    """Worker thread for executing a single task at a time."""

    # Amount to block waiting for getting an item from the queue in seconds.
    TIMEOUT = 5

    def __init__(
        self,
        queue,  # type: queue.Queue[_ExecutorService.CallableTask]
        index):
      super(_ExecutorService._ExecutorServiceWorker, self).__init__()
      self.queue = queue
      self._index = index
      self._default_name = 'ExecutorServiceWorker-' + str(index)
      self._update_name()
      self.shutdown_requested = False

      # Stop worker thread when main thread exits.
      self.daemon = True
      self.start()

    def _update_name(self, task=None):
      if task and task.name:
        name = task.name
      else:
        name = self._default_name
      self.name = 'Thread: %d, %s (%s)' % (
          self._index, name, 'executing' if task else 'idle')

    def _get_task_or_none(self):
      # type: () -> Optional[_ExecutorService.CallableTask]
      try:
        # Do not block indefinitely, otherwise we may not act for a requested
        # shutdown.
        return self.queue.get(
            timeout=_ExecutorService._ExecutorServiceWorker.TIMEOUT)
      except queue.Empty:
        return None

    def run(self):
      state_sampler = statesampler.StateSampler('', counters.CounterFactory())
      statesampler.set_current_tracker(state_sampler)
      while not self.shutdown_requested:
        task = self._get_task_or_none()
        if task:
          try:
            if not self.shutdown_requested:
              self._update_name(task)
              task.call(state_sampler)
              self._update_name()
          finally:
            self.queue.task_done()

    def shutdown(self):
      self.shutdown_requested = True

  def __init__(self, num_workers):
    self.queue = queue.Queue(
    )  # type: queue.Queue[_ExecutorService.CallableTask]
    self.workers = [
        _ExecutorService._ExecutorServiceWorker(self.queue, i)
        for i in range(num_workers)
    ]
    self.shutdown_requested = False

  def submit(self, task):
    # type: (_ExecutorService.CallableTask) -> None
    assert isinstance(task, _ExecutorService.CallableTask)
    if not self.shutdown_requested:
      self.queue.put(task)

  def await_completion(self):
    for worker in self.workers:
      worker.join()

  def shutdown(self):
    self.shutdown_requested = True

    for worker in self.workers:
      worker.shutdown()

    # Consume all the remaining items in the queue
    while not self.queue.empty():
      try:
        self.queue.get_nowait()
        self.queue.task_done()
      except queue.Empty:
        continue
    # All existing threads will eventually terminate (after they complete their
    # last task).


class _TransformEvaluationState(object):
  def __init__(
      self,
      executor_service,
      scheduled  # type: Set[TransformExecutor]
  ):
    self.executor_service = executor_service
    self.scheduled = scheduled

  def schedule(self, work):
    self.scheduled.add(work)
    self.executor_service.submit(work)

  def complete(self, completed_work):
    self.scheduled.remove(completed_work)


class _ParallelEvaluationState(_TransformEvaluationState):
  """A TransformEvaluationState with unlimited parallelism.

  Any TransformExecutor scheduled will be immediately submitted to the
  ExecutorService.

  A principal use of this is for evaluators that can generate output bundles
  only using the input bundle (e.g. ParDo).
  """
  pass


class _SerialEvaluationState(_TransformEvaluationState):
  """A TransformEvaluationState with a single work queue.

  Any TransformExecutor scheduled will be placed on the work queue. Only one
  item of work will be submitted to the ExecutorService at any time.

  A principal use of this is for evaluators that keeps a global state such as
  _GroupByKeyOnly.
  """
  def __init__(self, executor_service, scheduled):
    super(_SerialEvaluationState, self).__init__(executor_service, scheduled)
    self.serial_queue = collections.deque()
    self.currently_evaluating = None
    self._lock = threading.Lock()

  def complete(self, completed_work):
    self._update_currently_evaluating(None, completed_work)
    super(_SerialEvaluationState, self).complete(completed_work)

  def schedule(self, new_work):
    self._update_currently_evaluating(new_work, None)

  def _update_currently_evaluating(self, new_work, completed_work):
    with self._lock:
      if new_work:
        self.serial_queue.append(new_work)
      if completed_work:
        assert self.currently_evaluating == completed_work
        self.currently_evaluating = None
      if self.serial_queue and not self.currently_evaluating:
        next_work = self.serial_queue.pop()
        self.currently_evaluating = next_work
        super(_SerialEvaluationState, self).schedule(next_work)


class _TransformExecutorServices(object):
  """Schedules and completes TransformExecutors.

  Controls the concurrency as appropriate for the applied transform the executor
  exists for.
  """
  def __init__(self, executor_service):
    # type: (_ExecutorService) -> None
    self._executor_service = executor_service
    self._scheduled = set()  # type: Set[TransformExecutor]
    self._parallel = _ParallelEvaluationState(
        self._executor_service, self._scheduled)
    self._serial_cache = WeakValueDictionary(
    )  # type: WeakValueDictionary[Any, _SerialEvaluationState]

  def parallel(self):
    # type: () -> _ParallelEvaluationState
    return self._parallel

  def serial(self, step):
    # type: (Any) -> _SerialEvaluationState
    cached = self._serial_cache.get(step)
    if not cached:
      cached = _SerialEvaluationState(self._executor_service, self._scheduled)
      self._serial_cache[step] = cached
    return cached

  @property
  def executors(self):
    # type: () -> FrozenSet[TransformExecutor]
    return frozenset(self._scheduled)


class _CompletionCallback(object):
  """The default completion callback.

  The default completion callback is used to complete transform evaluations
  that are triggered due to the arrival of elements from an upstream transform,
  or for a source transform.
  """

  def __init__(self,
               evaluation_context,  # type: EvaluationContext
               all_updates,
               timer_firings=None
              ):
    self._evaluation_context = evaluation_context
    self._all_updates = all_updates
    self._timer_firings = timer_firings or []

  def handle_result(
      self, transform_executor, input_committed_bundle, transform_result):
    output_committed_bundles = self._evaluation_context.handle_result(
        input_committed_bundle, self._timer_firings, transform_result)
    for output_committed_bundle in output_committed_bundles:
      self._all_updates.offer(
          _ExecutorServiceParallelExecutor._ExecutorUpdate(
              transform_executor, committed_bundle=output_committed_bundle))
    for unprocessed_bundle in transform_result.unprocessed_bundles:
      self._all_updates.offer(
          _ExecutorServiceParallelExecutor._ExecutorUpdate(
              transform_executor, unprocessed_bundle=unprocessed_bundle))
    return output_committed_bundles

  def handle_exception(self, transform_executor, exception):
    self._all_updates.offer(
        _ExecutorServiceParallelExecutor._ExecutorUpdate(
            transform_executor, exception=exception))


class TransformExecutor(_ExecutorService.CallableTask):
  """For internal use only; no backwards-compatibility guarantees.

  TransformExecutor will evaluate a bundle using an applied ptransform.

  A CallableTask responsible for constructing a TransformEvaluator and
  evaluating it on some bundle of input, and registering the result using the
  completion callback.
  """

  _MAX_RETRY_PER_BUNDLE = 4

  def __init__(self,
               transform_evaluator_registry,  # type: TransformEvaluatorRegistry
               evaluation_context,  # type: EvaluationContext
               input_bundle,  # type: _Bundle
               fired_timers,
               applied_ptransform,
               completion_callback,
               transform_evaluation_state  # type: _TransformEvaluationState
              ):
    self._transform_evaluator_registry = transform_evaluator_registry
    self._evaluation_context = evaluation_context
    self._input_bundle = input_bundle
    # For non-empty bundles, store the window of the max EOW.
    # TODO(mariagh): Move to class _Bundle's inner _StackedWindowedValues
    self._latest_main_input_window = None
    if input_bundle.has_elements():
      self._latest_main_input_window = input_bundle._elements[0].windows[0]
      for elem in input_bundle.get_elements_iterable():
        if elem.windows[0].end > self._latest_main_input_window.end:
          self._latest_main_input_window = elem.windows[0]
    self._fired_timers = fired_timers
    self._applied_ptransform = applied_ptransform
    self._completion_callback = completion_callback
    self._transform_evaluation_state = transform_evaluation_state
    self._side_input_values = {}  # type: Dict[pvalue.AsSideInput, Any]
    self.blocked = False
    self._call_count = 0
    self._retry_count = 0
    self._max_retries_per_bundle = TransformExecutor._MAX_RETRY_PER_BUNDLE

  def call(self, state_sampler):
    self._call_count += 1
    assert self._call_count <= (1 + len(self._applied_ptransform.side_inputs))
    metrics_container = MetricsContainer(self._applied_ptransform.full_label)
    start_state = state_sampler.scoped_state(
        self._applied_ptransform.full_label,
        'start',
        metrics_container=metrics_container)
    process_state = state_sampler.scoped_state(
        self._applied_ptransform.full_label,
        'process',
        metrics_container=metrics_container)
    finish_state = state_sampler.scoped_state(
        self._applied_ptransform.full_label,
        'finish',
        metrics_container=metrics_container)

    with start_state:
      # Side input initialization should be accounted for in start_state.
      for side_input in self._applied_ptransform.side_inputs:
        # Find the projection of main's window onto the side input's window.
        window_mapping_fn = side_input._view_options().get(
            'window_mapping_fn', sideinputs._global_window_mapping_fn)
        main_onto_side_window = window_mapping_fn(
            self._latest_main_input_window)
        block_until = main_onto_side_window.end

        if side_input not in self._side_input_values:
          value = self._evaluation_context.get_value_or_block_until_ready(
              side_input, self, block_until)
          if not value:
            # Monitor task will reschedule this executor once the side input is
            # available.
            return
          self._side_input_values[side_input] = value
      side_input_values = [
          self._side_input_values[side_input]
          for side_input in self._applied_ptransform.side_inputs
      ]

    while self._retry_count < self._max_retries_per_bundle:
      try:
        self.attempt_call(
            metrics_container,
            side_input_values,
            start_state,
            process_state,
            finish_state)
        break
      except Exception as e:
        self._retry_count += 1
        _LOGGER.error(
            'Exception at bundle %r, due to an exception.\n %s',
            self._input_bundle,
            traceback.format_exc())
        if self._retry_count == self._max_retries_per_bundle:
          _LOGGER.error(
              'Giving up after %s attempts.', self._max_retries_per_bundle)
          self._completion_callback.handle_exception(self, e)

    self._evaluation_context.metrics().commit_physical(
        self._input_bundle, metrics_container.get_cumulative())
    self._transform_evaluation_state.complete(self)

  def attempt_call(
      self,
      metrics_container,
      side_input_values,
      start_state,
      process_state,
      finish_state):
    """Attempts to run a bundle."""
    evaluator = self._transform_evaluator_registry.get_evaluator(
        self._applied_ptransform, self._input_bundle, side_input_values)

    with start_state:
      evaluator.start_bundle()

    with process_state:
      if self._fired_timers:
        for timer_firing in self._fired_timers:
          evaluator.process_timer_wrapper(timer_firing)

      if self._input_bundle:
        for value in self._input_bundle.get_elements_iterable():
          evaluator.process_element(value)

    with finish_state:
      result = evaluator.finish_bundle()
      result.logical_metric_updates = metrics_container.get_cumulative()

    self._completion_callback.handle_result(self, self._input_bundle, result)
    return result


class Executor(object):
  """For internal use only; no backwards-compatibility guarantees."""
  def __init__(self, *args, **kwargs):
    self._executor = _ExecutorServiceParallelExecutor(*args, **kwargs)

  def start(self, roots):
    self._executor.start(roots)

  def await_completion(self):
    self._executor.await_completion()

  def shutdown(self):
    self._executor.request_shutdown()


class _ExecutorServiceParallelExecutor(object):
  """An internal implementation for Executor."""

  NUM_WORKERS = 1

  def __init__(
      self,
      value_to_consumers,
      transform_evaluator_registry,
      evaluation_context  # type: EvaluationContext
  ):
    self.executor_service = _ExecutorService(
        _ExecutorServiceParallelExecutor.NUM_WORKERS)
    self.transform_executor_services = _TransformExecutorServices(
        self.executor_service)
    self.value_to_consumers = value_to_consumers
    self.transform_evaluator_registry = transform_evaluator_registry
    self.evaluation_context = evaluation_context
    self.all_updates = _ExecutorServiceParallelExecutor._TypedUpdateQueue(
        _ExecutorServiceParallelExecutor._ExecutorUpdate)
    self.visible_updates = _ExecutorServiceParallelExecutor._TypedUpdateQueue(
        _ExecutorServiceParallelExecutor._VisibleExecutorUpdate)
    self.default_completion_callback = _CompletionCallback(
        evaluation_context, self.all_updates)

  def start(self, roots):
    self.root_nodes = frozenset(roots)
    self.all_nodes = frozenset(
        itertools.chain(
            roots, *itertools.chain(self.value_to_consumers.values())))
    self.node_to_pending_bundles = {}
    for root_node in self.root_nodes:
      provider = (
          self.transform_evaluator_registry.get_root_bundle_provider(root_node))
      self.node_to_pending_bundles[root_node] = provider.get_root_bundles()
    self.executor_service.submit(
        _ExecutorServiceParallelExecutor._MonitorTask(self))

  def await_completion(self):
    update = self.visible_updates.take()
    try:
      if update.exception:
        t, v, tb = update.exc_info
        raise_(t, v, tb)
    finally:
      self.executor_service.shutdown()
      self.executor_service.await_completion()

  def request_shutdown(self):
    self.executor_service.shutdown()
    self.executor_service.await_completion()
    self.evaluation_context.shutdown()

  def schedule_consumers(self, committed_bundle):
    # type: (_Bundle) -> None
    if committed_bundle.pcollection in self.value_to_consumers:
      consumers = self.value_to_consumers[committed_bundle.pcollection]
      for applied_ptransform in consumers:
        self.schedule_consumption(
            applied_ptransform,
            committed_bundle, [],
            self.default_completion_callback)

  def schedule_unprocessed_bundle(self, applied_ptransform, unprocessed_bundle):
    self.node_to_pending_bundles[applied_ptransform].append(unprocessed_bundle)

  def schedule_consumption(self,
                           consumer_applied_ptransform,
                           committed_bundle,  # type: _Bundle
                           fired_timers,
                           on_complete
                          ):
    """Schedules evaluation of the given bundle with the transform."""
    assert consumer_applied_ptransform
    assert committed_bundle
    assert on_complete
    if self.transform_evaluator_registry.should_execute_serially(
        consumer_applied_ptransform):
      transform_executor_service = self.transform_executor_services.serial(
          consumer_applied_ptransform)  # type: _TransformEvaluationState
    else:
      transform_executor_service = self.transform_executor_services.parallel()

    transform_executor = TransformExecutor(
        self.transform_evaluator_registry,
        self.evaluation_context,
        committed_bundle,
        fired_timers,
        consumer_applied_ptransform,
        on_complete,
        transform_executor_service)
    transform_executor_service.schedule(transform_executor)

  class _TypedUpdateQueue(object):
    """Type checking update queue with blocking and non-blocking operations."""
    def __init__(self, item_type):
      self._item_type = item_type
      self._queue = queue.Queue()

    def poll(self):
      try:
        item = self._queue.get_nowait()
        self._queue.task_done()
        return item
      except queue.Empty:
        return None

    def take(self):
      # The implementation of Queue.Queue.get() does not propagate
      # KeyboardInterrupts when a timeout is not used.  We therefore use a
      # one-second timeout in the following loop to allow KeyboardInterrupts
      # to be correctly propagated.
      while True:
        try:
          item = self._queue.get(timeout=1)
          self._queue.task_done()
          return item
        except queue.Empty:
          pass

    def offer(self, item):
      assert isinstance(item, self._item_type)
      self._queue.put_nowait(item)

  class _ExecutorUpdate(object):
    """An internal status update on the state of the executor."""
    def __init__(
        self,
        transform_executor,
        committed_bundle=None,
        unprocessed_bundle=None,
        exception=None):
      self.transform_executor = transform_executor
      # Exactly one of them should be not-None
      assert sum(
          [bool(committed_bundle), bool(unprocessed_bundle),
           bool(exception)]) == 1
      self.committed_bundle = committed_bundle
      self.unprocessed_bundle = unprocessed_bundle
      self.exception = exception
      self.exc_info = sys.exc_info()
      if self.exc_info[1] is not exception:
        # Not the right exception.
        self.exc_info = (exception, None, None)

  class _VisibleExecutorUpdate(object):
    """An update of interest to the user.

    Used for awaiting the completion to decide whether to return normally or
    raise an exception.
    """
    def __init__(self, exc_info=(None, None, None)):
      self.finished = exc_info[0] is not None
      self.exception = exc_info[1] or exc_info[0]
      self.exc_info = exc_info

  class _MonitorTask(_ExecutorService.CallableTask):
    """MonitorTask continuously runs to ensure that pipeline makes progress."""
    def __init__(self, executor):
      # type: (_ExecutorServiceParallelExecutor) -> None
      self._executor = executor

    @property
    def name(self):
      return 'monitor'

    def call(self, state_sampler):
      try:
        update = self._executor.all_updates.poll()
        while update:
          if update.committed_bundle:
            self._executor.schedule_consumers(update.committed_bundle)
          elif update.unprocessed_bundle:
            self._executor.schedule_unprocessed_bundle(
                update.transform_executor._applied_ptransform,
                update.unprocessed_bundle)
          else:
            assert update.exception
            _LOGGER.warning(
                'A task failed with exception: %s', update.exception)
            self._executor.visible_updates.offer(
                _ExecutorServiceParallelExecutor._VisibleExecutorUpdate(
                    update.exc_info))
          update = self._executor.all_updates.poll()
        self._executor.evaluation_context.schedule_pending_unblocked_tasks(
            self._executor.executor_service)
        self._add_work_if_necessary(self._fire_timers())
      except Exception as e:  # pylint: disable=broad-except
        _LOGGER.error('Monitor task died due to exception.\n %s', e)
        self._executor.visible_updates.offer(
            _ExecutorServiceParallelExecutor._VisibleExecutorUpdate(
                sys.exc_info()))
      finally:
        if not self._should_shutdown():
          self._executor.executor_service.submit(self)

    def _should_shutdown(self):
      # type: () -> bool

      """Checks whether the pipeline is completed and should be shut down.

      If there is anything in the queue of tasks to do or
      if there are any realtime timers set, do not shut down.

      Otherwise, check if all the transforms' watermarks are complete.
      If they are not, the pipeline is not progressing (stall detected).
      Whether the pipeline has stalled or not, the executor should shut
      down the pipeline.

      Returns:
        True only if the pipeline has reached a terminal state and should
        be shut down.

      """
      if self._is_executing():
        # There are some bundles still in progress.
        return False

      watermark_manager = self._executor.evaluation_context._watermark_manager
      _, any_unfired_realtime_timers = watermark_manager.extract_all_timers()
      if any_unfired_realtime_timers:
        return False

      else:
        if self._executor.evaluation_context.is_done():
          self._executor.visible_updates.offer(
              _ExecutorServiceParallelExecutor._VisibleExecutorUpdate())
        else:
          # Nothing is scheduled for execution, but watermarks incomplete.
          self._executor.visible_updates.offer(
              _ExecutorServiceParallelExecutor._VisibleExecutorUpdate((
                  Exception('Monitor task detected a pipeline stall.'),
                  None,
                  None)))
        self._executor.executor_service.shutdown()
        return True

    def _fire_timers(self):
      """Schedules triggered consumers if any timers fired.

      Returns:
        True if timers fired.
      """
      transform_fired_timers, _ = (
          self._executor.evaluation_context.extract_all_timers())
      for applied_ptransform, fired_timers in transform_fired_timers:
        # Use an empty committed bundle. just to trigger.
        empty_bundle = (
            self._executor.evaluation_context.create_empty_committed_bundle(
                applied_ptransform.inputs[0]))
        timer_completion_callback = _CompletionCallback(
            self._executor.evaluation_context,
            self._executor.all_updates,
            timer_firings=fired_timers)

        self._executor.schedule_consumption(
            applied_ptransform,
            empty_bundle,
            fired_timers,
            timer_completion_callback)
      return bool(transform_fired_timers)

    def _is_executing(self):
      # type: () -> bool

      """Checks whether the job is still executing.

      Returns:
        True if there is at least one non-blocked TransformExecutor active."""

      executors = self._executor.transform_executor_services.executors
      if not executors:
        # Nothing is executing.
        return False

      # Ensure that at least one of those executors is not blocked.
      for transform_executor in executors:
        if not transform_executor.blocked:
          return True
      return False

    def _add_work_if_necessary(self, timers_fired):
      """Adds more work from the roots if pipeline requires more input.

      If all active TransformExecutors are in a blocked state, add more work
      from root nodes that may have additional work. This ensures that if a
      pipeline has elements available from the root nodes it will add those
      elements when necessary.

      Args:
        timers_fired: True if any timers fired prior to this call.
      """
      # If any timers have fired, they will add more work; No need to add more.
      if timers_fired:
        return

      if self._is_executing():
        # We have at least one executor that can proceed without adding
        # additional work.
        return

      # All current TransformExecutors are blocked; add more work from any
      # pending bundles.
      for applied_ptransform in self._executor.all_nodes:
        if not self._executor.evaluation_context.is_done(applied_ptransform):
          pending_bundles = self._executor.node_to_pending_bundles.get(
              applied_ptransform, [])
          for bundle in pending_bundles:
            self._executor.schedule_consumption(
                applied_ptransform,
                bundle, [],
                self._executor.default_completion_callback)
          self._executor.node_to_pending_bundles[applied_ptransform] = []
