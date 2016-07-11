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

from __future__ import absolute_import

import collections
import logging
import Queue
import threading
import traceback
from weakref import WeakValueDictionary


class ExecutorService(object):
  """Thread pool for executing tasks in parallel."""

  class CallableTask(object):

    def __call__(self):
      pass

    @property
    def name(self):
      return None

  class ExecutorServiceWorker(threading.Thread):
    """Worker thread for executing a single task at a time."""

    # Amount to block waiting for getting an item from the queue in seconds.
    TIMEOUT = 5

    def __init__(self, queue, index):
      super(ExecutorService.ExecutorServiceWorker, self).__init__()
      self.queue = queue
      self._index = index
      self._default_name = 'ExecutorServiceWorker-' + str(index)
      self._update_name()
      self.shutdown_requested = False
      self.start()

    def _update_name(self, task=None):
      if task and task.name:
        name = task.name
      else:
        name = self._default_name
      self.name = 'Thread: %d, %s (%s)' % (
          self._index, name, 'executing' if task else 'idle')

    def _get_task_or_none(self):
      try:
        # Do not block indefinitely, otherwise we may not act for a requested
        # shutdown.
        return self.queue.get(
            timeout=ExecutorService.ExecutorServiceWorker.TIMEOUT)
      except Queue.Empty:
        return None

    def run(self):
      while not self.shutdown_requested:
        task = self._get_task_or_none()
        if task:
          try:
            if not self.shutdown_requested:
              self._update_name(task)
              task()
              self._update_name()
          finally:
            self.queue.task_done()

    def shutdown(self):
      self.shutdown_requested = True

  def __init__(self, num_workers):
    self.queue = Queue.Queue()
    self.workers = [ExecutorService.ExecutorServiceWorker(
        self.queue, i) for i in range(num_workers)]
    self.shutdown_requested = False

  def submit(self, task):
    assert isinstance(task, ExecutorService.CallableTask)
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
      except Queue.Empty:
        continue
    # All existing threads will eventually terminate (after they complete their
    # last task).


class TransformEvaluationState(object):

  def __init__(self, executor_service, scheduled):
    self.executor_service = executor_service
    self.scheduled = scheduled

  def schedule(self, work):
    self.scheduled.add(work)
    self.executor_service.submit(work)

  def complete(self, completed_work):
    self.scheduled.remove(completed_work)


class ParallelEvaluationState(TransformEvaluationState):
  """A TransformEvaluationState with unlimited parallelism.

  Any TransformExecutor scheduled will be immediately submitted to the
  ExecutorService.

  A principal use of this is for evaluators that can generate output bundles
  only using the input bundle (e.g. ParDo).
  """
  pass


class SerialEvaluationState(TransformEvaluationState):
  """A TransformEvaluationState with a single work queue.

  Any TransformExecutor scheduled will be placed on the work queue. Only one
  item of work will be submitted to the ExecutorService at any time.

  A principal use of this is for evaluators that keeps a global state such as
  GroupByKeyOnly.
  """

  def __init__(self, executor_service, scheduled):
    super(SerialEvaluationState, self).__init__(executor_service, scheduled)
    self.serial_queue = collections.deque()
    self.currently_evaluating = None
    self._lock = threading.Lock()

  def complete(self, completed_work):
    self._update_currently_evaluating(None, completed_work)
    super(SerialEvaluationState, self).complete(completed_work)

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
        super(SerialEvaluationState, self).schedule(next_work)


class TransformExecutorServices(object):
  """Schedules and completes TransformExecutors.

  Controls the concurrency as appropriate for the applied transform the executor
  exists for.
  """

  def __init__(self, executor_service):
    self._executor_service = executor_service
    self._scheduled = set()
    self._parallel = ParallelEvaluationState(
        self._executor_service, self._scheduled)
    self._serial_cache = WeakValueDictionary()

  def parallel(self):
    return self._parallel

  def serial(self, step):
    cached = self._serial_cache.get(step)
    if not cached:
      cached = SerialEvaluationState(self._executor_service, self._scheduled)
      self._serial_cache[step] = cached
    return  cached

  @property
  def executors(self):
    return frozenset(self._scheduled)


class _CompletionCallback(object):
  """The default completion callback.

  The default completion callback is used to complete transform evaluations
  that are triggered due to the arrival of elements from an upstream transform,
  or for a source transform.
  """

  def __init__(self, evaluation_context, all_updates, timers=None):
    self._evaluation_context = evaluation_context
    self._all_updates = all_updates
    self._timers = timers

  def handle_result(self, input_committed_bundle, transform_result):
    output_committed_bundles = self._evaluation_context.handle_result(
        input_committed_bundle, self._timers, transform_result)
    for output_committed_bundle in output_committed_bundles:
      self._all_updates.offer(_ExecutorServiceParallelExecutor.ExecutorUpdate(
          output_committed_bundle, None))
    return output_committed_bundles

  def handle_exception(self, exception):
    self._all_updates.offer(
        _ExecutorServiceParallelExecutor.ExecutorUpdate(None, exception))


class _TimerCompletionCallback(_CompletionCallback):

  def __init__(self, evaluation_context, all_updates, timers):
    super(_TimerCompletionCallback, self).__init__(
        evaluation_context, all_updates, timers)


class TransformExecutor(ExecutorService.CallableTask):
  """TransformExecutor will evaluate a bundle using an applied ptransform.

  A CallableTask responsible for constructing a TransformEvaluator andevaluating
  it on some bundle of input, and registering the result using the completion
  callback.
  """

  def __init__(self, transform_evaluator_registry, evaluation_context,
               input_bundle, applied_transform, completion_callback,
               transform_evaluation_state):
    self._transform_evaluator_registry = transform_evaluator_registry
    self._evaluation_context = evaluation_context
    self._input_bundle = input_bundle
    self._applied_transform = applied_transform
    self._completion_callback = completion_callback
    self._transform_evaluation_state = transform_evaluation_state
    self._side_input_values = {}
    self.blocked = False
    self._call_count = 0

  def __call__(self):
    self._call_count += 1
    assert self._call_count <= (1 + len(self._applied_transform.side_inputs))

    for side_input in self._applied_transform.side_inputs:
      if side_input not in self._side_input_values:
        has_result, value = (
            self._evaluation_context.get_value_or_schedule_after_output(
                side_input, self))
        if not has_result:
          # Monitor task will reschedule this executor once the side input is
          # available.
          return
        self._side_input_values[side_input] = value

    side_input_values = [self._side_input_values[side_input]
                         for side_input in self._applied_transform.side_inputs]

    try:
      evaluator = self._transform_evaluator_registry.for_application(
          self._applied_transform, self._input_bundle, side_input_values)

      if self._input_bundle:
        for value in self._input_bundle.elements:
          evaluator.process_element(value)

      result = evaluator.finish_bundle()

      if self._evaluation_context.has_cache:
        for uncommitted_bundle in result.output_bundles:
          self._evaluation_context.append_to_cache(
              self._applied_transform, uncommitted_bundle.tag,
              uncommitted_bundle.elements)
        undeclared_tag_values = result.undeclared_tag_values
        if undeclared_tag_values:
          for tag, value in undeclared_tag_values.iteritems():
            self._evaluation_context.append_to_cache(
                self._applied_transform, tag, value)

      self._completion_callback.handle_result(self._input_bundle, result)
      return result
    except Exception as e:  # pylint: disable=broad-except
      logging.warning('Task failed: %s', traceback.format_exc(), exc_info=True)
      self._completion_callback.handle_exception(e)
    finally:
      self._transform_evaluation_state.complete(self)


class InProcessExecutor(object):

  def __init__(self, *args, **kwargs):
    self._executor = _ExecutorServiceParallelExecutor(*args, **kwargs)

  def start(self, roots):
    self._executor.start(roots)

  def await_completion(self):
    self._executor.await_completion()


class _ExecutorServiceParallelExecutor(object):
  """An internal implementation for InProcessExecutor."""

  NUM_WORKERS = 1

  def __init__(self, value_to_consumers, transform_evaluator_registry,
               evaluation_context):
    self.executor_service = ExecutorService(
        _ExecutorServiceParallelExecutor.NUM_WORKERS)
    self.transform_executor_services = TransformExecutorServices(
        self.executor_service)
    self.value_to_consumers = value_to_consumers
    self.transform_evaluator_registry = transform_evaluator_registry
    self.evaluation_context = evaluation_context
    self.all_updates = _ExecutorServiceParallelExecutor._TypedUpdateQueue(
        _ExecutorServiceParallelExecutor.ExecutorUpdate)
    self.visible_updates = _ExecutorServiceParallelExecutor._TypedUpdateQueue(
        _ExecutorServiceParallelExecutor.VisibleExecutorUpdate)
    self.default_completion_callback = _CompletionCallback(
        evaluation_context, self.all_updates)

  def start(self, roots):
    self.root_nodes = frozenset(roots)
    self.executor_service.submit(
        _ExecutorServiceParallelExecutor._MonitorTask(self))

  def await_completion(self):
    update = self.visible_updates.take()
    try:
      if update.exception:
        raise update.exception
    finally:
      self.executor_service.shutdown()

  def schedule_consumers(self, committed_bundle):
    if committed_bundle.pcollection in self.value_to_consumers:
      consumers = self.value_to_consumers[committed_bundle.pcollection]
      for applied_ptransform in consumers:
        self.schedule_consumption(applied_ptransform, committed_bundle,
                                  self.default_completion_callback)

  def schedule_consumption(self, consumer_applied_transform, committed_bundle,
                           on_complete):
    """Schedules evaluation of the given bundle with the transform."""
    assert all([consumer_applied_transform, on_complete])
    assert committed_bundle or consumer_applied_transform in self.root_nodes
    if (committed_bundle
        and self.transform_evaluator_registry.should_execute_serially(
            consumer_applied_transform)):
      transform_executor_service = self.transform_executor_services.serial(
          consumer_applied_transform)
    else:
      transform_executor_service = self.transform_executor_services.parallel()

    transform_executor = TransformExecutor(
        self.transform_evaluator_registry, self.evaluation_context,
        committed_bundle, consumer_applied_transform, on_complete,
        transform_executor_service)
    transform_executor_service.schedule(transform_executor)

  class _TypedUpdateQueue(object):
    """Type checking update queue with blocking and non-blocking operations."""

    def __init__(self, item_type):
      self._item_type = item_type
      self._queue = Queue.Queue()

    def poll(self):
      try:
        item = self._queue.get_nowait()
        self._queue.task_done()
        return  item
      except Queue.Empty:
        return None

    def take(self):
      item = self._queue.get()
      self._queue.task_done()
      return item

    def offer(self, item):
      assert isinstance(item, self._item_type)
      self._queue.put_nowait(item)

  class ExecutorUpdate(object):
    """An internal status update on the state of the executor."""

    def __init__(self, produced_bundle=None, exception=None):
      # Exactly one of them should be not-None
      assert bool(produced_bundle) != bool(exception)
      self.committed_bundle = produced_bundle
      self.exception = exception

  class VisibleExecutorUpdate(object):
    """An update of interest to the user.

    Used for awaiting the completion to decide whether to return normally or
    raise an exception.
    """

    def __init__(self, exception=None):
      self.finished = exception is not None
      self.exception = exception

  class _MonitorTask(ExecutorService.CallableTask):
    """MonitorTask continuously runs to ensure that pipeline makes progress."""

    def __init__(self, executor):
      self._executor = executor

    @property
    def name(self):
      return 'monitor'

    def __call__(self):
      try:
        update = self._executor.all_updates.poll()
        while update:
          if update.committed_bundle:
            self._executor.schedule_consumers(update.committed_bundle)
          else:
            assert update.exception
            logging.warning('A task failed with exception.\n %s',
                            update.exception)
            self._executor.visible_updates.offer(
                _ExecutorServiceParallelExecutor.VisibleExecutorUpdate(
                    update.exception))
          update = self._executor.all_updates.poll()
        self._executor.evaluation_context.schedule_pending_unblocked_tasks(
            self._executor.executor_service)
        self._add_work_if_necessary(self._fire_timers())
      except Exception as e:  # pylint: disable=broad-except
        logging.error('Monitor task died due to exception.\n %s', e)
        self._executor.visible_updates.offer(
            _ExecutorServiceParallelExecutor.VisibleExecutorUpdate(e))
      finally:
        if not self._should_shutdown():
          self._executor.executor_service.submit(self)

    def _should_shutdown(self):
      """_should_shutdown checks whether pipeline is completed or not.

      It will check for successful completion by checking the watermarks of all
      transforms. If they all reached the maximum watermark it means that
      pipeline successfully reached to completion.

      If the above is not true, it will check that at least one executor is
      making progress. Otherwise pipeline will be declared stalled.

      If the pipeline reached to a terminal state as explained above
      _should_shutdown will request executor to gracefully shutdown.

      Returns:
        True if pipeline reached a terminal state and monitor task could finish.
        Otherwise monitor task should schedule itself again for future
        execution.
      """
      if self._executor.evaluation_context.is_done():
        self._executor.visible_updates.offer(
            _ExecutorServiceParallelExecutor.VisibleExecutorUpdate())
        self._executor.executor_service.shutdown()
        return True
      elif not self._is_executing:
        self._executor.visible_updates.offer(
            _ExecutorServiceParallelExecutor.VisibleExecutorUpdate(
                Exception('Monitor task detected a pipeline stall.')))
        self._executor.executor_service.shutdown()
        return True
      return False

    def _fire_timers(self):
      """Schedules triggered consumers if any timers fired.

      Returns:
        True if timers fired.
      """
      fired_timers = self._executor.evaluation_context.extract_fired_timers()
      for applied_ptransform in fired_timers:
        # Use an empty committed bundle. just to trigger.
        empty_bundle = (
            self._executor.evaluation_context.create_empty_committed_bundle(
                applied_ptransform.inputs[0]))
        timer_completion_callback = _TimerCompletionCallback(
            self._executor.evaluation_context, self._executor.all_updates,
            applied_ptransform)

        self._executor.schedule_consumption(
            applied_ptransform, empty_bundle, timer_completion_callback)
      return bool(fired_timers)

    def _is_executing(self):
      """Returns True if there is at least one non-blocked TransformExecutor."""
      for transform_executor in (
          self._executor.transform_executor_services.executors):
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

      # All current TransformExecutors are blocked; add more work from the
      # roots.
      for applied_transform in self._executor.root_nodes:
        if not self._executor.evaluation_context.is_done(applied_transform):
          self._executor.schedule_consumption(
              applied_transform, None,
              self._executor.default_completion_callback)
