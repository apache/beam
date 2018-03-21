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

# cython: profile=True

"""Worker operations executor."""

import collections
import itertools
import logging

from apache_beam import pvalue
from apache_beam.internal import pickler
from apache_beam.io import iobase
from apache_beam.metrics.execution import MetricsContainer
from apache_beam.metrics.execution import ScopedMetricsContainer
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.runners import common
from apache_beam.runners.common import Receiver
from apache_beam.runners.dataflow.internal.names import PropertyNames
from apache_beam.runners.worker import logger
from apache_beam.runners.worker import opcounters
from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker import sideinputs
from apache_beam.transforms import sideinputs as apache_sideinputs
from apache_beam.transforms import combiners
from apache_beam.transforms import core
from apache_beam.transforms.combiners import PhasedCombineFnExecutor
from apache_beam.transforms.combiners import curry_combine_fn
from apache_beam.transforms.window import GlobalWindows
from apache_beam.utils.windowed_value import WindowedValue

# Allow some "pure mode" declarations.
try:
  import cython
except ImportError:
  class FakeCython(object):
    @staticmethod
    def cast(type, value):
      return value
  globals()['cython'] = FakeCython()


_globally_windowed_value = GlobalWindows.windowed_value(None)
_global_window_type = type(_globally_windowed_value.windows[0])


class ConsumerSet(Receiver):
  """A ConsumerSet represents a graph edge between two Operation nodes.

  The ConsumerSet object collects information from the output of the
  Operation at one end of its edge and the input of the Operation at
  the other edge.
  ConsumerSet are attached to the outputting Operation.
  """

  def __init__(
      self, counter_factory, step_name, output_index, consumers, coder):
    self.consumers = consumers
    self.opcounter = opcounters.OperationCounters(
        counter_factory, step_name, coder, output_index)
    # Used in repr.
    self.step_name = step_name
    self.output_index = output_index
    self.coder = coder

  def receive(self, windowed_value):
    self.update_counters_start(windowed_value)
    for consumer in self.consumers:
      cython.cast(Operation, consumer).process(windowed_value)
    self.update_counters_finish()

  def update_counters_start(self, windowed_value):
    self.opcounter.update_from(windowed_value)

  def update_counters_finish(self):
    self.opcounter.update_collect()

  def __repr__(self):
    return '%s[%s.out%s, coder=%s, len(consumers)=%s]' % (
        self.__class__.__name__, self.step_name, self.output_index, self.coder,
        len(self.consumers))


class Operation(object):
  """An operation representing the live version of a work item specification.

  An operation can have one or more outputs and for each output it can have
  one or more receiver operations that will take that as input.
  """

  def __init__(self, operation_name, spec, counter_factory, state_sampler):
    """Initializes a worker operation instance.

    Args:
      operation_name: The system name assigned by the runner for this
        operation.
      spec: A operation_specs.Worker* instance.
      counter_factory: The CounterFactory to use for our counters.
      state_sampler: The StateSampler for the current operation.
    """
    self.operation_name = operation_name
    self.spec = spec
    self.counter_factory = counter_factory
    self.consumers = collections.defaultdict(list)

    # These are overwritten in the legacy harness.
    self.step_name = operation_name
    self.metrics_container = MetricsContainer(self.step_name)
    self.scoped_metrics_container = ScopedMetricsContainer(
        self.metrics_container)

    self.state_sampler = state_sampler
    self.scoped_start_state = self.state_sampler.scoped_state(
        self.operation_name, 'start')
    self.scoped_process_state = self.state_sampler.scoped_state(
        self.operation_name, 'process')
    self.scoped_finish_state = self.state_sampler.scoped_state(
        self.operation_name, 'finish')
    # TODO(ccy): the '-abort' state can be added when the abort is supported in
    # Operations.
    self.receivers = []

  def start(self):
    """Start operation."""
    self.debug_logging_enabled = logging.getLogger().isEnabledFor(
        logging.DEBUG)
    # Everything except WorkerSideInputSource, which is not a
    # top-level operation, should have output_coders
    if getattr(self.spec, 'output_coders', None):
      self.receivers = [ConsumerSet(self.counter_factory, self.step_name,
                                    i, self.consumers[i], coder)
                        for i, coder in enumerate(self.spec.output_coders)]

  def finish(self):
    """Finish operation."""
    pass

  def process(self, o):
    """Process element in operation."""
    pass

  def output(self, windowed_value, output_index=0):
    cython.cast(Receiver, self.receivers[output_index]).receive(windowed_value)

  def add_receiver(self, operation, output_index=0):
    """Adds a receiver operation for the specified output."""
    self.consumers[output_index].append(operation)

  def progress_metrics(self):
    return beam_fn_api_pb2.Metrics.PTransform(
        processed_elements=beam_fn_api_pb2.Metrics.PTransform.ProcessedElements(
            measured=beam_fn_api_pb2.Metrics.PTransform.Measured(
                total_time_spent=(
                    self.scoped_start_state.sampled_seconds()
                    + self.scoped_process_state.sampled_seconds()
                    + self.scoped_finish_state.sampled_seconds()),
                # Multi-output operations should override this.
                output_element_counts=(
                    # If there is exactly one output, we can unambiguously
                    # fix its name later, which we do.
                    # TODO(robertwb): Plumb the actual name here.
                    {'ONLY_OUTPUT': self.receivers[0].opcounter
                                    .element_counter.value()}
                    if len(self.receivers) == 1
                    else None))),
        user=self.metrics_container.to_runner_api())

  def __str__(self):
    """Generates a useful string for this object.

    Compactly displays interesting fields.  In particular, pickled
    fields are not displayed.  Note that we collapse the fields of the
    contained Worker* object into this object, since there is a 1-1
    mapping between Operation and operation_specs.Worker*.

    Returns:
      Compact string representing this object.
    """
    return self.str_internal()

  def str_internal(self, is_recursive=False):
    """Internal helper for __str__ that supports recursion.

    When recursing on receivers, keep the output short.
    Args:
      is_recursive: whether to omit some details, particularly receivers.
    Returns:
      Compact string representing this object.
    """
    printable_name = self.__class__.__name__
    if hasattr(self, 'step_name'):
      printable_name += ' %s' % self.step_name
      if is_recursive:
        # If we have a step name, stop here, no more detail needed.
        return '<%s>' % printable_name

    if self.spec is None:
      printable_fields = []
    else:
      printable_fields = operation_specs.worker_printable_fields(self.spec)

    if not is_recursive and getattr(self, 'receivers', []):
      printable_fields.append('receivers=[%s]' % ', '.join([
          str(receiver) for receiver in self.receivers]))

    return '<%s %s>' % (printable_name, ', '.join(printable_fields))


class ReadOperation(Operation):

  def start(self):
    with self.scoped_start_state:
      with self.scoped_metrics_container:
        super(ReadOperation, self).start()
        range_tracker = self.spec.source.source.get_range_tracker(
            self.spec.source.start_position, self.spec.source.stop_position)
        for value in self.spec.source.source.read(range_tracker):
          if isinstance(value, WindowedValue):
            windowed_value = value
          else:
            windowed_value = _globally_windowed_value.with_value(value)
          self.output(windowed_value)


class InMemoryWriteOperation(Operation):
  """A write operation that will write to an in-memory sink."""

  def process(self, o):
    with self.scoped_process_state:
      if self.debug_logging_enabled:
        logging.debug('Processing [%s] in %s', o, self)
      self.spec.output_buffer.append(
          o if self.spec.write_windowed_values else o.value)


class _TaggedReceivers(dict):

  def __init__(self, counter_factory, step_name):
    self._counter_factory = counter_factory
    self._step_name = step_name

  def __missing__(self, tag):
    self[tag] = receiver = ConsumerSet(
        self._counter_factory, self._step_name, tag, [], None)
    return receiver


class DoOperation(Operation):
  """A Do operation that will execute a custom DoFn for each input element."""

  def __init__(
      self, name, spec, counter_factory, sampler, side_input_maps=None):
    super(DoOperation, self).__init__(name, spec, counter_factory, sampler)
    self.side_input_maps = side_input_maps
    self.tagged_receivers = None

  def _read_side_inputs(self, tags_and_types):
    """Generator reading side inputs in the order prescribed by tags_and_types.

    Args:
      tags_and_types: List of tuples (tag, type). Each side input has a string
        tag that is specified in the worker instruction. The type is actually
        a boolean which is True for singleton input (read just first value)
        and False for collection input (read all values).

    Yields:
      With each iteration it yields the result of reading an entire side source
      either in singleton or collection mode according to the tags_and_types
      argument.
    """
    # Only call this on the old path where side_input_maps was not
    # provided directly.
    assert self.side_input_maps is None

    # Get experiments active in the worker to check for side input metrics exp.
    experiments = RuntimeValueProvider.get_value('experiments', list, [])

    # We will read the side inputs in the order prescribed by the
    # tags_and_types argument because this is exactly the order needed to
    # replace the ArgumentPlaceholder objects in the args/kwargs of the DoFn
    # getting the side inputs.
    #
    # Note that for each tag there could be several read operations in the
    # specification. This can happen for instance if the source has been
    # sharded into several files.
    for i, (side_tag, view_class, view_options) in enumerate(tags_and_types):
      sources = []
      # Using the side_tag in the lambda below will trigger a pylint warning.
      # However in this case it is fine because the lambda is used right away
      # while the variable has the value assigned by the current iteration of
      # the for loop.
      # pylint: disable=cell-var-from-loop
      for si in itertools.ifilter(
          lambda o: o.tag == side_tag, self.spec.side_inputs):
        if not isinstance(si, operation_specs.WorkerSideInputSource):
          raise NotImplementedError('Unknown side input type: %r' % si)
        sources.append(si.source)
        # The tracking of time spend reading and bytes read from side inputs is
        # behind an experiment flag to test its performance impact.
        if 'sideinput_io_metrics' in experiments:
          si_counter = opcounters.SideInputReadCounter(
              self.counter_factory,
              self.state_sampler,
              declaring_step=self.operation_name,
              # Inputs are 1-indexed, so we add 1 to i in the side input id
              input_index=i + 1)
        else:
          si_counter = opcounters.TransformIOCounter()
      iterator_fn = sideinputs.get_iterator_fn_for_sources(
          sources, read_counter=si_counter)

      # Backwards compatibility for pre BEAM-733 SDKs.
      if isinstance(view_options, tuple):
        if view_class == pvalue.AsSingleton:
          has_default, default = view_options
          view_options = {'default': default} if has_default else {}
        else:
          view_options = {}

      yield apache_sideinputs.SideInputMap(
          view_class, view_options, sideinputs.EmulatedIterable(iterator_fn))

  def start(self):
    with self.scoped_start_state:
      super(DoOperation, self).start()

      # See fn_data in dataflow_runner.py
      fn, args, kwargs, tags_and_types, window_fn = (
          pickler.loads(self.spec.serialized_fn))

      state = common.DoFnState(self.counter_factory)
      state.step_name = self.step_name

      # Tag to output index map used to dispatch the side output values emitted
      # by the DoFn function to the appropriate receivers. The main output is
      # tagged with None and is associated with its corresponding index.
      self.tagged_receivers = _TaggedReceivers(
          self.counter_factory, self.step_name)

      output_tag_prefix = PropertyNames.OUT + '_'
      for index, tag in enumerate(self.spec.output_tags):
        if tag == PropertyNames.OUT:
          original_tag = None
        elif tag.startswith(output_tag_prefix):
          original_tag = tag[len(output_tag_prefix):]
        else:
          raise ValueError('Unexpected output name for operation: %s' % tag)
        self.tagged_receivers[original_tag] = self.receivers[index]

      if self.side_input_maps is None:
        if tags_and_types:
          self.side_input_maps = list(self._read_side_inputs(tags_and_types))
        else:
          self.side_input_maps = []

      self.dofn_runner = common.DoFnRunner(
          fn, args, kwargs, self.side_input_maps, window_fn,
          tagged_receivers=self.tagged_receivers,
          step_name=self.step_name,
          logging_context=logger.PerThreadLoggingContext(
              step_name=self.step_name),
          state=state,
          scoped_metrics_container=self.scoped_metrics_container)
      self.dofn_receiver = (self.dofn_runner
                            if isinstance(self.dofn_runner, Receiver)
                            else DoFnRunnerReceiver(self.dofn_runner))

      self.dofn_runner.start()

  def finish(self):
    with self.scoped_finish_state:
      self.dofn_runner.finish()

  def process(self, o):
    with self.scoped_process_state:
      self.dofn_receiver.receive(o)

  def progress_metrics(self):
    metrics = super(DoOperation, self).progress_metrics()
    if self.tagged_receivers:
      metrics.processed_elements.measured.output_element_counts.clear()
      for tag, receiver in self.tagged_receivers.items():
        metrics.processed_elements.measured.output_element_counts[
            str(tag)] = receiver.opcounter.element_counter.value()
    return metrics


class DoFnRunnerReceiver(Receiver):

  def __init__(self, dofn_runner):
    self.dofn_runner = dofn_runner

  def receive(self, windowed_value):
    self.dofn_runner.process(windowed_value)


class CombineOperation(Operation):
  """A Combine operation executing a CombineFn for each input element."""

  def __init__(self, operation_name, spec, counter_factory, state_sampler):
    super(CombineOperation, self).__init__(
        operation_name, spec, counter_factory, state_sampler)
    # Combiners do not accept deferred side-inputs (the ignored fourth argument)
    # and therefore the code to handle the extra args/kwargs is simpler than for
    # the DoFn's of ParDo.
    fn, args, kwargs = pickler.loads(self.spec.serialized_fn)[:3]
    self.phased_combine_fn = (
        PhasedCombineFnExecutor(self.spec.phase, fn, args, kwargs))

  def finish(self):
    logging.debug('Finishing %s', self)

  def process(self, o):
    if self.debug_logging_enabled:
      logging.debug('Processing [%s] in %s', o, self)
    key, values = o.value
    with self.scoped_metrics_container:
      self.output(
          o.with_value((key, self.phased_combine_fn.apply(values))))


def create_pgbk_op(step_name, spec, counter_factory, state_sampler):
  if spec.combine_fn:
    return PGBKCVOperation(step_name, spec, counter_factory, state_sampler)
  else:
    return PGBKOperation(step_name, spec, counter_factory, state_sampler)


class PGBKOperation(Operation):
  """Partial group-by-key operation.

  This takes (windowed) input (key, value) tuples and outputs
  (key, [value]) tuples, performing a best effort group-by-key for
  values in this bundle, memory permitting.
  """

  def __init__(self, operation_name, spec, counter_factory, state_sampler):
    super(PGBKOperation, self).__init__(
        operation_name, spec, counter_factory, state_sampler)
    assert not self.spec.combine_fn
    self.table = collections.defaultdict(list)
    self.size = 0
    # TODO(robertwb) Make this configurable.
    self.max_size = 10 * 1000

  def process(self, o):
    # TODO(robertwb): Structural (hashable) values.
    key = o.value[0], tuple(o.windows)
    self.table[key].append(o)
    self.size += 1
    if self.size > self.max_size:
      self.flush(9 * self.max_size // 10)

  def finish(self):
    self.flush(0)

  def flush(self, target):
    limit = self.size - target
    for ix, (kw, vs) in enumerate(self.table.items()):
      if ix >= limit:
        break
      del self.table[kw]
      key, windows = kw
      output_value = [v.value[1] for v in vs]
      windowed_value = WindowedValue(
          (key, output_value),
          vs[0].timestamp, windows)
      self.output(windowed_value)


class PGBKCVOperation(Operation):

  def __init__(self, operation_name, spec, counter_factory, state_sampler):
    super(PGBKCVOperation, self).__init__(
        operation_name, spec, counter_factory, state_sampler)
    # Combiners do not accept deferred side-inputs (the ignored fourth
    # argument) and therefore the code to handle the extra args/kwargs is
    # simpler than for the DoFn's of ParDo.
    fn, args, kwargs = pickler.loads(self.spec.combine_fn)[:3]
    self.combine_fn = curry_combine_fn(fn, args, kwargs)
    if (getattr(fn.add_input, 'im_func', None)
        is core.CombineFn.add_input.__func__):
      # Old versions of the SDK have CombineFns that don't implement add_input.
      self.combine_fn_add_input = (
          lambda a, e: self.combine_fn.add_inputs(a, [e]))
    else:
      self.combine_fn_add_input = self.combine_fn.add_input
    # Optimization for the (known tiny accumulator, often wide keyspace)
    # combine functions.
    # TODO(b/36567833): Bound by in-memory size rather than key count.
    self.max_keys = (
        1000 * 1000 if
        isinstance(fn, (combiners.CountCombineFn, combiners.MeanCombineFn)) or
        # TODO(b/36597732): Replace this 'or' part by adding the 'cy' optimized
        # combiners to the short list above.
        (isinstance(fn, core.CallableWrapperCombineFn) and
         fn._fn in (min, max, sum)) else 100 * 1000)  # pylint: disable=protected-access
    self.key_count = 0
    self.table = {}

  def process(self, wkv):
    key, value = wkv.value
    # pylint: disable=unidiomatic-typecheck
    # Optimization for the global window case.
    if len(wkv.windows) == 1 and type(wkv.windows[0]) is _global_window_type:
      wkey = 0, key
    else:
      wkey = tuple(wkv.windows), key
    entry = self.table.get(wkey, None)
    if entry is None:
      if self.key_count >= self.max_keys:
        target = self.key_count * 9 // 10
        old_wkeys = []
        # TODO(robertwb): Use an LRU cache?
        for old_wkey, old_wvalue in self.table.iteritems():
          old_wkeys.append(old_wkey)  # Can't mutate while iterating.
          self.output_key(old_wkey, old_wvalue[0])
          self.key_count -= 1
          if self.key_count <= target:
            break
        for old_wkey in reversed(old_wkeys):
          del self.table[old_wkey]
      self.key_count += 1
      # We save the accumulator as a one element list so we can efficiently
      # mutate when new values are added without searching the cache again.
      entry = self.table[wkey] = [self.combine_fn.create_accumulator()]
    entry[0] = self.combine_fn_add_input(entry[0], value)

  def finish(self):
    for wkey, value in self.table.iteritems():
      self.output_key(wkey, value[0])
    self.table = {}
    self.key_count = 0

  def output_key(self, wkey, value):
    windows, key = wkey
    if windows is 0:
      self.output(_globally_windowed_value.with_value((key, value)))
    else:
      self.output(WindowedValue((key, value), windows[0].end, windows))


class FlattenOperation(Operation):
  """Flatten operation.

  Receives one or more producer operations, outputs just one list
  with all the items.
  """

  def process(self, o):
    if self.debug_logging_enabled:
      logging.debug('Processing [%s] in %s', o, self)
    self.output(o)


def create_operation(operation_name, spec, counter_factory, step_name,
                     state_sampler, test_shuffle_source=None,
                     test_shuffle_sink=None, is_streaming=False):
  """Create Operation object for given operation specification."""
  if isinstance(spec, operation_specs.WorkerRead):
    if isinstance(spec.source, iobase.SourceBundle):
      op = ReadOperation(
          operation_name, spec, counter_factory, state_sampler)
    else:
      from dataflow_worker.native_operations import NativeReadOperation
      op = NativeReadOperation(
          operation_name, spec, counter_factory, state_sampler)
  elif isinstance(spec, operation_specs.WorkerWrite):
    from dataflow_worker.native_operations import NativeWriteOperation
    op = NativeWriteOperation(
        operation_name, spec, counter_factory, state_sampler)
  elif isinstance(spec, operation_specs.WorkerCombineFn):
    op = CombineOperation(
        operation_name, spec, counter_factory, state_sampler)
  elif isinstance(spec, operation_specs.WorkerPartialGroupByKey):
    op = create_pgbk_op(operation_name, spec, counter_factory, state_sampler)
  elif isinstance(spec, operation_specs.WorkerDoFn):
    op = DoOperation(operation_name, spec, counter_factory, state_sampler)
  elif isinstance(spec, operation_specs.WorkerGroupingShuffleRead):
    from dataflow_worker.shuffle_operations import GroupedShuffleReadOperation
    op = GroupedShuffleReadOperation(
        operation_name, spec, counter_factory, state_sampler,
        shuffle_source=test_shuffle_source)
  elif isinstance(spec, operation_specs.WorkerUngroupedShuffleRead):
    from dataflow_worker.shuffle_operations import UngroupedShuffleReadOperation
    op = UngroupedShuffleReadOperation(
        operation_name, spec, counter_factory, state_sampler,
        shuffle_source=test_shuffle_source)
  elif isinstance(spec, operation_specs.WorkerInMemoryWrite):
    op = InMemoryWriteOperation(
        operation_name, spec, counter_factory, state_sampler)
  elif isinstance(spec, operation_specs.WorkerShuffleWrite):
    from dataflow_worker.shuffle_operations import ShuffleWriteOperation
    op = ShuffleWriteOperation(
        operation_name, spec, counter_factory, state_sampler,
        shuffle_sink=test_shuffle_sink)
  elif isinstance(spec, operation_specs.WorkerFlatten):
    op = FlattenOperation(
        operation_name, spec, counter_factory, state_sampler)
  elif isinstance(spec, operation_specs.WorkerMergeWindows):
    from dataflow_worker.shuffle_operations import BatchGroupAlsoByWindowsOperation
    from dataflow_worker.shuffle_operations import StreamingGroupAlsoByWindowsOperation
    if is_streaming:
      op = StreamingGroupAlsoByWindowsOperation(
          operation_name, spec, counter_factory, state_sampler)
    else:
      op = BatchGroupAlsoByWindowsOperation(
          operation_name, spec, counter_factory, state_sampler)
  elif isinstance(spec, operation_specs.WorkerReifyTimestampAndWindows):
    from dataflow_worker.shuffle_operations import ReifyTimestampAndWindowsOperation
    op = ReifyTimestampAndWindowsOperation(
        operation_name, spec, counter_factory, state_sampler)
  else:
    raise TypeError('Expected an instance of operation_specs.Worker* class '
                    'instead of %s' % (spec,))
  op.step_name = step_name
  op.metrics_container = MetricsContainer(step_name)
  op.scoped_metrics_container = ScopedMetricsContainer(op.metrics_container)
  return op


class SimpleMapTaskExecutor(object):
  """An executor for map tasks.

   Stores progress of the read operation that is the first operation of a map
   task.
  """

  def __init__(
      self, map_task, counter_factory, state_sampler,
      test_shuffle_source=None, test_shuffle_sink=None):
    """Initializes SimpleMapTaskExecutor.

    Args:
      map_task: The map task we are to run.
      counter_factory: The CounterFactory instance for the work item.
      state_sampler: The StateSampler tracking the execution step.
      test_shuffle_source: Used during tests for dependency injection into
        shuffle read operation objects.
      test_shuffle_sink: Used during tests for dependency injection into
        shuffle write operation objects.
    """

    self._map_task = map_task
    self._counter_factory = counter_factory
    self._ops = []
    self._state_sampler = state_sampler
    self._test_shuffle_source = test_shuffle_source
    self._test_shuffle_sink = test_shuffle_sink

  def operations(self):
    return self._ops[:]

  def execute(self):
    """Executes all the operation_specs.Worker* instructions in a map task.

    We update the map_task with the execution status, expressed as counters.

    Raises:
      RuntimeError: if we find more than on read instruction in task spec.
      TypeError: if the spec parameter is not an instance of the recognized
        operation_specs.Worker* classes.
    """

    # operations is a list of operation_specs.Worker* instances.
    # The order of the elements is important because the inputs use
    # list indexes as references.

    step_names = (
        self._map_task.step_names or [None] * len(self._map_task.operations))
    for ix, spec in enumerate(self._map_task.operations):
      # This is used for logging and assigning names to counters.
      operation_name = self._map_task.system_names[ix]
      step_name = step_names[ix]
      op = create_operation(
          operation_name, spec, self._counter_factory, step_name,
          self._state_sampler,
          test_shuffle_source=self._test_shuffle_source,
          test_shuffle_sink=self._test_shuffle_sink)
      self._ops.append(op)

      # Add receiver operations to the appropriate producers.
      if hasattr(op.spec, 'input'):
        producer, output_index = op.spec.input
        self._ops[producer].add_receiver(op, output_index)
      # Flatten has 'inputs', not 'input'
      if hasattr(op.spec, 'inputs'):
        for producer, output_index in op.spec.inputs:
          self._ops[producer].add_receiver(op, output_index)

    for ix, op in reversed(list(enumerate(self._ops))):
      logging.debug('Starting op %d %s', ix, op)
      with op.scoped_metrics_container:
        op.start()
    for op in self._ops:
      with op.scoped_metrics_container:
        op.finish()
