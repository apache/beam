# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# cython: profile=True

"""Worker operations executor."""

import collections
import itertools
import logging
import random


from google.cloud.dataflow import pvalue
from google.cloud.dataflow.coders import BytesCoder
from google.cloud.dataflow.coders import TupleCoder
from google.cloud.dataflow.coders import WindowedValueCoder
from google.cloud.dataflow.internal import pickler
from google.cloud.dataflow.pvalue import EmptySideInput
from google.cloud.dataflow.runners import common
import google.cloud.dataflow.transforms as ptransform
from google.cloud.dataflow.transforms import combiners
from google.cloud.dataflow.transforms import trigger
from google.cloud.dataflow.transforms.combiners import curry_combine_fn
from google.cloud.dataflow.transforms.combiners import PhasedCombineFnExecutor
from google.cloud.dataflow.transforms.trigger import InMemoryUnmergedState
from google.cloud.dataflow.transforms.window import GlobalWindows
from google.cloud.dataflow.transforms.window import MIN_TIMESTAMP
from google.cloud.dataflow.transforms.window import WindowedValue
from google.cloud.dataflow.utils.names import PropertyNames
from google.cloud.dataflow.worker import logger
from google.cloud.dataflow.worker import maptask
from google.cloud.dataflow.worker import opcounters
from google.cloud.dataflow.worker import shuffle
from google.cloud.dataflow.worker import sideinputs


class ReceiverSet(object):
  """A ReceiverSet represents a graph edge between two Operation nodes.

  The ReceiverSet object collects information from the output of the
  Operation at one end of its edge and the input of the Operation at
  the other edge.
  ReceiverSets are attached to the outputting Operation.
  """

  def __init__(self, counter_factory, coder, output_index=0):
    self.receivers = []
    self.opcounter = None
    self.counter_factory = counter_factory
    self.output_index = output_index
    self.coder = coder

  def add_receiver(self, receiving_operation):
    self.receivers.append(receiving_operation)

  def start(self, step_name):
    self.opcounter = opcounters.OperationCounters(
        self.counter_factory, step_name, self.coder, self.output_index)

  def output(self, windowed_value, coder=None):
    self.update_counters_start(windowed_value, coder)
    for receiver in self.receivers:
      receiver.process(windowed_value)
    self.update_counters_finish()

  def update_counters_start(self, windowed_value, coder=None):
    if self.opcounter:
      self.opcounter.update_from(windowed_value, coder)

  def update_counters_finish(self):
    if self.opcounter:
      self.opcounter.update_collect()

  def __str__(self):
    return '[%s]' % ' '.join([r.str_internal(is_recursive=True)
                              for r in self.receivers])

  def __repr__(self):
    return '<%s %d %s [%s]>' % (
        self.__class__.__name__,
        self.output_index,
        self.coder,
        ' '.join([r.str_internal(is_recursive=True) for r in self.receivers]))


class Operation(object):
  """An operation representing the live version of a work item specification.

  An operation can have one or more outputs and for each output it can have
  one or more receiver operations that will take that as input.
  """

  def __init__(self, spec, counter_factory):
    """Initializes a worker operation instance.

    Args:
      spec: A maptask.Worker* instance.
      counter_factory: the counter_factory to use for our counters.
    """
    self.counter_factory = counter_factory
    self.spec = spec
    self.receivers = []
    # Everything except WorkerSideInputSource, which is not a
    # top-level operation, should have output_coders
    if getattr(self.spec, 'output_coders', None):
      for i, coder in enumerate(self.spec.output_coders):
        self.receivers.append(ReceiverSet(counter_factory, coder, i))

  def start(self):
    """Start operation."""
    self.debug_logging_enabled = logging.getLogger().isEnabledFor(
        logging.DEBUG)
    # Start our receivers, now that we know our step name.
    for receiver in self.receivers:
      receiver.start(self.step_name)

  def finish(self):
    """Finish operation."""
    pass

  def process(self, o):
    """Process element in operation."""
    pass

  def output(self, windowed_value, coder=None, output_index=0):
    self.receivers[output_index].output(windowed_value, coder)

  def add_receiver(self, operation, output_index=0):
    """Adds a receiver operation for the specified output."""
    self.receivers[output_index].add_receiver(operation)

  def __str__(self):
    """Generates a useful string for this object.

    Compactly displays interesting fields.  In particular, pickled
    fields are not displayed.  Note that we collapse the fields of the
    contained Worker* object into this object, since there is a 1-1
    mapping between Operation and maptask.Worker*.

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

    printable_fields = maptask.worker_printable_fields(self.spec)

    if not is_recursive and getattr(self, 'receivers', []):
      printable_fields.append('receivers=[%s]' % ', '.join([
          str(receiver) for receiver in self.receivers]))

    return '<%s %s>' % (printable_name, ', '.join(printable_fields))


class ReadOperation(Operation):
  """A generic read operation that reads from proper input source."""

  def __init__(self, spec, counter_factory):
    super(ReadOperation, self).__init__(spec, counter_factory)
    self._current_progress = None
    self._reader = None

  def start(self):
    # We cache reader progress to make sure that the progress reporting
    # thread does not get blocked due to any reader related operations.
    self._current_progress = None
    super(ReadOperation, self).start()
    with self.spec.source.reader() as reader:
      self._reader = reader
      for value in reader:
        self._current_progress = reader.get_progress()
        if reader.returns_windowed_values:
          windowed_value = value
        else:
          windowed_value = GlobalWindows.WindowedValue(value)
        self.output(windowed_value)

  def request_dynamic_split(self, dynamic_split_request):
    if self._reader is not None:
      return self._reader.request_dynamic_split(dynamic_split_request)
    else:
      logging.warning('Cannot complete the dynamic split request since the '
                      'reader is not set')

  def get_progress(self):
    """Returns the progress of the read operation.

    This method is invoked by the progress reporting thread. No need to lock
    the variable "current_progress" since it is updated by a simple variable
    assignment and we are OK with current_progress value returned here being
    slightly stale.

    Returns:
      Progress of the ReadOperation.
    """
    return self._current_progress


class WriteOperation(Operation):
  """A generic write operation that writes to a proper output sink."""

  def __init__(self, spec, counter_factory):
    super(WriteOperation, self).__init__(spec, counter_factory)
    self.writer = None
    self.use_windowed_value = False

  def start(self):
    super(WriteOperation, self).start()
    self.writer = self.spec.sink.writer()
    self.writer.__enter__()
    self.use_windowed_value = self.writer.takes_windowed_values

  def finish(self):
    self.writer.__exit__(None, None, None)

  def process(self, o):
    if self.debug_logging_enabled:
      logging.debug('Processing [%s] in %s', o, self)
    assert isinstance(o, WindowedValue)
    self.receivers[0].update_counters_start(o)
    if self.use_windowed_value:
      self.writer.Write(o)
    else:
      self.writer.Write(o.value)
    self.receivers[0].update_counters_finish()


class InMemoryWriteOperation(Operation):
  """A write operation that will write to an in-memory sink."""

  def __init__(self, spec, counter_factory):
    super(InMemoryWriteOperation, self).__init__(spec, counter_factory)
    self.spec = spec

  def process(self, o):
    if self.debug_logging_enabled:
      logging.debug('Processing [%s] in %s', o, self)
    assert isinstance(o, WindowedValue)
    self.receivers[0].update_counters_start(o)
    self.spec.output_buffer.append(o.value)
    self.receivers[0].update_counters_finish()


class GroupedShuffleReadOperation(Operation):
  """A shuffle read operation that will read from a grouped shuffle source."""

  def __init__(self, spec, counter_factory, shuffle_source=None):
    super(GroupedShuffleReadOperation, self).__init__(spec, counter_factory)
    self.shuffle_source = shuffle_source
    self._reader = None

  def start(self):
    super(GroupedShuffleReadOperation, self).start()
    write_coder = None
    if self.shuffle_source is None:
      coders = (self.spec.coder.key_coder(), self.spec.coder.value_coder())
      write_coder = WindowedValueCoder(TupleCoder(coders))
      self.shuffle_source = shuffle.GroupedShuffleSource(
          self.spec.shuffle_reader_config, coder=coders,
          start_position=self.spec.start_shuffle_position,
          end_position=self.spec.end_shuffle_position)
    with self.shuffle_source.reader() as reader:
      for key, key_values in reader:
        self._reader = reader
        windowed_value = GlobalWindows.WindowedValue((key, key_values))
        self.output(windowed_value, coder=write_coder)

  def get_progress(self):
    if self._reader is not None:
      return self._reader.get_progress()

  def request_dynamic_split(self, dynamic_split_request):
    if self._reader is not None:
      return self._reader.request_dynamic_split(dynamic_split_request)


class UngroupedShuffleReadOperation(Operation):
  """A shuffle read operation reading from an ungrouped shuffle source."""

  def __init__(self, spec, counter_factory, shuffle_source=None):
    super(UngroupedShuffleReadOperation, self).__init__(spec, counter_factory)
    self.shuffle_source = shuffle_source
    self._reader = None

  def start(self):
    super(UngroupedShuffleReadOperation, self).start()
    write_coder = None
    if self.shuffle_source is None:
      coders = (BytesCoder(), self.spec.coder)
      write_coder = WindowedValueCoder(TupleCoder(coders))
      self.shuffle_source = shuffle.UngroupedShuffleSource(
          self.spec.shuffle_reader_config, coder=coders,
          start_position=self.spec.start_shuffle_position,
          end_position=self.spec.end_shuffle_position)
    with self.shuffle_source.reader() as reader:
      for value in reader:
        self._reader = reader
        windowed_value = GlobalWindows.WindowedValue(value)
        self.output(windowed_value, coder=write_coder)

  def get_progress(self):
    # 'UngroupedShuffleReader' does not support progress reporting.
    pass

  def request_dynamic_split(self, dynamic_split_request):
    # 'UngroupedShuffleReader' does not support dynamic work rebalancing.
    pass


class ShuffleWriteOperation(Operation):
  """A shuffle write operation that will write to a shuffle sink."""

  def __init__(self, spec, counter_factory, shuffle_sink=None):
    super(ShuffleWriteOperation, self).__init__(spec, counter_factory)
    self.writer = None
    self.shuffle_sink = shuffle_sink

  def start(self):
    super(ShuffleWriteOperation, self).start()
    self.is_ungrouped = self.spec.shuffle_kind == 'ungrouped'
    coder = self.spec.output_coders[0]
    if self.is_ungrouped:
      coders = (BytesCoder(), coder)
    else:
      coders = (coder.key_coder(), coder.value_coder())
    self._write_coder = WindowedValueCoder(TupleCoder(coders))
    if self.shuffle_sink is None:
      self.shuffle_sink = shuffle.ShuffleSink(
          self.spec.shuffle_writer_config, coder=coders)
    self.writer = self.shuffle_sink.writer()
    self.writer.__enter__()

  def finish(self):
    logging.debug('Finishing %s', self)
    self.writer.__exit__(None, None, None)

  def process(self, o):
    if self.debug_logging_enabled:
      logging.debug('Processing [%s] in %s', o, self)
    assert isinstance(o, WindowedValue)
    self.receivers[0].update_counters_start(o, coder=self._write_coder)
    # We typically write into shuffle key/value pairs. This is the reason why
    # the else branch below expects the value attribute of the WindowedValue
    # argument to be a KV pair. However the service may write to shuffle in
    # 'ungrouped' mode in which case the value written is just a plain value and
    # the key is randomly generated on the spot. The random keys make sure that
    # the resulting KV pairs are uniformly distributed. The 'ungrouped' mode is
    # used to reshard workflow outputs into a fixed set of files. This is
    # achieved by using an UngroupedShuffleSource to read back the values
    # written in 'ungrouped' mode.
    if self.is_ungrouped:
      # We want to spread the values uniformly to all shufflers.
      k, v = str(random.getrandbits(64)), o.value
    else:
      k, v = o.value
    # TODO(silviuc): Use timestamps for the secondary key to get values in
    # times-sorted order.
    self.writer.Write(k, '', v)
    self.receivers[0].update_counters_finish()


class DoOperation(Operation):
  """A Do operation that will execute a custom DoFn for each input element."""

  def __init__(self, spec, counter_factory):
    super(DoOperation, self).__init__(spec, counter_factory)
    self.state = common.DoFnState(counter_factory)

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
    # We will read the side inputs in the order prescribed by the
    # tags_and_types argument because this is exactly the order needed to
    # replace the ArgumentPlaceholder objects in the args/kwargs of the DoFn
    # getting the side inputs.
    #
    # Note that for each tag there could be several read operations in the
    # specification. This can happen for instance if the source has been
    # sharded into several files.
    for side_tag, view_class, view_options in tags_and_types:
      sources = []
      # Using the side_tag in the lambda below will trigger a pylint warning.
      # However in this case it is fine because the lambda is used right away
      # while the variable has the value assigned by the current iteration of
      # the for loop.
      # pylint: disable=cell-var-from-loop
      for si in itertools.ifilter(
          lambda o: o.tag == side_tag, self.spec.side_inputs):
        if not isinstance(si, maptask.WorkerSideInputSource):
          raise NotImplementedError('Unknown side input type: %r' % si)
        sources.append(si.source)
      iterator_fn = sideinputs.get_iterator_fn_for_sources(sources)

      if view_class == pvalue.SingletonPCollectionView:
        has_default, default = view_options
        has_result = False
        result = None
        for v in iterator_fn():
          has_result = True
          result = v
          break
        if has_result:
          yield result
        elif has_default:
          yield default
        else:
          yield EmptySideInput()
      elif view_class == pvalue.IterablePCollectionView:
        yield sideinputs.EmulatedIterable(iterator_fn)
      elif view_class == pvalue.ListPCollectionView:
        # TODO(ccy): this is not yet suitable for lists that do not fit in
        # memory on a single machine.
        yield list(iterator_fn())
      elif view_class == pvalue.DictPCollectionView:
        # TODO(ccy): this is not yet suitable for dictionaries that do not fit
        # in memory on a single machine.
        yield dict(iterator_fn())
      else:
        raise NotImplementedError('Unknown PCollectionView type: %s' %
                                  view_class)

  def start(self):
    super(DoOperation, self).start()

    # See fn_data in dataflow_runner.py
    fn, args, kwargs, tags_and_types, window_fn = (
        pickler.loads(self.spec.serialized_fn))

    self.state.step_name = self.step_name

    # TODO(silviuc): What is the proper label here? PCollection being processed?
    self.context = ptransform.DoFnProcessContext('label', state=self.state)
    # Tag to output index map used to dispatch the side output values emitted
    # by the DoFn function to the appropriate receivers. The main output is
    # tagged with None and is associated with its corresponding index.
    tagged_receivers = {}
    output_tag_prefix = PropertyNames.OUT + '_'
    for index, tag in enumerate(self.spec.output_tags):
      if tag == PropertyNames.OUT:
        original_tag = None
      elif tag.startswith(output_tag_prefix):
        original_tag = tag[len(output_tag_prefix):]
      else:
        raise ValueError('Unexpected output name for operation: %s' % tag)
      tagged_receivers[original_tag] = self.receivers[index]

    self.dofn_runner = common.DoFnRunner(
        fn, args, kwargs, self._read_side_inputs(tags_and_types),
        window_fn, self.context, tagged_receivers,
        logger, self.step_name)

    self.dofn_runner.start()

  def finish(self):
    self.dofn_runner.finish()

  def process(self, o):
    self.dofn_runner.process(o)


class CombineOperation(Operation):
  """A Combine operation executing a CombineFn for each input element."""

  def __init__(self, spec, counter_factory):
    super(CombineOperation, self).__init__(spec, counter_factory)
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
    assert isinstance(o, WindowedValue)
    key, values = o.value
    self.output(
        o.with_value((key, self.phased_combine_fn.apply(values))))


def create_pgbk_op(spec, counter_factory):
  if spec.combine_fn:
    return PGBKCVOperation(spec, counter_factory)
  else:
    return PGBKOperation(spec, counter_factory)


class PGBKOperation(Operation):
  """Partial group-by-key operation.

  This takes (windowed) input (key, value) tuples and outputs
  (key, [value]) tuples, performing a best effort group-by-key for
  values in this bundle, memory permitting.
  """

  def __init__(self, spec, counter_factory):
    super(PGBKOperation, self).__init__(spec, counter_factory)
    assert not self.spec.combine_fn
    self.table = collections.defaultdict(list)
    self.size = 0
    # TODO(robertwb) Make this configurable.
    self.max_size = 10000

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

  def __init__(self, spec, counter_factory):
    super(PGBKCVOperation, self).__init__(spec, counter_factory)
    # Combiners do not accept deferred side-inputs (the ignored fourth
    # argument) and therefore the code to handle the extra args/kwargs is
    # simpler than for the DoFn's of ParDo.
    fn, args, kwargs = pickler.loads(self.spec.combine_fn)[:3]
    self.combine_fn = curry_combine_fn(fn, args, kwargs)
    # Optimization for the (known tiny accumulator, often wide keyspace)
    # count function.
    # TODO(robertwb): Bound by in-memory size rather than key count.
    self.max_keys = (
        1000000 if isinstance(fn, combiners.CountCombineFn) else 10000)
    self.key_count = 0
    self.table = {}

  def process(self, wkv):
    key, value = wkv.value
    wkey = tuple(wkv.windows), key
    entry = self.table.get(wkey, None)
    if entry is None:
      if self.key_count >= self.max_keys:
        target = self.key_count * 9 // 10
        old_wkeys = []
        # TODO(robertwb): Use an LRU cache?
        for old_wkey, old_wvalue in enumerate(self.table.iterkeys()):
          old_wkeys.append(old_wkey)  # Can't mutate while iterating.
          self.output_key(old_wkey, old_wvalue)
          self.key_count -= 1
          if self.key_count <= target:
            break
        for old_wkey in reversed(old_wkeys):
          del self.table[old_wkey]
      self.key_count += 1
      entry = self.table[wkey] = [self.combine_fn.create_accumulator()]
    entry[0] = self.combine_fn.add_inputs(entry[0], [value])

  def finish(self):
    for wkey, value in self.table.iteritems():
      self.output_key(wkey, value[0])
    self.table = {}
    self.key_count = 0

  def output_key(self, wkey, value):
    windows, key = wkey
    self.output(WindowedValue((key, value), windows[0].end, windows))


class FlattenOperation(Operation):
  """Flatten operation.

  Receives one or more producer operations, outputs just one list
  with all the items.
  """

  def process(self, o):
    if self.debug_logging_enabled:
      logging.debug('Processing [%s] in %s', o, self)
    assert isinstance(o, WindowedValue)
    self.output(o)


class ReifyTimestampAndWindowsOperation(Operation):
  """ReifyTimestampAndWindows operation.

  Maps each input KV item into a tuple of the original key and the value as the
  WindowedValue object of the original value and the original KV item's
  timestamp and windows.
  """

  def __init__(self, spec, counter_factory):
    super(ReifyTimestampAndWindowsOperation, self).__init__(
        spec, counter_factory)

  def process(self, o):
    if self.debug_logging_enabled:
      logging.debug('Processing [%s] in %s', o, self)
    assert isinstance(o, WindowedValue)
    k, v = o.value
    self.output(o.with_value((k, o.with_value(v))))


class BatchGroupAlsoByWindowsOperation(Operation):
  """BatchGroupAlsoByWindowsOperation operation.

  Implements GroupAlsoByWindow for batch pipelines.
  """

  def __init__(self, spec, counter_factory):
    super(BatchGroupAlsoByWindowsOperation, self).__init__(
        spec, counter_factory)
    self.windowing = pickler.loads(self.spec.window_fn)
    if self.spec.combine_fn:
      # Combiners do not accept deferred side-inputs (the ignored fourth
      # argument) and therefore the code to handle the extra args/kwargs is
      # simpler than for the DoFn's of ParDo.
      fn, args, kwargs = pickler.loads(self.spec.combine_fn)[:3]
      self.phased_combine_fn = (
          PhasedCombineFnExecutor(self.spec.phase, fn, args, kwargs))
    else:
      self.phased_combine_fn = None

  def process(self, o):
    """Process a given value."""
    if self.debug_logging_enabled:
      logging.debug('Processing [%s] in %s', o, self)
    assert isinstance(o, WindowedValue)
    k, vs = o.value
    driver = trigger.create_trigger_driver(
        self.windowing, is_batch=True, phased_combine_fn=self.phased_combine_fn)
    state = InMemoryUnmergedState()

    # TODO(robertwb): Process in smaller chunks.
    for wvalue in driver.process_elements(state, vs, MIN_TIMESTAMP):
      self.output(wvalue.with_value((k, wvalue.value)))

    while state.timers:
      timers = state.get_and_clear_timers()
      for timer_window, (name, time_domain, timestamp) in timers:
        for wvalue in driver.process_timer(
            timer_window, name, time_domain, timestamp, state):
          self.output(wvalue.with_value((k, wvalue.value)))


class StreamingGroupAlsoByWindowsOperation(Operation):
  """StreamingGroupAlsoByWindowsOperation operation.

  Implements GroupAlsoByWindow for streaming pipelines.
  """

  def __init__(self, spec, counter_factory):
    super(StreamingGroupAlsoByWindowsOperation, self).__init__(
        spec, counter_factory)
    self.windowing = pickler.loads(self.spec.window_fn)

  def process(self, o):
    if self.debug_logging_enabled:
      logging.debug('Processing [%s] in %s', o, self)
    assert isinstance(o, WindowedValue)
    keyed_work = o.value
    driver = trigger.create_trigger_driver(self.windowing)
    state = self.spec.context.state
    output_watermark = self.spec.context.output_data_watermark

    key = keyed_work.key
    for wvalue in driver.process_elements(
        state, keyed_work.elements(), output_watermark):
      self.output(wvalue.with_value((key, wvalue.value)))

    for timer in keyed_work.timers():
      timer_window = int(timer.namespace)
      for wvalue in driver.process_timer(
          timer_window, timer.name, timer.time_domain, timer.timestamp, state):
        self.output(wvalue.with_value((key, wvalue.value)))


class MapTaskExecutor(object):
  """A class for executing map tasks.

   Stores progress of the read operation that is the first operation of a map
   task.
  """

  multiple_read_instruction_error_msg = (
      'Found more than one \'read instruction\' in a single \'map task\'')

  def __init__(self):
    self._ops = []
    self._read_operation = None

  def get_progress(self):
    return (self._read_operation.get_progress()
            if self._read_operation is not None else None)

  def request_dynamic_split(self, dynamic_split_request):
    if self._read_operation is not None:
      return self._read_operation.request_dynamic_split(dynamic_split_request)

  def execute(self, map_task, test_shuffle_source=None, test_shuffle_sink=None):
    """Executes all the maptask.Worker* instructions in a map task.

    We update the map_task with the execution status, expressed as counters.

    Args:
      map_task: The map task we are to run.
      test_shuffle_source: Used during tests for dependency injection into
        shuffle read operation objects.
      test_shuffle_sink: Used during tests for dependency injection into
        shuffle write operation objects.

    Raises:
      RuntimeError: if we find more than on read instruction in task spec.
      TypeError: if the spec parameter is not an instance of the recognized
        maptask.Worker* classes.
    """

    # operations is a list of maptask.Worker* instances. The order of the
    # elements is important because the inputs use list indexes as references.
    for spec in map_task.operations:
      if isinstance(spec, maptask.WorkerRead):
        op = ReadOperation(spec, map_task.counter_factory)
        if self._read_operation is not None:
          raise RuntimeError(
              MapTaskExecutor.multiple_read_instruction_error_msg)
        else:
          self._read_operation = op
      elif isinstance(spec, maptask.WorkerWrite):
        op = WriteOperation(spec, map_task.counter_factory)
      elif isinstance(spec, maptask.WorkerCombineFn):
        op = CombineOperation(spec, map_task.counter_factory)
      elif isinstance(spec, maptask.WorkerPartialGroupByKey):
        op = create_pgbk_op(spec, map_task.counter_factory)
      elif isinstance(spec, maptask.WorkerDoFn):
        op = DoOperation(spec, map_task.counter_factory)
      elif isinstance(spec, maptask.WorkerGroupingShuffleRead):
        op = GroupedShuffleReadOperation(
            spec, map_task.counter_factory, shuffle_source=test_shuffle_source)
        if self._read_operation is not None:
          raise RuntimeError(
              MapTaskExecutor.multiple_read_instruction_error_msg)
        else:
          self._read_operation = op
      elif isinstance(spec, maptask.WorkerUngroupedShuffleRead):
        op = UngroupedShuffleReadOperation(
            spec, map_task.counter_factory, shuffle_source=test_shuffle_source)
        if self._read_operation is not None:
          raise RuntimeError(
              MapTaskExecutor.multiple_read_instruction_error_msg)
        else:
          self._read_operation = op
      elif isinstance(spec, maptask.WorkerInMemoryWrite):
        op = InMemoryWriteOperation(spec, map_task.counter_factory)
      elif isinstance(spec, maptask.WorkerShuffleWrite):
        op = ShuffleWriteOperation(
            spec, map_task.counter_factory, shuffle_sink=test_shuffle_sink)
      elif isinstance(spec, maptask.WorkerFlatten):
        op = FlattenOperation(spec, map_task.counter_factory)
      elif isinstance(spec, maptask.WorkerMergeWindows):
        if isinstance(spec.context, maptask.BatchExecutionContext):
          op = BatchGroupAlsoByWindowsOperation(spec, map_task.counter_factory)
        elif isinstance(spec.context, maptask.StreamingExecutionContext):
          op = StreamingGroupAlsoByWindowsOperation(spec,
                                                    map_task.counter_factory)
        else:
          raise RuntimeError('Unknown execution context: %s' % spec.context)
      elif isinstance(spec, maptask.WorkerReifyTimestampAndWindows):
        op = ReifyTimestampAndWindowsOperation(spec, map_task.counter_factory)
      else:
        raise TypeError('Expected an instance of maptask.Worker* class '
                        'instead of %s' % (spec,))
      self._ops.append(op)

      # Every MapTask must start with a read instruction.
      assert self._read_operation is not None

      # Add receiver operations to the appropriate producers.
      if hasattr(op.spec, 'input'):
        producer, output_index = op.spec.input
        self._ops[producer].add_receiver(op, output_index)
      # Flatten has 'inputs', not 'input'
      if hasattr(op.spec, 'inputs'):
        for producer, output_index in op.spec.inputs:
          self._ops[producer].add_receiver(op, output_index)

    # Inject the step names into the operations.
    # This is used for logging and assigning names to counters.
    if map_task.step_names is not None:
      for ix, op in enumerate(self._ops):
        op.step_name = map_task.step_names[ix]

    ix = len(self._ops)
    for op in reversed(self._ops):
      ix -= 1
      logging.debug('Starting op %d %s', ix, op)
      op.start()
    for op in self._ops:
      op.finish(*())
