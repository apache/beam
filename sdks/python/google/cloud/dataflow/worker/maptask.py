# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Worker utilities for parsing out a MapTask message.

Each MapTask represents a sequence of ParallelInstruction(s): read from a
source, write to a sink, parallel do, etc.
"""

import base64
import collections

from google.cloud.dataflow import coders
from google.cloud.dataflow import io
from google.cloud.dataflow.internal.json_value import from_json_value
from google.cloud.dataflow.utils.counters import CounterFactory
from google.cloud.dataflow.worker import concat_reader
from google.cloud.dataflow.worker import inmemory
from google.cloud.dataflow.worker import windmillio


def build_worker_instruction(*args):
  """Create an object representing a ParallelInstruction protobuf.

  This will be a collections.namedtuple with a custom __str__ method.

  Alas, this wrapper is not known to pylint, which thinks it creates
  constants.  You may have to put a disable=invalid-name pylint
  annotation on any use of this, depending on your names.

  Args:
    *args: first argument is the name of the type to create.  Should
      start with "Worker".  Second arguments is alist of the
      attributes of this object.
  Returns:
    A new class, a subclass of tuple, that represents the protobuf.
  """
  tuple_class = collections.namedtuple(*args)
  tuple_class.__str__ = worker_object_to_string
  tuple_class.__repr__ = worker_object_to_string
  return tuple_class


def worker_printable_fields(workerproto):
  """Returns the interesting fields of a Worker* object."""
  return ['%s=%s' % (name, value)
          # _asdict is the only way and cannot subclass this generated class
          # pylint: disable=protected-access
          for name, value in workerproto._asdict().iteritems()
          # want to output value 0 but not None nor []
          if (value or value == 0)
          and name not in
          ('coder', 'coders', 'output_coders',
           'elements',
           'combine_fn', 'serialized_fn', 'window_fn',
           'append_trailing_newlines', 'strip_trailing_newlines',
           'compression_type', 'context',
           'start_shuffle_position', 'end_shuffle_position',
           'shuffle_reader_config', 'shuffle_writer_config')]


def worker_object_to_string(worker_object):
  """Returns a string compactly representing a Worker* object."""
  return '%s(%s)' % (worker_object.__class__.__name__,
                     ', '.join(worker_printable_fields(worker_object)))


# All the following Worker* definitions will have these lint problems:
# pylint: disable=invalid-name
# pylint: disable=pointless-string-statement


WorkerRead = build_worker_instruction(
    'WorkerRead', ['source', 'output_coders'])
"""Worker details needed to read from a source.

Attributes:
  source: a source object.
  output_coders: 1-tuple of the coder for the output.
"""


WorkerSideInputSource = build_worker_instruction(
    'WorkerSideInputSource', ['source', 'tag'])
"""Worker details needed to read from a side input source.

Attributes:
  source: a source object.
  tag: string tag for this side input.
"""


WorkerGroupingShuffleRead = build_worker_instruction(
    'WorkerGroupingShuffleRead',
    ['start_shuffle_position', 'end_shuffle_position',
     'shuffle_reader_config', 'coder', 'output_coders'])
"""Worker details needed to read from a grouping shuffle source.

Attributes:
  start_shuffle_position: An opaque string to be passed to the shuffle
    source to indicate where to start reading.
  end_shuffle_position: An opaque string to be passed to the shuffle
    source to indicate where to stop reading.
  shuffle_reader_config: An opaque string used to initialize the shuffle
    reader. Contains things like connection endpoints for the shuffle
    server appliance and various options.
  coder: The KV coder used to decode shuffle entries.
  output_coders: 1-tuple of the coder for the output.
"""


WorkerUngroupedShuffleRead = build_worker_instruction(
    'WorkerUngroupedShuffleRead',
    ['start_shuffle_position', 'end_shuffle_position',
     'shuffle_reader_config', 'coder', 'output_coders'])
"""Worker details needed to read from an ungrouped shuffle source.

Attributes:
  start_shuffle_position: An opaque string to be passed to the shuffle
    source to indicate where to start reading.
  end_shuffle_position: An opaque string to be passed to the shuffle
    source to indicate where to stop reading.
  shuffle_reader_config: An opaque string used to initialize the shuffle
    reader. Contains things like connection endpoints for the shuffle
    server appliance and various options.
  coder: The value coder used to decode shuffle entries.
"""


WorkerWrite = build_worker_instruction(
    'WorkerWrite', ['sink', 'input', 'output_coders'])
"""Worker details needed to write to a sink.

Attributes:
  sink: a sink object.
  input: A (producer index, output index) tuple representing the
    ParallelInstruction operation whose output feeds into this operation.
    The output index is 0 except for multi-output operations (like ParDo).
  output_coders: 1-tuple, coder to use to estimate bytes written.
"""


WorkerInMemoryWrite = build_worker_instruction(
    'WorkerInMemoryWrite',
    ['output_buffer', 'input', 'output_coders'])
"""Worker details needed to write to a in-memory sink.

Used only for unit testing. It makes worker tests less cluttered with code like
"write to a file and then check file contents".

Attributes:
  output_buffer: list to which output elements will be appended
  input: A (producer index, output index) tuple representing the
    ParallelInstruction operation whose output feeds into this operation.
    The output index is 0 except for multi-output operations (like ParDo).
  output_coders: 1-tuple, coder to use to estimate bytes written.
"""


WorkerShuffleWrite = build_worker_instruction(
    'WorkerShuffleWrite',
    ['shuffle_kind', 'shuffle_writer_config', 'input', 'output_coders'])
"""Worker details needed to write to a shuffle sink.

Attributes:
  shuffle_kind: A string describing the shuffle kind. This can control the
    way the worker interacts with the shuffle sink. The possible values are:
    'ungrouped', 'group_keys', and 'group_keys_and_sort_values'.
  shuffle_writer_config: An opaque string used to initialize the shuffle
    write. Contains things like connection endpoints for the shuffle
    server appliance and various options.
  input: A (producer index, output index) tuple representing the
    ParallelInstruction operation whose output feeds into this operation.
    The output index is 0 except for multi-output operations (like ParDo).
  output_coders: 1-tuple of the coder for input elements. If the
    shuffle_kind is grouping, this is expected to be a KV coder.
"""


WorkerDoFn = build_worker_instruction(
    'WorkerDoFn',
    ['serialized_fn', 'output_tags', 'input', 'side_inputs', 'output_coders'])
"""Worker details needed to run a DoFn.
Attributes:
  serialized_fn: A serialized DoFn object to be run for each input element.
  output_tags: The string tags used to identify the outputs of a ParDo
    operation. The tag is present even if the ParDo has just one output
    (e.g., ['out'].
  output_coders: array of coders, one for each output.
  input: A (producer index, output index) tuple representing the
    ParallelInstruction operation whose output feeds into this operation.
    The output index is 0 except for multi-output operations (like ParDo).
  side_inputs: A list of Worker...Read instances describing sources to be
    used for getting values. The types supported right now are
    WorkerInMemoryRead and WorkerTextRead.
"""


WorkerReifyTimestampAndWindows = build_worker_instruction(
    'WorkerReifyTimestampAndWindows',
    ['output_tags', 'input', 'output_coders'])
"""Worker details needed to run a WindowInto.
Attributes:
  output_tags: The string tags used to identify the outputs of a ParDo
    operation. The tag is present even if the ParDo has just one output
    (e.g., ['out'].
  output_coders: array of coders, one for each output.
  input: A (producer index, output index) tuple representing the
    ParallelInstruction operation whose output feeds into this operation.
    The output index is 0 except for multi-output operations (like ParDo).
"""


WorkerMergeWindows = build_worker_instruction(
    'WorkerMergeWindows',
    ['window_fn', 'combine_fn', 'phase', 'output_tags', 'input', 'coders',
     'context', 'output_coders'])
"""Worker details needed to run a MergeWindows (aka. GroupAlsoByWindows).
Attributes:
  window_fn: A serialized Windowing object representing the windowing strategy.
  combine_fn: A serialized CombineFn object to be used after executing the
    GroupAlsoByWindows operation. May be None if not a combining operation.
  phase: Possible values are 'all', 'add', 'merge', and 'extract'.
    The dataflow optimizer may split the user combiner in 3 separate
    phases (ADD, MERGE, and EXTRACT), on separate VMs, as it sees
    fit. The phase attribute dictates which DoFn is actually running in
    the worker. May be None if not a combining operation.
  output_tags: The string tags used to identify the outputs of a ParDo
    operation. The tag is present even if the ParDo has just one output
    (e.g., ['out'].
  output_coders: array of coders, one for each output.
  input: A (producer index, output index) tuple representing the
    ParallelInstruction operation whose output feeds into this operation.
    The output index is 0 except for multi-output operations (like ParDo).
  coders: A 2-tuple of coders (key, value) to encode shuffle entries.
  context: The ExecutionContext object for the current work item.
"""


WorkerCombineFn = build_worker_instruction(
    'WorkerCombineFn',
    ['serialized_fn', 'phase', 'input', 'output_coders'])
"""Worker details needed to run a CombineFn.
Attributes:
  serialized_fn: A serialized CombineFn object to be used.
  phase: Possible values are 'all', 'add', 'merge', and 'extract'.
    The dataflow optimizer may split the user combiner in 3 separate
    phases (ADD, MERGE, and EXTRACT), on separate VMs, as it sees
    fit. The phase attribute dictates which DoFn is actually running in
    the worker.
  input: A (producer index, output index) tuple representing the
    ParallelInstruction operation whose output feeds into this operation.
    The output index is 0 except for multi-output operations (like ParDo).
  output_coders: 1-tuple of the coder for the output.
"""


WorkerPartialGroupByKey = build_worker_instruction(
    'WorkerPartialGroupByKey',
    ['combine_fn', 'input', 'output_coders'])
"""Worker details needed to run a partial group-by-key.
Attributes:
  combine_fn: A serialized CombineFn object to be used.
  input: A (producer index, output index) tuple representing the
    ParallelInstruction operation whose output feeds into this operation.
    The output index is 0 except for multi-output operations (like ParDo).
  output_coders: 1-tuple of the coder for the output.
"""


WorkerFlatten = build_worker_instruction(
    'WorkerFlatten',
    ['inputs', 'output_coders'])
"""Worker details needed to run a Flatten.
Attributes:
  inputs: A list of tuples, each (producer index, output index), representing
    the ParallelInstruction operations whose output feeds into this operation.
    The output index is 0 unless the input is from a multi-output
    operation (such as ParDo).
  output_coders: 1-tuple of the coder for the output.
"""


class ExecutionContext(object):
  """Context for executing the operations within a MapTask."""
  pass


class BatchExecutionContext(ExecutionContext):
  """ExecutionContext used in the batch worker."""
  pass


class StreamingExecutionContext(ExecutionContext):

  def start(self, computation_id, work_item, input_data_watermark,
            output_data_watermark, workitem_commit_request, windmill, state):
    self.computation_id = computation_id
    self.work_item = work_item
    self.input_data_watermark = input_data_watermark
    self.output_data_watermark = output_data_watermark
    self.workitem_commit_request = workitem_commit_request
    self.windmill = windmill
    self.state = state


def get_coder_from_spec(coder_spec, kv_pair=False):
  """Return a coder instance from a coder spec.

  Args:
    coder_spec: A dict where the value of the '@type' key is a pickled instance
      of a Coder instance.
    kv_pair: True if a 2-tuple of coders (key and value) must be returned.

  Returns:
    A coder instance (has encode/decode methods). It is possible to return
    a 2-tuple of (key coder, value coder) if the spec is for a shuffle source
    or sink. Such shuffle source and sinks can take a 2-tuple of coders as
    parameter.

  Raises:
    ValueError: if KV coder requested but coder spec is not of a KV coder.
  """
  assert coder_spec is not None

  # Ignore the wrappers in these encodings.
  ignored_wrappers = (
      'kind:stream',
      'com.google.cloud.dataflow.sdk.util.TimerOrElement$TimerOrElementCoder')
  if coder_spec['@type'] in ignored_wrappers:
    assert len(coder_spec['component_encodings']) == 1
    coder_spec = coder_spec['component_encodings'][0]
    return get_coder_from_spec(coder_spec, kv_pair=kv_pair)

  # We pass coders in the form "<coder_name>$<pickled_data>" to make the job
  # description JSON more readable.
  coder = coders.deserialize_coder(coder_spec['@type'])

  # If this is a coder with components potentially modified by the service,
  # use these components.
  #
  # TODO(ccy): This is necessary since the service may move around the
  # wrapped types of WindowedValueCoders and TupleCoders.  We should refactor
  # coder serialization so these special cases is not necessary.
  if isinstance(coder, coders.WindowedValueCoder):
    value_coder, timestamp_coder, window_coder = [
        get_coder_from_spec(c) for c in coder_spec['component_encodings']]
    coder = coders.WindowedValueCoder(value_coder, timestamp_coder,
                                      window_coder)
  elif isinstance(coder, coders.TupleCoder):
    component_coders = [
        get_coder_from_spec(c) for c in coder_spec['component_encodings']]
    coder = coders.TupleCoder(component_coders)

  if kv_pair:
    if not coder.is_kv_coder():
      raise ValueError('Coder is not a KV coder: %s.' % coder)
    return coder.key_coder(), coder.value_coder()
  else:
    return coder


def get_output_coders(work):
  """Return a list of coder instances for the output(s) of this work item.

  Args:
    work: a ParallelInstruction protobuf

  Returns:
    A list of coders.
  """
  return [get_coder_from_spec({p.key: from_json_value(p.value)
                               for p in output.codec.additionalProperties})
          for output in work.outputs]


def get_read_work_item(work, env, context):
  """Parses a read parallel instruction into the appropriate Worker* object."""
  specs = {p.key: from_json_value(p.value)
           for p in work.read.source.spec.additionalProperties}
  # Only sources for which a custom coder can be specified have the
  # codec property (e.g. TextSource).
  codec_specs = None
  if work.read.source.codec:
    codec_specs = {
        p.key: from_json_value(p.value)
        for p in work.read.source.codec.additionalProperties}

  source = env.parse_source(specs, codec_specs, context)
  if source:
    return WorkerRead(source, output_coders=get_output_coders(work))

  coder = get_coder_from_spec(codec_specs)
  # TODO(ccy): Reconcile WindowedValueCoder wrappings for sources with custom
  # coders so this special case won't be necessary.
  if isinstance(coder, coders.WindowedValueCoder):
    coder = coder.wrapped_value_coder
  if specs['@type'] == 'GroupingShuffleSource':
    return WorkerGroupingShuffleRead(
        start_shuffle_position=specs['start_shuffle_position']['value'],
        end_shuffle_position=specs['end_shuffle_position']['value'],
        shuffle_reader_config=specs['shuffle_reader_config']['value'],
        coder=coder,
        output_coders=get_output_coders(work))
  elif specs['@type'] == 'UngroupedShuffleSource':
    return WorkerUngroupedShuffleRead(
        start_shuffle_position=specs['start_shuffle_position']['value'],
        end_shuffle_position=specs['end_shuffle_position']['value'],
        shuffle_reader_config=specs['shuffle_reader_config']['value'],
        coder=coder,
        output_coders=get_output_coders(work))
  else:
    raise NotImplementedError('Unknown source type: %r' % specs)


# pylint: enable=invalid-name
# pylint: enable=pointless-string-statement


def get_input_spec(instruction_input):
  """Returns a (producer, output) indexes tuple based on input specified.

  Args:
    instruction_input: an InstructionInput protobuf.

  Returns:
    A (producer, output) indexes tuple.
  """
  producer_index = instruction_input.producerInstructionIndex
  if producer_index is None:
    producer_index = 0
  output_index = instruction_input.outputNum
  return producer_index, 0 if output_index is None else output_index


def get_side_input_sources(side_inputs_spec, env, context):
  """Returns a list of Worker...Read objects for the side sources specified."""
  side_inputs = []
  for side_spec in side_inputs_spec:
    assert side_spec.tag  # All side input sources have tags.
    # Make sure we got a side input type we understand.
    specs = {p.key: from_json_value(p.value)
             for p in side_spec.kind.additionalProperties}
    assert specs['@type'] == 'collection'
    for source in side_spec.sources:
      source_spec = {
          p.key: from_json_value(p.value)
          for p in source.spec.additionalProperties}
      # Only sources for which a custom coder can be specified have the
      # codec property (e.g. TextSource).
      if source.codec:
        source_codec_spec = {
            p.key: from_json_value(p.value)
            for p in source.codec.additionalProperties}

      parsed_source = env.parse_source(source_spec, source_codec_spec, context)
      if parsed_source:
        side_inputs.append(WorkerSideInputSource(parsed_source, side_spec.tag))
      else:
        raise NotImplementedError(
            'Unknown side input source type: %r' % source_spec)
  return side_inputs


def get_write_work_item(work, env, context):
  """Parses a write parallel instruction into the appropriate Worker* object."""
  specs = {p.key: from_json_value(p.value)
           for p in work.write.sink.spec.additionalProperties}
  # Only sinks for which a custom coder can be specified have the
  # codec property (e.g. TextSink.
  codec_specs = None
  if work.write.sink.codec:
    codec_specs = {
        p.key: from_json_value(p.value)
        for p in work.write.sink.codec.additionalProperties}

  sink = env.parse_sink(specs, codec_specs, context)
  if sink:
    write_coder = get_coder_from_spec(codec_specs)
    # All Worker items have an "output_coders", even if they have no
    # output, so that the executor can estimate bytes in a uniform way.
    return WorkerWrite(sink, input=get_input_spec(work.write.input),
                       output_coders=(write_coder,))
  if specs['@type'] == 'ShuffleSink':
    coder = get_coder_from_spec(codec_specs)
    # TODO(ccy): Reconcile WindowedValueCoder wrappings for sources with custom
    # coders so this special case won't be necessary.
    if isinstance(coder, coders.WindowedValueCoder):
      coder = coder.wrapped_value_coder
    return WorkerShuffleWrite(
        shuffle_kind=specs['shuffle_kind']['value'],
        shuffle_writer_config=specs['shuffle_writer_config']['value'],
        input=get_input_spec(work.write.input),
        output_coders=(coder,))
  else:
    raise NotImplementedError('Unknown sink type: %r' % specs)


def get_do_work_item(work, env, context):
  """Parses a do parallel instruction into the appropriate Worker* object."""
  # Get side inputs if any.
  side_inputs = []
  if hasattr(work.parDo, 'sideInputs'):
    side_inputs = get_side_input_sources(work.parDo.sideInputs, env, context)
  specs = {p.key: from_json_value(p.value)
           for p in work.parDo.userFn.additionalProperties}
  if specs['@type'] == 'DoFn':
    return WorkerDoFn(
        serialized_fn=specs['serialized_fn']['value'],
        output_tags=[o.tag for o in work.parDo.multiOutputInfos],
        output_coders=get_output_coders(work),
        input=get_input_spec(work.parDo.input),
        side_inputs=side_inputs)
  elif specs['@type'] == 'CombineValuesFn':
    # Note: CombineFn's do not take side inputs like DoFn's so far.
    return WorkerCombineFn(
        serialized_fn=specs['serialized_fn']['value'],
        phase=specs['phase']['value'],  # 'add' is one possible value.
        input=get_input_spec(work.parDo.input),
        output_coders=get_output_coders(work))
  elif specs['@type'] == 'ReifyTimestampAndWindowsDoFn':
    return WorkerReifyTimestampAndWindows(
        output_tags=[o.tag for o in work.parDo.multiOutputInfos],
        output_coders=get_output_coders(work),
        input=get_input_spec(work.parDo.input))
  elif specs['@type'] == 'MergeBucketsDoFn':
    return WorkerMergeWindows(
        window_fn=specs['serialized_fn']['value'],
        combine_fn=specs.get('combine_fn', {}).get('value', None),
        phase=specs.get('phase', {}).get('value', None),
        output_tags=[o.tag for o in work.parDo.multiOutputInfos],
        output_coders=get_output_coders(work),
        input=get_input_spec(work.parDo.input),
        coders=None,
        context=context)
  # AssignBucketsDoFn is intentionally unimplemented.  The implementation of
  # WindowInto in transforms/core.py does not use a service primitive.
  else:
    raise NotImplementedError('Unknown ParDo type: %r' % specs)


def get_flatten_work_item(instruction, unused_env, unused_context):
  """Parses a flatten instruction into the appropriate Worker* object.

  Args:
    instruction: a ParallelInstruction protobuf with a FlattenInstruction in it.

  Returns:
    A WorkerFlatten object.
  """
  return WorkerFlatten(
      inputs=[get_input_spec(inp) for inp in instruction.flatten.inputs],
      output_coders=get_output_coders(instruction))


def get_partial_gbk_work_item(instruction, unused_env, unused_context):
  """Parses a partial GBK instruction into the appropriate Worker* object.

  Args:
    instruction: a ParallelInstruction protobuf with a
                 PartialGroupByKeyInstruction in it.

  Returns:
    A WorkerPartialGroupByKey object.
  """
  combine_fn = None
  if instruction.partialGroupByKey.valueCombiningFn:
    combine_fn_specs = {
        p.key: from_json_value(p.value)
        for p in (instruction.partialGroupByKey.valueCombiningFn
                  .additionalProperties)}
    combine_fn = combine_fn_specs.get('serialized_fn', {}).get('value', None)
  return WorkerPartialGroupByKey(
      combine_fn=combine_fn,
      input=get_input_spec(instruction.partialGroupByKey.input),
      output_coders=get_output_coders(instruction))


class MapTask(object):
  """A map task decoded into operations and ready to be executed.

  Attributes:
    operations: A list of Worker* object created by parsing the instructions
      within the map task.
    stage_name: The name of this map task execution stage.
    step_names: The names of the step corresponding to each map task operation.
  """

  def __init__(self, operations, stage_name, step_names):
    self.operations = operations
    self.stage_name = stage_name
    self.step_names = step_names
    self.counter_factory = CounterFactory()

  def itercounters(self):
    for counter in self.counter_factory.get_counters():
      yield counter

  def __str__(self):
    return '<%s %s steps=%s>' % (self.__class__.__name__, self.stage_name,
                                 '+'.join(self.step_names))


class WorkerEnvironment(object):
  """A worker execution environment."""

  def __init__(self):
    self._sources = []
    self._sinks = []

    self._register_predefined()

  def _register_predefined(self):
    """Register predefined sources and sinks."""
    self.register_source_parser(WorkerEnvironment._parse_text_source)
    self.register_source_parser(WorkerEnvironment._parse_inmemory_source)
    self.register_source_parser(WorkerEnvironment._parse_avro_source)
    self.register_source_parser(WorkerEnvironment._parse_big_query_source)
    self.register_source_parser(WorkerEnvironment._parse_pubsub_source)
    self.register_source_parser(WorkerEnvironment._parse_concat_source)
    self.register_source_parser(WorkerEnvironment._parse_windmill_source)
    # TODO(silviuc): Implement support for PartitioningShuffleSource
    # TODO(silviuc): Implement support for AvroSource
    # TODO(silviuc): Implement support for custom sources
    self.register_sink_parser(WorkerEnvironment._parse_text_sink)
    self.register_sink_parser(WorkerEnvironment._parse_avro_sink)
    self.register_sink_parser(WorkerEnvironment._parse_pubsub_sink)
    self.register_sink_parser(WorkerEnvironment._parse_windmill_sink)

  def register_source_parser(self, fn):
    self._sources.append(fn)

  def register_sink_parser(self, fn):
    self._sinks.append(fn)

  def parse_source(self, specs, codec_specs, context):
    for fn in self._sources:
      result = fn(specs, codec_specs, context)
      if result:
        return result
    return None

  def parse_sink(self, specs, codec_specs, context):
    for fn in self._sinks:
      result = fn(specs, codec_specs, context)
      if result:
        return result
    return None

  @staticmethod
  def _parse_text_source(specs, codec_specs, unused_context):
    if specs['@type'] == 'TextSource':
      coder = get_coder_from_spec(codec_specs)
      start_offset = None
      if 'start_offset' in specs:
        start_offset = int(specs['start_offset']['value'])
      end_offset = None
      if 'end_offset' in specs:
        end_offset = int(specs['end_offset']['value'])
      return io.TextFileSource(
          file_path=specs['filename']['value'],
          start_offset=start_offset,
          end_offset=end_offset,
          compression_type=specs['compression_type']['value'],
          strip_trailing_newlines=specs['strip_trailing_newlines']['value'],
          coder=coder,
      )

  @staticmethod
  def _parse_concat_source(specs, _, unused_context):
    if specs['@type'] == 'ConcatSource':
      assert unused_context.worker_environment is not None
      sub_sources = []
      for sub_source_dict in specs['sources']:
        sub_source_specs = sub_source_dict['spec']
        sub_source_codec_specs = None
        if 'encoding' in sub_source_dict:
          sub_source_codec_specs = sub_source_dict['encoding']
        sub_sources.append(unused_context.worker_environment.parse_source(
            sub_source_specs, sub_source_codec_specs, unused_context))
      return concat_reader.ConcatSource(sub_sources)

  @staticmethod
  def _parse_inmemory_source(specs, codec_specs, unused_context):
    if specs['@type'] == 'InMemorySource':
      # We do not wrap values sent to the service in a Create transform and
      # received here in a WindowedValue wrapper, but the service needs to be
      # sent the wrapped encoding so subsequent GroupByKey operations work
      # correctly.
      #
      # Note: The service may create a dummy empty InMemorySource that is a
      # windowed value when processing a BigQuerySource.  In that case, we do
      # not unwrap this coder.
      # TODO(ccy): investigate if we can make these semantics cleaner.
      coder = get_coder_from_spec(codec_specs)
      if isinstance(coder, coders.WindowedValueCoder):
        coder = coder.wrapped_value_coder
      # Handle the case where 'elements' for an InMemory source is empty
      # list.
      if specs['elements']:
        # start_index/end_index could be missing if default behavior should be
        # used. For instance a list with one element will have start_index=0 and
        # end_index=1 by default.
        start_index = (
            None
            if 'start_index' not in specs else int(
                specs['start_index']['value']))
        end_index = (
            None if 'end_index' not in specs
            else int(specs['end_index']['value']))
        return inmemory.InMemorySource(
            elements=[base64.b64decode(v['value']) for v in specs['elements']],
            coder=coder,
            start_index=start_index, end_index=end_index)
      else:
        return inmemory.InMemorySource(elements=[], coder=coder)

  @staticmethod
  def _parse_avro_source(specs, unused_codec_specs, unused_context):
    if specs['@type'] == 'AvroSource':
      # Note that the worker does not really implement AVRO yet.It takes
      # advantage that both reading and writing is done through the worker to
      # choose a supported format (text files with one pickled object per line).
      start_offset = None
      if 'start_offset' in specs:
        start_offset = int(specs['start_offset']['value'])
      end_offset = None
      if 'end_offset' in specs:
        end_offset = int(specs['end_offset']['value'])
      return io.TextFileSource(
          file_path=specs['filename']['value'],
          start_offset=start_offset,
          end_offset=end_offset,
          strip_trailing_newlines=True,
          coder=coders.Base64PickleCoder())

  @staticmethod
  def _parse_big_query_source(specs, codec_specs, unused_context):
    if specs['@type'] == 'BigQuerySource':
      coder = get_coder_from_spec(codec_specs)
      if 'table' in specs:
        return io.BigQuerySource(
            project=specs['project']['value'],
            dataset=specs['dataset']['value'],
            table=specs['table']['value'],
            coder=coder)
      elif 'bigquery_query' in specs:
        return io.BigQuerySource(
            query=specs['bigquery_query']['value'],
            coder=coder)
      else:
        raise ValueError('BigQuery source spec must specify either a table'
                         ' or a query')

  @staticmethod
  def _parse_pubsub_source(specs, codec_specs, context):
    if specs['@type'] == 'PubsubReader':
      topic = specs['pubsub_topic']['value']
      subscription = specs['pubsub_subscription_name']['value']
      coder = coders.deserialize_coder(codec_specs['@type'])
      return windmillio.PubSubWindmillSource(context, topic, subscription,
                                             coder)

  @staticmethod
  def _parse_windmill_source(specs, codec_specs, context):
    if specs['@type'] == 'WindowingWindmillReader':
      stream_id = specs['stream_id']['value']
      coder = get_coder_from_spec(codec_specs)
      return windmillio.WindowingWindmillSource(context, stream_id, coder)

  @staticmethod
  def _parse_text_sink(specs, codec_specs, unused_context):
    if specs['@type'] == 'TextSink':
      coder = get_coder_from_spec(codec_specs)
      return io.TextFileSink(
          file_path_prefix=specs['filename']['value'],
          append_trailing_newlines=specs['append_trailing_newlines']['value'],
          coder=coder)

  @staticmethod
  def _parse_avro_sink(specs, unused_codec_specs, unused_context):
    # Note that the worker does not really implement AVRO yet.It takes
    # advantage that both reading and writing is done through the worker to
    # choose a supported format (text files with one pickled object per line).
    if specs['@type'] == 'AvroSink':
      return io.TextFileSink(
          specs['filename']['value'],
          append_trailing_newlines=True,
          coder=coders.Base64PickleCoder())

  @staticmethod
  def _parse_pubsub_sink(specs, codec_specs, context):
    if specs['@type'] == 'PubsubSink':
      coder = get_coder_from_spec(codec_specs)
      topic = specs['pubsub_topic']['value']
      timestamp_label = specs['pubsub_timestamp_label']['value']
      id_label = specs['pubsub_id_label']['value']
      return windmillio.PubSubWindmillSink(context, coder, topic,
                                           timestamp_label, id_label)

  @staticmethod
  def _parse_windmill_sink(specs, codec_specs, context):
    if specs['@type'] == 'WindmillSink':
      coder = get_coder_from_spec(codec_specs)
      stream_id = specs['stream_id']['value']
      return windmillio.WindmillSink(context, stream_id, coder)


def decode_map_task(map_task_proto, env=WorkerEnvironment(),
                    context=ExecutionContext()):
  """Parses a map task proto into operations within a MapTask object.

  The response is received by the worker as a result of a LeaseWorkItem
  request to the Dataflow service.

  Args:
    map_task_proto: A MapTask protobuf object returned by the service.
    env: An environment object with worker configuration.
    context: An ExecutionContext object providing context for operations to be
             executed.

  Returns:
    A tuple of work item id and the list of Worker* objects (see definitions
    above) representing the list of operations to be executed as part of the
    work item.
  """
  operations = []
  stage_name = map_task_proto.stageName
  step_names = []
  context.worker_environment = env
  # Parse the MapTask instructions.
  for work in map_task_proto.instructions:
    step_names.append(work.name)
    if work.read is not None:
      operations.append(get_read_work_item(work, env, context))
    elif work.write is not None:
      operations.append(get_write_work_item(work, env, context))
    elif work.parDo is not None:
      operations.append(get_do_work_item(work, env, context))
    elif work.flatten is not None:
      operations.append(get_flatten_work_item(work, env, context))
    elif work.partialGroupByKey is not None:
      operations.append(get_partial_gbk_work_item(work, env, context))
    else:
      raise NotImplementedError('Unknown instruction: %r' % work)
  return MapTask(operations, stage_name, step_names)
