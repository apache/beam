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

"""Worker utilities for representing MapTasks.

Each MapTask represents a sequence of ParallelInstruction(s): read from a
source, write to a sink, parallel do, etc.
"""

from __future__ import absolute_import

import collections

from apache_beam import coders
from apache_beam.runners import common

# This module is experimental. No backwards-compatibility guarantees.


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
          for name, value in workerproto._asdict().items()
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
    ['output_buffer', 'write_windowed_values', 'input', 'output_coders'])
"""Worker details needed to write to a in-memory sink.

Used only for unit testing. It makes worker tests less cluttered with code like
"write to a file and then check file contents".

Attributes:
  output_buffer: list to which output elements will be appended
  write_windowed_values: whether to record the entire WindowedValue outputs,
    or just the raw (unwindowed) value
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
    A runner optimizer may split the user combiner in 3 separate
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
    A runner optimizer may split the user combiner in 3 separate
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


def get_coder_from_spec(coder_spec):
  """Return a coder instance from a coder spec.

  Args:
    coder_spec: A dict where the value of the '@type' key is a pickled instance
      of a Coder instance.

  Returns:
    A coder instance (has encode/decode methods).
  """
  assert coder_spec is not None

  # Ignore the wrappers in these encodings.
  ignored_wrappers = (
      'com.google.cloud.dataflow.sdk.util.TimerOrElement$TimerOrElementCoder')
  if coder_spec['@type'] in ignored_wrappers:
    assert len(coder_spec['component_encodings']) == 1
    coder_spec = coder_spec['component_encodings'][0]
    return get_coder_from_spec(coder_spec)

  # Handle a few well known types of coders.
  if coder_spec['@type'] == 'kind:pair':
    assert len(coder_spec['component_encodings']) == 2
    component_coders = [
        get_coder_from_spec(c) for c in coder_spec['component_encodings']]
    return coders.TupleCoder(component_coders)
  elif coder_spec['@type'] == 'kind:stream':
    assert len(coder_spec['component_encodings']) == 1
    return coders.IterableCoder(
        get_coder_from_spec(coder_spec['component_encodings'][0]))
  elif coder_spec['@type'] == 'kind:windowed_value':
    assert len(coder_spec['component_encodings']) == 2
    value_coder, window_coder = [
        get_coder_from_spec(c) for c in coder_spec['component_encodings']]
    return coders.coders.WindowedValueCoder(
        value_coder, window_coder=window_coder)
  elif coder_spec['@type'] == 'kind:interval_window':
    assert ('component_encodings' not in coder_spec
            or not coder_spec['component_encodings'])
    return coders.coders.IntervalWindowCoder()
  elif coder_spec['@type'] == 'kind:global_window':
    assert ('component_encodings' not in coder_spec
            or not coder_spec['component_encodings'])
    return coders.coders.GlobalWindowCoder()
  elif coder_spec['@type'] == 'kind:varint':
    assert ('component_encodings' not in coder_spec
            or len(coder_spec['component_encodings'] == 0))
    return coders.coders.VarIntCoder()
  elif coder_spec['@type'] == 'kind:length_prefix':
    assert len(coder_spec['component_encodings']) == 1
    return coders.coders.LengthPrefixCoder(
        get_coder_from_spec(coder_spec['component_encodings'][0]))
  elif coder_spec['@type'] == 'kind:bytes':
    assert ('component_encodings' not in coder_spec
            or len(coder_spec['component_encodings'] == 0))
    return coders.BytesCoder()

  # We pass coders in the form "<coder_name>$<pickled_data>" to make the job
  # description JSON more readable.
  return coders.coders.deserialize_coder(
      coder_spec['@type'].encode('ascii'))


class MapTask(object):
  """A map task decoded into operations and ready to be executed.

  Attributes:
    operations: A list of Worker* object created by parsing the instructions
      within the map task.
    stage_name: The name of this map task execution stage.
    system_names: The system names of the step corresponding to each map task
      operation in the execution graph.
    step_names: The user-given names of the step corresponding to each map task
      operation (e.g. Foo/Bar/ParDo).
    original_names: The internal name of a step in the original workflow graph.
    name_contexts: A common.NameContext object containing name information
      about a step.
  """

  def __init__(self, operations, stage_name,
               system_names=None,
               step_names=None,
               original_names=None,
               name_contexts=None):
    # TODO(BEAM-4028): Remove arguments other than name_contexts.
    self.operations = operations
    self.stage_name = stage_name
    self.name_contexts = name_contexts or self._make_name_contexts(
        original_names, step_names, system_names)

  @staticmethod
  def _make_name_contexts(original_names, user_names, system_names):
    # TODO(BEAM-4028): Remove method once map task relies on name contexts.
    return [common.DataflowNameContext(step_name, user_name, system_name)
            for step_name, user_name, system_name in zip(original_names,
                                                         user_names,
                                                         system_names)]

  @property
  def system_names(self):
    """Returns a list containing the system names of steps.

    A System name is the name of a step in the optimized Dataflow graph.
    """
    return [nc.system_name for nc in self.name_contexts]

  @property
  def original_names(self):
    """Returns a list containing the original names of steps.

    An original name is the internal name of a step in the Dataflow graph
    (e.g. 's2').
    """
    return [nc.step_name for nc in self.name_contexts]

  @property
  def step_names(self):
    """Returns a list containing the user names of steps.

    In this context, a step name is the user-given name of a step in the
    Dataflow graph (e.g. 's2').
    """
    return [nc.user_name for nc in self.name_contexts]

  def __str__(self):
    return '<%s %s steps=%s>' % (self.__class__.__name__, self.stage_name,
                                 '+'.join(self.step_names))
