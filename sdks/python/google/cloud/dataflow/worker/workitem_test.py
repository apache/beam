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

"""Tests for LeaseWorkItemResponse parsing utilities.

The tests create synthetic LeaseWorkItemResponse messages and then check if the
utility routines get the expected maptask.Worker* objects and attributes. The
messages built are not necessarily correct in all respects but are good
enough to exercise the functionality.
"""


import base64
import logging
import unittest


from google.cloud.dataflow import coders
from google.cloud.dataflow import io
from google.cloud.dataflow.internal.json_value import to_json_value
from google.cloud.dataflow.worker import concat_reader
from google.cloud.dataflow.worker import inmemory
from google.cloud.dataflow.worker import maptask
from google.cloud.dataflow.worker import workitem

import google.cloud.dataflow.internal.clients.dataflow as dataflow


# Sample specifications for various worker operations. Note that these samples
# are used just to test that the specifications are parsed correctly by the
# worker code even though they may not be logically correct. For instance
# offsets and indeces may be out of range.


TEXT_SOURCE_SPEC = {
    '@type': 'TextSource',
    'start_offset': {'value': '123', '@type': 'http://int'},
    'end_offset': {'value': '123123', '@type': 'http://int'},
    'filename': {'value': 'gs://somefile', '@type': 'http://text'},
    'compression_type': {'value': 'AUTO', '@type': 'http://text'},
    'strip_trailing_newlines': {'value': True, '@type': 'http://bool'},
    }

IN_MEMORY_ELEMENTS = [
    {'value': base64.b64encode('1'), '@type': 'http://int'},
    {'value': base64.b64encode('2'), '@type': 'http://int'},
    {'value': base64.b64encode('3'), '@type': 'http://int'}]

IN_MEMORY_SOURCE_SPEC = {
    '@type': 'InMemorySource',
    'start_index': {'value': '1', '@type': 'http://int'},
    'end_index': {'value': '3', '@type': 'http://int'},
    'elements': IN_MEMORY_ELEMENTS,
    }

GROUPING_SHUFFLE_SOURCE_SPEC = {
    '@type': 'GroupingShuffleSource',
    'start_shuffle_position': {'value': 'opaque', '@type': 'xyz'},
    'end_shuffle_position': {'value': 'opaque', '@type': 'xyz'},
    'shuffle_reader_config': {'value': 'opaque', '@type': 'xyz'},
    }

UNGROUPED_SHUFFLE_SOURCE_SPEC = {
    '@type': 'UngroupedShuffleSource',
    'start_shuffle_position': {'value': 'opaque', '@type': 'xyz'},
    'end_shuffle_position': {'value': 'opaque', '@type': 'xyz'},
    'shuffle_reader_config': {'value': 'opaque', '@type': 'xyz'},
    }

TEXT_SINK_SPEC = {
    '@type': 'TextSink',
    'filename': {'value': 'gs://somefile', '@type': 'http://text'},
    'append_trailing_newlines': {'value': True, '@type': 'http://bool'},
    }

SHUFFLE_SINK_SPEC = {
    '@type': 'ShuffleSink',
    'shuffle_kind': {'value': 'group_keys', '@type': 'xyz'},
    'shuffle_writer_config': {'value': 'opaque', '@type': 'xyz'},
    }

PARDO_DOFN_SPEC = {
    '@type': 'DoFn',
    'serialized_fn': {'value': 'code', '@type': 'xyz'},
    }

PARDO_COMBINEFN_SPEC = {
    '@type': 'DoFn',
    'serialized_fn': {'value': 'code', '@type': 'xyz'},
    'phase': {'value': 'add', '@type': 'xyz'},
    }

CONCAT_SOURCE_SPEC = {
    'sources':
    [
        {
            'spec': {
                'strip_trailing_newlines':
                {'@type': 'http://schema.org/Boolean', 'value': True},
                'start_offset':
                {'@type': 'http://schema.org/Integer', 'value': '0'},
                'compression_type':
                {'@type': 'http://schema.org/Text', 'value': 'AUTO'},
                'end_offset':
                {'@type': 'http://schema.org/Integer', 'value': '1000000'},
                'filename':
                {'@type': 'http://schema.org/Text',
                 'value': 'gs://sort_g/input_small_files/'
                          'ascii_sort_1MB_input.0000006'},
                '@type': 'TextSource'
            },
            'encoding': {
                'component_encodings':
                [{'@type': 'notused'}, {'@type': 'notused'}],
                '@type': coders.serialize_coder(coders.PickleCoder())
            }
        },
        {
            'spec': {
                'strip_trailing_newlines':
                {'@type': 'http://schema.org/Boolean', 'value': True},
                'start_offset':
                {'@type': 'http://schema.org/Integer', 'value': '0'},
                'compression_type':
                {'@type': 'http://schema.org/Text', 'value': 'AUTO'},
                'end_offset':
                {'@type': 'http://schema.org/Integer', 'value': '1000000'},
                'filename':
                {'@type': 'http://schema.org/Text',
                 'value': 'gs://sort_g/input_small_files/'
                          'ascii_sort_1MB_input.0000007'},
                '@type': 'TextSource'
            },
            'encoding': {
                'component_encodings':
                [{'@type': 'notused'}, {'@type': 'notused'}],
                '@type': coders.serialize_coder(coders.PickleCoder())
            }
        }
    ],
    '@type': 'ConcatSource'
}


CODER = coders.PickleCoder()
WINDOWED_CODER = coders.WindowedValueCoder(CODER)

CODER_SPEC = CODER.as_cloud_object()
WINDOWED_CODER_SPEC = WINDOWED_CODER.as_cloud_object()


def add_source_codec_spec(target):
  target.source.codec = dataflow.Source.CodecValue()
  for k, v in CODER_SPEC.iteritems():
    target.source.codec.additionalProperties.append(
        dataflow.Source.CodecValue.AdditionalProperty(
            key=k, value=to_json_value(v)))


def add_source_windowed_codec_spec(target):
  target.source.codec = dataflow.Source.CodecValue()
  for k, v in WINDOWED_CODER_SPEC.iteritems():
    target.source.codec.additionalProperties.append(
        dataflow.Source.CodecValue.AdditionalProperty(
            key=k, value=to_json_value(v)))


def add_sink_codec_spec(target):
  target.sink.codec = dataflow.Sink.CodecValue()
  for k, v in CODER_SPEC.iteritems():
    target.sink.codec.additionalProperties.append(
        dataflow.Sink.CodecValue.AdditionalProperty(
            key=k, value=to_json_value(v)))


def get_instruction_with_outputs(num_outputs=1, **kwargs):
  pi = dataflow.ParallelInstruction(**kwargs)
  for _ in xrange(num_outputs):
    output = dataflow.InstructionOutput()
    output.codec = dataflow.InstructionOutput.CodecValue()
    for k, v in CODER_SPEC.iteritems():
      output.codec.additionalProperties.append(
          dataflow.InstructionOutput.CodecValue.AdditionalProperty(
              key=k, value=to_json_value(v)))
    pi.outputs.append(output)
  return pi


def get_concat_source_to_shuffle_sink_message():
  ri = dataflow.ReadInstruction()
  ri.source = dataflow.Source()
  ri.source.spec = dataflow.Source.SpecValue()

  for k, v in CONCAT_SOURCE_SPEC.iteritems():
    ri.source.spec.additionalProperties.append(
        dataflow.Source.SpecValue.AdditionalProperty(
            key=k, value=to_json_value(v)))

  di = dataflow.ParDoInstruction()
  di.input = dataflow.InstructionInput()
  di.input.producerInstructionIndex = 1
  di.multiOutputInfos = [dataflow.MultiOutputInfo(tag='out')]
  di.userFn = dataflow.ParDoInstruction.UserFnValue()
  for k, v in PARDO_DOFN_SPEC.iteritems():
    di.userFn.additionalProperties.append(
        dataflow.ParDoInstruction.UserFnValue.AdditionalProperty(
            key=k, value=to_json_value(v)))

  wsi = dataflow.WriteInstruction()
  wsi.input = dataflow.InstructionInput()
  wsi.input.producerInstructionIndex = 1
  di.input.outputNum = 0
  wsi.sink = dataflow.Sink()
  wsi.sink.spec = dataflow.Sink.SpecValue()
  for k, v in SHUFFLE_SINK_SPEC.iteritems():
    wsi.sink.spec.additionalProperties.append(
        dataflow.Sink.SpecValue.AdditionalProperty(
            key=k, value=to_json_value(v)))
  add_sink_codec_spec(wsi)

  mt = dataflow.MapTask()
  mt.instructions.append(get_instruction_with_outputs(read=ri))
  mt.instructions.append(get_instruction_with_outputs(parDo=di))
  mt.instructions.append(dataflow.ParallelInstruction(write=wsi))

  wi = dataflow.WorkItem()
  wi.id = 1234
  wi.projectId = 'project'
  wi.jobId = 'job'
  wi.mapTask = mt

  m = dataflow.LeaseWorkItemResponse()
  m.workItems.append(wi)
  return m


def get_text_source_to_shuffle_sink_message():
  ri = dataflow.ReadInstruction()
  ri.source = dataflow.Source()
  ri.source.spec = dataflow.Source.SpecValue()
  for k, v in TEXT_SOURCE_SPEC.iteritems():
    ri.source.spec.additionalProperties.append(
        dataflow.Source.SpecValue.AdditionalProperty(
            key=k, value=to_json_value(v)))
  add_source_codec_spec(ri)

  di = dataflow.ParDoInstruction()
  di.input = dataflow.InstructionInput()
  di.input.producerInstructionIndex = 1
  di.multiOutputInfos = [dataflow.MultiOutputInfo(tag='out')]
  di.userFn = dataflow.ParDoInstruction.UserFnValue()
  for k, v in PARDO_DOFN_SPEC.iteritems():
    di.userFn.additionalProperties.append(
        dataflow.ParDoInstruction.UserFnValue.AdditionalProperty(
            key=k, value=to_json_value(v)))

  wsi = dataflow.WriteInstruction()
  wsi.input = dataflow.InstructionInput()
  wsi.input.producerInstructionIndex = 1
  di.input.outputNum = 0
  wsi.sink = dataflow.Sink()
  wsi.sink.spec = dataflow.Sink.SpecValue()
  for k, v in SHUFFLE_SINK_SPEC.iteritems():
    wsi.sink.spec.additionalProperties.append(
        dataflow.Sink.SpecValue.AdditionalProperty(
            key=k, value=to_json_value(v)))
  add_sink_codec_spec(wsi)

  mt = dataflow.MapTask()
  mt.instructions.append(get_instruction_with_outputs(read=ri))
  mt.instructions.append(get_instruction_with_outputs(parDo=di))
  mt.instructions.append(dataflow.ParallelInstruction(write=wsi))

  wi = dataflow.WorkItem()
  wi.id = 1234
  wi.projectId = 'project'
  wi.jobId = 'job'
  wi.mapTask = mt

  m = dataflow.LeaseWorkItemResponse()
  m.workItems.append(wi)
  return m


def get_shuffle_source_to_text_sink_message(shuffle_source_spec):
  rsi = dataflow.ReadInstruction()
  rsi.source = dataflow.Source()
  rsi.source.spec = dataflow.Source.SpecValue()
  for k, v in shuffle_source_spec.iteritems():
    rsi.source.spec.additionalProperties.append(
        dataflow.Source.SpecValue.AdditionalProperty(
            key=k, value=to_json_value(v)))
  add_source_codec_spec(rsi)

  wi = dataflow.WriteInstruction()
  wi.input = dataflow.InstructionInput()
  wi.sink = dataflow.Sink()
  wi.sink.spec = dataflow.Sink.SpecValue()
  for k, v in TEXT_SINK_SPEC.iteritems():
    wi.sink.spec.additionalProperties.append(
        dataflow.Sink.SpecValue.AdditionalProperty(
            key=k, value=to_json_value(v)))
  add_sink_codec_spec(wi)

  mt = dataflow.MapTask()
  mt.instructions.append(get_instruction_with_outputs(read=rsi))
  mt.instructions.append(dataflow.ParallelInstruction(write=wi))

  wi = dataflow.WorkItem()
  wi.id = 1234
  wi.projectId = 'project'
  wi.jobId = 'job'
  wi.mapTask = mt

  m = dataflow.LeaseWorkItemResponse()
  m.workItems.append(wi)
  return m


def get_in_memory_source_to_text_sink_message():
  rsi = dataflow.ReadInstruction()
  rsi.source = dataflow.Source()
  rsi.source.spec = dataflow.Source.SpecValue()
  for k, v in IN_MEMORY_SOURCE_SPEC.iteritems():
    rsi.source.spec.additionalProperties.append(
        dataflow.Source.SpecValue.AdditionalProperty(
            key=k, value=to_json_value(v)))
  # Note that the in-memory source spec requires a windowed coder.
  add_source_windowed_codec_spec(rsi)

  wi = dataflow.WriteInstruction()
  wi.input = dataflow.InstructionInput()
  wi.sink = dataflow.Sink()
  wi.sink.spec = dataflow.Sink.SpecValue()
  for k, v in TEXT_SINK_SPEC.iteritems():
    wi.sink.spec.additionalProperties.append(
        dataflow.Sink.SpecValue.AdditionalProperty(
            key=k, value=to_json_value(v)))
  add_sink_codec_spec(wi)

  mt = dataflow.MapTask()
  mt.instructions.append(get_instruction_with_outputs(read=rsi))
  mt.instructions.append(dataflow.ParallelInstruction(write=wi))

  wi = dataflow.WorkItem()
  wi.id = 1234
  wi.projectId = 'project'
  wi.jobId = 'job'
  wi.mapTask = mt

  m = dataflow.LeaseWorkItemResponse()
  m.workItems.append(wi)
  return m


def get_in_memory_source_to_flatten_message():
  rsi = dataflow.ReadInstruction()
  rsi.source = dataflow.Source()
  add_source_codec_spec(rsi)
  rsi.source.spec = dataflow.Source.SpecValue()
  for k, v in IN_MEMORY_SOURCE_SPEC.iteritems():
    rsi.source.spec.additionalProperties.append(
        dataflow.Source.SpecValue.AdditionalProperty(
            key=k, value=to_json_value(v)))
  # Note that the in-memory source spec requires a windowed coder.
  add_source_windowed_codec_spec(rsi)

  fi = dataflow.FlattenInstruction()
  fi.inputs = [dataflow.InstructionInput()]

  mt = dataflow.MapTask()
  mt.instructions.append(get_instruction_with_outputs(read=rsi))
  mt.instructions.append(get_instruction_with_outputs(flatten=fi))

  wi = dataflow.WorkItem()
  wi.id = 1234
  wi.projectId = 'project'
  wi.jobId = 'job'
  wi.mapTask = mt

  m = dataflow.LeaseWorkItemResponse()
  m.workItems.append(wi)
  return m


class WorkItemTest(unittest.TestCase):

  def test_concat_source_to_shuffle_sink(self):
    work = workitem.get_work_items(get_concat_source_to_shuffle_sink_message())
    self.assertIsNotNone(work)
    expected_sub_sources = []
    expected_sub_sources.append(
        io.TextFileSource(
            file_path='gs://sort_g/input_small_files/'
            'ascii_sort_1MB_input.0000006',
            start_offset=0, end_offset=1000000,
            strip_trailing_newlines=True, coder=CODER))
    expected_sub_sources.append(
        io.TextFileSource(
            file_path='gs://sort_g/input_small_files/'
            'ascii_sort_1MB_input.0000007',
            start_offset=0, end_offset=1000000,
            strip_trailing_newlines=True, coder=CODER))

    expected_concat_source = concat_reader.ConcatSource(expected_sub_sources)

    self.assertEqual(
        (work.proto.id, work.map_task.operations),
        (1234, [
            maptask.WorkerRead(
                expected_concat_source, output_coders=[CODER]),
            maptask.WorkerDoFn(
                serialized_fn='code', output_tags=['out'], input=(1, 0),
                side_inputs=[], output_coders=[CODER]),
            maptask.WorkerShuffleWrite(
                shuffle_kind='group_keys',
                shuffle_writer_config='opaque',
                input=(1, 0),
                output_coders=(CODER,))]))

  def test_text_source_to_shuffle_sink(self):
    work = workitem.get_work_items(get_text_source_to_shuffle_sink_message())
    self.assertEqual(
        (work.proto.id, work.map_task.operations),
        (1234, [
            maptask.WorkerRead(io.TextFileSource(
                file_path='gs://somefile',
                start_offset=123,
                end_offset=123123,
                strip_trailing_newlines=True,
                coder=CODER), output_coders=[CODER]),
            maptask.WorkerDoFn(
                serialized_fn='code', output_tags=['out'], input=(1, 0),
                side_inputs=[], output_coders=[CODER]),
            maptask.WorkerShuffleWrite(
                shuffle_kind='group_keys',
                shuffle_writer_config='opaque',
                input=(1, 0),
                output_coders=(CODER,))]))

  def test_shuffle_source_to_text_sink(self):
    work = workitem.get_work_items(
        get_shuffle_source_to_text_sink_message(GROUPING_SHUFFLE_SOURCE_SPEC))
    self.assertEqual(
        (work.proto.id, work.map_task.operations),
        (1234, [
            maptask.WorkerGroupingShuffleRead(
                start_shuffle_position='opaque',
                end_shuffle_position='opaque',
                shuffle_reader_config='opaque',
                coder=CODER,
                output_coders=[CODER]),
            maptask.WorkerWrite(io.TextFileSink(
                file_path_prefix='gs://somefile',
                append_trailing_newlines=True,
                coder=CODER), input=(0, 0), output_coders=(CODER,))]))

  def test_ungrouped_shuffle_source_to_text_sink(self):
    work = workitem.get_work_items(
        get_shuffle_source_to_text_sink_message(UNGROUPED_SHUFFLE_SOURCE_SPEC))
    self.assertEqual(
        (work.proto.id, work.map_task.operations),
        (1234, [
            maptask.WorkerUngroupedShuffleRead(
                start_shuffle_position='opaque',
                end_shuffle_position='opaque',
                shuffle_reader_config='opaque',
                coder=CODER,
                output_coders=[CODER]),
            maptask.WorkerWrite(io.TextFileSink(
                file_path_prefix='gs://somefile',
                append_trailing_newlines=True,
                coder=CODER), input=(0, 0), output_coders=(CODER,))]))

  def test_in_memory_source_to_text_sink(self):
    work = workitem.get_work_items(get_in_memory_source_to_text_sink_message())
    self.assertEqual(
        (work.proto.id, work.map_task.operations),
        (1234, [
            maptask.WorkerRead(
                inmemory.InMemorySource(
                    start_index=1,
                    end_index=3,
                    elements=[base64.b64decode(v['value'])
                              for v in IN_MEMORY_ELEMENTS],
                    coder=CODER),
                output_coders=[CODER]),
            maptask.WorkerWrite(io.TextFileSink(
                file_path_prefix='gs://somefile',
                append_trailing_newlines=True,
                coder=CODER), input=(0, 0), output_coders=(CODER,))]))

  def test_in_memory_source_to_flatten(self):
    work = workitem.get_work_items(get_in_memory_source_to_flatten_message())
    self.assertEqual(
        (work.proto.id, work.map_task.operations),
        (1234, [
            maptask.WorkerRead(
                inmemory.InMemorySource(
                    start_index=1,
                    end_index=3,
                    elements=[base64.b64decode(v['value'])
                              for v in IN_MEMORY_ELEMENTS],
                    coder=CODER),
                output_coders=[CODER]),
            maptask.WorkerFlatten(
                inputs=[(0, 0)], output_coders=[CODER])]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
