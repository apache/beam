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

"""Tests for work item executor functionality."""

import logging
import tempfile
import unittest

from google.cloud.dataflow import coders
from google.cloud.dataflow import pvalue
from google.cloud.dataflow.internal import pickler
from google.cloud.dataflow.internal import util
from google.cloud.dataflow.io import bigquery
from google.cloud.dataflow.io import fileio
import google.cloud.dataflow.transforms as ptransform
from google.cloud.dataflow.transforms import core
from google.cloud.dataflow.transforms import window
from google.cloud.dataflow.worker import executor
from google.cloud.dataflow.worker import inmemory
from google.cloud.dataflow.worker import maptask
import mock


def pickle_with_side_inputs(fn, tag_and_type=None):
  tags_and_types = []
  args = []
  if tag_and_type is not None:
    args.append(util.ArgumentPlaceholder())
    tags_and_types.append(tag_and_type)
  return pickler.dumps((fn, args, {}, tags_and_types,
                        core.Windowing(window.GlobalWindows())))


def get_bigquery_source_coder():
  return bigquery.RowAsDictJsonCoder


def make_map_task(operation_list):
  """Make a skeletal MapTask that is good enough to pass to the executor.

  Args:
    operation_list: the work operations to embed.
  Returns:
    A maptask.MapTask object.
  """
  return maptask.MapTask(operation_list, '',
                         ['step-%d' % n for n in xrange(len(operation_list))])


class DoFnUsingStartBundle(ptransform.DoFn):
  """A DoFn class defining start_bundle, finish_bundle, and process methods."""

  def __init__(self, finish_path):
    self.finish_path = finish_path

  def start_bundle(self, context, *args, **kwargs):
    context.state = 'XYZ'

  def process(self, context, *args, **kwargs):
    return ['%s: %s' % (context.state, context.element)]

  def finish_bundle(self, context, *args, **kwargs):
    """Writes a string to a file as a side effect to be checked by tests."""
    with open(self.finish_path, 'w') as f:
      f.write('finish called.')


class ProgressRequestRecordingInMemoryReader(inmemory.InMemoryReader):

  def __init__(self, source):
    self.progress_record = []
    super(ProgressRequestRecordingInMemoryReader, self).__init__(source)

  def get_progress(self):
    next_progress = super(ProgressRequestRecordingInMemoryReader,
                          self).get_progress()
    self.progress_record.append(next_progress.position.record_index)
    return next_progress


class ProgressRequestRecordingInMemorySource(inmemory.InMemorySource):

  def reader(self):
    self.last_reader = ProgressRequestRecordingInMemoryReader(self)
    return self.last_reader


class ExecutorTest(unittest.TestCase):

  SHUFFLE_CODER = coders.PickleCoder()
  OUTPUT_CODER = coders.PickleCoder()

  def create_temp_file(self, content_text):
    """Creates a temporary file with content and returns the path to it."""
    temp = tempfile.NamedTemporaryFile(delete=False)
    with temp.file as tmp:
      tmp.write(content_text)
    return temp.name

  def test_read_do_write(self):
    input_path = self.create_temp_file('01234567890123456789\n0123456789')
    output_path = '%s.out' % input_path
    executor.MapTaskExecutor().execute(make_map_task([
        maptask.WorkerRead(
            fileio.TextFileSource(file_path=input_path,
                                  start_offset=0,
                                  end_offset=15,
                                  strip_trailing_newlines=True,
                                  coder=coders.StrUtf8Coder()),
            output_coders=[self.OUTPUT_CODER]),
        maptask.WorkerDoFn(serialized_fn=pickle_with_side_inputs(
            ptransform.CallableWrapperDoFn(lambda x: ['XYZ: %s' % x])),
                           output_tags=['out'],
                           output_coders=[self.OUTPUT_CODER],
                           input=(0, 0),
                           side_inputs=None),
        maptask.WorkerWrite(
            fileio.TextFileSink(file_path_prefix=output_path,
                                append_trailing_newlines=True,
                                coder=coders.ToStringCoder()),
            input=(1, 0),
            output_coders=(coders.ToStringCoder(),))
    ]))
    with open(output_path) as f:
      self.assertEqual('XYZ: 01234567890123456789\n', f.read())

  def test_read_do_write_with_start_bundle(self):
    input_path = self.create_temp_file('01234567890123456789\n0123456789')
    output_path = '%s.out' % input_path
    finish_path = '%s.finish' % input_path
    executor.MapTaskExecutor().execute(make_map_task([
        maptask.WorkerRead(
            fileio.TextFileSource(file_path=input_path,
                                  start_offset=0,
                                  end_offset=15,
                                  strip_trailing_newlines=True,
                                  coder=coders.StrUtf8Coder()),
            output_coders=[self.OUTPUT_CODER]),
        maptask.WorkerDoFn(serialized_fn=pickle_with_side_inputs(
            DoFnUsingStartBundle(finish_path)),
                           output_tags=['out'],
                           output_coders=[self.OUTPUT_CODER],
                           input=(0, 0),
                           side_inputs=None),
        maptask.WorkerWrite(
            fileio.TextFileSink(file_path_prefix=output_path,
                                append_trailing_newlines=True,
                                coder=coders.ToStringCoder()),
            input=(1, 0),
            output_coders=(coders.ToStringCoder(),))
    ]))
    with open(output_path) as f:
      self.assertEqual('XYZ: 01234567890123456789\n', f.read())
    # Check that the finish_bundle method of the custom DoFn object left the
    # expected side-effect by writing a file with a specific content.
    with open(finish_path) as f:
      self.assertEqual('finish called.', f.read())

  def test_read_do_shuffle_write(self):
    input_path = self.create_temp_file('a\nb\nc\nd\n')
    work_spec = [
        maptask.WorkerRead(
            fileio.TextFileSource(file_path=input_path,
                                  start_offset=0,
                                  end_offset=8,
                                  strip_trailing_newlines=True,
                                  coder=coders.StrUtf8Coder()),
            output_coders=[self.OUTPUT_CODER]),
        maptask.WorkerDoFn(serialized_fn=pickle_with_side_inputs(
            ptransform.CallableWrapperDoFn(lambda x: [(x, 1)])),
                           output_tags=['out'],
                           output_coders=[self.OUTPUT_CODER],
                           input=(0, 0),
                           side_inputs=None),
        maptask.WorkerShuffleWrite(shuffle_kind='group_keys',
                                   shuffle_writer_config='none',
                                   input=(1, 0),
                                   output_coders=(self.SHUFFLE_CODER,))
    ]
    shuffle_sink_mock = mock.MagicMock()
    executor.MapTaskExecutor().execute(
        make_map_task(work_spec),
        test_shuffle_sink=shuffle_sink_mock)
    # Make sure we have seen all the (k, v) writes.
    shuffle_sink_mock.writer().Write.assert_has_calls(
        [mock.call('a', '', 1), mock.call('b', '', 1),
         mock.call('c', '', 1), mock.call('d', '', 1)])

  def test_shuffle_read_do_write(self):
    output_path = self.create_temp_file('n/a')
    work_spec = [
        maptask.WorkerGroupingShuffleRead(shuffle_reader_config='none',
                                          start_shuffle_position='aaa',
                                          end_shuffle_position='zzz',
                                          coder=self.SHUFFLE_CODER,
                                          output_coders=[self.SHUFFLE_CODER]),
        maptask.WorkerDoFn(serialized_fn=pickle_with_side_inputs(
            ptransform.CallableWrapperDoFn(
                lambda (k, vs): [str((k, v)) for v in vs])),
                           output_tags=['out'],
                           output_coders=[self.OUTPUT_CODER],
                           input=(0, 0),
                           side_inputs=None),
        maptask.WorkerWrite(
            fileio.TextFileSink(file_path_prefix=output_path,
                                append_trailing_newlines=True,
                                coder=coders.ToStringCoder()),
            input=(1, 0),
            output_coders=(coders.ToStringCoder(),))
    ]
    shuffle_source_mock = mock.MagicMock()
    shuffle_source_mock.reader().__enter__().__iter__.return_value = [
        (10, [1, 2]), (20, [3])]
    executor.MapTaskExecutor().execute(
        make_map_task(work_spec),
        test_shuffle_source=shuffle_source_mock)
    with open(output_path) as f:
      self.assertEqual('(10, 1)\n(10, 2)\n(20, 3)\n', f.read())

  def test_ungrouped_shuffle_read_and_write(self):
    output_path = self.create_temp_file('n/a')
    work_spec = [
        maptask.WorkerUngroupedShuffleRead(shuffle_reader_config='none',
                                           start_shuffle_position='aaa',
                                           end_shuffle_position='zzz',
                                           coder=self.SHUFFLE_CODER,
                                           output_coders=[self.SHUFFLE_CODER]),
        maptask.WorkerWrite(
            fileio.TextFileSink(file_path_prefix=output_path,
                                append_trailing_newlines=True,
                                coder=coders.ToStringCoder()),
            input=(0, 0),
            output_coders=(coders.ToStringCoder(),))
    ]
    shuffle_source_mock = mock.MagicMock()
    shuffle_source_mock.reader().__enter__().__iter__.return_value = [1, 2, 3]
    executor.MapTaskExecutor().execute(
        make_map_task(work_spec),
        test_shuffle_source=shuffle_source_mock)
    with open(output_path) as f:
      self.assertEqual('1\n2\n3\n', f.read())

  def test_create_do_write(self):
    output_path = self.create_temp_file('n/a')
    elements = ['abc', 'def', 'ghi']
    executor.MapTaskExecutor().execute(make_map_task([
        maptask.WorkerRead(
            inmemory.InMemorySource(
                elements=[pickler.dumps(e) for e in elements],
                # Start at the last element.
                start_index=2,
                # Go beyond the end to test that case is handled.
                end_index=15),
            output_coders=[coders.ToStringCoder()]),
        maptask.WorkerDoFn(serialized_fn=pickle_with_side_inputs(
            ptransform.CallableWrapperDoFn(lambda x: ['XYZ: %s' % x])),
                           output_tags=['out'],
                           output_coders=[self.OUTPUT_CODER],
                           input=(0, 0),
                           side_inputs=None),
        maptask.WorkerWrite(
            fileio.TextFileSink(file_path_prefix=output_path,
                                append_trailing_newlines=True,
                                coder=coders.ToStringCoder()),
            input=(1, 0),
            output_coders=(coders.ToStringCoder(),))
    ]))
    with open(output_path) as f:
      self.assertEqual('XYZ: ghi\n', f.read())

  def test_create_do_avro_write(self):
    output_path = self.create_temp_file('n/a')
    elements = ['abc', 'def', 'ghi']
    executor.MapTaskExecutor().execute(make_map_task([
        maptask.WorkerRead(
            inmemory.InMemorySource(
                elements=[pickler.dumps(e) for e in elements],
                start_index=2,  # Start at the last element.
                end_index=3),
            output_coders=[self.OUTPUT_CODER]),
        maptask.WorkerDoFn(
            serialized_fn=pickle_with_side_inputs(
                ptransform.CallableWrapperDoFn(lambda x: ['XYZ: %s' % x])),
            output_tags=['out'], input=(0, 0), side_inputs=None,
            output_coders=[self.OUTPUT_CODER]),
        maptask.WorkerWrite(fileio.TextFileSink(
            file_path_prefix=output_path,
            append_trailing_newlines=True,
            coder=coders.Base64PickleCoder()),
                            input=(1, 0),
                            output_coders=(self.OUTPUT_CODER,))]))
    with open(output_path) as f:
      self.assertEqual('XYZ: ghi', pickler.loads(f.read().strip()))

  def test_create_do_with_side_in_memory_write(self):
    elements = ['abc', 'def', 'ghi']
    side_elements = ['x', 'y', 'z']
    output_buffer = []
    executor.MapTaskExecutor().execute(make_map_task([
        maptask.WorkerRead(
            inmemory.InMemorySource(
                elements=[pickler.dumps(e) for e in elements],
                start_index=0,
                end_index=3),
            output_coders=[self.OUTPUT_CODER]),
        maptask.WorkerDoFn(
            serialized_fn=pickle_with_side_inputs(
                ptransform.CallableWrapperDoFn(
                    lambda x, side: ['%s:%s' % (x, side)]),
                tag_and_type=('inmemory', pvalue.SingletonPCollectionView,
                              (False, None))),
            output_tags=['out'], input=(0, 0),
            side_inputs=[
                maptask.WorkerSideInputSource(
                    inmemory.InMemorySource(
                        elements=[pickler.dumps(e) for e in side_elements],
                        start_index=None,
                        end_index=None),
                    tag='inmemory')],
            output_coders=[self.OUTPUT_CODER]),
        maptask.WorkerInMemoryWrite(
            output_buffer=output_buffer,
            input=(1, 0),
            output_coders=(self.OUTPUT_CODER,))]))
    # The side source was specified as singleton therefore we should see
    # only the first element appended.
    self.assertEqual(['abc:x', 'def:x', 'ghi:x'], output_buffer)

  def test_in_memory_source_progress_reporting(self):
    elements = [101, 201, 301, 401, 501, 601, 701]
    output_buffer = []
    source = ProgressRequestRecordingInMemorySource(
        elements=[pickler.dumps(e) for e in elements])
    executor.MapTaskExecutor().execute(make_map_task([
        maptask.WorkerRead(source, output_coders=[self.OUTPUT_CODER]),
        maptask.WorkerInMemoryWrite(output_buffer=output_buffer,
                                    input=(0, 0),
                                    output_coders=(self.OUTPUT_CODER,))
    ]))
    self.assertEqual(elements, output_buffer)

    expected_progress_record = range(len(elements))
    self.assertEqual(expected_progress_record,
                     source.last_reader.progress_record)

  def test_create_do_with_side_text_file_write(self):
    input_path = self.create_temp_file('x\ny\n')
    elements = ['aa', 'bb']
    output_buffer = []
    executor.MapTaskExecutor().execute(make_map_task([
        maptask.WorkerRead(
            inmemory.InMemorySource(
                elements=[pickler.dumps(e) for e in elements],
                start_index=0,
                end_index=2),
            output_coders=[self.OUTPUT_CODER]),
        maptask.WorkerDoFn(
            serialized_fn=pickle_with_side_inputs(
                ptransform.CallableWrapperDoFn(
                    lambda x, side: ['%s:%s' % (x, s) for s in side]),
                tag_and_type=('textfile', pvalue.IterablePCollectionView, ())),
            output_tags=['out'], input=(0, 0),
            side_inputs=[
                maptask.WorkerSideInputSource(fileio.TextFileSource(
                    file_path=input_path, start_offset=None, end_offset=None,
                    strip_trailing_newlines=True,
                    coder=coders.StrUtf8Coder()),
                                              tag='textfile')],
            output_coders=[self.OUTPUT_CODER]),
        maptask.WorkerInMemoryWrite(output_buffer=output_buffer,
                                    input=(1, 0),
                                    output_coders=(self.OUTPUT_CODER,))]))
    # The side source was specified as collection therefore we should see
    # all elements of the side source.
    self.assertEqual([u'aa:x', u'aa:y', u'bb:x', u'bb:y'],
                     sorted(output_buffer))

  def test_create_do_with_singleton_side_bigquery_write(self):
    elements = ['abc', 'def', 'ghi']
    side_elements = ['x', 'y', 'z']
    output_buffer = []
    patch_target = 'google.cloud.dataflow.io.bigquery.BigQueryReader'
    with mock.patch(target=patch_target) as mock_class:
      # Setup the reader so it will yield the values in 'side_elements'.
      reader_mock = mock_class.return_value
      reader_mock.__enter__.return_value = reader_mock
      reader_mock.__iter__.return_value = (x for x in side_elements)

      pickled_elements = [pickler.dumps(e) for e in elements]
      executor.MapTaskExecutor().execute(make_map_task([
          maptask.WorkerRead(
              inmemory.InMemorySource(elements=pickled_elements,
                                      start_index=0,
                                      end_index=3),
              output_coders=[self.OUTPUT_CODER]),
          maptask.WorkerDoFn(
              serialized_fn=pickle_with_side_inputs(
                  ptransform.CallableWrapperDoFn(
                      lambda x, side: ['%s:%s' % (x, side)]),
                  tag_and_type=('bigquery', pvalue.SingletonPCollectionView,
                                (False, None))),
              output_tags=['out'], input=(0, 0),
              side_inputs=[
                  maptask.WorkerSideInputSource(
                      bigquery.BigQuerySource(
                          project='project',
                          dataset='dataset',
                          table='table',
                          coder=get_bigquery_source_coder()),
                      tag='bigquery')],
              output_coders=[self.OUTPUT_CODER]),
          maptask.WorkerInMemoryWrite(
              output_buffer=output_buffer,
              input=(1, 0),
              output_coders=(self.OUTPUT_CODER,))]))
    # The side source was specified as singleton therefore we should see
    # only the first element appended.
    self.assertEqual(['abc:x', 'def:x', 'ghi:x'], output_buffer)

  def test_create_do_with_collection_side_bigquery_write(self):
    elements = ['aa', 'bb']
    side_elements = ['x', 'y']
    output_buffer = []
    patch_target = 'google.cloud.dataflow.io.bigquery.BigQueryReader'
    with mock.patch(target=patch_target) as mock_class:
      # Setup the reader so it will yield the values in 'side_elements'.
      reader_mock = mock_class.return_value
      reader_mock.__enter__.return_value = reader_mock
      reader_mock.__iter__.return_value = (x for x in side_elements)

      executor.MapTaskExecutor().execute(make_map_task([
          maptask.WorkerRead(
              inmemory.InMemorySource(
                  elements=[pickler.dumps(e) for e in elements],
                  start_index=0,
                  end_index=3),
              output_coders=[self.OUTPUT_CODER]),
          maptask.WorkerDoFn(
              serialized_fn=pickle_with_side_inputs(
                  ptransform.CallableWrapperDoFn(
                      lambda x, side: ['%s:%s' % (x, s) for s in side]),
                  tag_and_type=('bigquery', pvalue.IterablePCollectionView,
                                ())),
              output_tags=['out'], input=(0, 0),
              side_inputs=[
                  maptask.WorkerSideInputSource(
                      bigquery.BigQuerySource(
                          project='project',
                          dataset='dataset',
                          table='table',
                          coder=get_bigquery_source_coder()),
                      tag='bigquery')],
              output_coders=[self.OUTPUT_CODER]),
          maptask.WorkerInMemoryWrite(
              output_buffer=output_buffer,
              input=(1, 0),
              output_coders=(self.OUTPUT_CODER,))]))
    # The side source was specified as collection therefore we should see
    # all elements of the side source.
    self.assertEqual(['aa:x', 'aa:y', 'bb:x', 'bb:y'],
                     sorted(output_buffer))

  def test_create_do_with_side_avro_file_write(self):
    input_path1 = self.create_temp_file('%s\n' % pickler.dumps('x'))
    input_path2 = self.create_temp_file('%s\n' % pickler.dumps('y'))
    elements = ['aa', 'bb']
    output_buffer = []
    executor.MapTaskExecutor().execute(make_map_task([
        maptask.WorkerRead(
            inmemory.InMemorySource(
                elements=[pickler.dumps(e) for e in elements],
                start_index=0,
                end_index=2),
            output_coders=[self.OUTPUT_CODER]),
        maptask.WorkerDoFn(
            serialized_fn=pickle_with_side_inputs(
                ptransform.CallableWrapperDoFn(
                    lambda x, side: ['%s:%s' % (x, s) for s in side]),
                tag_and_type=('sometag', pvalue.IterablePCollectionView, ())),
            output_tags=['out'], input=(0, 0),
            # Note that the two side inputs have the same tag. This is quite
            # common for intermediary PCollections used as side inputs that
            # are saved as AVRO files. The files will contain the sharded
            # PCollection.
            side_inputs=[
                maptask.WorkerSideInputSource(
                    fileio.TextFileSource(
                        file_path=input_path1,
                        coder=coders.Base64PickleCoder()),
                    tag='sometag'),
                maptask.WorkerSideInputSource(
                    fileio.TextFileSource(file_path=input_path2,
                                          coder=coders.Base64PickleCoder()),
                    tag='sometag')],
            output_coders=[self.OUTPUT_CODER]),
        maptask.WorkerInMemoryWrite(
            output_buffer=output_buffer,
            input=(1, 0),
            output_coders=(self.OUTPUT_CODER,))]))
    # The side source was specified as collection therefore we should see
    # all three elements of the side source.
    self.assertEqual([u'aa:x', u'aa:y', u'bb:x', u'bb:y'],
                     sorted(output_buffer))

  def test_combine(self):
    elements = [('a', [1, 2, 3]), ('b', [10])]
    output_buffer = []
    executor.MapTaskExecutor().execute(make_map_task([
        maptask.WorkerRead(
            inmemory.InMemorySource(
                elements=[pickler.dumps(e) for e in elements],
                start_index=0,
                end_index=100),
            output_coders=[self.OUTPUT_CODER]),
        maptask.WorkerCombineFn(serialized_fn=pickle_with_side_inputs(
            ptransform.CombineFn.from_callable(sum)),
                                phase='all',
                                input=(0, 0),
                                output_coders=[self.OUTPUT_CODER]),
        maptask.WorkerInMemoryWrite(output_buffer=output_buffer,
                                    input=(1, 0),
                                    output_coders=(self.OUTPUT_CODER,))
    ]))
    self.assertEqual([('a', 6), ('b', 10)], output_buffer)

  def test_pgbk(self):
    elements = [('a', 1), ('b', 2), ('a', 3), ('a', 4)]
    output_buffer = []
    executor.MapTaskExecutor().execute(make_map_task([
        maptask.WorkerRead(
            inmemory.InMemorySource(elements=[pickler.dumps(e) for e in elements
                                             ],
                                    start_index=0,
                                    end_index=100),
            output_coders=[self.OUTPUT_CODER]),
        maptask.WorkerPartialGroupByKey(
            combine_fn=None,
            input=(0, 0),
            output_coders=[self.OUTPUT_CODER]),
        maptask.WorkerInMemoryWrite(output_buffer=output_buffer,
                                    input=(1, 0),
                                    output_coders=(self.OUTPUT_CODER,))
    ]))
    self.assertEqual([('a', [1, 3, 4]), ('b', [2])], sorted(output_buffer))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
