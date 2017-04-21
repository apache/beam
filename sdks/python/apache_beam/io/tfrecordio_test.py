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

import binascii
import cStringIO
import glob
import gzip
import logging
import os
import pickle
import random
import shutil
import tempfile
import unittest

import apache_beam as beam
from apache_beam import coders
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.tfrecordio import _TFRecordSink
from apache_beam.io.tfrecordio import _TFRecordSource
from apache_beam.io.tfrecordio import _TFRecordUtil
from apache_beam.io.tfrecordio import ReadFromTFRecord
from apache_beam.io.tfrecordio import WriteToTFRecord
from apache_beam.test_pipeline import TestPipeline
import crcmod


try:
  import tensorflow as tf  # pylint: disable=import-error
except ImportError:
  tf = None  # pylint: disable=invalid-name
  logging.warning('Tensorflow is not installed, so skipping some tests.')

# Created by running following code in python:
# >>> import tensorflow as tf
# >>> import base64
# >>> writer = tf.python_io.TFRecordWriter('/tmp/python_foo.tfrecord')
# >>> writer.write('foo')
# >>> writer.close()
# >>> with open('/tmp/python_foo.tfrecord', 'rb') as f:
# ...   data =  base64.b64encode(f.read())
# ...   print data
FOO_RECORD_BASE64 = 'AwAAAAAAAACwmUkOZm9vYYq+/g=='

# Same as above but containing two records ['foo', 'bar']
FOO_BAR_RECORD_BASE64 = 'AwAAAAAAAACwmUkOZm9vYYq+/gMAAAAAAAAAsJlJDmJhckYA5cg='


class TestTFRecordUtil(unittest.TestCase):

  def setUp(self):
    self.record = binascii.a2b_base64(FOO_RECORD_BASE64)

  def _as_file_handle(self, contents):
    result = cStringIO.StringIO()
    result.write(contents)
    result.reset()
    return result

  def _increment_value_at_index(self, value, index):
    l = list(value)
    l[index] = chr(ord(l[index]) + 1)
    return ''.join(l)

  def _test_error(self, record, error_text):
    with self.assertRaises(ValueError) as context:
      _TFRecordUtil.read_record(self._as_file_handle(record))
    self.assertIn(error_text, context.exception.message)

  def test_masked_crc32c(self):
    self.assertEqual(0xfd7fffa, _TFRecordUtil._masked_crc32c('\x00' * 32))
    self.assertEqual(0xf909b029, _TFRecordUtil._masked_crc32c('\xff' * 32))
    self.assertEqual(0xfebe8a61, _TFRecordUtil._masked_crc32c('foo'))
    self.assertEqual(
        0xe4999b0,
        _TFRecordUtil._masked_crc32c('\x03\x00\x00\x00\x00\x00\x00\x00'))

  def test_masked_crc32c_crcmod(self):
    crc32c_fn = crcmod.predefined.mkPredefinedCrcFun('crc-32c')
    self.assertEqual(
        0xfd7fffa,
        _TFRecordUtil._masked_crc32c(
            '\x00' * 32, crc32c_fn=crc32c_fn))
    self.assertEqual(
        0xf909b029,
        _TFRecordUtil._masked_crc32c(
            '\xff' * 32, crc32c_fn=crc32c_fn))
    self.assertEqual(
        0xfebe8a61, _TFRecordUtil._masked_crc32c(
            'foo', crc32c_fn=crc32c_fn))
    self.assertEqual(
        0xe4999b0,
        _TFRecordUtil._masked_crc32c(
            '\x03\x00\x00\x00\x00\x00\x00\x00', crc32c_fn=crc32c_fn))

  def test_write_record(self):
    file_handle = cStringIO.StringIO()
    _TFRecordUtil.write_record(file_handle, 'foo')
    self.assertEqual(self.record, file_handle.getvalue())

  def test_read_record(self):
    actual = _TFRecordUtil.read_record(self._as_file_handle(self.record))
    self.assertEqual('foo', actual)

  def test_read_record_invalid_record(self):
    self._test_error('bar', 'Not a valid TFRecord. Fewer than 12 bytes')

  def test_read_record_invalid_length_mask(self):
    record = self._increment_value_at_index(self.record, 9)
    self._test_error(record, 'Mismatch of length mask')

  def test_read_record_invalid_data_mask(self):
    record = self._increment_value_at_index(self.record, 16)
    self._test_error(record, 'Mismatch of data mask')

  def test_compatibility_read_write(self):
    for record in ['', 'blah', 'another blah']:
      file_handle = cStringIO.StringIO()
      _TFRecordUtil.write_record(file_handle, record)
      file_handle.reset()
      actual = _TFRecordUtil.read_record(file_handle)
      self.assertEqual(record, actual)


class _TestCaseWithTempDirCleanUp(unittest.TestCase):
  """Base class for TestCases that deals with TempDir clean-up.

  Inherited test cases will call self._new_tempdir() to start a temporary dir
  which will be deleted at the end of the tests (when tearDown() is called).
  """

  def setUp(self):
    self._tempdirs = []

  def tearDown(self):
    for path in self._tempdirs:
      if os.path.exists(path):
        shutil.rmtree(path)
    self._tempdirs = []

  def _new_tempdir(self):
    result = tempfile.mkdtemp()
    self._tempdirs.append(result)
    return result


class TestTFRecordSink(_TestCaseWithTempDirCleanUp):

  def _write_lines(self, sink, path, lines):
    f = sink.open(path)
    for l in lines:
      sink.write_record(f, l)
    sink.close(f)

  def test_write_record_single(self):
    path = os.path.join(self._new_tempdir(), 'result')
    record = binascii.a2b_base64(FOO_RECORD_BASE64)
    sink = _TFRecordSink(
        path,
        coder=coders.BytesCoder(),
        file_name_suffix='',
        num_shards=0,
        shard_name_template=None,
        compression_type=CompressionTypes.UNCOMPRESSED)
    self._write_lines(sink, path, ['foo'])

    with open(path, 'r') as f:
      self.assertEqual(f.read(), record)

  def test_write_record_multiple(self):
    path = os.path.join(self._new_tempdir(), 'result')
    record = binascii.a2b_base64(FOO_BAR_RECORD_BASE64)
    sink = _TFRecordSink(
        path,
        coder=coders.BytesCoder(),
        file_name_suffix='',
        num_shards=0,
        shard_name_template=None,
        compression_type=CompressionTypes.UNCOMPRESSED)
    self._write_lines(sink, path, ['foo', 'bar'])

    with open(path, 'r') as f:
      self.assertEqual(f.read(), record)


@unittest.skipIf(tf is None, 'tensorflow not installed.')
class TestWriteToTFRecord(TestTFRecordSink):

  def test_write_record_gzip(self):
    file_path_prefix = os.path.join(self._new_tempdir(), 'result')
    with TestPipeline() as p:
      input_data = ['foo', 'bar']
      _ = p | beam.Create(input_data) | WriteToTFRecord(
          file_path_prefix, compression_type=CompressionTypes.GZIP)

    actual = []
    file_name = glob.glob(file_path_prefix + '-*')[0]
    for r in tf.python_io.tf_record_iterator(
        file_name, options=tf.python_io.TFRecordOptions(
            tf.python_io.TFRecordCompressionType.GZIP)):
      actual.append(r)
    self.assertEqual(actual, input_data)

  def test_write_record_auto(self):
    file_path_prefix = os.path.join(self._new_tempdir(), 'result')
    with TestPipeline() as p:
      input_data = ['foo', 'bar']
      _ = p | beam.Create(input_data) | WriteToTFRecord(
          file_path_prefix, file_name_suffix='.gz')

    actual = []
    file_name = glob.glob(file_path_prefix + '-*.gz')[0]
    for r in tf.python_io.tf_record_iterator(
        file_name, options=tf.python_io.TFRecordOptions(
            tf.python_io.TFRecordCompressionType.GZIP)):
      actual.append(r)
    self.assertEqual(actual, input_data)


class TestTFRecordSource(_TestCaseWithTempDirCleanUp):

  def _write_file(self, path, base64_records):
    record = binascii.a2b_base64(base64_records)
    with open(path, 'wb') as f:
      f.write(record)

  def _write_file_gzip(self, path, base64_records):
    record = binascii.a2b_base64(base64_records)
    with gzip.GzipFile(path, 'wb') as f:
      f.write(record)

  def test_process_single(self):
    path = os.path.join(self._new_tempdir(), 'result')
    self._write_file(path, FOO_RECORD_BASE64)
    with TestPipeline() as p:
      result = (p
                | beam.Read(
                    _TFRecordSource(
                        path,
                        coder=coders.BytesCoder(),
                        compression_type=CompressionTypes.AUTO,
                        validate=True)))
      beam.assert_that(result, beam.equal_to(['foo']))

  def test_process_multiple(self):
    path = os.path.join(self._new_tempdir(), 'result')
    self._write_file(path, FOO_BAR_RECORD_BASE64)
    with TestPipeline() as p:
      result = (p
                | beam.Read(
                    _TFRecordSource(
                        path,
                        coder=coders.BytesCoder(),
                        compression_type=CompressionTypes.AUTO,
                        validate=True)))
      beam.assert_that(result, beam.equal_to(['foo', 'bar']))

  def test_process_gzip(self):
    path = os.path.join(self._new_tempdir(), 'result')
    self._write_file_gzip(path, FOO_BAR_RECORD_BASE64)
    with TestPipeline() as p:
      result = (p
                | beam.Read(
                    _TFRecordSource(
                        path,
                        coder=coders.BytesCoder(),
                        compression_type=CompressionTypes.GZIP,
                        validate=True)))
      beam.assert_that(result, beam.equal_to(['foo', 'bar']))

  def test_process_auto(self):
    path = os.path.join(self._new_tempdir(), 'result.gz')
    self._write_file_gzip(path, FOO_BAR_RECORD_BASE64)
    with TestPipeline() as p:
      result = (p
                | beam.Read(
                    _TFRecordSource(
                        path,
                        coder=coders.BytesCoder(),
                        compression_type=CompressionTypes.AUTO,
                        validate=True)))
      beam.assert_that(result, beam.equal_to(['foo', 'bar']))


class TestReadFromTFRecordSource(TestTFRecordSource):

  def test_process_gzip(self):
    path = os.path.join(self._new_tempdir(), 'result')
    self._write_file_gzip(path, FOO_BAR_RECORD_BASE64)
    with TestPipeline() as p:
      result = (p
                | ReadFromTFRecord(
                    path, compression_type=CompressionTypes.GZIP))
      beam.assert_that(result, beam.equal_to(['foo', 'bar']))

  def test_process_gzip_auto(self):
    path = os.path.join(self._new_tempdir(), 'result.gz')
    self._write_file_gzip(path, FOO_BAR_RECORD_BASE64)
    with TestPipeline() as p:
      result = (p
                | ReadFromTFRecord(
                    path, compression_type=CompressionTypes.AUTO))
      beam.assert_that(result, beam.equal_to(['foo', 'bar']))


class TestEnd2EndWriteAndRead(_TestCaseWithTempDirCleanUp):

  def create_inputs(self):
    input_array = [[random.random() - 0.5 for _ in xrange(15)]
                   for _ in xrange(12)]
    memfile = cStringIO.StringIO()
    pickle.dump(input_array, memfile)
    return memfile.getvalue()

  def test_end2end(self):
    file_path_prefix = os.path.join(self._new_tempdir(), 'result')

    # Generate a TFRecord file.
    with TestPipeline() as p:
      expected_data = [self.create_inputs() for _ in range(0, 10)]
      _ = p | beam.Create(expected_data) | WriteToTFRecord(file_path_prefix)

    # Read the file back and compare.
    with TestPipeline() as p:
      actual_data = p | ReadFromTFRecord(file_path_prefix + '-*')
      beam.assert_that(actual_data, beam.equal_to(expected_data))

  def test_end2end_auto_compression(self):
    file_path_prefix = os.path.join(self._new_tempdir(), 'result')

    # Generate a TFRecord file.
    with TestPipeline() as p:
      expected_data = [self.create_inputs() for _ in range(0, 10)]
      _ = p | beam.Create(expected_data) | WriteToTFRecord(
          file_path_prefix, file_name_suffix='.gz')

    # Read the file back and compare.
    with TestPipeline() as p:
      actual_data = p | ReadFromTFRecord(file_path_prefix + '-*')
      beam.assert_that(actual_data, beam.equal_to(expected_data))

  def test_end2end_auto_compression_unsharded(self):
    file_path_prefix = os.path.join(self._new_tempdir(), 'result')

    # Generate a TFRecord file.
    with TestPipeline() as p:
      expected_data = [self.create_inputs() for _ in range(0, 10)]
      _ = p | beam.Create(expected_data) | WriteToTFRecord(
          file_path_prefix + '.gz', shard_name_template='')

    # Read the file back and compare.
    with TestPipeline() as p:
      actual_data = p | ReadFromTFRecord(file_path_prefix + '.gz')
      beam.assert_that(actual_data, beam.equal_to(expected_data))

  @unittest.skipIf(tf is None, 'tensorflow not installed.')
  def test_end2end_example_proto(self):
    file_path_prefix = os.path.join(self._new_tempdir(), 'result')

    example = tf.train.Example()
    example.features.feature['int'].int64_list.value.extend(range(3))
    example.features.feature['bytes'].bytes_list.value.extend(
        [b'foo', b'bar'])

    with TestPipeline() as p:
      _ = p | beam.Create([example]) | WriteToTFRecord(
          file_path_prefix, coder=beam.coders.ProtoCoder(example.__class__))

    # Read the file back and compare.
    with TestPipeline() as p:
      actual_data = (p | ReadFromTFRecord(
          file_path_prefix + '-*',
          coder=beam.coders.ProtoCoder(example.__class__)))
      beam.assert_that(actual_data, beam.equal_to([example]))

  def test_end2end_read_write_read(self):
    path = os.path.join(self._new_tempdir(), 'result')
    with TestPipeline() as p:
      # Initial read to validate the pipeline doesn't fail before the file is
      # created.
      _ = p | ReadFromTFRecord(path + '-*', validate=False)
      expected_data = [self.create_inputs() for _ in range(0, 10)]
      _ = p | beam.Create(expected_data) | WriteToTFRecord(
          path, file_name_suffix='.gz')

    # Read the file back and compare.
    with TestPipeline() as p:
      actual_data = p | ReadFromTFRecord(path+'-*', validate=True)
      beam.assert_that(actual_data, beam.equal_to(expected_data))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
