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

# pytype: skip-file

import binascii
import glob
import gzip
import io
import logging
import os
import pickle
import random
import re
import unittest
import zlib

import crcmod

import apache_beam as beam
from apache_beam import Create
from apache_beam import coders
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.tfrecordio import ReadAllFromTFRecord
from apache_beam.io.tfrecordio import ReadFromTFRecord
from apache_beam.io.tfrecordio import WriteToTFRecord
from apache_beam.io.tfrecordio import _TFRecordSink
from apache_beam.io.tfrecordio import _TFRecordUtil
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import TempDir
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

try:
  import tensorflow.compat.v1 as tf  # pylint: disable=import-error
except ImportError:
  try:
    import tensorflow as tf  # pylint: disable=import-error
  except ImportError:
    tf = None  # pylint: disable=invalid-name
    logging.warning('Tensorflow is not installed, so skipping some tests.')

# Created by running following code in python:
# >>> import tensorflow as tf
# >>> import base64
# >>> writer = tf.python_io.TFRecordWriter('/tmp/python_foo.tfrecord')
# >>> writer.write(b'foo')
# >>> writer.close()
# >>> with open('/tmp/python_foo.tfrecord', 'rb') as f:
# ...   data =  base64.b64encode(f.read())
# ...   print(data)
FOO_RECORD_BASE64 = b'AwAAAAAAAACwmUkOZm9vYYq+/g=='

# Same as above but containing two records [b'foo', b'bar']
FOO_BAR_RECORD_BASE64 = b'AwAAAAAAAACwmUkOZm9vYYq+/gMAAAAAAAAAsJlJDmJhckYA5cg='


def _write_file(path, base64_records):
  record = binascii.a2b_base64(base64_records)
  with open(path, 'wb') as f:
    f.write(record)


def _write_file_deflate(path, base64_records):
  record = binascii.a2b_base64(base64_records)
  with open(path, 'wb') as f:
    f.write(zlib.compress(record))


def _write_file_gzip(path, base64_records):
  record = binascii.a2b_base64(base64_records)
  with gzip.GzipFile(path, 'wb') as f:
    f.write(record)


class TestTFRecordUtil(unittest.TestCase):
  def setUp(self):
    self.record = binascii.a2b_base64(FOO_RECORD_BASE64)

  def _as_file_handle(self, contents):
    result = io.BytesIO()
    result.write(contents)
    result.seek(0)
    return result

  def _increment_value_at_index(self, value, index):
    l = list(value)
    l[index] = l[index] + 1
    return bytes(l)

  def _test_error(self, record, error_text):
    with self.assertRaisesRegex(ValueError, re.escape(error_text)):
      _TFRecordUtil.read_record(self._as_file_handle(record))

  def test_masked_crc32c(self):
    self.assertEqual(0xfd7fffa, _TFRecordUtil._masked_crc32c(b'\x00' * 32))
    self.assertEqual(0xf909b029, _TFRecordUtil._masked_crc32c(b'\xff' * 32))
    self.assertEqual(0xfebe8a61, _TFRecordUtil._masked_crc32c(b'foo'))
    self.assertEqual(
        0xe4999b0,
        _TFRecordUtil._masked_crc32c(b'\x03\x00\x00\x00\x00\x00\x00\x00'))

  def test_masked_crc32c_crcmod(self):
    crc32c_fn = crcmod.predefined.mkPredefinedCrcFun('crc-32c')
    self.assertEqual(
        0xfd7fffa,
        _TFRecordUtil._masked_crc32c(b'\x00' * 32, crc32c_fn=crc32c_fn))
    self.assertEqual(
        0xf909b029,
        _TFRecordUtil._masked_crc32c(b'\xff' * 32, crc32c_fn=crc32c_fn))
    self.assertEqual(
        0xfebe8a61, _TFRecordUtil._masked_crc32c(b'foo', crc32c_fn=crc32c_fn))
    self.assertEqual(
        0xe4999b0,
        _TFRecordUtil._masked_crc32c(
            b'\x03\x00\x00\x00\x00\x00\x00\x00', crc32c_fn=crc32c_fn))

  def test_write_record(self):
    file_handle = io.BytesIO()
    _TFRecordUtil.write_record(file_handle, b'foo')
    self.assertEqual(self.record, file_handle.getvalue())

  def test_read_record(self):
    actual = _TFRecordUtil.read_record(self._as_file_handle(self.record))
    self.assertEqual(b'foo', actual)

  def test_read_record_invalid_record(self):
    self._test_error(b'bar', 'Not a valid TFRecord. Fewer than 12 bytes')

  def test_read_record_invalid_length_mask(self):
    record = self._increment_value_at_index(self.record, 9)
    self._test_error(record, 'Mismatch of length mask')

  def test_read_record_invalid_data_mask(self):
    record = self._increment_value_at_index(self.record, 16)
    self._test_error(record, 'Mismatch of data mask')

  def test_compatibility_read_write(self):
    for record in [b'', b'blah', b'another blah']:
      file_handle = io.BytesIO()
      _TFRecordUtil.write_record(file_handle, record)
      file_handle.seek(0)
      actual = _TFRecordUtil.read_record(file_handle)
      self.assertEqual(record, actual)


class TestTFRecordSink(unittest.TestCase):
  def _write_lines(self, sink, path, lines):
    f = sink.open(path)
    for l in lines:
      sink.write_record(f, l)
    sink.close(f)

  def test_write_record_single(self):
    with TempDir() as temp_dir:
      path = temp_dir.create_temp_file('result')
      record = binascii.a2b_base64(FOO_RECORD_BASE64)
      sink = _TFRecordSink(
          path,
          coder=coders.BytesCoder(),
          file_name_suffix='',
          num_shards=0,
          shard_name_template=None,
          compression_type=CompressionTypes.UNCOMPRESSED)
      self._write_lines(sink, path, [b'foo'])

      with open(path, 'rb') as f:
        self.assertEqual(f.read(), record)

  def test_write_record_multiple(self):
    with TempDir() as temp_dir:
      path = temp_dir.create_temp_file('result')
      record = binascii.a2b_base64(FOO_BAR_RECORD_BASE64)
      sink = _TFRecordSink(
          path,
          coder=coders.BytesCoder(),
          file_name_suffix='',
          num_shards=0,
          shard_name_template=None,
          compression_type=CompressionTypes.UNCOMPRESSED)
      self._write_lines(sink, path, [b'foo', b'bar'])

      with open(path, 'rb') as f:
        self.assertEqual(f.read(), record)


@unittest.skipIf(tf is None, 'tensorflow not installed.')
class TestWriteToTFRecord(TestTFRecordSink):
  def test_write_record_gzip(self):
    with TempDir() as temp_dir:
      file_path_prefix = temp_dir.create_temp_file('result')
      with TestPipeline() as p:
        input_data = [b'foo', b'bar']
        _ = p | beam.Create(input_data) | WriteToTFRecord(
            file_path_prefix, compression_type=CompressionTypes.GZIP)

      actual = []
      file_name = glob.glob(file_path_prefix + '-*')[0]
      for r in tf.python_io.tf_record_iterator(
          file_name,
          options=tf.python_io.TFRecordOptions(
              tf.python_io.TFRecordCompressionType.GZIP)):
        actual.append(r)
      self.assertEqual(sorted(actual), sorted(input_data))

  def test_write_record_auto(self):
    with TempDir() as temp_dir:
      file_path_prefix = temp_dir.create_temp_file('result')
      with TestPipeline() as p:
        input_data = [b'foo', b'bar']
        _ = p | beam.Create(input_data) | WriteToTFRecord(
            file_path_prefix, file_name_suffix='.gz')

      actual = []
      file_name = glob.glob(file_path_prefix + '-*.gz')[0]
      for r in tf.python_io.tf_record_iterator(
          file_name,
          options=tf.python_io.TFRecordOptions(
              tf.python_io.TFRecordCompressionType.GZIP)):
        actual.append(r)
      self.assertEqual(sorted(actual), sorted(input_data))


class TestReadFromTFRecord(unittest.TestCase):
  def test_process_single(self):
    with TempDir() as temp_dir:
      path = temp_dir.create_temp_file('result')
      _write_file(path, FOO_RECORD_BASE64)
      with TestPipeline() as p:
        result = (
            p
            | ReadFromTFRecord(
                path,
                coder=coders.BytesCoder(),
                compression_type=CompressionTypes.AUTO,
                validate=True))
        assert_that(result, equal_to([b'foo']))

  def test_process_multiple(self):
    with TempDir() as temp_dir:
      path = temp_dir.create_temp_file('result')
      _write_file(path, FOO_BAR_RECORD_BASE64)
      with TestPipeline() as p:
        result = (
            p
            | ReadFromTFRecord(
                path,
                coder=coders.BytesCoder(),
                compression_type=CompressionTypes.AUTO,
                validate=True))
        assert_that(result, equal_to([b'foo', b'bar']))

  def test_process_deflate(self):
    with TempDir() as temp_dir:
      path = temp_dir.create_temp_file('result')
      _write_file_deflate(path, FOO_BAR_RECORD_BASE64)
      with TestPipeline() as p:
        result = (
            p
            | ReadFromTFRecord(
                path,
                coder=coders.BytesCoder(),
                compression_type=CompressionTypes.DEFLATE,
                validate=True))
        assert_that(result, equal_to([b'foo', b'bar']))

  def test_process_gzip_with_coder(self):
    with TempDir() as temp_dir:
      path = temp_dir.create_temp_file('result')
      _write_file_gzip(path, FOO_BAR_RECORD_BASE64)
      with TestPipeline() as p:
        result = (
            p
            | ReadFromTFRecord(
                path,
                coder=coders.BytesCoder(),
                compression_type=CompressionTypes.GZIP,
                validate=True))
        assert_that(result, equal_to([b'foo', b'bar']))

  def test_process_gzip_without_coder(self):
    with TempDir() as temp_dir:
      path = temp_dir.create_temp_file('result')
      _write_file_gzip(path, FOO_BAR_RECORD_BASE64)
      with TestPipeline() as p:
        result = (
            p
            | ReadFromTFRecord(path, compression_type=CompressionTypes.GZIP))
        assert_that(result, equal_to([b'foo', b'bar']))

  def test_process_auto(self):
    with TempDir() as temp_dir:
      path = temp_dir.create_temp_file('result.gz')
      _write_file_gzip(path, FOO_BAR_RECORD_BASE64)
      with TestPipeline() as p:
        result = (
            p
            | ReadFromTFRecord(
                path,
                coder=coders.BytesCoder(),
                compression_type=CompressionTypes.AUTO,
                validate=True))
        assert_that(result, equal_to([b'foo', b'bar']))

  def test_process_gzip_auto(self):
    with TempDir() as temp_dir:
      path = temp_dir.create_temp_file('result.gz')
      _write_file_gzip(path, FOO_BAR_RECORD_BASE64)
      with TestPipeline() as p:
        result = (
            p
            | ReadFromTFRecord(path, compression_type=CompressionTypes.AUTO))
        assert_that(result, equal_to([b'foo', b'bar']))


class TestReadAllFromTFRecord(unittest.TestCase):
  def _write_glob(self, temp_dir, suffix, include_empty=False):
    for _ in range(3):
      path = temp_dir.create_temp_file(suffix)
      _write_file(path, FOO_BAR_RECORD_BASE64)
    if include_empty:
      path = temp_dir.create_temp_file(suffix)
      _write_file(path, '')

  def test_process_single(self):
    with TempDir() as temp_dir:
      path = temp_dir.create_temp_file('result')
      _write_file(path, FOO_RECORD_BASE64)
      with TestPipeline() as p:
        result = (
            p
            | Create([path])
            | ReadAllFromTFRecord(
                coder=coders.BytesCoder(),
                compression_type=CompressionTypes.AUTO))
        assert_that(result, equal_to([b'foo']))

  def test_process_multiple(self):
    with TempDir() as temp_dir:
      path = temp_dir.create_temp_file('result')
      _write_file(path, FOO_BAR_RECORD_BASE64)
      with TestPipeline() as p:
        result = (
            p
            | Create([path])
            | ReadAllFromTFRecord(
                coder=coders.BytesCoder(),
                compression_type=CompressionTypes.AUTO))
        assert_that(result, equal_to([b'foo', b'bar']))

  def test_process_with_filename(self):
    with TempDir() as temp_dir:
      num_files = 3
      files = []
      for i in range(num_files):
        path = temp_dir.create_temp_file('result%s' % i)
        _write_file(path, FOO_BAR_RECORD_BASE64)
        files.append(path)
      content = [b'foo', b'bar']
      expected = [(file, line) for file in files for line in content]
      pattern = temp_dir.get_path() + os.path.sep + '*'

      with TestPipeline() as p:
        result = (
            p
            | Create([pattern])
            | ReadAllFromTFRecord(
                coder=coders.BytesCoder(),
                compression_type=CompressionTypes.AUTO,
                with_filename=True))
        assert_that(result, equal_to(expected))

  def test_process_glob(self):
    with TempDir() as temp_dir:
      self._write_glob(temp_dir, 'result')
      glob = temp_dir.get_path() + os.path.sep + '*result'
      with TestPipeline() as p:
        result = (
            p
            | Create([glob])
            | ReadAllFromTFRecord(
                coder=coders.BytesCoder(),
                compression_type=CompressionTypes.AUTO))
        assert_that(result, equal_to([b'foo', b'bar'] * 3))

  def test_process_glob_with_empty_file(self):
    with TempDir() as temp_dir:
      self._write_glob(temp_dir, 'result', include_empty=True)
      glob = temp_dir.get_path() + os.path.sep + '*result'
      with TestPipeline() as p:
        result = (
            p
            | Create([glob])
            | ReadAllFromTFRecord(
                coder=coders.BytesCoder(),
                compression_type=CompressionTypes.AUTO))
        assert_that(result, equal_to([b'foo', b'bar'] * 3))

  def test_process_multiple_globs(self):
    with TempDir() as temp_dir:
      globs = []
      for i in range(3):
        suffix = 'result' + str(i)
        self._write_glob(temp_dir, suffix)
        globs.append(temp_dir.get_path() + os.path.sep + '*' + suffix)

      with TestPipeline() as p:
        result = (
            p
            | Create(globs)
            | ReadAllFromTFRecord(
                coder=coders.BytesCoder(),
                compression_type=CompressionTypes.AUTO))
        assert_that(result, equal_to([b'foo', b'bar'] * 9))

  def test_process_deflate(self):
    with TempDir() as temp_dir:
      path = temp_dir.create_temp_file('result')
      _write_file_deflate(path, FOO_BAR_RECORD_BASE64)
      with TestPipeline() as p:
        result = (
            p
            | Create([path])
            | ReadAllFromTFRecord(
                coder=coders.BytesCoder(),
                compression_type=CompressionTypes.DEFLATE))
        assert_that(result, equal_to([b'foo', b'bar']))

  def test_process_gzip(self):
    with TempDir() as temp_dir:
      path = temp_dir.create_temp_file('result')
      _write_file_gzip(path, FOO_BAR_RECORD_BASE64)
      with TestPipeline() as p:
        result = (
            p
            | Create([path])
            | ReadAllFromTFRecord(
                coder=coders.BytesCoder(),
                compression_type=CompressionTypes.GZIP))
        assert_that(result, equal_to([b'foo', b'bar']))

  def test_process_auto(self):
    with TempDir() as temp_dir:
      path = temp_dir.create_temp_file('result.gz')
      _write_file_gzip(path, FOO_BAR_RECORD_BASE64)
      with TestPipeline() as p:
        result = (
            p
            | Create([path])
            | ReadAllFromTFRecord(
                coder=coders.BytesCoder(),
                compression_type=CompressionTypes.AUTO))
        assert_that(result, equal_to([b'foo', b'bar']))


class TestEnd2EndWriteAndRead(unittest.TestCase):
  def create_inputs(self):
    input_array = [[random.random() - 0.5 for _ in range(15)]
                   for _ in range(12)]
    memfile = io.BytesIO()
    pickle.dump(input_array, memfile)
    return memfile.getvalue()

  def test_end2end(self):
    with TempDir() as temp_dir:
      file_path_prefix = temp_dir.create_temp_file('result')

      # Generate a TFRecord file.
      with TestPipeline() as p:
        expected_data = [self.create_inputs() for _ in range(0, 10)]
        _ = p | beam.Create(expected_data) | WriteToTFRecord(file_path_prefix)

      # Read the file back and compare.
      with TestPipeline() as p:
        actual_data = p | ReadFromTFRecord(file_path_prefix + '-*')
        assert_that(actual_data, equal_to(expected_data))

  def test_end2end_auto_compression(self):
    with TempDir() as temp_dir:
      file_path_prefix = temp_dir.create_temp_file('result')

      # Generate a TFRecord file.
      with TestPipeline() as p:
        expected_data = [self.create_inputs() for _ in range(0, 10)]
        _ = p | beam.Create(expected_data) | WriteToTFRecord(
            file_path_prefix, file_name_suffix='.gz')

      # Read the file back and compare.
      with TestPipeline() as p:
        actual_data = p | ReadFromTFRecord(file_path_prefix + '-*')
        assert_that(actual_data, equal_to(expected_data))

  def test_end2end_auto_compression_unsharded(self):
    with TempDir() as temp_dir:
      file_path_prefix = temp_dir.create_temp_file('result')

      # Generate a TFRecord file.
      with TestPipeline() as p:
        expected_data = [self.create_inputs() for _ in range(0, 10)]
        _ = p | beam.Create(expected_data) | WriteToTFRecord(
            file_path_prefix + '.gz', shard_name_template='')

      # Read the file back and compare.
      with TestPipeline() as p:
        actual_data = p | ReadFromTFRecord(file_path_prefix + '.gz')
        assert_that(actual_data, equal_to(expected_data))

  @unittest.skipIf(tf is None, 'tensorflow not installed.')
  def test_end2end_example_proto(self):
    with TempDir() as temp_dir:
      file_path_prefix = temp_dir.create_temp_file('result')

      example = tf.train.Example()
      example.features.feature['int'].int64_list.value.extend(list(range(3)))
      example.features.feature['bytes'].bytes_list.value.extend(
          [b'foo', b'bar'])

      with TestPipeline() as p:
        _ = p | beam.Create([example]) | WriteToTFRecord(
            file_path_prefix, coder=beam.coders.ProtoCoder(example.__class__))

      # Read the file back and compare.
      with TestPipeline() as p:
        actual_data = (
            p | ReadFromTFRecord(
                file_path_prefix + '-*',
                coder=beam.coders.ProtoCoder(example.__class__)))
        assert_that(actual_data, equal_to([example]))

  def test_end2end_read_write_read(self):
    with TempDir() as temp_dir:
      path = temp_dir.create_temp_file('result')
      with TestPipeline() as p:
        # Initial read to validate the pipeline doesn't fail before the file is
        # created.
        _ = p | ReadFromTFRecord(path + '-*', validate=False)
        expected_data = [self.create_inputs() for _ in range(0, 10)]
        _ = p | beam.Create(expected_data) | WriteToTFRecord(
            path, file_name_suffix='.gz')

      # Read the file back and compare.
      with TestPipeline() as p:
        actual_data = p | ReadFromTFRecord(path + '-*', validate=True)
        assert_that(actual_data, equal_to(expected_data))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
