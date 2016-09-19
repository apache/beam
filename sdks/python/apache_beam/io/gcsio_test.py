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
"""Tests for Google Cloud Storage client."""

import logging
import multiprocessing
import os
import random
import threading
import unittest

import httplib2
from apitools.base.py.exceptions import HttpError
from apache_beam.internal.clients import storage

from apache_beam.io import gcsio
from mock import patch


class FakeGcsClient(object):
  # Fake storage client.  Usage in gcsio.py is client.objects.Get(...) and
  # client.objects.Insert(...).

  def __init__(self):
    self.objects = FakeGcsObjects()


class FakeFile(object):

  def __init__(self, bucket, obj, contents, generation):
    self.bucket = bucket
    self.object = obj
    self.contents = contents
    self.generation = generation

  def get_metadata(self):
    return storage.Object(
        bucket=self.bucket,
        name=self.object,
        generation=self.generation,
        size=len(self.contents))


class FakeGcsObjects(object):

  def __init__(self):
    self.files = {}
    # Store the last generation used for a given object name.  Note that this
    # has to persist even past the deletion of the object.
    self.last_generation = {}
    self.list_page_tokens = {}

  def add_file(self, f):
    self.files[(f.bucket, f.object)] = f
    self.last_generation[(f.bucket, f.object)] = f.generation

  def get_file(self, bucket, obj):
    return self.files.get((bucket, obj), None)

  def delete_file(self, bucket, obj):
    del self.files[(bucket, obj)]

  def get_last_generation(self, bucket, obj):
    return self.last_generation.get((bucket, obj), 0)

  def Get(self, get_request, download=None):  # pylint: disable=invalid-name
    f = self.get_file(get_request.bucket, get_request.object)
    if f is None:
      # Failing with a HTTP 404 if file does not exist.
      raise HttpError({'status': 404}, None, None)
    if download is None:
      return f.get_metadata()
    else:
      stream = download.stream

      def get_range_callback(start, end):
        assert start >= 0 and end >= start and end < len(f.contents)
        stream.write(f.contents[start:end + 1])

      download.GetRange = get_range_callback

  def Insert(self, insert_request, upload=None):  # pylint: disable=invalid-name
    assert upload is not None
    generation = self.get_last_generation(insert_request.bucket,
                                          insert_request.name) + 1
    f = FakeFile(insert_request.bucket, insert_request.name, '', generation)

    # Stream data into file.
    stream = upload.stream
    data_list = []
    while True:
      data = stream.read(1024 * 1024)
      if not data:
        break
      data_list.append(data)
    f.contents = ''.join(data_list)

    self.add_file(f)

  def Copy(self, copy_request):  # pylint: disable=invalid-name
    src_file = self.get_file(copy_request.sourceBucket,
                             copy_request.sourceObject)
    if not src_file:
      raise HttpError(
          httplib2.Response({'status': '404'}), '404 Not Found',
          'https://fake/url')
    generation = self.get_last_generation(copy_request.destinationBucket,
                                          copy_request.destinationObject) + 1
    dest_file = FakeFile(copy_request.destinationBucket,
                         copy_request.destinationObject, src_file.contents,
                         generation)
    self.add_file(dest_file)

  def Delete(self, delete_request):  # pylint: disable=invalid-name
    # Here, we emulate the behavior of the GCS service in raising a 404 error
    # if this object already exists.
    if self.get_file(delete_request.bucket, delete_request.object):
      self.delete_file(delete_request.bucket, delete_request.object)
    else:
      raise HttpError(
          httplib2.Response({'status': '404'}), '404 Not Found',
          'https://fake/url')

  def List(self, list_request):  # pylint: disable=invalid-name
    bucket = list_request.bucket
    prefix = list_request.prefix or ''
    matching_files = []
    for file_bucket, file_name in sorted(iter(self.files)):
      if bucket == file_bucket and file_name.startswith(prefix):
        file_object = self.files[(file_bucket, file_name)].get_metadata()
        matching_files.append(file_object)

    # Handle pagination.
    items_per_page = 5
    if not list_request.pageToken:
      range_start = 0
    else:
      if list_request.pageToken not in self.list_page_tokens:
        raise ValueError('Invalid page token.')
      range_start = self.list_page_tokens[list_request.pageToken]
      del self.list_page_tokens[list_request.pageToken]

    result = storage.Objects(
        items=matching_files[range_start:range_start + items_per_page])
    if range_start + items_per_page < len(matching_files):
      next_range_start = range_start + items_per_page
      next_page_token = '_page_token_%s_%s_%d' % (bucket, prefix,
                                                  next_range_start)
      self.list_page_tokens[next_page_token] = next_range_start
      result.nextPageToken = next_page_token
    return result


class TestGCSPathParser(unittest.TestCase):

  def test_gcs_path(self):
    self.assertEqual(
        gcsio.parse_gcs_path('gs://bucket/name'), ('bucket', 'name'))
    self.assertEqual(
        gcsio.parse_gcs_path('gs://bucket/name/sub'), ('bucket', 'name/sub'))

  def test_bad_gcs_path(self):
    self.assertRaises(ValueError, gcsio.parse_gcs_path, 'gs://')
    self.assertRaises(ValueError, gcsio.parse_gcs_path, 'gs://bucket')
    self.assertRaises(ValueError, gcsio.parse_gcs_path, 'gs://bucket/')
    self.assertRaises(ValueError, gcsio.parse_gcs_path, 'gs:///name')
    self.assertRaises(ValueError, gcsio.parse_gcs_path, 'gs:///')
    self.assertRaises(ValueError, gcsio.parse_gcs_path, 'gs:/blah/bucket/name')


class TestGCSIO(unittest.TestCase):

  def _insert_random_file(self, client, path, size, generation=1):
    bucket, name = gcsio.parse_gcs_path(path)
    f = FakeFile(bucket, name, os.urandom(size), generation)
    client.objects.add_file(f)
    return f

  def setUp(self):
    self.client = FakeGcsClient()
    self.gcs = gcsio.GcsIO(self.client)

  def test_exists(self):
    file_name = 'gs://gcsio-test/dummy_file'
    file_size = 1234
    self._insert_random_file(self.client, file_name, file_size)
    self.assertFalse(self.gcs.exists(file_name + 'xyz'))
    self.assertTrue(self.gcs.exists(file_name))

  @patch.object(FakeGcsObjects, 'Get')
  def test_exists_failure(self, mock_get):
    # Raising an error other than 404. Raising 404 is a valid failure for
    # exists() call.
    mock_get.side_effect = HttpError({'status': 400}, None, None)
    file_name = 'gs://gcsio-test/dummy_file'
    file_size = 1234
    self._insert_random_file(self.client, file_name, file_size)
    with self.assertRaises(HttpError) as cm:
      self.gcs.exists(file_name)
    self.assertEquals(400, cm.exception.status_code)

  def test_size(self):
    file_name = 'gs://gcsio-test/dummy_file'
    file_size = 1234

    self._insert_random_file(self.client, file_name, file_size)
    self.assertTrue(self.gcs.exists(file_name))
    self.assertEqual(1234, self.gcs.size(file_name))

  def test_file_mode(self):
    file_name = 'gs://gcsio-test/dummy_mode_file'
    with self.gcs.open(file_name, 'wb') as f:
      assert f.mode == 'wb'
    with self.gcs.open(file_name, 'rb') as f:
      assert f.mode == 'rb'

  def test_bad_file_modes(self):
    file_name = 'gs://gcsio-test/dummy_mode_file'
    with self.assertRaises(ValueError):
      self.gcs.open(file_name, 'w+')
    with self.assertRaises(ValueError):
      self.gcs.open(file_name, 'r+b')

  def test_delete(self):
    file_name = 'gs://gcsio-test/delete_me'
    file_size = 1024

    # Test deletion of non-existent file.
    self.gcs.delete(file_name)

    self._insert_random_file(self.client, file_name, file_size)
    self.assertTrue(
        gcsio.parse_gcs_path(file_name) in self.client.objects.files)

    self.gcs.delete(file_name)

    self.assertFalse(
        gcsio.parse_gcs_path(file_name) in self.client.objects.files)

  def test_copy(self):
    src_file_name = 'gs://gcsio-test/source'
    dest_file_name = 'gs://gcsio-test/dest'
    file_size = 1024
    self._insert_random_file(self.client, src_file_name, file_size)
    self.assertTrue(
        gcsio.parse_gcs_path(src_file_name) in self.client.objects.files)
    self.assertFalse(
        gcsio.parse_gcs_path(dest_file_name) in self.client.objects.files)

    self.gcs.copy(src_file_name, dest_file_name)

    self.assertTrue(
        gcsio.parse_gcs_path(src_file_name) in self.client.objects.files)
    self.assertTrue(
        gcsio.parse_gcs_path(dest_file_name) in self.client.objects.files)

    self.assertRaises(IOError, self.gcs.copy, 'gs://gcsio-test/non-existent',
                      'gs://gcsio-test/non-existent-destination')

  def test_copytree(self):
    src_dir_name = 'gs://gcsio-test/source/'
    dest_dir_name = 'gs://gcsio-test/dest/'
    file_size = 1024
    paths = ['a', 'b/c', 'b/d']
    for path in paths:
      src_file_name = src_dir_name + path
      dest_file_name = dest_dir_name + path
      self._insert_random_file(self.client, src_file_name, file_size)
      self.assertTrue(
          gcsio.parse_gcs_path(src_file_name) in self.client.objects.files)
      self.assertFalse(
          gcsio.parse_gcs_path(dest_file_name) in self.client.objects.files)

    self.gcs.copytree(src_dir_name, dest_dir_name)

    for path in paths:
      src_file_name = src_dir_name + path
      dest_file_name = dest_dir_name + path
      self.assertTrue(
          gcsio.parse_gcs_path(src_file_name) in self.client.objects.files)
      self.assertTrue(
          gcsio.parse_gcs_path(dest_file_name) in self.client.objects.files)

  def test_rename(self):
    src_file_name = 'gs://gcsio-test/source'
    dest_file_name = 'gs://gcsio-test/dest'
    file_size = 1024
    self._insert_random_file(self.client, src_file_name, file_size)
    self.assertTrue(
        gcsio.parse_gcs_path(src_file_name) in self.client.objects.files)
    self.assertFalse(
        gcsio.parse_gcs_path(dest_file_name) in self.client.objects.files)

    self.gcs.rename(src_file_name, dest_file_name)

    self.assertFalse(
        gcsio.parse_gcs_path(src_file_name) in self.client.objects.files)
    self.assertTrue(
        gcsio.parse_gcs_path(dest_file_name) in self.client.objects.files)

  def test_full_file_read(self):
    file_name = 'gs://gcsio-test/full_file'
    file_size = 5 * 1024 * 1024 + 100
    random_file = self._insert_random_file(self.client, file_name, file_size)
    f = self.gcs.open(file_name)
    self.assertEqual(f.mode, 'r')
    f.seek(0, os.SEEK_END)
    self.assertEqual(f.tell(), file_size)
    self.assertEqual(f.read(), '')
    f.seek(0)
    self.assertEqual(f.read(), random_file.contents)

  def test_file_random_seek(self):
    file_name = 'gs://gcsio-test/seek_file'
    file_size = 5 * 1024 * 1024 - 100
    random_file = self._insert_random_file(self.client, file_name, file_size)

    f = self.gcs.open(file_name)
    random.seed(0)
    for _ in range(0, 10):
      a = random.randint(0, file_size - 1)
      b = random.randint(0, file_size - 1)
      start, end = min(a, b), max(a, b)
      f.seek(start)
      self.assertEqual(f.tell(), start)
      self.assertEqual(
          f.read(end - start + 1), random_file.contents[start:end + 1])
      self.assertEqual(f.tell(), end + 1)

  def test_file_read_line(self):
    file_name = 'gs://gcsio-test/read_line_file'
    lines = []

    # Set a small buffer size to exercise refilling the buffer.
    # First line is carefully crafted so the newline falls as the last character
    # of the buffer to exercise this code path.
    read_buffer_size = 1024
    lines.append('x' * 1023 + '\n')

    for _ in range(1, 1000):
      line_length = random.randint(100, 500)
      line = os.urandom(line_length).replace('\n', ' ') + '\n'
      lines.append(line)
    contents = ''.join(lines)

    file_size = len(contents)
    bucket, name = gcsio.parse_gcs_path(file_name)
    self.client.objects.add_file(FakeFile(bucket, name, contents, 1))

    f = self.gcs.open(file_name, read_buffer_size=read_buffer_size)

    # Test read of first two lines.
    f.seek(0)
    self.assertEqual(f.readline(), lines[0])
    self.assertEqual(f.tell(), len(lines[0]))
    self.assertEqual(f.readline(), lines[1])

    # Test read at line boundary.
    f.seek(file_size - len(lines[-1]) - 1)
    self.assertEqual(f.readline(), '\n')

    # Test read at end of file.
    f.seek(file_size)
    self.assertEqual(f.readline(), '')

    # Test reads at random positions.
    random.seed(0)
    for _ in range(0, 10):
      start = random.randint(0, file_size - 1)
      line_index = 0
      # Find line corresponding to start index.
      chars_left = start
      while True:
        next_line_length = len(lines[line_index])
        if chars_left - next_line_length < 0:
          break
        chars_left -= next_line_length
        line_index += 1
      f.seek(start)
      self.assertEqual(f.readline(), lines[line_index][chars_left:])

  def test_file_write(self):
    file_name = 'gs://gcsio-test/write_file'
    file_size = 5 * 1024 * 1024 + 2000
    contents = os.urandom(file_size)
    f = self.gcs.open(file_name, 'w')
    self.assertEqual(f.mode, 'w')
    f.write(contents[0:1000])
    f.write(contents[1000:1024 * 1024])
    f.write(contents[1024 * 1024:])
    f.close()
    bucket, name = gcsio.parse_gcs_path(file_name)
    self.assertEqual(
        self.client.objects.get_file(bucket, name).contents, contents)

  def test_file_close(self):
    file_name = 'gs://gcsio-test/close_file'
    file_size = 5 * 1024 * 1024 + 2000
    contents = os.urandom(file_size)
    f = self.gcs.open(file_name, 'w')
    self.assertEqual(f.mode, 'w')
    f.write(contents)
    f.close()
    f.close()  # This should not crash.
    bucket, name = gcsio.parse_gcs_path(file_name)
    self.assertEqual(
        self.client.objects.get_file(bucket, name).contents, contents)

  def test_file_flush(self):
    file_name = 'gs://gcsio-test/flush_file'
    file_size = 5 * 1024 * 1024 + 2000
    contents = os.urandom(file_size)
    bucket, name = gcsio.parse_gcs_path(file_name)
    f = self.gcs.open(file_name, 'w')
    self.assertEqual(f.mode, 'w')
    f.write(contents[0:1000])
    f.flush()
    f.write(contents[1000:1024 * 1024])
    f.flush()
    f.flush()  # Should be a NOOP.
    f.write(contents[1024 * 1024:])
    f.close()  # This should already call the equivalent of flush() in its body.
    self.assertEqual(
        self.client.objects.get_file(bucket, name).contents, contents)

  def test_context_manager(self):
    # Test writing with a context manager.
    file_name = 'gs://gcsio-test/context_manager_file'
    file_size = 1024
    contents = os.urandom(file_size)
    with self.gcs.open(file_name, 'w') as f:
      f.write(contents)
    bucket, name = gcsio.parse_gcs_path(file_name)
    self.assertEqual(
        self.client.objects.get_file(bucket, name).contents, contents)

    # Test reading with a context manager.
    with self.gcs.open(file_name) as f:
      self.assertEqual(f.read(), contents)

    # Test that exceptions are not swallowed by the context manager.
    with self.assertRaises(ZeroDivisionError):
      with self.gcs.open(file_name) as f:
        f.read(0 / 0)

  def test_glob(self):
    bucket_name = 'gcsio-test'
    object_names = [
        'cow/cat/fish',
        'cow/cat/blubber',
        'cow/dog/blubber',
        'apple/dog/blubber',
        'apple/fish/blubber',
        'apple/fish/blowfish',
        'apple/fish/bambi',
        'apple/fish/balloon',
        'apple/fish/cat',
        'apple/fish/cart',
        'apple/fish/carl',
        'apple/dish/bat',
        'apple/dish/cat',
        'apple/dish/carl',
    ]
    for object_name in object_names:
      file_name = 'gs://%s/%s' % (bucket_name, object_name)
      self._insert_random_file(self.client, file_name, 0)
    test_cases = [
        ('gs://gcsio-test/*', [
            'cow/cat/fish',
            'cow/cat/blubber',
            'cow/dog/blubber',
            'apple/dog/blubber',
            'apple/fish/blubber',
            'apple/fish/blowfish',
            'apple/fish/bambi',
            'apple/fish/balloon',
            'apple/fish/cat',
            'apple/fish/cart',
            'apple/fish/carl',
            'apple/dish/bat',
            'apple/dish/cat',
            'apple/dish/carl',
        ]),
        ('gs://gcsio-test/cow/*', [
            'cow/cat/fish',
            'cow/cat/blubber',
            'cow/dog/blubber',
        ]),
        ('gs://gcsio-test/cow/ca*', [
            'cow/cat/fish',
            'cow/cat/blubber',
        ]),
        ('gs://gcsio-test/apple/[df]ish/ca*', [
            'apple/fish/cat',
            'apple/fish/cart',
            'apple/fish/carl',
            'apple/dish/cat',
            'apple/dish/carl',
        ]),
        ('gs://gcsio-test/apple/fish/car?', [
            'apple/fish/cart',
            'apple/fish/carl',
        ]),
        ('gs://gcsio-test/apple/fish/b*', [
            'apple/fish/blubber',
            'apple/fish/blowfish',
            'apple/fish/bambi',
            'apple/fish/balloon',
        ]),
        ('gs://gcsio-test/apple/dish/[cb]at', [
            'apple/dish/bat',
            'apple/dish/cat',
        ]),
    ]
    for file_pattern, expected_object_names in test_cases:
      expected_file_names = ['gs://%s/%s' % (bucket_name, o)
                             for o in expected_object_names]
      self.assertEqual(
          set(self.gcs.glob(file_pattern)), set(expected_file_names))


class TestPipeStream(unittest.TestCase):

  def _read_and_verify(self, stream, expected, buffer_size):
    data_list = []
    bytes_read = 0
    seen_last_block = False
    while True:
      data = stream.read(buffer_size)
      self.assertLessEqual(len(data), buffer_size)
      if len(data) < buffer_size:
        # Test the constraint that the pipe stream returns less than the buffer
        # size only when at the end of the stream.
        if data:
          self.assertFalse(seen_last_block)
        seen_last_block = True
      if not data:
        break
      data_list.append(data)
      bytes_read += len(data)
      self.assertEqual(stream.tell(), bytes_read)
    self.assertEqual(''.join(data_list), expected)

  def test_pipe_stream(self):
    block_sizes = list(4**i for i in range(0, 12))
    data_blocks = list(os.urandom(size) for size in block_sizes)
    expected = ''.join(data_blocks)

    buffer_sizes = [100001, 512 * 1024, 1024 * 1024]

    for buffer_size in buffer_sizes:
      parent_conn, child_conn = multiprocessing.Pipe()
      stream = gcsio.GcsBufferedWriter.PipeStream(child_conn)
      child_thread = threading.Thread(
          target=self._read_and_verify, args=(stream, expected, buffer_size))
      child_thread.start()
      for data in data_blocks:
        parent_conn.send_bytes(data)
      parent_conn.close()
      child_thread.join()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
