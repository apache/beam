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

"""Tests for HttpIO."""
# pytype: skip-file

from __future__ import absolute_import

import logging
import os
import random
import time
import unittest
import re

from apache_beam.io import httpio

TEST_DATA_PATH = "https://raw.githubusercontent.com/apache/beam/5ff5313f0913ec81d31ad306400ad30c0a928b34/NOTICE"
TEST_DATA_CONTENTS = """Apache Beam
Copyright 2016-2018 The Apache Software Foundation

This product includes software developed at
The Apache Software Foundation (http://www.apache.org/).

Based on source code originally developed by
Google (http://www.google.com/).

This product includes software developed at
Google (http://www.google.com/).
"""
TEST_DATA_SIZE = len(TEST_DATA_CONTENTS)

class FakeHttpClient:
  responses = {
    TEST_DATA_PATH: {
      "GET": {
        "status": 200,
        "body": TEST_DATA_CONTENTS
      }
    }
  }
  # def request(uri, method='GET', headers={}):
  #   if headers["Range"]:
  #     pass
  #   resp = {
  #     "Content-Length": len()
  #   }

class TestHttpIO(unittest.TestCase):

  def setUp(self):

    # These tests can be run locally against a mock httplib2 client, or as
    # integration tests against the real httplib2 client.
    self.USE_MOCK = False

    if self.USE_MOCK:
      self.httpio = httpio.HttpIO(client=FakeHttpClient())
    else:
      self.httpio = httpio.HttpIO()

  def test_size(self):
    self.assertEqual(TEST_DATA_SIZE, self.httpio.size(TEST_DATA_PATH))

  def test_file_read(self):
    f = self.httpio.open(TEST_DATA_PATH)
    self.assertEqual(f.mode, 'r')
    f.seek(0, os.SEEK_END)
    self.assertEqual(f.tell(), TEST_DATA_SIZE)
    self.assertEqual(f.read(), b'')
    f.seek(0)
    self.assertEqual(f.read(), TEST_DATA_CONTENTS.encode())


  # def test_file_mime_type(self):
  #   if self.USE_MOCK:
  #     self.skipTest("The boto3_client mock doesn't support mime_types")

  #   mime_type = 'example/example'
  #   file_name = self.TEST_DATA_PATH + 'write_file'
  #   f = self.aws.open(file_name, 'w', mime_type=mime_type)
  #   f.write(b'a string of binary text')
  #   f.close()

  #   bucket, key = s3io.parse_s3_path(file_name)
  #   metadata = self.client.get_object_metadata(messages.GetRequest(bucket, key))

  #   self.assertEqual(mime_type, metadata.mime_type)

  #   # Clean up
  #   self.aws.delete(file_name)

  # def test_file_random_seek(self):
  #   file_name = self.TEST_DATA_PATH + 'write_seek_file'
  #   file_size = 5 * 1024 * 1024 - 100
  #   contents = os.urandom(file_size)
  #   with self.aws.open(file_name, 'w') as wf:
  #     wf.write(contents)

  #   f = self.aws.open(file_name)
  #   random.seed(0)

  #   for _ in range(0, 10):
  #     a = random.randint(0, file_size - 1)
  #     b = random.randint(0, file_size - 1)
  #     start, end = min(a, b), max(a, b)
  #     f.seek(start)

  #     self.assertEqual(f.tell(), start)

  #     self.assertEqual(f.read(end - start + 1), contents[start:end + 1])
  #     self.assertEqual(f.tell(), end + 1)

  #   # Clean up
  #   self.aws.delete(file_name)

  # def test_file_iterator(self):
  #   file_name = self.TEST_DATA_PATH + 'iterate_file'
  #   lines = []
  #   line_count = 10
  #   for _ in range(line_count):
  #     line_length = random.randint(100, 500)
  #     line = os.urandom(line_length).replace(b'\n', b' ') + b'\n'
  #     lines.append(line)

  #   contents = b''.join(lines)

  #   with self.aws.open(file_name, 'w') as wf:
  #     wf.write(contents)

  #   f = self.aws.open(file_name)

  #   read_lines = 0
  #   for line in f:
  #     read_lines += 1

  #   self.assertEqual(read_lines, line_count)

  #   # Clean up
  #   self.aws.delete(file_name)

  # def test_file_read_line(self):
  #   file_name = self.TEST_DATA_PATH + 'read_line_file'
  #   lines = []

  #   # Set a small buffer size to exercise refilling the buffer.
  #   # First line is carefully crafted so the newline falls as the last character
  #   # of the buffer to exercise this code path.
  #   read_buffer_size = 1099
  #   lines.append(b'x' * 1023 + b'\n')

  #   for _ in range(1, 1000):
  #     line_length = random.randint(100, 500)
  #     line = os.urandom(line_length).replace(b'\n', b' ') + b'\n'
  #     lines.append(line)
  #   contents = b''.join(lines)

  #   file_size = len(contents)

  #   with self.aws.open(file_name, 'wb') as wf:
  #     wf.write(contents)

  #   f = self.aws.open(file_name, 'rb', read_buffer_size=read_buffer_size)

  #   # Test read of first two lines.
  #   f.seek(0)
  #   self.assertEqual(f.readline(), lines[0])
  #   self.assertEqual(f.tell(), len(lines[0]))
  #   self.assertEqual(f.readline(), lines[1])

  #   # Test read at line boundary.
  #   f.seek(file_size - len(lines[-1]) - 1)
  #   self.assertEqual(f.readline(), b'\n')

  #   # Test read at end of file.
  #   f.seek(file_size)
  #   self.assertEqual(f.readline(), b'')

  #   # Test reads at random positions.
  #   random.seed(0)
  #   for _ in range(0, 10):
  #     start = random.randint(0, file_size - 1)
  #     line_index = 0
  #     # Find line corresponding to start index.
  #     chars_left = start
  #     while True:
  #       next_line_length = len(lines[line_index])
  #       if chars_left - next_line_length < 0:
  #         break
  #       chars_left -= next_line_length
  #       line_index += 1
  #     f.seek(start)
  #     self.assertEqual(f.readline(), lines[line_index][chars_left:])

  #   # Clean up
  #   self.aws.delete(file_name)

  # def test_file_close(self):
  #   file_name = self.TEST_DATA_PATH + 'close_file'
  #   file_size = 5 * 1024 * 1024 + 2000
  #   contents = os.urandom(file_size)
  #   f = self.aws.open(file_name, 'w')
  #   self.assertEqual(f.mode, 'w')
  #   f.write(contents)
  #   f.close()
  #   f.close()  # This should not crash.

  #   with self.aws.open(file_name, 'r') as f:
  #     read_contents = f.read()

  #   self.assertEqual(read_contents, contents)

  #   # Clean up
  #   self.aws.delete(file_name)

  # def test_context_manager(self):
  #   # Test writing with a context manager.
  #   file_name = self.TEST_DATA_PATH + 'context_manager_file'
  #   file_size = 1024
  #   contents = os.urandom(file_size)
  #   with self.aws.open(file_name, 'w') as f:
  #     f.write(contents)

  #   with self.aws.open(file_name, 'r') as f:
  #     self.assertEqual(f.read(), contents)

  #   # Clean up
  #   self.aws.delete(file_name)

  # def test_list_prefix(self):

  #   objects = [
  #       ('jerry/pigpen/phil', 5),
  #       ('jerry/pigpen/bobby', 3),
  #       ('jerry/billy/bobby', 4),
  #   ]

  #   for (object_name, size) in objects:
  #     file_name = self.TEST_DATA_PATH + object_name
  #     self._insert_random_file(self.aws.client, file_name, size)

  #   test_cases = [
  #       (
  #           self.TEST_DATA_PATH + 'j',
  #           [
  #               ('jerry/pigpen/phil', 5),
  #               ('jerry/pigpen/bobby', 3),
  #               ('jerry/billy/bobby', 4),
  #           ]),
  #       (
  #           self.TEST_DATA_PATH + 'jerry/',
  #           [
  #               ('jerry/pigpen/phil', 5),
  #               ('jerry/pigpen/bobby', 3),
  #               ('jerry/billy/bobby', 4),
  #           ]),
  #       (
  #           self.TEST_DATA_PATH + 'jerry/pigpen/phil', [
  #               ('jerry/pigpen/phil', 5),
  #           ]),
  #   ]

  #   for file_pattern, expected_object_names in test_cases:
  #     expected_file_names = [(self.TEST_DATA_PATH + object_name, size)
  #                            for (object_name, size) in expected_object_names]
  #     self.assertEqual(
  #         set(self.aws.list_prefix(file_pattern).items()),
  #         set(expected_file_names))

  #   # Clean up
  #   for (object_name, size) in objects:
  #     self.aws.delete(self.TEST_DATA_PATH + object_name)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
