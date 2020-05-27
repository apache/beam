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
from httplib2 import Response

from apache_beam.io import httpio

# Tests in TestHttpIOSuccess can be run locally against a mock
# httplib2 client, or as integration tests against the real
# httplib2 client.
USE_MOCK = True

# Test paths and results used in mocks. TEST_DATA_PATH will be the actual
# URL that is queried when mocks are turned off.
TEST_DATA_PATH = "https://raw.githubusercontent.com/apache/beam/5ff5313f0913ec81d31ad306400ad30c0a928b34/NOTICE"
TEST_DATA_CONTENTS = b"""Apache Beam
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
  def __init__(self, success=True):
    self._success = success

  def request(self, uri, method, headers = {}):
    resp = Response({"content-length": TEST_DATA_SIZE})
    if uri != TEST_DATA_PATH:
      resp.status = 404
      resp.reason = "Not Found"
    elif self._success:
      resp.status = 200
      resp.reason = "Ok"
    else:
      resp.status = 500
      resp.reason = "Internal Server Error"
    return resp, TEST_DATA_CONTENTS


class TestHttpIOSuccess(unittest.TestCase):
  def setUp(self):
    self.httpio = httpio.HttpIO(client=FakeHttpClient()) if USE_MOCK else httpio.HttpIO()

  def test_size(self):
    self.assertEqual(TEST_DATA_SIZE, self.httpio.size(TEST_DATA_PATH))

  def test_file_read(self):
    f = self.httpio.open(TEST_DATA_PATH)
    self.assertEqual(f.mode, 'r')
    f.seek(0, os.SEEK_END)
    self.assertEqual(f.tell(), TEST_DATA_SIZE)
    self.assertEqual(f.read(), b'')
    f.seek(0)
    self.assertEqual(f.read(), TEST_DATA_CONTENTS)

  def test_file_random_seek(self):
    file_name = TEST_DATA_PATH
    file_size = TEST_DATA_SIZE
    contents = TEST_DATA_CONTENTS

    f = self.httpio.open(file_name)
    random.seed(0)

    for _ in range(0, 10):
      a = random.randint(0, file_size - 1)
      b = random.randint(0, file_size - 1)
      start, end = min(a, b), max(a, b)
      f.seek(start)

      self.assertEqual(f.tell(), start)

      self.assertEqual(f.read(end - start + 1), contents[start:end + 1])
      self.assertEqual(f.tell(), end + 1)

  def test_file_iterator(self):
    file_name = TEST_DATA_PATH
    contents = TEST_DATA_CONTENTS

    f = self.httpio.open(file_name)

    read_lines = 0
    for line in f:
      read_lines += 1

    self.assertEqual(read_lines, len(contents.split(b"\n")) - 1)

  def test_file_read_line(self):
    file_name = TEST_DATA_PATH
    file_size = TEST_DATA_SIZE
    lines = [line + b"\n" for line in TEST_DATA_CONTENTS.split(b"\n")]

    f = self.httpio.open(file_name)

    # Test read of first two lines.
    f.seek(0)
    self.assertEqual(f.readline(), lines[0])
    self.assertEqual(f.tell(), len(lines[0]))
    self.assertEqual(f.readline(), lines[1])

    # Test read at line boundary.
    f.seek(file_size - len(lines[-1]) - 1)
    self.assertEqual(f.readline(), b'.\n')

    # Test read at end of file.
    f.seek(file_size)
    self.assertEqual(f.readline(), b'')

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

  def test_list_prefix(self):
    expected_file_names = [
      (TEST_DATA_PATH, TEST_DATA_SIZE),
    ]
    self.assertEqual(
        set(self.httpio.list_prefix(TEST_DATA_PATH).items()),
        set(expected_file_names))

  def test_exists(self):
    self.assertTrue(self.httpio.exists(TEST_DATA_PATH))
    self.assertFalse(self.httpio.exists(TEST_DATA_PATH + "/invalid"))


class TestHttpIOFailure(unittest.TestCase):
  def setUp(self):
    self.httpio = httpio.HttpIO(client=FakeHttpClient(success=False))

  def test_size(self):
    with self.assertRaisesRegex(Exception, '500'):
      self.httpio.size(TEST_DATA_PATH)

  def test_file_read(self):
    with self.assertRaisesRegex(Exception, '500'):
      f = self.httpio.open(TEST_DATA_PATH)
  
  def test_file_list_prefix(self):
    with self.assertRaisesRegex(Exception, '500'):
      self.httpio.list_prefix(TEST_DATA_PATH).items()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
