# -*- coding: utf-8 -*-
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

"""Unit tests for the S3 File System"""

# pytype: skip-file

from __future__ import absolute_import

import logging
import unittest

import mock

from apache_beam.io.aws.clients.s3 import messages
from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import FileMetadata
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import httpfilesystem


class HttpFileSystemTest(unittest.TestCase):
  def setUp(self):
    pipeline_options = PipelineOptions()
    self.fs = httpfilesystem.HttpFileSystem(pipeline_options=pipeline_options)

  def test_scheme(self):
    self.assertEqual(self.fs.scheme(), 'http')
    self.assertEqual(httpfilesystem.HttpFileSystem.scheme(), 'http')

  def test_join(self):
    self.assertEqual(
        'http://example.com/path/to/file',
        self.fs.join('http://example.com/path', 'to', 'file'))
    self.assertEqual(
        'http://example.com/path/to/file', self.fs.join('http://example.com/path', 'to/file'))
    self.assertEqual(
        'http://example.com/path/to/file',
        self.fs.join('http://example.com/path', '/to/file'))
    self.assertEqual(
        'http://example.com/path/to/file',
        self.fs.join('http://example.com/path/', 'to', 'file'))
    self.assertEqual(
        'http://example.com/path/to/file',
        self.fs.join('http://example.com/path/', 'to/file'))
    self.assertEqual(
        'http://example.com/path/to/file',
        self.fs.join('http://example.com/path/', '/to/file'))
    with self.assertRaises(ValueError):
      self.fs.join('/example.com/path/', '/to/file')

  def test_split(self):
    self.assertEqual(('http://foo.com/bar', 'baz'), self.fs.split('http://foo.com/bar/baz'))
    self.assertEqual(('http://foo.com', ''), self.fs.split('http://foo.com/'))
    self.assertEqual(('http://foo.com', ''), self.fs.split('http://foo.com'))

    with self.assertRaises(ValueError):
      self.fs.split('/no/prefix')

  @mock.patch('apache_beam.io.httpfilesystem.HttpFileSystem')
  def test_match_multiples(self, unused_mock_arg):
    # Prepare mocks.
    httpio_mock = mock.MagicMock()
    httpfilesystem.httpio.HttpIO = lambda: httpio_mock  # type: ignore[misc]
    httpio_mock.size.return_value = 1
    expected_results = set([
        FileMetadata('http://example.com/file1', 1)
    ])
    match_result = self.fs.match(['http://example.com/file1'])[0]

    self.assertEqual(set(match_result.metadata_list), expected_results)
    httpio_mock.size.assert_called_once_with('http://example.com/file1')
    httpio_mock.list_prefix.assert_not_called()

  @mock.patch('apache_beam.io.httpfilesystem.HttpFileSystem')
  def test_match_multiples_error(self, unused_mock_arg):
    # Prepare mocks.
    httpio_mock = mock.MagicMock()
    httpfilesystem.httpio.HttpIO = lambda: httpio_mock  # type: ignore[misc]
    exception = IOError('Failed')
    httpio_mock.list_prefix.side_effect = exception

    with self.assertRaises(BeamIOError) as error:
      self.fs.match(['http://example.com/'])

    self.assertIn('Match operation failed', str(error.exception))
    httpio_mock.list_prefix.assert_called_once_with('http://example.com/')

  @mock.patch('apache_beam.io.httpfilesystem.HttpFileSystem')
  def test_open(self, unused_mock_arg):
    # Prepare mocks.
    httpio_mock = mock.MagicMock()
    httpfilesystem.httpio.HttpIO = lambda: httpio_mock  # type: ignore[misc]
    # Issue file copy
    _ = self.fs.open('http://example.com/from1', 'application/octet-stream')

    httpio_mock.open.assert_called_once_with(
        'http://example.com/from1', 'rb')


class HttpsFileSystemTest(unittest.TestCase):
  """There's need to exhaustively test all cases here because
  HttpsFileSystemTest is just like HttpFileSystem, but with
  a different scheme.
  """
  
  def setUp(self):
    pipeline_options = PipelineOptions()
    self.fs = httpfilesystem.HttpsFileSystem(pipeline_options=pipeline_options)

  def test_scheme(self):
    self.assertEqual(self.fs.scheme(), 'https')
    self.assertEqual(httpfilesystem.HttpsFileSystem.scheme(), 'https')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
