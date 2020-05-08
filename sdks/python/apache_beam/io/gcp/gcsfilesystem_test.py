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

"""Unit tests for GCS File System."""

# pytype: skip-file

from __future__ import absolute_import

import logging
import unittest
from builtins import zip

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import
import mock

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import FileMetadata
from apache_beam.options.pipeline_options import PipelineOptions

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.io.gcp import gcsfilesystem
except ImportError:
  gcsfilesystem = None  # type: ignore
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(gcsfilesystem is None, 'GCP dependencies are not installed')
class GCSFileSystemTest(unittest.TestCase):
  def setUp(self):
    pipeline_options = PipelineOptions()
    self.fs = gcsfilesystem.GCSFileSystem(pipeline_options=pipeline_options)

  def test_scheme(self):
    self.assertEqual(self.fs.scheme(), 'gs')
    self.assertEqual(gcsfilesystem.GCSFileSystem.scheme(), 'gs')

  def test_join(self):
    self.assertEqual(
        'gs://bucket/path/to/file',
        self.fs.join('gs://bucket/path', 'to', 'file'))
    self.assertEqual(
        'gs://bucket/path/to/file', self.fs.join('gs://bucket/path', 'to/file'))
    self.assertEqual(
        'gs://bucket/path/to/file',
        self.fs.join('gs://bucket/path', '/to/file'))
    self.assertEqual(
        'gs://bucket/path/to/file',
        self.fs.join('gs://bucket/path/', 'to', 'file'))
    self.assertEqual(
        'gs://bucket/path/to/file',
        self.fs.join('gs://bucket/path/', 'to/file'))
    self.assertEqual(
        'gs://bucket/path/to/file',
        self.fs.join('gs://bucket/path/', '/to/file'))
    with self.assertRaises(ValueError):
      self.fs.join('/bucket/path/', '/to/file')

  def test_split(self):
    self.assertEqual(('gs://foo/bar', 'baz'), self.fs.split('gs://foo/bar/baz'))
    self.assertEqual(('gs://foo', ''), self.fs.split('gs://foo/'))
    self.assertEqual(('gs://foo', ''), self.fs.split('gs://foo'))

    with self.assertRaises(ValueError):
      self.fs.split('/no/gcs/prefix')

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_match_multiples(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    gcsio_mock.list_prefix.return_value = {
        'gs://bucket/file1': 1, 'gs://bucket/file2': 2
    }
    expected_results = set([
        FileMetadata('gs://bucket/file1', 1),
        FileMetadata('gs://bucket/file2', 2)
    ])
    match_result = self.fs.match(['gs://bucket/'])[0]
    self.assertEqual(set(match_result.metadata_list), expected_results)
    gcsio_mock.list_prefix.assert_called_once_with('gs://bucket/')

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_match_multiples_limit(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    limit = 1
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    gcsio_mock.list_prefix.return_value = {'gs://bucket/file1': 1}
    expected_results = set([FileMetadata('gs://bucket/file1', 1)])
    match_result = self.fs.match(['gs://bucket/'], [limit])[0]
    self.assertEqual(set(match_result.metadata_list), expected_results)
    self.assertEqual(len(match_result.metadata_list), limit)
    gcsio_mock.list_prefix.assert_called_once_with('gs://bucket/')

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_match_multiples_error(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    exception = IOError('Failed')
    gcsio_mock.list_prefix.side_effect = exception

    with self.assertRaisesRegex(BeamIOError,
                                r'^Match operation failed') as error:
      self.fs.match(['gs://bucket/'])
    self.assertRegex(
        str(error.exception.exception_details), r'gs://bucket/.*%s' % exception)
    gcsio_mock.list_prefix.assert_called_once_with('gs://bucket/')

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_match_multiple_patterns(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    gcsio_mock.list_prefix.side_effect = [
        {
            'gs://bucket/file1': 1
        },
        {
            'gs://bucket/file2': 2
        },
    ]
    expected_results = [[FileMetadata('gs://bucket/file1', 1)],
                        [FileMetadata('gs://bucket/file2', 2)]]
    result = self.fs.match(['gs://bucket/file1*', 'gs://bucket/file2*'])
    self.assertEqual([mr.metadata_list for mr in result], expected_results)

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_create(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    # Issue file copy
    _ = self.fs.create('gs://bucket/from1', 'application/octet-stream')

    gcsio_mock.open.assert_called_once_with(
        'gs://bucket/from1', 'wb', mime_type='application/octet-stream')

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_open(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    # Issue file copy
    _ = self.fs.open('gs://bucket/from1', 'application/octet-stream')

    gcsio_mock.open.assert_called_once_with(
        'gs://bucket/from1', 'rb', mime_type='application/octet-stream')

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_copy_file(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    sources = ['gs://bucket/from1']
    destinations = ['gs://bucket/to1']

    # Issue file copy
    self.fs.copy(sources, destinations)

    gcsio_mock.copy.assert_called_once_with(
        'gs://bucket/from1', 'gs://bucket/to1')

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_copy_file_error(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    sources = ['gs://bucket/from1']
    destinations = ['gs://bucket/to1']

    exception = IOError('Failed')
    gcsio_mock.copy.side_effect = exception

    # Issue batch rename.
    expected_results = {
        (s, d): exception
        for s, d in zip(sources, destinations)
    }

    # Issue batch copy.
    with self.assertRaisesRegex(BeamIOError,
                                r'^Copy operation failed') as error:
      self.fs.copy(sources, destinations)
    self.assertEqual(error.exception.exception_details, expected_results)

    gcsio_mock.copy.assert_called_once_with(
        'gs://bucket/from1', 'gs://bucket/to1')

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_copy_tree(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    sources = ['gs://bucket1/']
    destinations = ['gs://bucket2/']

    # Issue directory copy
    self.fs.copy(sources, destinations)

    gcsio_mock.copytree.assert_called_once_with(
        'gs://bucket1/', 'gs://bucket2/')

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_rename(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    sources = [
        'gs://bucket/from1',
        'gs://bucket/from2',
        'gs://bucket/from3',
    ]
    destinations = [
        'gs://bucket/to1',
        'gs://bucket/to2',
        'gs://bucket/to3',
    ]
    gcsio_mock.copy_batch.side_effect = [[
        ('gs://bucket/from1', 'gs://bucket/to1', None),
        ('gs://bucket/from2', 'gs://bucket/to2', None),
        ('gs://bucket/from3', 'gs://bucket/to3', None),
    ]]
    gcsio_mock.delete_batch.side_effect = [[
        ('gs://bucket/from1', None),
        ('gs://bucket/from2', None),
        ('gs://bucket/from3', None),
    ]]

    # Issue batch rename.
    self.fs.rename(sources, destinations)

    gcsio_mock.copy_batch.assert_called_once_with([
        ('gs://bucket/from1', 'gs://bucket/to1'),
        ('gs://bucket/from2', 'gs://bucket/to2'),
        ('gs://bucket/from3', 'gs://bucket/to3'),
    ])
    gcsio_mock.delete_batch.assert_called_once_with([
        'gs://bucket/from1',
        'gs://bucket/from2',
        'gs://bucket/from3',
    ])

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_rename_error(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    sources = [
        'gs://bucket/from1',
        'gs://bucket/from2',
        'gs://bucket/from3',
    ]
    destinations = [
        'gs://bucket/to1',
        'gs://bucket/to2',
        'gs://bucket/to3',
    ]
    exception = IOError('Failed')
    gcsio_mock.delete_batch.side_effect = [[(f, exception) for f in sources]]
    gcsio_mock.copy_batch.side_effect = [[
        ('gs://bucket/from1', 'gs://bucket/to1', None),
        ('gs://bucket/from2', 'gs://bucket/to2', None),
        ('gs://bucket/from3', 'gs://bucket/to3', None),
    ]]

    # Issue batch rename.
    expected_results = {
        (s, d): exception
        for s, d in zip(sources, destinations)
    }

    # Issue batch rename.
    with self.assertRaisesRegex(BeamIOError,
                                r'^Rename operation failed') as error:
      self.fs.rename(sources, destinations)
    self.assertEqual(error.exception.exception_details, expected_results)

    gcsio_mock.copy_batch.assert_called_once_with([
        ('gs://bucket/from1', 'gs://bucket/to1'),
        ('gs://bucket/from2', 'gs://bucket/to2'),
        ('gs://bucket/from3', 'gs://bucket/to3'),
    ])
    gcsio_mock.delete_batch.assert_called_once_with([
        'gs://bucket/from1',
        'gs://bucket/from2',
        'gs://bucket/from3',
    ])

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_delete(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    gcsio_mock.size.return_value = 0
    files = [
        'gs://bucket/from1',
        'gs://bucket/from2',
        'gs://bucket/from3',
    ]

    # Issue batch delete.
    self.fs.delete(files)
    gcsio_mock.delete_batch.assert_called()

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_delete_error(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    exception = IOError('Failed')
    gcsio_mock.delete_batch.side_effect = exception
    gcsio_mock.size.return_value = 0
    files = [
        'gs://bucket/from1',
        'gs://bucket/from2',
        'gs://bucket/from3',
    ]
    expected_results = {f: exception for f in files}

    # Issue batch delete.
    with self.assertRaisesRegex(BeamIOError,
                                r'^Delete operation failed') as error:
      self.fs.delete(files)
    self.assertEqual(error.exception.exception_details, expected_results)
    gcsio_mock.delete_batch.assert_called()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
