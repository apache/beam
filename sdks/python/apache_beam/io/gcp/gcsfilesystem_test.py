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

import unittest

import mock

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import FileMetadata

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.io.gcp import gcsfilesystem
except ImportError:
  gcsfilesystem = None
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(gcsfilesystem is None, 'GCP dependencies are not installed')
class GCSFileSystemTest(unittest.TestCase):

  def test_scheme(self):
    file_system = gcsfilesystem.GCSFileSystem()
    self.assertEqual(file_system.scheme(), 'gs')
    self.assertEqual(gcsfilesystem.GCSFileSystem.scheme(), 'gs')

  def test_join(self):
    file_system = gcsfilesystem.GCSFileSystem()
    self.assertEqual('gs://bucket/path/to/file',
                     file_system.join('gs://bucket/path', 'to', 'file'))
    self.assertEqual('gs://bucket/path/to/file',
                     file_system.join('gs://bucket/path', 'to/file'))
    self.assertEqual('gs://bucket/path/to/file',
                     file_system.join('gs://bucket/path', '/to/file'))
    self.assertEqual('gs://bucket/path/to/file',
                     file_system.join('gs://bucket/path/', 'to', 'file'))
    self.assertEqual('gs://bucket/path/to/file',
                     file_system.join('gs://bucket/path/', 'to/file'))
    self.assertEqual('gs://bucket/path/to/file',
                     file_system.join('gs://bucket/path/', '/to/file'))
    with self.assertRaises(ValueError):
      file_system.join('/bucket/path/', '/to/file')

  def test_split(self):
    file_system = gcsfilesystem.GCSFileSystem()
    self.assertEqual(('gs://foo/bar', 'baz'),
                     file_system.split('gs://foo/bar/baz'))
    self.assertEqual(('gs://foo', ''),
                     file_system.split('gs://foo/'))
    self.assertEqual(('gs://foo', ''),
                     file_system.split('gs://foo'))

    with self.assertRaises(ValueError):
      file_system.split('/no/gcs/prefix')

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_match_multiples(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    gcsio_mock.size_of_files_in_glob.return_value = {
        'gs://bucket/file1': 1,
        'gs://bucket/file2': 2
    }
    expected_results = set([
        FileMetadata('gs://bucket/file1', 1),
        FileMetadata('gs://bucket/file2', 2)
    ])
    file_system = gcsfilesystem.GCSFileSystem()
    match_result = file_system.match(['gs://bucket/'])[0]
    self.assertEqual(
        set(match_result.metadata_list),
        expected_results)
    gcsio_mock.size_of_files_in_glob.assert_called_once_with(
        'gs://bucket/*', None)

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_match_multiples_limit(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    limit = 1
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    gcsio_mock.size_of_files_in_glob.return_value = {
        'gs://bucket/file1': 1
    }
    expected_results = set([
        FileMetadata('gs://bucket/file1', 1)
    ])
    file_system = gcsfilesystem.GCSFileSystem()
    match_result = file_system.match(['gs://bucket/'], [limit])[0]
    self.assertEqual(
        set(match_result.metadata_list),
        expected_results)
    self.assertEqual(
        len(match_result.metadata_list),
        limit)
    gcsio_mock.size_of_files_in_glob.assert_called_once_with(
        'gs://bucket/*', 1)

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_match_multiples_error(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    exception = IOError('Failed')
    gcsio_mock.size_of_files_in_glob.side_effect = exception
    expected_results = {'gs://bucket/': exception}

    file_system = gcsfilesystem.GCSFileSystem()
    with self.assertRaises(BeamIOError) as error:
      file_system.match(['gs://bucket/'])
    self.assertTrue(
        error.exception.message.startswith('Match operation failed'))
    self.assertEqual(error.exception.exception_details, expected_results)
    gcsio_mock.size_of_files_in_glob.assert_called_once_with(
        'gs://bucket/*', None)

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_match_multiple_patterns(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    gcsio_mock.size_of_files_in_glob.side_effect = [
        {'gs://bucket/file1': 1},
        {'gs://bucket/file2': 2},
    ]
    expected_results = [
        [FileMetadata('gs://bucket/file1', 1)],
        [FileMetadata('gs://bucket/file2', 2)]
    ]
    file_system = gcsfilesystem.GCSFileSystem()
    result = file_system.match(['gs://bucket/file1*', 'gs://bucket/file2*'])
    self.assertEqual(
        [mr.metadata_list for mr in result],
        expected_results)

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_create(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    # Issue file copy
    file_system = gcsfilesystem.GCSFileSystem()
    _ = file_system.create('gs://bucket/from1', 'application/octet-stream')

    gcsio_mock.open.assert_called_once_with(
        'gs://bucket/from1', 'wb', mime_type='application/octet-stream')

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_open(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    # Issue file copy
    file_system = gcsfilesystem.GCSFileSystem()
    _ = file_system.open('gs://bucket/from1', 'application/octet-stream')

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
    file_system = gcsfilesystem.GCSFileSystem()
    file_system.copy(sources, destinations)

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
    expected_results = {(s, d):exception for s, d in zip(sources, destinations)}

    # Issue batch copy.
    file_system = gcsfilesystem.GCSFileSystem()
    with self.assertRaises(BeamIOError) as error:
      file_system.copy(sources, destinations)
    self.assertTrue(
        error.exception.message.startswith('Copy operation failed'))
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
    file_system = gcsfilesystem.GCSFileSystem()
    file_system.copy(sources, destinations)

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
    file_system = gcsfilesystem.GCSFileSystem()
    file_system.rename(sources, destinations)

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
    expected_results = {(s, d):exception for s, d in zip(sources, destinations)}

    # Issue batch rename.
    file_system = gcsfilesystem.GCSFileSystem()
    with self.assertRaises(BeamIOError) as error:
      file_system.rename(sources, destinations)
    self.assertTrue(
        error.exception.message.startswith('Rename operation failed'))
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
    files = [
        'gs://bucket/from1',
        'gs://bucket/from2',
        'gs://bucket/from3',
    ]

    # Issue batch delete.
    file_system = gcsfilesystem.GCSFileSystem()
    file_system.delete(files)
    gcsio_mock.delete_batch.assert_called()

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_delete_error(self, mock_gcsio):
    # Prepare mocks.
    gcsio_mock = mock.MagicMock()
    gcsfilesystem.gcsio.GcsIO = lambda: gcsio_mock
    exception = IOError('Failed')
    gcsio_mock.delete_batch.side_effect = exception
    files = [
        'gs://bucket/from1',
        'gs://bucket/from2',
        'gs://bucket/from3',
    ]
    expected_results = {f:exception for f in files}

    # Issue batch delete.
    file_system = gcsfilesystem.GCSFileSystem()
    with self.assertRaises(BeamIOError) as error:
      file_system.delete(files)
    self.assertTrue(
        error.exception.message.startswith('Delete operation failed'))
    self.assertEqual(error.exception.exception_details, expected_results)
    gcsio_mock.delete_batch.assert_called()
