# -*- coding: utf-8 -*-
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

"""Unit tests for Azure Blob Storage File System."""

# pytype: skip-file

from __future__ import absolute_import

import logging
import unittest

# patches unittest.TestCase to be python3 compatible.
import future.tests.base  # pylint: disable=unused-import
import mock

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import FileMetadata
from apache_beam.options.pipeline_options import PipelineOptions

# Protect against environments where azure library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.io.azure import blobstorageio
  from apache_beam.io.azure import blobstoragefilesystem
except ImportError:
  blobstoragefilesystem = None  # type: ignore[assignment]
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(
    blobstoragefilesystem is None, 'Azure dependencies are not installed')
class BlobStorageFileSystemTest(unittest.TestCase):
  def setUp(self):
    pipeline_options = PipelineOptions()
    mock_client = mock.MagicMock()
    self.fs = blobstoragefilesystem.BlobStorageFileSystem(
        pipeline_options=pipeline_options, client=mock_client)

  def test_scheme(self):
    self.assertEqual(self.fs.scheme(), 'azfs')
    self.assertEqual(
        blobstoragefilesystem.BlobStorageFileSystem.scheme(), 'azfs')

  def test_join(self):
    self.assertEqual(
        'azfs://account-name/container/path/to/file',
        self.fs.join('azfs://account-name/container/path', 'to', 'file'))
    self.assertEqual(
        'azfs://account-name/container/path/to/file',
        self.fs.join('azfs://account-name/container/path', 'to/file'))
    self.assertEqual(
        'azfs://account-name/container/path/to/file',
        self.fs.join('azfs://account-name/container/path', '/to/file'))
    self.assertEqual(
        'azfs://account-name/container/path/to/file',
        self.fs.join('azfs://account-name/container/path', 'to', 'file'))
    self.assertEqual(
        'azfs://account-name/container/path/to/file',
        self.fs.join('azfs://account-name/container/path', 'to/file'))
    self.assertEqual(
        'azfs://account-name/container/path/to/file',
        self.fs.join('azfs://account-name/container/path', '/to/file'))
    with self.assertRaises(ValueError):
      self.fs.join('account-name/container/path', '/to/file')

  def test_split(self):
    self.assertEqual(('azfs://foo/bar', 'baz'),
                     self.fs.split('azfs://foo/bar/baz'))
    self.assertEqual(('azfs://foo', ''), self.fs.split('azfs://foo/'))
    self.assertEqual(('azfs://foo', ''), self.fs.split('azfs://foo'))

    with self.assertRaises(ValueError):
      self.fs.split('/no/azfs/prefix')

  def test_match_multiples(self):
    # self.fs.azfs is a mock.
    self.fs.azfs.list_prefix.return_value = {
        'azfs://storageaccount/container/file1': 1,
        'azfs://storageaccount/container/file2': 2,
    }
    expected_results = set([
        FileMetadata('azfs://storageaccount/container/file1', 1),
        FileMetadata('azfs://storageaccount/container/file2', 2),
    ])
    match_result = self.fs.match(['azfs://storageaccount/container/'])[0]
    print()
    self.assertEqual(set(match_result.metadata_list), expected_results)
    # self.fs.azfs is a mock.
    self.fs.azfs.list_prefix.assert_called_once_with(
        'azfs://storageaccount/container/')

  def test_match_multiples_limit(self):
    limit = 1
    # self.fs.azfs is a mock.
    self.fs.azfs.list_prefix.return_value = {
        'azfs://storageaccount/container/file1': 1
    }
    expected_results = set(
        [FileMetadata('azfs://storageaccount/container/file1', 1)])
    match_result = self.fs.match(['azfs://storageaccount/container/'],
                                 [limit])[0]
    self.assertEqual(set(match_result.metadata_list), expected_results)
    self.assertEqual(len(match_result.metadata_list), limit)
    # self.fs.azfs is a mock.
    self.fs.azfs.list_prefix.assert_called_once_with(
        'azfs://storageaccount/container/')

  def test_match_multiples_error(self):
    exception = IOError('Failed')
    # self.fs.azfs is a mock.
    self.fs.azfs.list_prefix.side_effect = exception

    with self.assertRaisesRegex(BeamIOError,
                                r'^Match operation failed') as error:
      self.fs.match(['azfs://storageaccount/container/'])

    self.assertRegex(
        str(error.exception.exception_details),
        r'azfs://storageaccount/container/.*%s' % exception)
    # self.fs.azfs is a mock.
    self.fs.azfs.list_prefix.assert_called_once_with(
        'azfs://storageaccount/container/')

  def test_match_multiple_patterns(self):
    # self.fs.azfs is a mock.
    self.fs.azfs.list_prefix.side_effect = [
        {
            'azfs://storageaccount/container/file1': 1
        },
        {
            'azfs://storageaccount/container/file2': 2
        },
    ]
    expected_results = [
        [FileMetadata('azfs://storageaccount/container/file1', 1)],
        [FileMetadata('azfs://storageaccount/container/file2', 2)]
    ]
    result = self.fs.match([
        'azfs://storageaccount/container/file1*',
        'azfs://storageaccount/container/file2*'
    ])
    self.assertEqual([mr.metadata_list for mr in result], expected_results)

  def test_create(self):
    # Issue file copy.
    _ = self.fs.create(
        'azfs://storageaccount/container/file1', 'application/octet-stream')
    # self.fs.azfs is a mock.
    self.fs.azfs.open.assert_called_once_with(
        'azfs://storageaccount/container/file1',
        'wb',
        mime_type='application/octet-stream')

  def test_open(self):
    # Issue file copy.
    _ = self.fs.open(
        'azfs://storageaccount/container/file1', 'application/octet-stream')
    # self.fs.azfs is a mock.
    self.fs.azfs.open.assert_called_once_with(
        'azfs://storageaccount/container/file1',
        'rb',
        mime_type='application/octet-stream')

  def test_copy_file(self):
    sources = [
        'azfs://storageaccount/container/from1',
        'azfs://storageaccount/container/from2',
    ]
    destinations = [
        'azfs://storageaccount/container/to1',
        'azfs://storageaccount/container/to2',
    ]

    # Issue file copy.
    self.fs.copy(sources, destinations)

    src_dest_pairs = list(zip(sources, destinations))
    # self.fs.azfs is a mock.
    self.fs.azfs.copy_paths.assert_called_once_with(src_dest_pairs)

  def test_copy_file_error(self):
    sources = [
        'azfs://storageaccount/container/from1',
        'azfs://storageaccount/container/from2',
        'azfs://storageaccount/container/from3',
    ]
    destinations = [
        'azfs://storageaccount/container/to1',
        'azfs://storageaccount/container/to2',
    ]

    # Issue file copy.
    with self.assertRaises(BeamIOError):
      self.fs.copy(sources, destinations)

  def test_delete(self):
    # self.fs.azfs is a mock.
    self.fs.azfs.size.return_value = 0
    files = [
        'azfs://storageaccount/container/from1',
        'azfs://storageaccount/container/from2',
        'azfs://storageaccount/container/from3',
    ]
    # Issue batch delete operation.
    self.fs.delete(files)
    # self.fs.azfs is a mock.
    self.fs.azfs.delete_paths.assert_called_once_with(files)

  def test_delete_error(self):
    nonexistent_directory = 'azfs://storageaccount/nonexistent-container/tree/'
    exception = blobstorageio.BlobStorageError('Not found', 404)
    # self.fs.azfs is a mock.
    self.fs.azfs.delete_paths.return_value = {
        nonexistent_directory: exception,
        'azfs://storageaccount/container/blob1': None,
        'azfs://storageaccount/container/blob2': None,
    }
    # self.fs.azfs is a mock.
    self.fs.azfs.size.return_value = 0
    files = [
        nonexistent_directory,
        'azfs://storageaccount/container/blob1',
        'azfs://storageaccount/container/blob2',
    ]
    expected_results = {nonexistent_directory: exception}

    # Issue batch delete.
    with self.assertRaises(BeamIOError) as error:
      self.fs.delete(files)

    self.assertIn('Delete operation failed', str(error.exception))
    self.assertEqual(error.exception.exception_details, expected_results)
    # self.fs.azfs is a mock.
    self.fs.azfs.delete_paths.assert_called()

  def test_rename(self):

    sources = [
        'azfs://storageaccount/container/original_blob1',
        'azfs://storageaccount/container/original_blob2',
    ]
    destinations = [
        'azfs://storageaccount/container/renamed_blob1',
        'azfs://storageaccount/container/renamed_blob2',
    ]

    # Issue bath rename.
    self.fs.rename(sources, destinations)

    src_dest_pairs = list(zip(sources, destinations))
    # self.fs.azfs is a mock.
    self.fs.azfs.rename_files.assert_called_once_with(src_dest_pairs)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
