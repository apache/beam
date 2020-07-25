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

"""Unit tests for Azure Blob Storage File System."""

# pytype: skip-file

from __future__ import absolute_import

import logging
import unittest

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import
import mock

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import FileMetadata
from apache_beam.options.pipeline_options import PipelineOptions

# Protect against environments where apitools library is not available.
#pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.io.azure import blobstoragefilesystem
except ImportError:
  blobstoragefilesystem = None  # type: ignore
#pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(
    blobstoragefilesystem is None, 'Azure dependencies are not installed')
class BlobStorageFileSystemTest(unittest.TestCase):
  def setUp(self):
    pipeline_options = PipelineOptions()
    self.fs = blobstoragefilesystem.BlobStorageFileSystem(
        pipeline_options=pipeline_options)

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

  @mock.patch('apache_beam.io.azure.blobstoragefilesystem.blobstorageio')
  def test_match_multiples(self, unused_mock_blobstorageio):
    # Prepare mocks.
    blobstorageio_mock = mock.MagicMock()
    blobstoragefilesystem.blobstorageio.BlobStorageIO = lambda: blobstorageio_mock
    blobstorageio_mock.list_prefix.return_value = {
        'azfs://storageaccount/container/file1': 1,
        'azfs://storageaccount/container/file2': 2
    }
    expected_results = set([
        FileMetadata('azfs://storageaccount/container/file1', 1),
        FileMetadata('azfs://storageaccount/container/file2', 2)
    ])
    match_result = self.fs.match(['azfs://storageaccount/container/'])[0]

    self.assertEqual(set(match_result.metadata_list), expected_results)
    blobstorageio_mock.list_prefix.assert_called_once_with(
        'azfs://storageaccount/container/')

  @mock.patch('apache_beam.io.azure.blobstoragefilesystem.blobstorageio')
  def test_match_multiples_limit(self, unused_mock_blobstorageio):
    # Prepare mocks.
    blobstorageio_mock = mock.MagicMock()
    limit = 1
    blobstoragefilesystem.blobstorageio.BlobStorageIO = lambda: blobstorageio_mock
    blobstorageio_mock.list_prefix.return_value = {
        'azfs://storageaccount/container/file1': 1
    }
    expected_results = set(
        [FileMetadata('azfs://storageaccount/container/file1', 1)])
    match_result = self.fs.match(['azfs://storageaccount/container/'],
                                 [limit])[0]
    self.assertEqual(set(match_result.metadata_list), expected_results)
    self.assertEqual(len(match_result.metadata_list), limit)
    blobstorageio_mock.list_prefix.assert_called_once_with(
        'azfs://storageaccount/container/')

  @mock.patch('apache_beam.io.azure.blobstoragefilesystem.blobstorageio')
  def test_match_multiples_error(self, unused_mock_blobstorageio):
    # Prepare mocks.
    blobstorageio_mock = mock.MagicMock()
    blobstoragefilesystem.blobstorageio.BlobStorageIO = lambda: blobstorageio_mock
    exception = IOError('Failed')
    blobstorageio_mock.list_prefix.side_effect = exception

    with self.assertRaisesRegex(BeamIOError,
                                r'^Match operation failed') as error:
      self.fs.match(['azfs://storageaccount/container/'])

    self.assertRegex(
        str(error.exception.exception_details),
        r'azfs://storageaccount/container/.*%s' % exception)
    blobstorageio_mock.list_prefix.assert_called_once_with(
        'azfs://storageaccount/container/')

  @mock.patch('apache_beam.io.azure.blobstoragefilesystem.blobstorageio')
  def test_match_multiple_patterns(self, unused_mock_blobstorageio):
    # Prepare mocks.
    blobstorageio_mock = mock.MagicMock()
    blobstoragefilesystem.blobstorageio.BlobStorageIO = lambda: blobstorageio_mock
    blobstorageio_mock.list_prefix.side_effect = [
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

  @mock.patch('apache_beam.io.azure.blobstoragefilesystem.blobstorageio')
  def test_create(self, unused_mock_blobstorageio):
    # Prepare mocks.
    blobstorageio_mock = mock.MagicMock()
    blobstoragefilesystem.blobstorageio.BlobStorageIO = lambda: blobstorageio_mock
    # Issue file copy
    _ = self.fs.create(
        'azfs://storageaccount/container/file1', 'application/octet-stream')

    blobstorageio_mock.open.assert_called_once_with(
        'azfs://storageaccount/container/file1',
        'wb',
        mime_type='application/octet-stream')   


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
