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

"""Unit tests for local and GCS sources and sinks."""

import unittest

import mock
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

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_file_metadata_match(self, mock_gcsio):
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
    self.assertEqual(
        set(gcsfilesystem.GCSFileSystem.match('gs://bucket/*')),
        expected_results)
    gcsio_mock.size_of_files_in_glob.assert_called_once_with('gs://bucket/*')

  @mock.patch('apache_beam.io.gcp.gcsfilesystem.gcsio')
  def test_rename_batch(self, mock_gcsio):
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
    gcsfilesystem.GCSFileSystem.rename(sources, destinations)

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
