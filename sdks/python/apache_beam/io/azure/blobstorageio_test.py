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

"""Tests for Azure Blob Storage client.
"""
# pytype: skip-file

import logging
import unittest

# Protect against environments where azure library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.io.azure import blobstorageio
except ImportError:
  blobstorageio = None  # type: ignore[assignment]
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(blobstorageio is None, 'Azure dependencies are not installed')
class TestAZFSPathParser(unittest.TestCase):

  BAD_AZFS_PATHS = [
      'azfs://'
      'azfs://storage-account/'
      'azfs://storage-account/**'
      'azfs://storage-account/**/*'
      'azfs://container'
      'azfs:///name'
      'azfs:///'
      'azfs:/blah/container/name'
      'azfs://ab/container/name'
      'azfs://accountwithmorethan24chars/container/name'
      'azfs://***/container/name'
      'azfs://storageaccount/my--container/name'
      'azfs://storageaccount/CONTAINER/name'
      'azfs://storageaccount/ct/name'
  ]

  def test_azfs_path(self):
    self.assertEqual(
        blobstorageio.parse_azfs_path(
            'azfs://storageaccount/container/name', get_account=True),
        ('storageaccount', 'container', 'name'))
    self.assertEqual(
        blobstorageio.parse_azfs_path(
            'azfs://storageaccount/container/name/sub', get_account=True),
        ('storageaccount', 'container', 'name/sub'))

  def test_bad_azfs_path(self):
    for path in self.BAD_AZFS_PATHS:
      self.assertRaises(ValueError, blobstorageio.parse_azfs_path, path)
    self.assertRaises(
        ValueError,
        blobstorageio.parse_azfs_path,
        'azfs://storageaccount/container/')

  def test_azfs_path_blob_optional(self):
    self.assertEqual(
        blobstorageio.parse_azfs_path(
            'azfs://storageaccount/container/name',
            blob_optional=True,
            get_account=True), ('storageaccount', 'container', 'name'))
    self.assertEqual(
        blobstorageio.parse_azfs_path(
            'azfs://storageaccount/container/',
            blob_optional=True,
            get_account=True), ('storageaccount', 'container', ''))

  def test_bad_azfs_path_blob_optional(self):
    for path in self.BAD_AZFS_PATHS:
      self.assertRaises(ValueError, blobstorageio.parse_azfs_path, path, True)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
