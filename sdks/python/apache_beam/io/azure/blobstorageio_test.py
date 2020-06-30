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

"""Tests for Azure Blob Storage client."""
# pytype: skip-file

from __future__ import absolute_import

import unittest

from apache_beam.io.azure import blobstorageio


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
  ]


  def test_azfs_path(self):
    self.assertEqual(
      blobstorageio.parse_azfs_path('azfs://storageaccount/container/name'), ('storageaccount', 'container', 'name'))
    self.assertEqual(
      blobstorageio.parse_azfs_path('azfs://storageaccount/container/name/sub'), ('storageaccount', 'container', 'name/sub'))

  def test_bad_azfs_path(self):
    for path in self.BAD_AZFS_PATHS:
      self.assertRaises(ValueError, blobstorageio.parse_azfs_path, path)
    self.assertRaises(ValueError, blobstorageio.parse_azfs_path, 'azfs://storageaccount/container/')

  def test_azfs_path_object_optional(self):
    self.assertEqual(
        blobstorageio.parse_azfs_path('azfs://storageaccount/container/name', object_optional=True),
        ('storageaccount', 'container', 'name'))
    self.assertEqual(
        blobstorageio.parse_azfs_path('azfs://storageaccount/container/', object_optional=True),
        ('storageaccount', 'container', ''))

  def test_bad_gcs_path_object_optional(self):
    for path in self.BAD_AZFS_PATHS:
      self.assertRaises(ValueError, blobstorageio.parse_azfs_path, path, True)

