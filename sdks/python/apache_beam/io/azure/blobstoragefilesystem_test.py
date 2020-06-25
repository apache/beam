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

@unittest.skipIf(blobstoragefilesystem is None, 'Azure dependencies are not installed')
class BlobStorageFileSystemTest(unittest.TestCase):
  def setUp(self):
    pipeline_options = PipelineOptions()
    self.fs = blobstoragefilesystem.BlobStorageFileSystem(pipeline_options=pipeline_options)



if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()