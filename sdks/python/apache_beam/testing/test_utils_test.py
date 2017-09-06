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

"""Unittest for testing utilities,"""

import logging
import os
import tempfile
import unittest

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystems import FileSystems
from apache_beam.testing import test_utils as utils


class TestUtilsTest(unittest.TestCase):

  def setUp(self):
    utils.patch_retry(self, utils)
    self.tmpdir = tempfile.mkdtemp()

  def test_delete_files_succeeds(self):
    path = os.path.join(self.tmpdir, 'f1')

    with open(path, 'a') as f:
      f.write('test')

    assert FileSystems.exists(path)
    utils.delete_files([path])
    assert not FileSystems.exists(path)

  def test_delete_files_fails_with_io_error(self):
    path = os.path.join(self.tmpdir, 'f2')

    with self.assertRaises(BeamIOError) as error:
      utils.delete_files([path])
    self.assertTrue(
        error.exception.message.startswith('Delete operation failed'))
    self.assertEqual(error.exception.exception_details.keys(), [path])

  def test_delete_files_fails_with_invalid_arg(self):
    with self.assertRaises(RuntimeError):
      utils.delete_files([])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
