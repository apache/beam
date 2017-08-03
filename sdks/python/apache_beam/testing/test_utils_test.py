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
import tempfile
import unittest
from mock import patch

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystems import FileSystems
from apache_beam.testing import test_utils as utils


class TestUtilsTest(unittest.TestCase):

  def setUp(self):
    utils.patch_retry(self, utils)
    self.tmpdir = tempfile.mkdtemp()

  def test_delete_files_succeeds(self):
    f = tempfile.NamedTemporaryFile(dir=self.tmpdir, delete=False)
    assert FileSystems.exists(f.name)
    utils.delete_files([f.name])
    assert not FileSystems.exists(f.name)

  @patch.object(FileSystems, 'delete', side_effect=BeamIOError(''))
  def test_delete_files_fails_with_io_error(self, mocked_delete):
    f = tempfile.NamedTemporaryFile(dir=self.tmpdir, delete=False)
    assert FileSystems.exists(f.name)

    with self.assertRaises(BeamIOError):
      utils.delete_files([f.name])
    self.assertTrue(mocked_delete.called)
    self.assertEqual(mocked_delete.call_count, 4)

  def test_delete_files_fails_with_invalid_arg(self):
    with self.assertRaises(RuntimeError):
      utils.delete_files([])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()