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
"""Unit tests for the path module."""

import unittest


import mock

from apache_beam.utils import path


def _gen_fake_join(separator):
  """Returns a callable that joins paths with the given separator."""

  def _join(first_path, *paths):
    return separator.join((first_path,) + paths)

  return _join


class Path(unittest.TestCase):

  def setUp(self):
    pass

  @mock.patch('apache_beam.utils.path.os')
  def test_gcs_path(self, *unused_mocks):
    # Test joining of GCS paths when os.path.join uses Windows-style separator.
    path.os.path.join.side_effect = _gen_fake_join('\\')
    self.assertEqual('gs://bucket/path/to/file',
                     path.join('gs://bucket/path', 'to', 'file'))
    self.assertEqual('gs://bucket/path/to/file',
                     path.join('gs://bucket/path', 'to/file'))
    self.assertEqual('gs://bucket/path//to/file',
                     path.join('gs://bucket/path', '/to/file'))

  @mock.patch('apache_beam.utils.path.os')
  def test_unix_path(self, *unused_mocks):
    # Test joining of Unix paths.
    path.os.path.join.side_effect = _gen_fake_join('/')
    self.assertEqual('/tmp/path/to/file', path.join('/tmp/path', 'to', 'file'))
    self.assertEqual('/tmp/path/to/file', path.join('/tmp/path', 'to/file'))

  @mock.patch('apache_beam.utils.path.os')
  def test_windows_path(self, *unused_mocks):
    # Test joining of Windows paths.
    path.os.path.join.side_effect = _gen_fake_join('\\')
    self.assertEqual(r'C:\tmp\path\to\file',
                     path.join(r'C:\tmp\path', 'to', 'file'))
    self.assertEqual(r'C:\tmp\path\to\file',
                     path.join(r'C:\tmp\path', r'to\file'))


if __name__ == '__main__':
  unittest.main()
