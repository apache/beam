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

"""Unit tests for :class:`HadoopFileSystem`."""

# pytype: skip-file

from __future__ import absolute_import

import logging
import posixpath
import sys
import unittest

from parameterized import parameterized_class
from pyarrow.fs import LocalFileSystem

from apache_beam.io import hadoopfilesystem_pyarrow as hdfs
from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.hadoopfilesystem_pyarrow import HadoopFileSystem
from apache_beam.options.pipeline_options import HadoopFileSystemOptions
from apache_beam.options.pipeline_options import PipelineOptions


@parameterized_class(('full_urls', ), [(False, ), (True, )])
class HadoopFileSystemTest(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    # Method has been renamed in Python 3
    if sys.version_info[0] < 3:
      cls.assertCountEqual = cls.assertItemsEqual

  def setUp(self):
    pipeline_options = PipelineOptions()
    hdfs_pipeline_options = pipeline_options.view_as(HadoopFileSystemOptions)
    local_pyarrow_fs = LocalFileSystem()
    self.fs = HadoopFileSystem(hdfs_pipeline_options, user_hdfs_fs=local_pyarrow_fs)
    self.fs._full_urls = self.full_urls

    if not self.full_urls:
      self.tmpdir = 'hdfs://tmp/pyarrowtest'
    else:
      self.tmpdir = 'hdfs://server/tmp/pyarrowtest'

    if self.fs.exists(self.tmpdir):
      self.fs.delete([self.tmpdir])

    # create a temp dir for tests
    if not self.fs.exists(self.tmpdir):
      self.fs.mkdirs(self.tmpdir)

    for filename in ['old_file1', 'old_file2']:
      url = self.fs.join(self.tmpdir, filename)
      self.fs.create(url)

  def test_scheme(self):
    self.assertEqual(self.fs.scheme(), 'hdfs')
    self.assertEqual(hdfs.HadoopFileSystem.scheme(), 'hdfs')

  def test_parse_url(self):
    cases = [
        ('hdfs://', ('', '/'), False),
        ('hdfs://', None, True),
        ('hdfs://a', ('', '/a'), False),
        ('hdfs://a', ('a', '/'), True),
        ('hdfs://a/', ('', '/a/'), False),
        ('hdfs://a/', ('a', '/'), True),
        ('hdfs://a/b', ('', '/a/b'), False),
        ('hdfs://a/b', ('a', '/b'), True),
        ('hdfs://a/b/', ('', '/a/b/'), False),
        ('hdfs://a/b/', ('a', '/b/'), True),
        ('hdfs:/a/b', None, False),
        ('hdfs:/a/b', None, True),
        ('invalid', None, False),
        ('invalid', None, True),
    ]
    for url, expected, full_urls in cases:
      if self.full_urls != full_urls:
        continue
      try:
        result = self.fs._parse_url(url)
      except ValueError:
        self.assertIsNone(expected, msg=(url, expected, full_urls))
        continue
      self.assertEqual(expected, result, msg=(url, expected, full_urls))

  def test_url_join(self):
    self.assertEqual(
        'hdfs://tmp/path/to/file',
        self.fs.join('hdfs://tmp/path', 'to', 'file'))
    self.assertEqual(
        'hdfs://tmp/path/to/file', self.fs.join('hdfs://tmp/path', 'to/file'))
    self.assertEqual('hdfs://tmp/path/', self.fs.join('hdfs://tmp/path/', ''))

    if not self.full_urls:
      self.assertEqual('hdfs://bar', self.fs.join('hdfs://foo', '/bar'))
      self.assertEqual('hdfs://bar', self.fs.join('hdfs://foo/', '/bar'))
      with self.assertRaises(ValueError):
        self.fs.join('/no/scheme', 'file')
    else:
      self.assertEqual('hdfs://foo/bar', self.fs.join('hdfs://foo', '/bar'))
      self.assertEqual('hdfs://foo/bar', self.fs.join('hdfs://foo/', '/bar'))

  def test_url_split(self):
    self.assertEqual(('hdfs://tmp/path/to', 'file'),
                     self.fs.split('hdfs://tmp/path/to/file'))
    if not self.full_urls:
      self.assertEqual(('hdfs://', 'tmp'), self.fs.split('hdfs://tmp'))
      self.assertEqual(('hdfs://tmp', ''), self.fs.split('hdfs://tmp/'))
      self.assertEqual(('hdfs://tmp', 'a'), self.fs.split('hdfs://tmp/a'))
    else:
      self.assertEqual(('hdfs://tmp/', ''), self.fs.split('hdfs://tmp'))
      self.assertEqual(('hdfs://tmp/', ''), self.fs.split('hdfs://tmp/'))
      self.assertEqual(('hdfs://tmp/', 'a'), self.fs.split('hdfs://tmp/a'))

    self.assertEqual(('hdfs://tmp/a', ''), self.fs.split('hdfs://tmp/a/'))
    with self.assertRaisesRegex(ValueError, r'parse'):
      self.fs.split('tmp')

  def test_mkdirs(self):
    url = self.fs.join(self.tmpdir, 't1/t2')
    self.fs.mkdirs(url)
    self.assertTrue(self.fs.exists(url))

  def test_mkdirs_failed(self):
    url = self.fs.join(self.tmpdir, 't1/t2')
    self.fs.mkdirs(url)

    with self.assertRaises(IOError):
      self.fs.mkdirs(url)

  def test_match_file(self):
    expected_files = [
        self.fs.join(self.tmpdir, filename)
        for filename in ['old_file1', 'old_file2']
    ]
    match_patterns = expected_files
    result = self.fs.match(match_patterns)
    returned_files = [
        f.path for match_result in result for f in match_result.metadata_list
    ]
    self.assertCountEqual(expected_files, returned_files)

  def test_match_file_with_limits(self):
    expected_files = [
        self.fs.join(self.tmpdir, filename)
        for filename in ['old_file1', 'old_file2']
    ]
    result = self.fs.match([self.tmpdir + '/'], [1])[0]
    files = [f.path for f in result.metadata_list]
    self.assertEqual(len(files), 1)
    self.assertIn(files[0], expected_files)

  def test_match_file_with_zero_limit(self):
    result = self.fs.match([self.tmpdir + '/'], [0])[0]
    self.assertEqual(len(result.metadata_list), 0)

  def test_match_file_empty(self):
    url = self.fs.join(self.tmpdir, 'nonexistent_file')
    result = self.fs.match([url])[0]
    files = [f.path for f in result.metadata_list]
    self.assertEqual(files, [])

  def test_match_file_error(self):
    url = self.fs.join(self.tmpdir, 'old_file1')
    bad_url = 'bad_url'
    with self.assertRaisesRegex(BeamIOError,
                                r'^Match operation failed .* %s' % bad_url):
      result = self.fs.match([bad_url, url])[0]
      files = [f.path for f in result.metadata_list]
      self.assertEqual(files, [self.fs._parse_url(url)])

  def test_match_directory(self):
    expected_files = [
        self.fs.join(self.tmpdir, filename)
        for filename in ['old_file1', 'old_file2']
    ]

    # Listing without a trailing '/' should return the directory itself and not
    # its contents. The fake HDFS client here has a "sparse" directory
    # structure, so listing without a '/' will return no results.
    result = self.fs.match([self.tmpdir + '/'])[0]
    files = [f.path for f in result.metadata_list]
    self.assertCountEqual(files, expected_files)

  def test_match_directory_trailing_slash(self):
    expected_files = [
        self.fs.join(self.tmpdir, filename)
        for filename in ['old_file1', 'old_file2']
    ]

    result = self.fs.match([self.tmpdir + '/'])[0]
    files = [f.path for f in result.metadata_list]
    self.assertCountEqual(files, expected_files)

  def test_create_success(self):
    url = self.fs.join(self.tmpdir, 'new_file')
    handle = self.fs.create(url)
    self.assertIsNotNone(handle)
    self.assertTrue(self.fs.exists(url))

  def test_open(self):
    url = self.fs.join(self.tmpdir, 'old_file1')
    handle = self.fs.open(url)
    expected_data = b''
    data = handle.read()
    self.assertEqual(data, expected_data)

  def _cmpfiles(self, url1, url2):
    with self.fs.open(url1) as f1:
      with self.fs.open(url2) as f2:
        data1 = f1.read()
        data2 = f2.read()
        return data1 == data2

  def test_copy_file(self):
    url1 = self.fs.join(self.tmpdir, 'new_file1')
    url2 = self.fs.join(self.tmpdir, 'new_file2')
    url3 = self.fs.join(self.tmpdir, 'new_file3')
    with self.fs.create(url1) as f1:
      f1.write(b'Hello')
    self.fs.copy([url1, url1], [url2, url3])
    self.assertTrue(self._cmpfiles(url1, url2))
    self.assertTrue(self._cmpfiles(url1, url3))

  def test_copy_file_overwrite_error(self):
    url1 = self.fs.join(self.tmpdir, 'new_file1')
    url2 = self.fs.join(self.tmpdir, 'new_file2')
    with self.fs.create(url1) as f1:
      f1.write(b'Hello')
    with self.fs.create(url2) as f2:
      f2.write(b'nope')
    with self.assertRaisesRegex(BeamIOError, r'already exists.*%s' %
                                posixpath.basename(url2)):
      self.fs.copy([url1], [url2])

  def test_copy_file_error(self):
    url1 = self.fs.join(self.tmpdir, 'new_file1')
    url2 = self.fs.join(self.tmpdir, 'new_file2')
    url3 = self.fs.join(self.tmpdir, 'new_file3')
    url4 = self.fs.join(self.tmpdir, 'new_file4')
    with self.fs.create(url3) as f:
      f.write(b'Hello')
    with self.assertRaisesRegex(BeamIOError,
                                r'^Copy operation failed .*%s.*%s.* not found' %
                                (url1, url2)):
      self.fs.copy([url1, url3], [url2, url4])
    self.assertTrue(self._cmpfiles(url3, url4))

  def test_copy_directory(self):
    url_t1 = self.fs.join(self.tmpdir, 't1')
    url_t1_inner = self.fs.join(self.tmpdir, 't1/inner')
    url_t2 = self.fs.join(self.tmpdir, 't2')
    url_t2_inner = self.fs.join(self.tmpdir, 't2/inner')
    self.fs.mkdirs(url_t1)
    self.fs.mkdirs(url_t1_inner)
    self.fs.mkdirs(url_t2)

    url1 = self.fs.join(url_t1_inner, 'f1')
    url2 = self.fs.join(url_t2_inner, 'f1')
    with self.fs.create(url1) as f:
      f.write(b'Hello')

    self.fs.copy([url_t1], [url_t2])
    self.assertTrue(self._cmpfiles(url1, url2))

  def test_copy_directory_overwrite_error(self):
    url_t1 = self.fs.join(self.tmpdir, 't1')
    url_t1_inner = self.fs.join(self.tmpdir, 't1/inner')
    url_t2 = self.fs.join(self.tmpdir, 't2')
    url_t2_inner = self.fs.join(self.tmpdir, 't2/inner')
    self.fs.mkdirs(url_t1)
    self.fs.mkdirs(url_t1_inner)
    self.fs.mkdirs(url_t2)
    self.fs.mkdirs(url_t2_inner)

    url1 = self.fs.join(url_t1, 'f1')
    url1_inner = self.fs.join(url_t1_inner, 'f2')
    url2 = self.fs.join(url_t2, 'f1')
    _ = self.fs.join(url_t2_inner, 'f2')
    url3_inner = self.fs.join(url_t2_inner, 'f3')
    for url in [url1, url1_inner, url3_inner]:
      with self.fs.create(url) as f:
        f.write(b'Hello')
    with self.fs.create(url2) as f:
      f.write(b'nope')

    with self.assertRaisesRegex(BeamIOError, r'already exists'):
      self.fs.copy([url_t1], [url_t2])

  def test_rename_file(self):
    url1 = self.fs.join(self.tmpdir, 'f1')
    url2 = self.fs.join(self.tmpdir, 'f2')
    with self.fs.create(url1) as f:
      f.write(b'Hello')

    self.fs.rename([url1], [url2])
    self.assertFalse(self.fs.exists(url1))
    self.assertTrue(self.fs.exists(url2))

  def test_rename_file_error(self):
    url1 = self.fs.join(self.tmpdir, 'f1')
    url2 = self.fs.join(self.tmpdir, 'f2')
    url3 = self.fs.join(self.tmpdir, 'f3')
    url4 = self.fs.join(self.tmpdir, 'f4')
    with self.fs.create(url3) as f:
      f.write(b'Hello')

    with self.assertRaisesRegex(BeamIOError,
                                r'^Rename operation failed .*%s.*%s' %
                                (url1, url2)):
      self.fs.rename([url1, url3], [url2, url4])
    self.assertFalse(self.fs.exists(url3))
    self.assertTrue(self.fs.exists(url4))

  def test_rename_directory(self):
    url_t1 = self.fs.join(self.tmpdir, 't1')
    url_t2 = self.fs.join(self.tmpdir, 't2')
    self.fs.mkdirs(url_t1)
    url1 = self.fs.join(url_t1, 'f1')
    url2 = self.fs.join(url_t2, 'f1')
    with self.fs.create(url1) as f:
      f.write(b'Hello')

    self.fs.rename([url_t1], [url_t2])
    self.assertFalse(self.fs.exists(url_t1))
    self.assertTrue(self.fs.exists(url_t2))
    self.assertFalse(self.fs.exists(url1))
    self.assertTrue(self.fs.exists(url2))

  def test_exists(self):
    url1 = self.fs.join(self.tmpdir, 'old_file1')
    url2 = self.fs.join(self.tmpdir, 'nonexistent')
    self.assertTrue(self.fs.exists(url1))
    self.assertFalse(self.fs.exists(url2))

  def test_size(self):
    url = self.fs.join(self.tmpdir, 'f1')
    with self.fs.create(url) as f:
      f.write(b'Hello')
    self.assertTrue(self.fs.size(url), 5)

  def test_checksum(self):
    url = self.fs.join(self.tmpdir, 'f1')
    with self.fs.create(url) as f:
      f.write(b'Hello')
    self.assertEqual(self.fs.checksum(url), '5')

  def test_delete_file(self):
    url = self.fs.join(self.tmpdir, 'old_file1')

    self.assertTrue(self.fs.exists(url))
    self.fs.delete([url])
    self.assertFalse(self.fs.exists(url))

  def test_delete_dir(self):
    url_t1 = self.fs.join(self.tmpdir, 'new_dir1')
    url_t2 = self.fs.join(url_t1, 'new_dir2')
    url1 = self.fs.join(url_t2, 'new_file1')
    url2 = self.fs.join(url_t2, 'new_file2')
    self.fs.mkdirs(url_t1)
    self.fs.mkdirs(url_t2)
    self.fs.create(url1)
    self.fs.create(url2)

    self.assertTrue(self.fs.exists(url1))
    self.assertTrue(self.fs.exists(url2))
    self.assertTrue(self.fs.exists(url_t1))
    self.assertTrue(self.fs.exists(url_t2))
    # delete base dir
    self.fs.delete([url_t1])
    self.assertFalse(self.fs.exists(url_t1))
    self.assertFalse(self.fs.exists(url_t2))
    self.assertFalse(self.fs.exists(url2))
    self.assertFalse(self.fs.exists(url1))

  def test_delete_error(self):
    url1 = self.fs.join(self.tmpdir, 'nonexistent')
    url2 = self.fs.join(self.tmpdir, 'old_file1')

    self.assertTrue(self.fs.exists(url2))
    _, path1 = self.fs._parse_url(url1)
    with self.assertRaisesRegex(BeamIOError, r'^Delete operation failed .* %s' % path1):
      self.fs.delete([url1, url2])
    self.assertFalse(self.fs.exists(url2))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
