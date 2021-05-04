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

import io
import logging
import posixpath
import unittest

from parameterized import parameterized_class

from apache_beam.io import hadoopfilesystem as hdfs
from apache_beam.io.filesystem import BeamIOError
from apache_beam.options.pipeline_options import HadoopFileSystemOptions
from apache_beam.options.pipeline_options import PipelineOptions


class FakeFile(io.BytesIO):
  """File object for FakeHdfs"""
  __hash__ = None  # type: ignore[assignment]

  def __init__(self, path, mode='', type='FILE'):
    io.BytesIO.__init__(self)

    self.stat = {
        'path': path,
        'mode': mode,
        'type': type,
    }
    self.saved_data = None

  def __eq__(self, other):
    return self.stat == other.stat and self.getvalue() == self.getvalue()

  def close(self):
    self.saved_data = self.getvalue()
    io.BytesIO.close(self)

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.close()

  @property
  def size(self):
    if self.closed:  # pylint: disable=using-constant-test
      if self.saved_data is None:
        return 0
      return len(self.saved_data)
    return len(self.getvalue())

  def get_file_status(self):
    """Returns a partial WebHDFS FileStatus object."""
    return {
        hdfs._FILE_STATUS_PATH_SUFFIX: posixpath.basename(self.stat['path']),
        hdfs._FILE_STATUS_LENGTH: self.size,
        hdfs._FILE_STATUS_TYPE: self.stat['type'],
    }

  def get_file_checksum(self):
    """Returns a WebHDFS FileChecksum object."""
    return {
        hdfs._FILE_CHECKSUM_ALGORITHM: 'fake_algo',
        hdfs._FILE_CHECKSUM_BYTES: 'checksum_byte_sequence',
        hdfs._FILE_CHECKSUM_LENGTH: 5,
    }


class FakeHdfsError(Exception):
  """Generic error for FakeHdfs methods."""


class FakeHdfs(object):
  """Fake implementation of ``hdfs.Client``."""
  def __init__(self):
    self.files = {}

  def write(self, path):
    if self.status(path, strict=False) is not None:
      raise FakeHdfsError('Path already exists: %s' % path)

    new_file = FakeFile(path, 'wb')
    self.files[path] = new_file
    return new_file

  def read(self, path, offset=0, length=None):
    old_file = self.files.get(path, None)
    if old_file is None:
      raise FakeHdfsError('Path not found: %s' % path)
    if old_file.stat['type'] == 'DIRECTORY':
      raise FakeHdfsError('Cannot open a directory: %s' % path)
    if not old_file.closed:
      raise FakeHdfsError('File already opened: %s' % path)

    # old_file is closed and can't be operated upon. Return a copy instead.
    new_file = FakeFile(path, 'rb')
    if old_file.saved_data:
      new_file.write(old_file.saved_data)
      new_file.seek(0)
    return new_file

  def list(self, path, status=False):
    if not status:
      raise ValueError('status must be True')
    fs = self.status(path, strict=False)
    if (fs is not None and
        fs[hdfs._FILE_STATUS_TYPE] == hdfs._FILE_STATUS_TYPE_FILE):
      raise ValueError(
          'list must be called on a directory, got file: %s' % path)

    result = []
    for file in self.files.values():
      if file.stat['path'].startswith(path):
        fs = file.get_file_status()
        result.append((fs[hdfs._FILE_STATUS_PATH_SUFFIX], fs))
    return result

  def makedirs(self, path):
    self.files[path] = FakeFile(path, type='DIRECTORY')

  def status(self, path, strict=True):
    f = self.files.get(path)
    if f is None:
      if strict:
        raise FakeHdfsError('Path not found: %s' % path)
      else:
        return f
    return f.get_file_status()

  def delete(self, path, recursive=True):
    if not recursive:
      raise FakeHdfsError('Non-recursive mode not implemented')

    _ = self.status(path)

    for filepath in list(self.files):
      if filepath.startswith(path):
        del self.files[filepath]

  def walk(self, path):
    paths = [path]
    while paths:
      path = paths.pop()
      files = []
      dirs = []
      for full_path in self.files:
        if not full_path.startswith(path):
          continue
        short_path = posixpath.relpath(full_path, path)
        if '/' not in short_path:
          if self.status(full_path)[hdfs._FILE_STATUS_TYPE] == 'DIRECTORY':
            if short_path != '.':
              dirs.append(short_path)
          else:
            files.append(short_path)

      yield path, dirs, files
      paths = [posixpath.join(path, dir) for dir in dirs]

  def rename(self, path1, path2):
    if self.status(path1, strict=False) is None:
      raise FakeHdfsError('Path1 not found: %s' % path1)

    files_to_rename = [
        path for path in self.files
        if path == path1 or path.startswith(path1 + '/')
    ]
    for fullpath in files_to_rename:
      f = self.files.pop(fullpath)
      newpath = path2 + fullpath[len(path1):]
      f.stat['path'] = newpath
      self.files[newpath] = f

  def checksum(self, path):
    f = self.files.get(path, None)
    if f is None:
      raise FakeHdfsError('Path not found: %s' % path)
    return f.get_file_checksum()


@parameterized_class(('full_urls', ), [(False, ), (True, )])
class HadoopFileSystemTest(unittest.TestCase):
  def setUp(self):
    self._fake_hdfs = FakeHdfs()
    hdfs.hdfs.InsecureClient = (lambda *args, **kwargs: self._fake_hdfs)
    pipeline_options = PipelineOptions()
    hdfs_options = pipeline_options.view_as(HadoopFileSystemOptions)
    hdfs_options.hdfs_host = ''
    hdfs_options.hdfs_port = 0
    hdfs_options.hdfs_user = ''

    self.fs = hdfs.HadoopFileSystem(pipeline_options)
    self.fs._full_urls = self.full_urls
    if self.full_urls:
      self.tmpdir = 'hdfs://test_dir'
    else:
      self.tmpdir = 'hdfs://server/test_dir'

    for filename in ['old_file1', 'old_file2']:
      url = self.fs.join(self.tmpdir, filename)
      self.fs.create(url).close()

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
    _, url = self.fs._parse_url(url)
    expected_file = FakeFile(url, 'wb')
    self.assertEqual(self._fake_hdfs.files[url], expected_file)

  def test_create_write_read_compressed(self):
    url = self.fs.join(self.tmpdir, 'new_file.gz')

    handle = self.fs.create(url)
    self.assertIsNotNone(handle)
    _, path = self.fs._parse_url(url)
    expected_file = FakeFile(path, 'wb')
    self.assertEqual(self._fake_hdfs.files[path], expected_file)
    data = b'abc' * 10
    handle.write(data)
    # Compressed data != original data
    self.assertNotEqual(data, self._fake_hdfs.files[path].getvalue())
    handle.close()

    handle = self.fs.open(url)
    read_data = handle.read(len(data))
    self.assertEqual(data, read_data)
    handle.close()

  def test_open(self):
    url = self.fs.join(self.tmpdir, 'old_file1')
    handle = self.fs.open(url)
    expected_data = b''
    data = handle.read()
    self.assertEqual(data, expected_data)

  def test_open_bad_path(self):
    with self.assertRaises(FakeHdfsError):
      self.fs.open(self.fs.join(self.tmpdir, 'nonexistent/path'))

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
    with self.assertRaisesRegex(BeamIOError,
                                r'already exists.*%s' %
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
    unused_url2_inner = self.fs.join(url_t2_inner, 'f2')
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
    self.assertEqual(5, self.fs.size(url))

  def test_checksum(self):
    url = self.fs.join(self.tmpdir, 'f1')
    with self.fs.create(url) as f:
      f.write(b'Hello')
    self.assertEqual(
        'fake_algo-5-checksum_byte_sequence', self.fs.checksum(url))

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
    self.fs.create(url1).close()
    self.fs.create(url2).close()

    self.assertTrue(self.fs.exists(url1))
    self.assertTrue(self.fs.exists(url2))
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
    with self.assertRaisesRegex(BeamIOError,
                                r'^Delete operation failed .* %s' % path1):
      self.fs.delete([url1, url2])
    self.assertFalse(self.fs.exists(url2))


class HadoopFileSystemRuntimeValueProviderTest(unittest.TestCase):
  """Tests pipeline_options, in the form of a
  RuntimeValueProvider.runtime_options object."""
  def setUp(self):
    self._fake_hdfs = FakeHdfs()
    hdfs.hdfs.InsecureClient = (lambda *args, **kwargs: self._fake_hdfs)

  def test_dict_options(self):
    pipeline_options = {
        'hdfs_host': '',
        'hdfs_port': 0,
        'hdfs_user': '',
    }

    self.fs = hdfs.HadoopFileSystem(pipeline_options=pipeline_options)
    self.assertFalse(self.fs._full_urls)

  def test_dict_options_missing(self):
    with self.assertRaisesRegex(ValueError, r'hdfs_host'):
      self.fs = hdfs.HadoopFileSystem(
          pipeline_options={
              'hdfs_port': 0,
              'hdfs_user': '',
          })

    with self.assertRaisesRegex(ValueError, r'hdfs_port'):
      self.fs = hdfs.HadoopFileSystem(
          pipeline_options={
              'hdfs_host': '',
              'hdfs_user': '',
          })

    with self.assertRaisesRegex(ValueError, r'hdfs_user'):
      self.fs = hdfs.HadoopFileSystem(
          pipeline_options={
              'hdfs_host': '',
              'hdfs_port': 0,
          })

  def test_dict_options_full_urls(self):
    pipeline_options = {
        'hdfs_host': '',
        'hdfs_port': 0,
        'hdfs_user': '',
        'hdfs_full_urls': 'invalid',
    }

    with self.assertRaisesRegex(ValueError, r'hdfs_full_urls'):
      self.fs = hdfs.HadoopFileSystem(pipeline_options=pipeline_options)

    pipeline_options['hdfs_full_urls'] = True
    self.fs = hdfs.HadoopFileSystem(pipeline_options=pipeline_options)
    self.assertTrue(self.fs._full_urls)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
