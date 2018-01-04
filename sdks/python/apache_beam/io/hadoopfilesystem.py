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

""":class:`~apache_beam.io.filesystem.FileSystem` implementation for accessing
Hadoop Distributed File System files."""

from __future__ import absolute_import

import logging
import posixpath
import re

from hdfs3 import HDFileSystem

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystem import FileSystem
from apache_beam.io.filesystem import MatchResult

__all__ = ['HadoopFileSystem']

_HDFS_PREFIX = 'hdfs:/'
_URL_RE = re.compile(r'^' + _HDFS_PREFIX + r'(/.*)')
_COPY_BUFFER_SIZE = 2 ** 16


# TODO(udim): Add @retry.with_exponential_backoff to some functions, like in
# gcsio.py.


class HadoopFileSystem(FileSystem):
  """``FileSystem`` implementation that supports HDFS.

  URL arguments to methods expect strings starting with ``hdfs://``.

  Uses client library :class:`hdfs3.core.HDFileSystem`.
  """

  def __init__(self):
    """Initializes a connection to HDFS.

    Connection configuration is done using :doc:`hdfs`.
    """
    super(HadoopFileSystem, self).__init__()
    self._hdfs_client = HDFileSystem()

  @classmethod
  def scheme(cls):
    return 'hdfs'

  @staticmethod
  def _parse_url(url):
    """Verifies that url begins with hdfs:// prefix, strips it and adds a
    leading /.

    Raises:
      ValueError if url doesn't begin with hdfs://.

    Args:
      url: A URL in the form hdfs://path/...

    Returns:
      For an input of 'hdfs://path/...', will return '/path/...'.
    """
    m = _URL_RE.match(url)
    if m is None:
      raise ValueError('Could not parse url: %s' % url)
    return m.group(1)

  def join(self, base_url, *paths):
    """Join two or more pathname components.

    Args:
      base_url: string path of the first component of the path.
        Must start with hdfs://.
      paths: path components to be added

    Returns:
      Full url after combining all the passed components.
    """
    basepath = self._parse_url(base_url)
    return _HDFS_PREFIX + self._join(basepath, *paths)

  def _join(self, basepath, *paths):
    return posixpath.join(basepath, *paths)

  def split(self, url):
    rel_path = self._parse_url(url)
    head, tail = posixpath.split(rel_path)
    return _HDFS_PREFIX + head, tail

  def mkdirs(self, url):
    path = self._parse_url(url)
    if self._exists(path):
      raise IOError('Path already exists: %s' % path)
    return self._mkdirs(path)

  def _mkdirs(self, path):
    self._hdfs_client.makedirs(path)

  def match(self, url_patterns, limits=None):
    if limits is None:
      limits = [None] * len(url_patterns)

    if len(url_patterns) != len(limits):
      raise BeamIOError(
          'Patterns and limits should be equal in length: %d != %d' % (
              len(url_patterns), len(limits)))

    # TODO(udim): Update client to allow batched results.
    def _match(path_pattern, limit):
      """Find all matching paths to the pattern provided."""
      file_infos = self._hdfs_client.ls(path_pattern, detail=True)[:limit]
      metadata_list = [FileMetadata(file_info['name'], file_info['size'])
                       for file_info in file_infos]
      return MatchResult(path_pattern, metadata_list)

    exceptions = {}
    result = []
    for url_pattern, limit in zip(url_patterns, limits):
      try:
        path_pattern = self._parse_url(url_pattern)
        result.append(_match(path_pattern, limit))
      except Exception as e:  # pylint: disable=broad-except
        exceptions[url_pattern] = e

    if exceptions:
      raise BeamIOError('Match operation failed', exceptions)
    return result

  def _open_hdfs(self, path, mode, mime_type, compression_type):
    if mime_type != 'application/octet-stream':
      logging.warning('Mime types are not supported. Got non-default mime_type:'
                      ' %s', mime_type)
    if compression_type == CompressionTypes.AUTO:
      compression_type = CompressionTypes.detect_compression_type(path)
    res = self._hdfs_client.open(path, mode)
    if compression_type != CompressionTypes.UNCOMPRESSED:
      res = CompressedFile(res)
    return res

  def create(self, url, mime_type='application/octet-stream',
             compression_type=CompressionTypes.AUTO):
    """
    Returns:
      *hdfs3.core.HDFile*: An Python File-like object.
    """
    path = self._parse_url(url)
    return self._create(path, mime_type, compression_type)

  def _create(self, path, mime_type='application/octet-stream',
              compression_type=CompressionTypes.AUTO):
    return self._open_hdfs(path, 'wb', mime_type, compression_type)

  def open(self, url, mime_type='application/octet-stream',
           compression_type=CompressionTypes.AUTO):
    """
    Returns:
      *hdfs3.core.HDFile*: An Python File-like object.
    """
    path = self._parse_url(url)
    return self._open(path, mime_type, compression_type)

  def _open(self, path, mime_type='application/octet-stream',
            compression_type=CompressionTypes.AUTO):
    return self._open_hdfs(path, 'rb', mime_type, compression_type)

  def copy(self, source_file_names, destination_file_names):
    """
    Will overwrite files and directories in destination_file_names.

    Raises ``BeamIOError`` if any error occurred.

    Args:
      source_file_names: iterable of URLs.
      destination_file_names: iterable of URLs.
    """
    if len(source_file_names) != len(destination_file_names):
      raise BeamIOError(
          'source_file_names and destination_file_names should '
          'be equal in length: %d != %d' % (
              len(source_file_names), len(destination_file_names)))

    def _copy_file(source, destination):
      with self._open(source) as f1:
        with self._create(destination) as f2:
          while True:
            buf = f1.read(_COPY_BUFFER_SIZE)
            if not buf:
              break
            f2.write(buf)

    def _copy_path(source, destination):
      """Recursively copy the file tree from the source to the destination."""
      if not self._hdfs_client.isdir(source):
        _copy_file(source, destination)
        return

      for path, dirs, files in self._hdfs_client.walk(source):
        for dir in dirs:
          new_dir = self._join(destination, dir)
          if not self._exists(new_dir):
            self._mkdirs(new_dir)

        rel_path = posixpath.relpath(path, source)
        if rel_path == '.':
          rel_path = ''
        for file in files:
          _copy_file(self._join(path, file),
                     self._join(destination, rel_path, file))

    exceptions = {}
    for source, destination in zip(source_file_names, destination_file_names):
      try:
        rel_source = self._parse_url(source)
        rel_destination = self._parse_url(destination)
        _copy_path(rel_source, rel_destination)
      except Exception as e:  # pylint: disable=broad-except
        exceptions[(source, destination)] = e

    if exceptions:
      raise BeamIOError('Copy operation failed', exceptions)

  def rename(self, source_file_names, destination_file_names):
    exceptions = {}
    for source, destination in zip(source_file_names, destination_file_names):
      try:
        rel_source = self._parse_url(source)
        rel_destination = self._parse_url(destination)
        if not self._hdfs_client.mv(rel_source, rel_destination):
          raise BeamIOError(
              'libhdfs error in renaming %s to %s' % (source, destination))
      except Exception as e:  # pylint: disable=broad-except
        exceptions[(source, destination)] = e

    if exceptions:
      raise BeamIOError('Rename operation failed', exceptions)

  def exists(self, url):
    """Checks existence of url in HDFS.

    Args:
      url: String in the form hdfs://...

    Returns:
      True if url exists as a file or directory in HDFS.
    """
    path = self._parse_url(url)
    return self._exists(path)

  def _exists(self, path):
    """Returns True if path exists as a file or directory in HDFS.

    Args:
      path: String in the form /...
    """
    return self._hdfs_client.exists(path)

  def delete(self, urls):
    exceptions = {}
    for url in urls:
      try:
        path = self._parse_url(url)
        self._hdfs_client.rm(path, recursive=True)
      except Exception as e:  # pylint: disable=broad-except
        exceptions[url] = e

    if exceptions:
      raise BeamIOError("Delete operation failed", exceptions)
