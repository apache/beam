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

import io
import logging
import posixpath
import re

import hdfs

from apache_beam.io import filesystemio
from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystem import FileSystem
from apache_beam.io.filesystem import MatchResult
from apache_beam.options.pipeline_options import HadoopFileSystemOptions

__all__ = ['HadoopFileSystem']

_HDFS_PREFIX = 'hdfs:/'
_URL_RE = re.compile(r'^' + _HDFS_PREFIX + r'(/.*)')
_COPY_BUFFER_SIZE = 2 ** 16
_DEFAULT_BUFFER_SIZE = 20 * 1024 * 1024

# WebHDFS FileStatus property constants.
_FILE_STATUS_NAME = 'name'
_FILE_STATUS_PATH_SUFFIX = 'pathSuffix'
_FILE_STATUS_TYPE = 'type'
_FILE_STATUS_TYPE_DIRECTORY = 'DIRECTORY'
_FILE_STATUS_TYPE_FILE = 'FILE'
_FILE_STATUS_SIZE = 'size'


class HdfsDownloader(filesystemio.Downloader):

  def __init__(self, hdfs_client, path):
    self._hdfs_client = hdfs_client
    self._path = path
    self._size = self._hdfs_client.status(path)[_FILE_STATUS_SIZE]

  @property
  def size(self):
    return self._size

  def get_range(self, start, end):
    with self._hdfs_client.read(
        self._path, offset=start, length=end - start + 1) as reader:
      return reader.read()


class HdfsUploader(filesystemio.Uploader):

  def __init__(self, hdfs_client, path):
    self._hdfs_client = hdfs_client
    if self._hdfs_client.status(path, strict=False) is not None:
      raise BeamIOError('Path already exists: %s' % path)

    self._handle_context = self._hdfs_client.write(path)
    self._handle = self._handle_context.__enter__()

  def put(self, data):
    self._handle.write(data)

  def finish(self):
    self._handle.__exit__(None, None, None)
    self._handle = None
    self._handle_context = None


class HadoopFileSystem(FileSystem):
  """``FileSystem`` implementation that supports HDFS.

  URL arguments to methods expect strings starting with ``hdfs://``.

  Experimental; TODO(BEAM-3600): Writes are experimental until file rename
    retries are better handled.
  """

  def __init__(self, pipeline_options):
    """Initializes a connection to HDFS.

    Connection configuration is done by passing pipeline options.
    See :class:`~apache_beam.options.pipeline_options.HadoopFileSystemOptions`.
    """
    super(HadoopFileSystem, self).__init__(pipeline_options)

    if pipeline_options is None:
      raise ValueError('pipeline_options is not set')
    hdfs_options = pipeline_options.view_as(HadoopFileSystemOptions)
    if hdfs_options.hdfs_host is None:
      raise ValueError('hdfs_host is not set')
    if hdfs_options.hdfs_port is None:
      raise ValueError('hdfs_port is not set')
    if hdfs_options.hdfs_user is None:
      raise ValueError('hdfs_user is not set')
    self._hdfs_client = hdfs.InsecureClient(
        'http://%s:%s' % (
            hdfs_options.hdfs_host, str(hdfs_options.hdfs_port)),
        user=hdfs_options.hdfs_user)

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
      raise BeamIOError('Path already exists: %s' % path)
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

    def _match(path_pattern, limit):
      """Find all matching paths to the pattern provided."""
      fs = self._hdfs_client.status(path_pattern, strict=False)
      if fs and fs[_FILE_STATUS_TYPE] == _FILE_STATUS_TYPE_FILE:
        file_statuses = [(fs[_FILE_STATUS_PATH_SUFFIX], fs)][:limit]
      else:
        file_statuses = self._hdfs_client.list(path_pattern,
                                               status=True)[:limit]
      metadata_list = [FileMetadata(file_status[1][_FILE_STATUS_NAME],
                                    file_status[1][_FILE_STATUS_SIZE])
                       for file_status in file_statuses]
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

  @staticmethod
  def _add_compression(stream, path, mime_type, compression_type):
    if mime_type != 'application/octet-stream':
      logging.warning('Mime types are not supported. Got non-default mime_type:'
                      ' %s', mime_type)
    if compression_type == CompressionTypes.AUTO:
      compression_type = CompressionTypes.detect_compression_type(path)
    if compression_type != CompressionTypes.UNCOMPRESSED:
      return CompressedFile(stream)

    return stream

  def create(self, url, mime_type='application/octet-stream',
             compression_type=CompressionTypes.AUTO):
    """
    Returns:
      A Python File-like object.
    """
    path = self._parse_url(url)
    return self._create(path, mime_type, compression_type)

  def _create(self, path, mime_type='application/octet-stream',
              compression_type=CompressionTypes.AUTO):
    stream = io.BufferedWriter(
        filesystemio.UploaderStream(
            HdfsUploader(self._hdfs_client, path)),
        buffer_size=_DEFAULT_BUFFER_SIZE)
    return self._add_compression(stream, path, mime_type, compression_type)

  def open(self, url, mime_type='application/octet-stream',
           compression_type=CompressionTypes.AUTO):
    """
    Returns:
      A Python File-like object.
    """
    path = self._parse_url(url)
    return self._open(path, mime_type, compression_type)

  def _open(self, path, mime_type='application/octet-stream',
            compression_type=CompressionTypes.AUTO):
    stream = io.BufferedReader(
        filesystemio.DownloaderStream(
            HdfsDownloader(self._hdfs_client, path)),
        buffer_size=_DEFAULT_BUFFER_SIZE)
    return self._add_compression(stream, path, mime_type, compression_type)

  def copy(self, source_file_names, destination_file_names):
    """
    It is an error if any file to copy already exists at the destination.

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
      if self._hdfs_client.status(
          source)[_FILE_STATUS_TYPE] != _FILE_STATUS_TYPE_DIRECTORY:
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
        try:
          self._hdfs_client.rename(rel_source, rel_destination)
        except hdfs.HdfsError as e:
          raise BeamIOError(
              'libhdfs error in renaming %s to %s' % (source, destination), e)
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
    return self._hdfs_client.status(path, strict=False) is not None

  def delete(self, urls):
    exceptions = {}
    for url in urls:
      try:
        path = self._parse_url(url)
        self._hdfs_client.delete(path, recursive=True)
      except Exception as e:  # pylint: disable=broad-except
        exceptions[url] = e

    if exceptions:
      raise BeamIOError("Delete operation failed", exceptions)
