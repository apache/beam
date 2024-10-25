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

# pytype: skip-file

import io
import logging
import posixpath
import re
from typing import BinaryIO  # pylint: disable=unused-import

import hdfs

from apache_beam.io import filesystemio
from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystem import FileSystem
from apache_beam.options.pipeline_options import HadoopFileSystemOptions
from apache_beam.options.pipeline_options import PipelineOptions

__all__ = ['HadoopFileSystem']

_HDFS_PREFIX = 'hdfs:/'
_URL_RE = re.compile(r'^' + _HDFS_PREFIX + r'(/.*)')
_FULL_URL_RE = re.compile(r'^' + _HDFS_PREFIX + r'/([^/]+)(/.*)*')
_COPY_BUFFER_SIZE = 2**16
_DEFAULT_BUFFER_SIZE = 20 * 1024 * 1024

# WebHDFS FileChecksum property constants.
_FILE_CHECKSUM_ALGORITHM = 'algorithm'
_FILE_CHECKSUM_BYTES = 'bytes'
_FILE_CHECKSUM_LENGTH = 'length'
# WebHDFS FileStatus property constants.
_FILE_STATUS_LENGTH = 'length'
_FILE_STATUS_UPDATED = 'modificationTime'
_FILE_STATUS_PATH_SUFFIX = 'pathSuffix'
_FILE_STATUS_TYPE = 'type'
_FILE_STATUS_TYPE_DIRECTORY = 'DIRECTORY'
_FILE_STATUS_TYPE_FILE = 'FILE'

_LOGGER = logging.getLogger(__name__)


class HdfsDownloader(filesystemio.Downloader):
  def __init__(self, hdfs_client, path):
    self._hdfs_client = hdfs_client
    self._path = path
    self._size = self._hdfs_client.status(path)[_FILE_STATUS_LENGTH]

  @property
  def size(self):
    return self._size

  def get_range(self, start, end):
    with self._hdfs_client.read(self._path, offset=start,
                                length=end - start) as reader:
      return reader.read()


class HdfsUploader(filesystemio.Uploader):
  def __init__(self, hdfs_client, path):
    self._hdfs_client = hdfs_client
    if self._hdfs_client.status(path, strict=False) is not None:
      raise BeamIOError('Path already exists: %s' % path)

    self._handle_context = self._hdfs_client.write(path)
    self._handle = self._handle_context.__enter__()

  def put(self, data):
    # hdfs uses an async writer which first add data to a queue. To avoid buffer
    # gets reused upstream a deepcopy is required here.
    self._handle.write(bytes(data))

  def finish(self):
    self._handle.__exit__(None, None, None)
    self._handle = None
    self._handle_context = None


class HadoopFileSystem(FileSystem):
  """``FileSystem`` implementation that supports HDFS.

  URL arguments to methods expect strings starting with ``hdfs://``.
  """
  def __init__(self, pipeline_options):
    """Initializes a connection to HDFS.

    Connection configuration is done by passing pipeline options.
    See :class:`~apache_beam.options.pipeline_options.HadoopFileSystemOptions`.
    """
    super().__init__(pipeline_options)
    logging.getLogger('hdfs.client').setLevel(logging.WARN)
    if pipeline_options is None:
      raise ValueError('pipeline_options is not set')
    if isinstance(pipeline_options, PipelineOptions):
      hdfs_options = pipeline_options.view_as(HadoopFileSystemOptions)
      hdfs_host = hdfs_options.hdfs_host
      hdfs_port = hdfs_options.hdfs_port
      hdfs_user = hdfs_options.hdfs_user
      self._full_urls = hdfs_options.hdfs_full_urls
    else:
      hdfs_host = pipeline_options.get('hdfs_host')
      hdfs_port = pipeline_options.get('hdfs_port')
      hdfs_user = pipeline_options.get('hdfs_user')
      self._full_urls = pipeline_options.get('hdfs_full_urls', False)

    if hdfs_host is None:
      raise ValueError('hdfs_host is not set')
    if hdfs_port is None:
      raise ValueError('hdfs_port is not set')
    if hdfs_user is None:
      raise ValueError('hdfs_user is not set')
    if not isinstance(self._full_urls, bool):
      raise ValueError(
          'hdfs_full_urls should be bool, got: %s', self._full_urls)
    self._hdfs_client = hdfs.InsecureClient(
        'http://%s:%s' % (hdfs_host, str(hdfs_port)), user=hdfs_user)

  @classmethod
  def scheme(cls):
    return 'hdfs'

  def _parse_url(self, url):
    """Verifies that url begins with hdfs:// prefix, strips it and adds a
    leading /.

    Parsing behavior is determined by HadoopFileSystemOptions.hdfs_full_urls.

    Args:
      url: (str) A URL in the form hdfs://path/...
        or in the form hdfs://server/path/...

    Raises:
      ValueError if the URL doesn't match the expect format.

    Returns:
      (str, str) If using hdfs_full_urls, for an input of
      'hdfs://server/path/...' will return (server, '/path/...').
      Otherwise, for an input of 'hdfs://path/...', will return
      ('', '/path/...').
    """
    if not self._full_urls:
      m = _URL_RE.match(url)
      if m is None:
        raise ValueError('Could not parse url: %s' % url)
      return '', m.group(1)
    else:
      m = _FULL_URL_RE.match(url)
      if m is None:
        raise ValueError('Could not parse url: %s' % url)
      return m.group(1), m.group(2) or '/'

  def join(self, base_url, *paths):
    """Join two or more pathname components.

    Args:
      base_url: string path of the first component of the path.
        Must start with hdfs://.
      paths: path components to be added

    Returns:
      Full url after combining all the passed components.
    """
    server, basepath = self._parse_url(base_url)
    return _HDFS_PREFIX + self._join(server, basepath, *paths)

  def _join(self, server, basepath, *paths):
    res = posixpath.join(basepath, *paths)
    if server:
      server = '/' + server
    return server + res

  def split(self, url):
    server, rel_path = self._parse_url(url)
    if server:
      server = '/' + server
    head, tail = posixpath.split(rel_path)
    return _HDFS_PREFIX + server + head, tail

  def mkdirs(self, url):
    _, path = self._parse_url(url)
    if self._exists(path):
      raise BeamIOError('Path already exists: %s' % path)
    return self._mkdirs(path)

  def _mkdirs(self, path):
    self._hdfs_client.makedirs(path)

  def has_dirs(self):
    return True

  def _list(self, url):
    try:
      server, path = self._parse_url(url)
      for res in self._hdfs_client.list(path, status=True):
        yield FileMetadata(
            _HDFS_PREFIX + self._join(server, path, res[0]),
            res[1][_FILE_STATUS_LENGTH],
            res[1][_FILE_STATUS_UPDATED] / 1000.0)
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError('List operation failed', {url: e})

  @staticmethod
  def _add_compression(stream, path, mime_type, compression_type):
    if mime_type != 'application/octet-stream':
      _LOGGER.warning(
          'Mime types are not supported. Got non-default mime_type:'
          ' %s',
          mime_type)
    if compression_type == CompressionTypes.AUTO:
      compression_type = CompressionTypes.detect_compression_type(path)
    if compression_type != CompressionTypes.UNCOMPRESSED:
      return CompressedFile(stream)

    return stream

  def create(
      self,
      url,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO) -> BinaryIO:
    """
    Returns:
      A Python File-like object.
    """
    _, path = self._parse_url(url)
    return self._create(path, mime_type, compression_type)

  def _create(
      self,
      path,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO):
    stream = io.BufferedWriter(
        filesystemio.UploaderStream(HdfsUploader(self._hdfs_client, path)),
        buffer_size=_DEFAULT_BUFFER_SIZE)
    return self._add_compression(stream, path, mime_type, compression_type)

  def open(
      self,
      url,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO) -> BinaryIO:
    """
    Returns:
      A Python File-like object.
    """
    _, path = self._parse_url(url)
    return self._open(path, mime_type, compression_type)

  def _open(
      self,
      path,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO):
    stream = io.BufferedReader(
        filesystemio.DownloaderStream(HdfsDownloader(self._hdfs_client, path)),
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
          'be equal in length: %d != %d' %
          (len(source_file_names), len(destination_file_names)))

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
          new_dir = self._join('', destination, dir)
          if not self._exists(new_dir):
            self._mkdirs(new_dir)

        rel_path = posixpath.relpath(path, source)
        if rel_path == '.':
          rel_path = ''
        for file in files:
          _copy_file(
              self._join('', path, file),
              self._join('', destination, rel_path, file))

    exceptions = {}
    for source, destination in zip(source_file_names, destination_file_names):
      try:
        _, rel_source = self._parse_url(source)
        _, rel_destination = self._parse_url(destination)
        _copy_path(rel_source, rel_destination)
      except Exception as e:  # pylint: disable=broad-except
        exceptions[(source, destination)] = e

    if exceptions:
      raise BeamIOError('Copy operation failed', exceptions)

  def rename(self, source_file_names, destination_file_names):
    exceptions = {}
    for source, destination in zip(source_file_names, destination_file_names):
      try:
        _, rel_source = self._parse_url(source)
        _, rel_destination = self._parse_url(destination)
        try:
          self._hdfs_client.rename(rel_source, rel_destination)
        except hdfs.HdfsError as e:
          raise BeamIOError(
              'libhdfs error in renaming %s to %s' % (source, destination), e)
      except Exception as e:  # pylint: disable=broad-except
        exceptions[(source, destination)] = e

    if exceptions:
      raise BeamIOError('Rename operation failed', exceptions)

  def exists(self, url: str) -> bool:
    """Checks existence of url in HDFS.

    Args:
      url: String in the form hdfs://...

    Returns:
      True if url exists as a file or directory in HDFS.
    """
    _, path = self._parse_url(url)
    return self._exists(path)

  def _exists(self, path):
    """Returns True if path exists as a file or directory in HDFS.

    Args:
      path: String in the form /...
    """
    return self._hdfs_client.status(path, strict=False) is not None

  def size(self, url):
    """Fetches file size for a URL.

    Returns:
      int size of path according to the FileSystem.

    Raises:
      ``BeamIOError``: if url doesn't exist.
    """
    return self.metadata(url).size_in_bytes

  def last_updated(self, url):
    """Fetches last updated time for a URL.

    Args:
      url: string url of file.

    Returns: float UNIX Epoch time

    Raises:
      ``BeamIOError``: if path doesn't exist.
    """
    return self.metadata(url).last_updated_in_seconds

  def checksum(self, url):
    """Fetches a checksum description for a URL.

    Returns:
      String describing the checksum.

    Raises:
      ``BeamIOError``: if url doesn't exist.
    """
    _, path = self._parse_url(url)
    file_checksum = self._hdfs_client.checksum(path)
    return '%s-%d-%s' % (
        file_checksum[_FILE_CHECKSUM_ALGORITHM],
        file_checksum[_FILE_CHECKSUM_LENGTH],
        file_checksum[_FILE_CHECKSUM_BYTES],
    )

  def metadata(self, url):
    """Fetch metadata fields of a file on the FileSystem.

    Args:
      url: string url of a file.

    Returns:
      :class:`~apache_beam.io.filesystem.FileMetadata`.

    Raises:
      ``BeamIOError``: if url doesn't exist.
    """
    _, path = self._parse_url(url)
    status = self._hdfs_client.status(path, strict=False)
    if status is None:
      raise BeamIOError('File not found: %s' % url)
    return FileMetadata(
        url, status[_FILE_STATUS_LENGTH], status[_FILE_STATUS_UPDATED] / 1000.0)

  def delete(self, urls):
    exceptions = {}
    for url in urls:
      try:
        _, path = self._parse_url(url)
        self._hdfs_client.delete(path, recursive=True)
      except Exception as e:  # pylint: disable=broad-except
        exceptions[url] = e

    if exceptions:
      raise BeamIOError("Delete operation failed", exceptions)
