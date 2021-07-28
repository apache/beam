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

from __future__ import absolute_import

import io
import logging
import os
import posixpath
import re
import subprocess
from builtins import zip
from pyarrow import fs as pyarrow_fs
from pyarrow.fs import FileInfo, FileType, FileSelector
from typing import BinaryIO  # pylint: disable=unused-import

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

_LOGGER = logging.getLogger(__name__)


class HdfsDownloader(filesystemio.Downloader):
  def __init__(self, hdfs_fs, path):
    self._hdfs_fs = hdfs_fs
    self._path = path

  @property
  def size(self):
    file_info: FileInfo = self._hdfs_fs.get_file_info(self._path)
    size = 0 if file_info.size is None else file_info.size
    return size

  def get_range(self, start, end):
    with self._hdfs_fs.open_input_file(self._path) as file_handle:
      return file_handle.read_at(nbytes=end - start, offset=start)


class HdfsUploader(filesystemio.Uploader):
  def __init__(self, hdfs_fs, path):
    self._hdfs_fs = hdfs_fs
    self._path = path
    if self._exists(path):
      raise BeamIOError(f'Path already exists: {path}')
    self._file_handle = self._hdfs_fs.open_output_stream(self._path)

  def _exists(self, path):
    file_info: FileInfo = self._hdfs_fs.get_file_info(path)
    return file_info.type != FileType.NotFound

  def put(self, data):
    self._file_handle.write(data)

  def finish(self):
    self._file_handle.flush()
    self._file_handle.close()


class HadoopFileSystem(FileSystem):
  """``FileSystem`` implementation that supports HDFS.

  URL arguments to methods expect strings starting with ``hdfs://``.
  """
  def __init__(self, pipeline_options, **kwargs):
    """Initializes a connection to HDFS.

    Connection configuration is done by passing pipeline options.
    See :class:`~apache_beam.options.pipeline_options.HadoopFileSystemOptions`.
    """
    super(HadoopFileSystem, self).__init__(pipeline_options)
    if pipeline_options is None:
      pipeline_options = PipelineOptions()
      """TODO
      pipeline_options will always be None as they are not set in the sdk_worker at runtime.
      Current beam versions have changed this; uncomment the line below and throw an error once upstream changes are merged.
      raise ValueError('pipeline_options is not set')
      """

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
    """
    TODO
    REMOVE this once open source upstream beam changes are merged to LinkedIn's li_trunk.
    Currently, beam doesn't pass the pipeline options to the sdk worker process. Inspite of setting hdfs_full_urls
    pipeline option to true, it will always be false. Tensorflow's hdfs file io requires the file format is in the
    form of hdfs://default/path/to/the/file. default here is the namenode. We want to ensure that beam hdfs filesystem
    can process files in this format and for that we need to set hdfs_full_urls to true.
    Latest changes in beam ensure that beam passes the pipeline options from the main program to the sdk worker. Once
    the changes are merged to li_trunk, we can remove the line below.
    """
    self._full_urls = True
    if not isinstance(self._full_urls, bool):
      raise ValueError(
        'hdfs_full_urls should be bool, got: %s', self._full_urls)

    if 'user_hdfs_fs' in kwargs:
      """ If the user passes the fs explicitly, use that and return. Used for testing"""
      self._hdfs_fs = kwargs.get('user_hdfs_fs')
      return

    """ CLASSPATH: must contain the Hadoop jars """
    classpath = subprocess.Popen([f"{os.environ['HADOOP_HOME']}/bin/hdfs", 'classpath', '--glob'],
                                 stdout=subprocess.PIPE).communicate()[0]
    os.environ['CLASSPATH'] = classpath.decode('utf-8').rstrip()

    if hdfs_host is None and \
            hdfs_port is None and \
            hdfs_user is None:
      try:
        self._hdfs_fs = pyarrow_fs.HadoopFileSystem('')
      except Exception as e:
        raise BeamIOError(
          'Error while trying to create pyarrow HadoopFileSystem', e)
    else:
      if hdfs_host is None:
        raise ValueError('hdfs_host is not set')
      if hdfs_port is None:
        raise ValueError('hdfs_port is not set')
      if hdfs_user is None:
        raise ValueError('hdfs_user is not set')
      try:
        self._hdfs_fs = pyarrow_fs.HadoopFileSystem(hdfs_host, hdfs_port, user=hdfs_user)
      except Exception as e:
        raise BeamIOError(
          'Error while trying to create pyarrow HadoopFileSystem', e)

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
    self._hdfs_fs.create_dir(path=path, recursive=True)

  def has_dirs(self):
    return True

  def _list(self, url):
    try:
      server, path = self._parse_url(url)
      for info in self._hdfs_fs.get_file_info(FileSelector(path)):
        # size 0 is returned as None, replace it with 0
        size = 0 if info.size is None else info.size
        yield FileMetadata(_HDFS_PREFIX + self._join(server, info.path), size)
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
          compression_type=CompressionTypes.AUTO):
    # type: (...  ) -> BinaryIO

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
      filesystemio.UploaderStream(HdfsUploader(self._hdfs_fs, path)),
      buffer_size=_DEFAULT_BUFFER_SIZE)
    return self._add_compression(stream, path, mime_type, compression_type)

  def open(
      self,
      url,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO):
    # type: (...) -> BinaryIO

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
        filesystemio.DownloaderStream(HdfsDownloader(self._hdfs_fs, path)),
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

    def _copy_file(src, dest):
      with self._open(src) as f1:
        with self._create(dest) as f2:
          while True:
            buf = f1.read(_COPY_BUFFER_SIZE)
            if not buf:
              break
            f2.write(buf)

    def _copy_path(src, dest):
      """Recursively copy the file tree from the source to the destination."""
      file_info: FileInfo = self._hdfs_fs.get_file_info(src)
      if file_info.type == FileType.NotFound:
        raise BeamIOError(f"Cannot copy file {file_info.path} as it was not found.")
      if file_info.type != FileType.Directory:
        _copy_file(src, dest)
        return
      base_path = src
      all_file_infos = self._hdfs_fs.get_file_info(FileSelector(base_path, recursive=True))
      files_info = [file_info for file_info in all_file_infos if file_info.type == FileType.File]
      dirs_info = [file_info for file_info in all_file_infos if file_info.type == FileType.Directory]

      # create dirs
      for dir_info in dirs_info:
        dir_rel_path = posixpath.relpath(dir_info.path, base_path)
        new_dir_path = self.join(destination, dir_rel_path)
        if not self.exists(new_dir_path):
          self.mkdirs(new_dir_path)

      # create files
      for file_info in files_info:
        file_path = file_info.path
        rel_path = posixpath.relpath(file_path, base_path)
        dest = self.join(destination, rel_path)
        _, dest_path = self._parse_url(dest)
        _copy_file(file_path, dest_path)

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
          self._hdfs_fs.move(src=rel_source, dest=rel_destination)
        except Exception as e:
          raise BeamIOError(
              'Error in renaming %s to %s' % (source, destination), e)
      except Exception as e:  # pylint: disable=broad-except
        exceptions[(source, destination)] = e

    if exceptions:
      raise BeamIOError('Rename operation failed', exceptions)

  def exists(self, url):
    # type: (str) -> bool

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
    file_info: FileInfo = self._hdfs_fs.get_file_info(path)
    return file_info.type != FileType.NotFound

  def size(self, url):
    _, path = self._parse_url(url)
    return self._size(path)

  def _size(self, path):
    file_info: FileInfo = self._hdfs_fs.get_file_info(path)
    # size 0 is returned as None, replace it with 0
    size = 0 if file_info.size is None else file_info.size
    return size

  def last_updated(self, url):
    _, path = self._parse_url(url)
    file_info: FileInfo = self._hdfs_fs.get_file_info(path)
    return int(file_info.mtime_ns / 10**6)

  def checksum(self, url):
    """Fetches a checksum description for a URL.

    Returns:
      String describing the checksum.
    """
    _, path = self._parse_url(url)
    # pyarrow HadoopFileSystem doesn't provide the checksum, use size as alternative
    return str(self._size(path=path))

  def delete(self, urls):
    exceptions = {}
    for url in urls:
      try:
        _, path = self._parse_url(url)
        file_info: FileInfo = self._hdfs_fs.get_file_info(path)
        file_type = file_info.type
        if file_type == FileType.File:
          self._delete_file(path)
        elif file_type == FileType.Directory:
          self._delete_dir(path)
        elif file_type == FileType.NotFound:
          raise Exception(f"File {path} not found and cannot be deleted.")
        else:
          raise Exception(f"File {path} not found and cannot be deleted.")
      except Exception as e:  # pylint: disable=broad-except
        exceptions[url] = e

    if exceptions:
      raise BeamIOError("Delete operation failed", exceptions)

  def _delete_dir(self, path):
    self._hdfs_fs.delete_dir(path)

  def _delete_file(self, path):
    self._hdfs_fs.delete_file(path)
