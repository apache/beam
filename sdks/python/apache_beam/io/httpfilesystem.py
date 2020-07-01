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

"""HTTP file system implementation for accessing files from a HTTP URL."""

# pytype: skip-file

from __future__ import absolute_import

from future.utils import iteritems

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystem import FileSystem
from apache_beam.io import httpio

__all__ = ['HttpFileSystem', 'HttpsFileSystem']


class HttpFileSystem(FileSystem):
  """A `FileSystem` implementation for accessing files from a http:// URL
  """
  @classmethod
  def scheme(cls):
    """URI scheme for the FileSystem
    """
    return 'http'

  def join(self, basepath, *paths):
    """Join two or more pathname components for the filesystem

    Args:
      basepath: string path of the first component of the path
      paths: path components to be added

    Returns: full path after combining all of the return nulled components
    """
    if not basepath.startswith(HttpFileSystem.scheme() + "://"):
      raise ValueError('Basepath %r must be a valid URL.' % basepath)

    path = basepath
    for p in paths:
      path = path.rstrip('/') + '/' + p.lstrip('/')
    return path

  def split(self, path):
    """Splits the given path into two parts.

    Splits the path into a pair (head, tail) such that tail contains the last
    component of the path and head contains everything up to that.

    Head will include the HTTP prefix ('http://').

    Args:
      path: path as a string
    Returns:
      a pair of path components as strings.
    """
    path = path.strip()
    if not path.startswith(HttpFileSystem.scheme() + "://"):
      raise ValueError('Path %r must be a valid URL.' % path)

    prefix_len = len(HttpFileSystem.scheme() + "://")
    last_sep = path[prefix_len:].rfind('/')
    if last_sep >= 0:
      last_sep += prefix_len

    if last_sep > 0:
      return (path[:last_sep], path[last_sep + 1:])
    elif last_sep < 0:
      return (path, '')
    else:
      raise ValueError('Invalid path: %s' % path)

  def has_dirs(self):
    """Whether this FileSystem supports directories."""
    return False

  def _list(self, dir_or_prefix):
    """List files in a location.

    Listing is non-recursive, for filesystems that support directories.

    Args:
      dir_or_prefix: (string) A directory or location prefix (for filesystems
        that don't have directories).

    Returns:
      Generator of ``FileMetadata`` objects.

    Raises:
      ``BeamIOError``: if listing fails, but not if no files were found.
    """
    try:
      for path, size in iteritems(httpio.HttpIO().list_prefix(dir_or_prefix)):
        yield FileMetadata(path, size)
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError("List operation failed", {dir_or_prefix: e})

  def _path_open(
      self,
      path,
      mode,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO):
    """Helper functions to open a file in the provided mode.
    """
    compression_type = FileSystem._get_compression_type(path, compression_type)
    mime_type = CompressionTypes.mime_type(compression_type, mime_type)
    # TODO: pass mime_type into .open() once HttpIO supports write operations.
    raw_file = httpio.HttpIO().open(path, mode)
    if compression_type == CompressionTypes.UNCOMPRESSED:
      return raw_file
    return CompressedFile(raw_file, compression_type=compression_type)

  def open(
      self,
      path,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO):
    """Returns a read channel for the given file path.

    Args:
      path: string path of the file object to be written to the system
      mime_type: MIME type to specify the type of content in the file object
      compression_type: Type of compression to be used for this object

    Returns: file handle with a close function for the user to use
    """
    return self._path_open(path, 'rb', mime_type, compression_type)

  def size(self, path):
    """Get size of path on the FileSystem.

    Args:
      path: string path in question.

    Returns: int size of path according to the FileSystem.

    Raises:
      ``BeamIOError``: if path doesn't exist.
    """
    try:
      return httpio.HttpIO().size(path)
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError("size() operation failed", {path: e})

  def exists(self, path):
    """Check if the provided path exists on the FileSystem.

    Args:
      path: string path that needs to be checked.

    Returns: boolean flag indicating if path exists
    """
    try:
      return httpio.HttpIO().exists(path)
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError("exists() operation failed", {path: e})

  def mkdirs(self, path):
    raise NotImplementedError

  def create(
      self,
      path,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO):
    raise NotImplementedError

  def copy(self, source_file_names, destination_file_names):
    raise NotImplementedError

  def rename(self, source_file_names, destination_file_names):
    raise NotImplementedError

  def last_updated(self, path):
    raise NotImplementedError

  def delete(self, paths):
    raise NotImplementedError


class HttpsFileSystem(HttpFileSystem):
  """A `FileSystem` implementation for accessing files from a https:// URL
  """
  @classmethod
  def scheme(cls):
    """URI scheme for the FileSystem
    """
    return 'https'
