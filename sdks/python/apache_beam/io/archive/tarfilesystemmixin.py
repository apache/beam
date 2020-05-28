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

"""Tar file system mixin."""

# pytype: skip-file

from __future__ import absolute_import

from future.utils import iteritems

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystem import FileSystem
from apache_beam.io.archive.archivefilesystemmixinbase import ArchiveFileSystemMixinBase
from six import BytesIO

from tarfile import TarFile, TarInfo
import datetime

__all__ = ['TarFileSystemMixin']

class TarFileSystemMixin(ArchiveFileSystemMixinBase):
  """An mixin for `FileSystem` representing the contents of .tar files.
  
  Usage:

  class CustomFileSystem(TarFileSystemMixin, LocalFileSystem):
    pass
  system = CustomFileSystem(pipeline_options=pipeline_options, archive_path=archive_path)

  """
  
  @classmethod
  def extension(cls):
    """File extension
    """
    # TODO: how to deal with .tar.gz's?
    return "tar"

  def _read_archive_file(self):
    return TarFile(super(ArchiveFileSystemMixinBase, self).open(self.archive_path), "r")

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
      with self._read_archive_file() as f:
        for info in f:
          if info.name.startswith(dir_or_prefix):
            if not dir_or_prefix: # if empty string
              rest_of_path = dir_or_prefix
            else:
              rest_of_path = info.name.split(dir_or_prefix)[1]
            if rest_of_path.strip("/").count("/") == 0:
              # We only want to include items that are a single directory below --
              # infolist() gives us a recursive list by default.
              yield FileMetadata(info.name, info.size, _parent_archive_paths = [self.archive_path] )
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError("List operation failed", {dir_or_prefix: e})

  def _path_open_helper(
      self,
      path,
      mode,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO):
    """Helper functions to open a file in the provided mode.
    """
    with self._read_archive_file() as f:
      raw_file = f.open(path, mode)
      if compression_type == CompressionTypes.UNCOMPRESSED:
        return raw_file
      return CompressedFile(raw_file, compression_type=compression_type)

  def create(
      self,
      path,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO):
    """Returns a write channel for the given file path.

    Args:
      path: string path of the file object to be written to the system
      mime_type: MIME type to specify the type of content in the file object
      compression_type: Type of compression to be used for this object

    Returns: file handle with a close function for the user to use
    """
    return self._path_open_helper(path, 'w', mime_type, compression_type)

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
    return self._path_open_helper(path, 'r', mime_type, compression_type)

  def exists(self, path):
    """Check if the provided path exists on the FileSystem.

    Args:
      path: string path that needs to be checked.

    Returns: boolean flag indicating if path exists
    """
    try:
      with self._read_archive_file() as f:
        try:
          raw_file = f.getmember(path)
          return True
        except KeyError:
          return False
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError("exists() operation failed", {path: e})

  def size(self, path):
    """Get size of path on the FileSystem.

    Args:
      path: string path in question.

    Returns: int size of path according to the FileSystem.

    Raises:
      ``BeamIOError``: if path doesn't exist.
    """
    try:
      with self._read_archive_file() as f:
        info = f.getmember(path)
        return info.size
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError("size() operation failed", {path: e})

  def last_updated(self, path):
    """Get UNIX Epoch time in seconds on the FileSystem.

    Args:
      path: string path of file.

    Returns: float UNIX Epoch time

    Raises:
      ``BeamIOError``: if path doesn't exist.
    """
    try:
      with self._read_archive_file() as f:
        info = f.getmember(path)
        return info.mtime
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError("last_updated operation failed", {path: e})

  def checksum(self, path):
    """Fetch checksum metadata of a file on the
    :class:`~apache_beam.io.filesystem.FileSystem`.

    Args:
      path: string path of a file.

    Returns: string containing checksum (in this case, just the file size)

    Raises:
      ``BeamIOError``: if path isn't a file or doesn't exist.
    """
    try:
      with self._read_archive_file() as f:
        info = f.getmember(path)
        return info.size
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError("Checksum operation failed", {path: e})