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

"""Archive system."""

# pytype: skip-file

from __future__ import absolute_import

from future.utils import iteritems

from apache_beam.io.aws import s3io
from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystem import FileSystem
from six import BytesIO

from zipfile import ZipFile, ZipInfo
import datetime

__all__ = ['ZipFileSystemMixin']

class ArchiveFileSystemMixin(FileSystem):
  """An mixin for `FileSystem` representing the contents of an archive file.
  
  Usage:

  class CustomFileSystem(ZipFileSystemMixin, LocalFileSystem):
    pass

  """
  def __init__(self, *args, **kwargs):
    self.archive_path = kwargs.pop("archive_path")
    super().__init__(*args, **kwargs)

  @classmethod
  def scheme(cls):
    """URI scheme for the FileSystem
    """
    return "NOSCHEME"

class ZipFileSystemMixin(ArchiveFileSystemMixin):
  """An mixin for `FileSystem` representing the contents of .zip files.
  
  Usage:

  class CustomFileSystem(ZipFileSystemMixin, LocalFileSystem):
    pass

  """
  
  @classmethod
  def extension(cls):
    """File extensions
    """
    return "zip"

  def _read_archive_file(self):
    return ZipFile(super().open(self.archive_path), "r")

  def mkdirs(self, path):
    """Recursively create directories for the provided path.

    Args:
      path: string path of the directory structure that should be created

    Raises:
      IOError: if leaf directory already exists.
    """
    pass

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
      with self._read_archive_file() as f:
        for info in f.infolist():
          if info.filename.startswith(dir_or_prefix):
            if not dir_or_prefix: # if empty string
              rest_of_path = dir_or_prefix
            else:
              rest_of_path = info.filename.split(dir_or_prefix)[1]
            if rest_of_path.strip("/").count("/") == 0:
              # We only want to include items that are a single directory below --
              # infolist() gives us a recursive list by default.
              yield FileMetadata(info.filename, info.file_size, _parent_archive_paths = [self.archive_path] )
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
    return self._path_open_helper(path, 'wb', mime_type, compression_type)

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
    return self._path_open_helper(path, 'rb', mime_type, compression_type)

  def copy(self, source_file_names, destination_file_names):
    """Recursively copy the file tree from the source to the destination

    Args:
      source_file_names: list of source file objects that needs to be copied
      destination_file_names: list of destination of the new object

    Raises:
      ``BeamIOError``: if any of the copy operations fail
    """
    raise NotImplementedError

  def rename(self, source_file_names, destination_file_names):
    """Rename the files at the source list to the destination list.
    Source and destination lists should be of the same size.

    Args:
      source_file_names: List of file paths that need to be moved
      destination_file_names: List of destination_file_names for the files

    Raises:
      ``BeamIOError``: if any of the rename operations fail
    """
    raise NotImplementedError

  def exists(self, path):
    """Check if the provided path exists on the FileSystem.

    Args:
      path: string path that needs to be checked.

    Returns: boolean flag indicating if path exists
    """
    try:
      with self._read_archive_file() as f:
        try:
          raw_file = f.getinfo(path)
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
        info = f.getinfo(path)
        return info.file_size # Uncompressed size
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
        info = f.getinfo(path)
        return datetime.datetime(*info.date_time).timestamp()
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError("last_updated operation failed", {path: e})

  def checksum(self, path):
    """Fetch checksum metadata of a file on the
    :class:`~apache_beam.io.filesystem.FileSystem`.

    Args:
      path: string path of a file.

    Returns: string containing checksum

    Raises:
      ``BeamIOError``: if path isn't a file or doesn't exist.
    """
    try:
      with self._read_archive_file() as f:
        info = f.getinfo(path)
        return str(info.CRC)
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError("Checksum operation failed", {path: e})

  def delete(self, paths):
    """Deletes files or directories at the provided paths.
    Directories will be deleted recursively.

    Args:
      paths: list of paths that give the file objects to be deleted
    """
    raise NotImplementedError
