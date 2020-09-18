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

"""Local File system implementation for accessing files on disk."""

# pytype: skip-file

from __future__ import absolute_import

import io
import os
import shutil
from builtins import zip
from typing import BinaryIO  # pylint: disable=unused-import

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystem import FileSystem

__all__ = ['LocalFileSystem']


class LocalFileSystem(FileSystem):
  """A Local ``FileSystem`` implementation for accessing files on disk.
  """
  @classmethod
  def scheme(cls):
    """URI scheme for the FileSystem
    """
    return None

  def join(self, basepath, *paths):
    """Join two or more pathname components for the filesystem

    Args:
      basepath: string path of the first component of the path
      paths: path components to be added

    Returns: full path after combining all the passed components
    """
    return os.path.join(basepath, *paths)

  def split(self, path):
    """Splits the given path into two parts.

    Splits the path into a pair (head, tail) such that tail contains the last
    component of the path and head contains everything up to that.

    Args:
      path: path as a string
    Returns:
      a pair of path components as strings.
    """
    return os.path.split(os.path.abspath(path))

  def mkdirs(self, path):
    """Recursively create directories for the provided path.

    Args:
      path: string path of the directory structure that should be created

    Raises:
      IOError: if leaf directory already exists.
    """
    try:
      os.makedirs(path)
    except OSError as err:
      raise IOError(err)

  def has_dirs(self):
    """Whether this FileSystem supports directories."""
    return True

  def _url_dirname(self, url_or_path):
    """Pass through to os.path.dirname.

    This version uses os.path instead of posixpath to be compatible with the
    host OS.

    Args:
      url_or_path: A string in the form of /some/path.
    """
    return os.path.dirname(url_or_path)

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
    if not self.exists(dir_or_prefix):
      return

    def list_files(root):
      for dirpath, _, files in os.walk(root):
        for filename in files:
          yield self.join(dirpath, filename)

    try:
      for f in list_files(dir_or_prefix):
        try:
          yield FileMetadata(f, os.path.getsize(f))
        except OSError:
          # Files may disappear, such as when listing /tmp.
          pass
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
    raw_file = io.open(path, mode)
    if compression_type == CompressionTypes.UNCOMPRESSED:
      return raw_file
    else:
      return CompressedFile(raw_file, compression_type=compression_type)

  def create(
      self,
      path,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO) -> BinaryIO:

    """Returns a write channel for the given file path.

    Args:
      path: string path of the file object to be written to the system
      mime_type: MIME type to specify the type of content in the file object
      compression_type: Type of compression to be used for this object

    Returns: file handle with a close function for the user to use
    """
    if not os.path.exists(os.path.dirname(path)):
      # TODO(Py3): Add exist_ok parameter.
      os.makedirs(os.path.dirname(path))
    return self._path_open(path, 'wb', mime_type, compression_type)

  def open(
      self,
      path,
      mime_type='application/octet-stream',
      compression_type=CompressionTypes.AUTO) -> BinaryIO:

    """Returns a read channel for the given file path.

    Args:
      path: string path of the file object to be written to the system
      mime_type: MIME type to specify the type of content in the file object
      compression_type: Type of compression to be used for this object

    Returns: file handle with a close function for the user to use
    """
    return self._path_open(path, 'rb', mime_type, compression_type)

  def copy(self, source_file_names, destination_file_names):
    """Recursively copy the file tree from the source to the destination

    Args:
      source_file_names: list of source file objects that needs to be copied
      destination_file_names: list of destination of the new object

    Raises:
      ``BeamIOError``: if any of the copy operations fail
    """
    err_msg = (
        "source_file_names and destination_file_names should "
        "be equal in length")
    assert len(source_file_names) == len(destination_file_names), err_msg

    def _copy_path(source, destination):
      """Recursively copy the file tree from the source to the destination
      """
      try:
        if os.path.exists(destination):
          if os.path.isdir(destination):
            shutil.rmtree(destination)
          else:
            os.remove(destination)
        if os.path.isdir(source):
          shutil.copytree(source, destination)
        else:
          shutil.copy2(source, destination)
      except OSError as err:
        raise IOError(err)

    exceptions = {}
    for source, destination in zip(source_file_names, destination_file_names):
      try:
        _copy_path(source, destination)
      except Exception as e:  # pylint: disable=broad-except
        exceptions[(source, destination)] = e

    if exceptions:
      raise BeamIOError("Copy operation failed", exceptions)

  def rename(self, source_file_names, destination_file_names):
    """Rename the files at the source list to the destination list.
    Source and destination lists should be of the same size.

    Args:
      source_file_names: List of file paths that need to be moved
      destination_file_names: List of destination_file_names for the files

    Raises:
      ``BeamIOError``: if any of the rename operations fail
    """
    err_msg = (
        "source_file_names and destination_file_names should "
        "be equal in length")
    assert len(source_file_names) == len(destination_file_names), err_msg

    def _rename_file(source, destination):
      """Rename a single file object"""
      try:
        os.rename(source, destination)
      except OSError as err:
        raise IOError(err)

    exceptions = {}
    for source, destination in zip(source_file_names, destination_file_names):
      try:
        _rename_file(source, destination)
      except Exception as e:  # pylint: disable=broad-except
        exceptions[(source, destination)] = e

    if exceptions:
      raise BeamIOError("Rename operation failed", exceptions)

  def exists(self, path):
    """Check if the provided path exists on the FileSystem.

    Args:
      path: string path that needs to be checked.

    Returns: boolean flag indicating if path exists
    """
    return os.path.exists(path)

  def size(self, path):
    """Get size of path on the FileSystem.

    Args:
      path: string path in question.

    Returns: int size of path according to the FileSystem.

    Raises:
      ``BeamIOError``: if path doesn't exist.
    """
    try:
      return os.path.getsize(path)
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError("Size operation failed", {path: e})

  def last_updated(self, path):
    """Get UNIX Epoch time in seconds on the FileSystem.

    Args:
      path: string path of file.

    Returns: float UNIX Epoch time

    Raises:
      ``BeamIOError``: if path doesn't exist.
    """
    if not self.exists(path):
      raise BeamIOError('Path does not exist: %s' % path)
    return os.path.getmtime(path)

  def checksum(self, path):
    """Fetch checksum metadata of a file on the
    :class:`~apache_beam.io.filesystem.FileSystem`.

    Args:
      path: string path of a file.

    Returns: string containing file size.

    Raises:
      ``BeamIOError``: if path isn't a file or doesn't exist.
    """
    if not self.exists(path):
      raise BeamIOError('Path does not exist: %s' % path)
    return str(os.path.getsize(path))

  def delete(self, paths):
    """Deletes files or directories at the provided paths.
    Directories will be deleted recursively.

    Args:
      paths: list of paths that give the file objects to be deleted

    Raises:
      ``BeamIOError``: if any of the delete operations fail
    """
    def _delete_path(path):
      """Recursively delete the file or directory at the provided path.
      """
      try:
        if os.path.isdir(path):
          shutil.rmtree(path)
        else:
          os.remove(path)
      except OSError as err:
        raise IOError(err)

    exceptions = {}

    def try_delete(path):
      try:
        _delete_path(path)
      except Exception as e:  # pylint: disable=broad-except
        exceptions[path] = e

    for match_result in self.match(paths):
      metadata_list = match_result.metadata_list

      if not metadata_list:
        exceptions[match_result.pattern] = \
          IOError('No files found to delete under: %s' % match_result.pattern)

      for metadata in match_result.metadata_list:
        try_delete(metadata.path)

    if exceptions:
      raise BeamIOError("Delete operation failed", exceptions)
