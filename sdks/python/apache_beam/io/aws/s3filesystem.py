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

"""S3 file system implementation for accessing files on AWS S3."""

# pytype: skip-file

from __future__ import absolute_import

from future.utils import iteritems

from apache_beam.io.aws import s3io
from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystem import FileSystem

__all__ = ['S3FileSystem']


class S3FileSystem(FileSystem):
  """An S3 `FileSystem` implementation for accessing files on AWS S3
  """

  CHUNK_SIZE = s3io.MAX_BATCH_OPERATION_SIZE
  S3_PREFIX = 's3://'

  @classmethod
  def scheme(cls):
    """URI scheme for the FileSystem
    """
    return 's3'

  def join(self, basepath, *paths):
    """Join two or more pathname components for the filesystem

    Args:
      basepath: string path of the first component of the path
      paths: path components to be added

    Returns: full path after combining all of the return nulled components
    """
    if not basepath.startswith(S3FileSystem.S3_PREFIX):
      raise ValueError('Basepath %r must be S3 path.' % basepath)

    path = basepath
    for p in paths:
      path = path.rstrip('/') + '/' + p.lstrip('/')
    return path

  def split(self, path):
    """Splits the given path into two parts.

    Splits the path into a pair (head, tail) such that tail contains the last
    component of the path and head contains everything up to that.

    Head will include the S3 prefix ('s3://').

    Args:
      path: path as a string
    Returns:
      a pair of path components as strings.
    """
    path = path.strip()
    if not path.startswith(S3FileSystem.S3_PREFIX):
      raise ValueError('Path %r must be S3 path.' % path)

    prefix_len = len(S3FileSystem.S3_PREFIX)
    last_sep = path[prefix_len:].rfind('/')
    if last_sep >= 0:
      last_sep += prefix_len

    if last_sep > 0:
      return (path[:last_sep], path[last_sep + 1:])
    elif last_sep < 0:
      return (path, '')
    else:
      raise ValueError('Invalid path: %s' % path)

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
      for path, size in iteritems(s3io.S3IO().list_prefix(dir_or_prefix)):
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
    raw_file = s3io.S3IO().open(path, mode, mime_type=mime_type)
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
    return self._path_open(path, 'wb', mime_type, compression_type)

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

  def copy(self, source_file_names, destination_file_names):
    """Recursively copy the file tree from the source to the destination

    Args:
      source_file_names: list of source file objects that needs to be copied
      destination_file_names: list of destination of the new object

    Raises:
      ``BeamIOError``: if any of the copy operations fail
    """
    if not len(source_file_names) == len(destination_file_names):
      message = 'Unable to copy unequal number of sources and destinations'
      raise BeamIOError(message)
    src_dest_pairs = list(zip(source_file_names, destination_file_names))
    return s3io.S3IO().copy_paths(src_dest_pairs)

  def rename(self, source_file_names, destination_file_names):
    """Rename the files at the source list to the destination list.
    Source and destination lists should be of the same size.

    Args:
      source_file_names: List of file paths that need to be moved
      destination_file_names: List of destination_file_names for the files

    Raises:
      ``BeamIOError``: if any of the rename operations fail
    """
    if not len(source_file_names) == len(destination_file_names):
      message = 'Unable to rename unequal number of sources and destinations'
      raise BeamIOError(message)
    src_dest_pairs = list(zip(source_file_names, destination_file_names))
    results = s3io.S3IO().rename_files(src_dest_pairs)
    exceptions = {(src, dest): error
                  for (src, dest, error) in results if error is not None}
    if exceptions:
      raise BeamIOError("Rename operation failed", exceptions)

  def exists(self, path):
    """Check if the provided path exists on the FileSystem.

    Args:
      path: string path that needs to be checked.

    Returns: boolean flag indicating if path exists
    """
    try:
      return s3io.S3IO().exists(path)
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
      return s3io.S3IO().size(path)
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
      return s3io.S3IO().last_updated(path)
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
      return s3io.S3IO().checksum(path)
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError("Checksum operation failed", {path: e})

  def delete(self, paths):
    """Deletes files or directories at the provided paths.
    Directories will be deleted recursively.

    Args:
      paths: list of paths that give the file objects to be deleted
    """
    results = s3io.S3IO().delete_paths(paths)
    exceptions = {
        path: error
        for (path, error) in results.items() if error is not None
    }
    if exceptions:
      raise BeamIOError("Delete operation failed", exceptions)
