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

"""GCS file system implementation for accessing files on GCS.

**Updates to the I/O connector code**

For any significant updates to this I/O connector, please consider involving
corresponding code reviewers mentioned in
https://github.com/apache/beam/blob/master/sdks/python/OWNERS
"""

# pytype: skip-file

from typing import BinaryIO  # pylint: disable=unused-import

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystem import FileSystem
from apache_beam.io.gcp import gcsio

__all__ = ['GCSFileSystem']


class GCSFileSystem(FileSystem):
  """A GCS ``FileSystem`` implementation for accessing files on GCS.
  """

  CHUNK_SIZE = gcsio.MAX_BATCH_OPERATION_SIZE  # Chuck size in batch operations
  GCS_PREFIX = 'gs://'

  def __init__(self, pipeline_options):
    super().__init__(pipeline_options)
    self._pipeline_options = pipeline_options

  @classmethod
  def scheme(cls):
    """URI scheme for the FileSystem
    """
    return 'gs'

  def join(self, basepath, *paths):
    """Join two or more pathname components for the filesystem

    Args:
      basepath: string path of the first component of the path
      paths: path components to be added

    Returns: full path after combining all the passed components
    """
    if not basepath.startswith(GCSFileSystem.GCS_PREFIX):
      raise ValueError('Basepath %r must be GCS path.' % basepath)
    path = basepath
    for p in paths:
      path = path.rstrip('/') + '/' + p.lstrip('/')
    return path

  def split(self, path):
    """Splits the given path into two parts.

    Splits the path into a pair (head, tail) such that tail contains the last
    component of the path and head contains everything up to that.

    Head will include the GCS prefix ('gs://').

    Args:
      path: path as a string
    Returns:
      a pair of path components as strings.
    """
    path = path.strip()
    if not path.startswith(GCSFileSystem.GCS_PREFIX):
      raise ValueError('Path %r must be GCS path.' % path)

    prefix_len = len(GCSFileSystem.GCS_PREFIX)
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
      for path, (size, updated) in self._gcsIO().list_files(dir_or_prefix,
                                                            with_metadata=True):
        yield FileMetadata(path, size, updated)
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError("List operation failed", {dir_or_prefix: e})

  def _gcsIO(self):
    return gcsio.GcsIO(pipeline_options=self._pipeline_options)

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
    raw_file = self._gcsIO().open(path, mode, mime_type=mime_type)
    if compression_type == CompressionTypes.UNCOMPRESSED:
      return raw_file
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
      if not destination.startswith(GCSFileSystem.GCS_PREFIX):
        raise ValueError('Destination %r must be GCS path.' % destination)
      # Use copy_tree if the path ends with / as it is a directory
      if source.endswith('/'):
        self._gcsIO().copytree(source, destination)
      else:
        self._gcsIO().copy(source, destination)

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

    gcs_batches = []
    gcs_current_batch = []
    for src, dest in zip(source_file_names, destination_file_names):
      gcs_current_batch.append((src, dest))
      if len(gcs_current_batch) == self.CHUNK_SIZE:
        gcs_batches.append(gcs_current_batch)
        gcs_current_batch = []
    if gcs_current_batch:
      gcs_batches.append(gcs_current_batch)

    # Execute GCS renames if any and return exceptions.
    exceptions = {}
    for batch in gcs_batches:
      copy_statuses = self._gcsIO().copy_batch(batch)
      copy_succeeded = {}
      delete_targets = []
      for src, dest, exception in copy_statuses:
        if exception:
          exceptions[(src, dest)] = exception
        else:
          copy_succeeded[src] = dest
          delete_targets.append(src)
      delete_statuses = self._gcsIO().delete_batch(delete_targets)
      for src, exception in delete_statuses:
        if exception:
          dest = copy_succeeded[src]
          exceptions[(src, dest)] = exception

    if exceptions:
      raise BeamIOError("Rename operation failed", exceptions)

  def exists(self, path):
    """Check if the provided path exists on the FileSystem.

    Args:
      path: string path that needs to be checked.

    Returns: boolean flag indicating if path exists
    """
    return self._gcsIO().exists(path)

  def size(self, path):
    """Get size of path on the FileSystem.

    Args:
      path: string path in question.

    Returns: int size of path according to the FileSystem.

    Raises:
      ``BeamIOError``: if path doesn't exist.
    """
    return self._gcsIO().size(path)

  def last_updated(self, path):
    """Get UNIX Epoch time in seconds on the FileSystem.

    Args:
      path: string path of file.

    Returns: float UNIX Epoch time

    Raises:
      ``BeamIOError``: if path doesn't exist.
    """
    return self._gcsIO().last_updated(path)

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
      return self._gcsIO().checksum(path)
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError("Checksum operation failed", {path: e})

  def metadata(self, path):
    """Fetch metadata fields of a file on the FileSystem.

    Args:
      path: string path of a file.

    Returns:
      :class:`~apache_beam.io.filesystem.FileMetadata`.

    Raises:
      ``BeamIOError``: if path isn't a file or doesn't exist.
    """
    try:
      file_metadata = self._gcsIO()._status(path)
      return FileMetadata(path, file_metadata['size'], file_metadata['updated'])
    except Exception as e:  # pylint: disable=broad-except
      raise BeamIOError("Metadata operation failed", {path: e})

  def delete(self, paths):
    """Deletes files or directories at the provided paths.
    Directories will be deleted recursively.

    Args:
      paths: list of paths that give the file objects to be deleted
    """

    exceptions = {}

    for path in paths:
      if path.endswith('/'):
        path_to_use = path + '*'
      else:
        path_to_use = path
      match_result = self.match([path_to_use])[0]
      statuses = self._gcsIO().delete_batch(
          [m.path for m in match_result.metadata_list])
      for target, exception in statuses:
        if exception:
          exceptions[target] = exception

    if exceptions:
      raise BeamIOError("Delete operation failed", exceptions)

  def report_lineage(self, path, lineage):
    try:
      components = gcsio.parse_gcs_path(path, object_optional=True)
    except ValueError:
      # report lineage is fail-safe
      return
    if len(components) > 1 and components[-1] == '':
      # bucket only
      components = components[:-1]
    lineage.add('gcs', *components)
