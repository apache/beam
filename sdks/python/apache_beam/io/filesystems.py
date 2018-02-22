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

"""FileSystems interface class for accessing the correct filesystem"""

import re

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileSystem

# All filesystem implements should be added here as
# best effort imports. We don't want to force loading
# a module if the user doesn't supply the correct
# packages that these filesystems rely on.
#
# pylint: disable=wrong-import-position, unused-import
try:
  from apache_beam.io.hadoopfilesystem import HadoopFileSystem
except ImportError:
  pass

try:
  from apache_beam.io.localfilesystem import LocalFileSystem
except ImportError:
  pass

try:
  from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
except ImportError:
  pass
# pylint: enable=wrong-import-position, unused-import

__all__ = ['FileSystems']


class FileSystems(object):
  """A class that defines the functions that can be performed on a filesystem.
  All methods are static and access the underlying registered filesystems.
  """
  URI_SCHEMA_PATTERN = re.compile('(?P<scheme>[a-zA-Z][-a-zA-Z0-9+.]*)://.*')

  _pipeline_options = None

  @classmethod
  def set_options(cls, pipeline_options):
    """Set filesystem options.

    Args:
      pipeline_options: Instance of ``PipelineOptions``.
    """
    cls._pipeline_options = pipeline_options

  @staticmethod
  def get_scheme(path):
    match_result = FileSystems.URI_SCHEMA_PATTERN.match(path.strip())
    if match_result is None:
      return None
    return match_result.groupdict()['scheme']

  @staticmethod
  def get_filesystem(path):
    """Get the correct filesystem for the specified path
    """
    try:
      path_scheme = FileSystems.get_scheme(path)
      systems = [fs for fs in FileSystem.get_all_subclasses()
                 if fs.scheme() == path_scheme]
      if len(systems) == 0:
        raise ValueError('Unable to get the Filesystem for path %s' % path)
      elif len(systems) == 1:
        return systems[0](pipeline_options=FileSystems._pipeline_options)
      else:
        raise ValueError('Found more than one filesystem for path %s' % path)
    except ValueError:
      raise
    except Exception as e:
      raise BeamIOError('Unable to get the Filesystem', {path: e})

  @staticmethod
  def join(basepath, *paths):
    """Join two or more pathname components for the filesystem

    Args:
      basepath: string path of the first component of the path
      paths: path components to be added

    Returns: full path after combining all the passed components
    """
    filesystem = FileSystems.get_filesystem(basepath)
    return filesystem.join(basepath, *paths)

  @staticmethod
  def split(path):
    """Splits the given path into two parts.

    Splits the path into a pair (head, tail) such that tail contains the last
    component of the path and head contains everything up to that.

    For file-systems other than the local file-system, head should include the
    prefix.

    Args:
      path: path as a string
    Returns:
      a pair of path components as strings.
    """
    filesystem = FileSystems.get_filesystem(path)
    return filesystem.split(path)

  @staticmethod
  def mkdirs(path):
    """Recursively create directories for the provided path.

    Args:
      path: string path of the directory structure that should be created

    Raises:
      IOError if leaf directory already exists.
    """
    filesystem = FileSystems.get_filesystem(path)
    return filesystem.mkdirs(path)

  @staticmethod
  def match(patterns, limits=None):
    """Find all matching paths to the patterns provided.

    Args:
      patterns: list of string for the file path pattern to match against
      limits: list of maximum number of responses that need to be fetched

    Returns: list of ``MatchResult`` objects.

    Raises:
      ``BeamIOError`` if any of the pattern match operations fail
    """
    if len(patterns) == 0:
      return []
    filesystem = FileSystems.get_filesystem(patterns[0])
    return filesystem.match(patterns, limits)

  @staticmethod
  def create(path, mime_type='application/octet-stream',
             compression_type=CompressionTypes.AUTO):
    """Returns a write channel for the given file path.

    Args:
      path: string path of the file object to be written to the system
      mime_type: MIME type to specify the type of content in the file object
      compression_type: Type of compression to be used for this object. See
        ``CompressionTypes`` for possible values.

    Returns: file handle with a ``close`` function for the user to use.
    """
    filesystem = FileSystems.get_filesystem(path)
    return filesystem.create(path, mime_type, compression_type)

  @staticmethod
  def open(path, mime_type='application/octet-stream',
           compression_type=CompressionTypes.AUTO):
    """Returns a read channel for the given file path.

    Args:
      path: string path of the file object to be written to the system
      mime_type: MIME type to specify the type of content in the file object
      compression_type: Type of compression to be used for this object. See
        ``CompressionTypes`` for possible values.

    Returns: file handle with a ``close`` function for the user to use.
    """
    filesystem = FileSystems.get_filesystem(path)
    return filesystem.open(path, mime_type, compression_type)

  @staticmethod
  def copy(source_file_names, destination_file_names):
    """Recursively copy the file list from the source to the destination

    Args:
      source_file_names: list of source file objects that needs to be copied
      destination_file_names: list of destination of the new object

    Raises:
      ``BeamIOError`` if any of the copy operations fail
    """
    if len(source_file_names) == 0:
      return
    filesystem = FileSystems.get_filesystem(source_file_names[0])
    return filesystem.copy(source_file_names, destination_file_names)

  @staticmethod
  def rename(source_file_names, destination_file_names):
    """Rename the files at the source list to the destination list.
    Source and destination lists should be of the same size.

    Args:
      source_file_names: List of file paths that need to be moved
      destination_file_names: List of destination_file_names for the files

    Raises:
      ``BeamIOError`` if any of the rename operations fail
    """
    if len(source_file_names) == 0:
      return
    filesystem = FileSystems.get_filesystem(source_file_names[0])
    return filesystem.rename(source_file_names, destination_file_names)

  @staticmethod
  def exists(path):
    """Check if the provided path exists on the FileSystem.

    Args:
      path: string path that needs to be checked.

    Returns: boolean flag indicating if path exists
    """
    filesystem = FileSystems.get_filesystem(path)
    return filesystem.exists(path)

  @staticmethod
  def delete(paths):
    """Deletes files or directories at the provided paths.
    Directories will be deleted recursively.

    Args:
      paths: list of paths that give the file objects to be deleted

    Raises:
      ``BeamIOError`` if any of the delete operations fail
    """
    if len(paths) == 0:
      return
    filesystem = FileSystems.get_filesystem(paths[0])
    return filesystem.delete(paths)

  @staticmethod
  def get_chunk_size(path):
    """Get the correct chunk size for the FileSystem.

    Args:
      path: string path that needs to be checked.

    Returns: integer size for parallelization in the FS operations.
    """
    filesystem = FileSystems.get_filesystem(path)
    return filesystem.CHUNK_SIZE
