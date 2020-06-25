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
"""Azure Blob Storage Implementation for accesing files on Azure Blob Storage."""

from __future__ import absolute_import


from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystem import FileSystem


__all__ = ['BlobStorageFileSystem']


class BlobStorageFileSystem(FileSystem):
  """An Azure Blob Storage ``FileSystem`` implementation for accesing files on Azure
  Blob Storage.
  """

  CHUNK_SIZE = 1 
  AZURE_FILE_SYSTEM_PREFIX = 'azfs://'

  @classmethod
  def scheme(cls):
    """URI scheme for the FileSystem
    """
    raise NotImplementedError


  def join(self, basepath, *paths):
    # type: (str, *str) -> str

    """Join two or more pathname components for the filesystem

    Args:
      basepath: string path of the first component of the path
      paths: path components to be added

    Returns: full path after combining all the passed components
    """
    raise NotImplementedError


  def split(self, path):
    # type: (str) -> Tuple[str, str]

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
    raise NotImplementedError


  def mkdirs(self, path):
    """Recursively create directories for the provided path.

    Args:
      path: string path of the directory structure that should be created

    Raises:
      IOError: if leaf directory already exists.
    """
    raise NotImplementedError

  
  def has_dirs(self):
    """Whether this FileSystem supports directories."""
    raise NotImplementedError