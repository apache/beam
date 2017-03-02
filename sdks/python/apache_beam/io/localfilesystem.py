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
"""Local File system for file based sources and sinks."""

from __future__ import absolute_import

import glob
import os
import shutil

from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileSystem
from apache_beam.io.filesystem import FileMetadata


class LocalFileSystem(FileSystem):
  """A local file system implementation for accessing files on disk.
  """

  @staticmethod
  def mkdirs(path):
    try:
      os.makedirs(path)
    except OSError as err:
      raise IOError(err)

  @staticmethod
  def match(pattern, limit=None):
    files = glob.glob(pattern)
    return [FileMetadata(f, os.path.getsize(f)) for f in files[:limit]]

  @staticmethod
  def _path_open(path, mode, mime_type='application/octet-stream',
                 compression_type=CompressionTypes.AUTO):
    compression_type = FileSystem._get_compression_type(path, compression_type)
    raw_file = open(path, mode)
    if compression_type == CompressionTypes.UNCOMPRESSED:
      return raw_file
    else:
      return CompressedFile(raw_file, compression_type=compression_type)

  @staticmethod
  def create(path, mime_type='application/octet-stream',
             compression_type=CompressionTypes.AUTO):
    return LocalFileSystem._path_open(path, 'wb', mime_type, compression_type)

  @staticmethod
  def open(path, mime_type='application/octet-stream',
           compression_type=CompressionTypes.AUTO):
    return LocalFileSystem._path_open(path, 'rb', mime_type, compression_type)

  @staticmethod
  def copy(source, destination):
    try:
      if os.path.exists(destination):
        shutil.rmtree(destination)
      shutil.copytree(source, destination)
    except OSError as err:
      raise IOError(err)

  @staticmethod
  def _rename_file(src, dest):
    try:
      os.rename(src, dest)
    except OSError as err:
      raise IOError(err)

  @staticmethod
  def rename(sources, destinations):
    exceptions = []
    for src, dest in zip(sources, destinations):
      try:
        LocalFileSystem._rename_file(src, dest)
      except Exception as e:  # pylint: disable=broad-except
        exceptions.append((src, dest, e))
    return exceptions

  @staticmethod
  def exists(path):
    return os.path.exists(path)

  @staticmethod
  def delete(path):
    try:
      if os.path.isdir(path):
        shutil.rmtree(path)
      else:
        os.remove(path)
    except OSError as err:
      raise IOError(err)

  @staticmethod
  def delete_directory(path):
    if os.path.isdir(path):
      LocalFileSystem.delete(path)
    else:
      raise IOError("Path %s is not a directory" % path)
