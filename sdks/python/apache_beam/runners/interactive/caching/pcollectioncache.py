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
import abc

from future.utils import with_metaclass


class PCollectionCache(with_metaclass(abc.ABCMeta)):

  @property
  @abc.abstractmethod
  def _reader_class(self):
    """A reader PTransform to be used for reading a PCollection from cache.

    Returns:
      PTransform: A PTransform which reads from a file.
    """
    raise NotImplementedError

  @property
  @abc.abstractmethod
  def _writer_class(self):
    """A writer PTransform to be used for writing a PCollection to cache.

    Returns:
      PTransform: A PTransform which writes to a file.
    """
    raise NotImplementedError

  @property
  @abc.abstractmethod
  def _reader_passthrough_arguments(self):
    """Writer arguments, which if provided, shoud be passed onto the reader.

    Returns:
      Set[str]: A set of writer argument names.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def __init__(self, location, **writer_kwargs):
    """Initialize PCollectionCache.

    Args:
      location (str): Location where the cache data should be stored.
      **writer_kwargs: Arguments to pass to the underlying writer class.
    """
    raise NotImplementedError

  @property
  @abc.abstractmethod
  def timestamp(self):
    """Return a timestamp indicating the last time this instance was modified.

    Returns:
      float: A non-negative number indicating the last time the cache was
          modified. Caches that were modified more recently should return
          a larger number.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def reader(self, **reader_kwargs):
    """Return a reader PTransform which can read a PCollection from cache.

    Args:
      **reader_kwargs: Arguments to pass to the underlying reader class.

    Returns:
      A source from which we can read a PCollection.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def writer(self):
    """Return a writer PTransform which can write a PCollection to cache.

    Returns:
      A sink to which we can write a PCollection.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def read(self, limit=None, **reader_kwargs):
    """Return a list of elements inside the cache.

    Args:
      limit: Maximum number of elements that should be returned.
      **reader_kwargs: Arguments to pass to the underlying reader class.

    Returns:
      List[Any]: A list of elements in the PCollections.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def write(self):
    raise NotImplementedError

  @abc.abstractmethod
  def clear(self):
    """Delete PCollection data from the underlying persistence layer."""
    raise NotImplementedError
