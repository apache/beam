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
"""A PCollectionCache abstract base class defines an interface for reading and
writing bounded and unbounded PCollections.

This module is experimental. No backwards-compatibility guarantees.
"""

from __future__ import absolute_import

import abc

from future.utils import with_metaclass


class PCollectionCache(with_metaclass(abc.ABCMeta)):

  @abc.abstractmethod
  def __init__(self, cache_spec, overwrite, persist, **kwargs):
    """Initialize PCollectionCache.

    Args:
      cache_spec (str): Location where the cache data should be stored.
      overwrite (bool): If ``True``, raise an IOError if data matching
          `cache_spec` already exist. If ``False``, remove existing data
          instead.
      persist (bool): A flag indicating whether the underlying data should be
          destroyed when the cache instance goes out of scope.
      kwargs (Dict[str, Any]): Arguments to be passed to the underlying writer
          and reader PTransforms.
    """
    raise NotImplementedError

  @property
  @abc.abstractmethod
  def persist(self):
    """Whether to persist the underlying data when the object goes out of scope.

    Returns:
      bool: ``True`` if the unerlying data will be persisted once the object
          goes out of scope, ``False`` otherwise.
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
      reader_kwargs (Dict[str, Any]): Arguments to be passed to the
          underlying reader PTransform.

    Returns:
      A PTransform which reads a PCollection from cache.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def writer(self, **writer_kwargs):
    """Return a writer PTransform which can write a PCollection to cache.

    Args:
      writer_kwargs (Dict[str, Any]): Arguments to be passed to the
          underlying writer PTransform.

    Returns:
      A PTransform which writes a PCollection to cache.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def read(self, **reader_kwargs):
    """Return a list of elements inside the cache.

    Args:
      reader_kwargs (Dict[str, Any]): Arguments to be passed to the
          underlying reader PTransform.

    Returns:
      List[Any]: A list of elements in the PCollections.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def write(self, elements, **writer_kwargs):
    """Writes a collection of elements into the cache.

    Args:
      elements (Iterable[Any]): A collection of elements to be written to cache.
      writer_kwargs (Dict[str, Any]): Arguments to be passed to the
          underlying writer PTransform.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def truncate(self):
    """Delete PCollection data from the underlying persistence layer."""
    raise NotImplementedError

  @abc.abstractmethod
  def delete(self):
    """Delete all data and metadata from the cache."""
    raise NotImplementedError
