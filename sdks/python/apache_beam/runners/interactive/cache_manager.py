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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import datetime
import os
import tempfile

import apache_beam as beam
from apache_beam.io import filesystems
from apache_beam.runners.interactive.caching import TextBasedCache
from apache_beam.runners.interactive.caching import TFRecordBasedCache
from apache_beam.transforms import combiners


class CacheManager(object):
  """Abstract class for caching PCollections.

  A PCollection cache is identified by labels, which consist of a prefix (either
  'full' or 'sample') and a cache_label which is a hash of the PCollection
  derivation.
  """

  def exists(self, labels):
    """Returns if the PCollection cache exists."""
    raise NotImplementedError

  def is_latest_version(self, version, labels):
    """Returns if the given version number is the latest."""
    return version == self._latest_version(labels)

  def _latest_version(self, labels):
    """Returns the latest version number of the PCollection cache."""
    raise NotImplementedError

  def read(self, labels):
    """Return the PCollection as a list as well as the version number.

    Args:
      labels: List of labels for PCollection instance.

    Returns:
      Tuple[List[Any], int]: A tuple containing a list of items in the
        PCollection and the version number.

    It is possible that the version numbers from read() and_latest_version()
    are different. This usually means that the cache's been evicted (thus
    unavailable => read() returns version = -1), but it had reached version n
    before eviction.
    """
    raise NotImplementedError

  def create(self, labels):
    """Create a new cache instance and associate it with labels.

    Args:
      labels: List of labels uniquely identifying a cache.

    Returns:
      PCollectionCache: The created cache instance.
    """
    raise NotImplementedError

  def get(self, labels):
    """Return the cache instance associated with labels.

    Args:
      labels: List of labels uniquely identifying a cache.

    Returns:
      PCollectionCache: The cache associated with the given labels.
    """
    raise NotImplementedError

  def remove(self, labels):
    """Remove and clear the cache instance associated with labels.

    Args:
      labels: List of labels uniquely identifying a cache.
    """
    raise NotImplementedError

  def cleanup(self):
    """Cleans up all the PCollection caches."""
    raise NotImplementedError


class FileBasedCacheManager(CacheManager):
  """Maps PCollections to local temp files for materialization."""

  _available_formats = {
      'text': TextBasedCache,
      'tfrecord': TFRecordBasedCache,
  }

  def __init__(self, cache_dir=None, cache_format='text'):
    if cache_dir:
      self._cache_dir = filesystems.FileSystems.join(
          cache_dir,
          datetime.datetime.now().strftime("cache-%y-%m-%d-%H_%M_%S"))
    else:
      self._cache_dir = tempfile.mkdtemp(
          prefix='interactive-temp-', dir=os.environ.get('TEST_TMPDIR', None))
    self._versions = collections.defaultdict(lambda: self._CacheVersion())

    if cache_format not in self._available_formats:
      raise ValueError("Unsupported cache format: '%s'." % cache_format)
    self._cache_class = self._available_formats[cache_format]
    self._cache_store = {}

  def exists(self, labels):
    return labels in self._cache_store

  def _latest_version(self, labels):
    timestamp = self.get(labels).timestamp if self.exists(labels) else 0
    result = self._versions["-".join(labels)].get_version(timestamp)
    return result

  def read(self, labels):
    # TODO(ostrokach): This method is kept for backwards-compatibility.
    # It should be removed in favor of `cache_manager.get(labels).read()`.
    if not self.exists(labels):
      return [], -1

    cache = self.get(labels)
    try:
      result = list(cache.read())
    except IOError:
      return [], -1
    version = self._latest_version(labels)
    return result, version

  def create(self, labels, if_exists="overwrite"):
    if_exists_options = ["error", "append", "overwrite"]
    if if_exists not in if_exists_options:
      raise ValueError(
          "if_exists must be one of: '{}'.".format(if_exists_options))

    if self.exists(labels):
      if if_exists == "error":
        raise IOError(
            "A cache with the labels '{}' already exists.".format(labels))
      elif if_exists == "overwrite":
        self.remove(labels)
      elif if_exists == "append":
        return self.get(labels)

    cache_location = self._path(labels)
    self._cache_store[labels] = self._cache_class(cache_location)
    return self._cache_store[labels]

  def get(self, labels):
    return self._cache_store[labels]

  def remove(self, labels):
    if self.exists(labels):
      self._cache_store.pop(labels).clear()

  def cleanup(self):
    for key in list(self._cache_store):
      self._cache_store.pop(key).clear()
    if filesystems.FileSystems.exists(self._cache_dir):
      filesystems.FileSystems.delete([self._cache_dir])

  def _path(self, labels):
    if isinstance(labels, (list, tuple)):
      return filesystems.FileSystems.join(self._cache_dir, *labels)
    else:
      return filesystems.FileSystems.join(self._cache_dir, labels)

  class _CacheVersion(object):
    """This class keeps track of the timestamp and the corresponding version."""

    def __init__(self):
      self.current_version = -1
      self.current_timestamp = 0

    def get_version(self, timestamp):
      """Updates version if necessary and returns the version number.

      Args:
        timestamp: (int) unix timestamp when the cache is updated. This value is
            zero if the cache has been evicted or doesn't exist.
      """
      # Do not update timestamp if the cache's been evicted.
      if timestamp != 0 and timestamp != self.current_timestamp:
        assert timestamp > self.current_timestamp
        self.current_version = self.current_version + 1
        self.current_timestamp = timestamp
      return self.current_version


class ReadCache(beam.PTransform):
  """A PTransform that reads the PCollections from the cache."""
  def __init__(self, cache_manager, label):
    self._cache_manager = cache_manager
    self._label = label

  def expand(self, pbegin):
    # pylint: disable=expression-not-assigned
    return pbegin | 'Read' >> self._cache_manager.get(
        ('full', self._label,)).reader()


class WriteCache(beam.PTransform):
  """A PTransform that writes the PCollections to the cache."""
  def __init__(self, cache_manager, label, sample=False, sample_size=0):
    self._cache_manager = cache_manager
    self._label = label
    self._sample = sample
    self._sample_size = sample_size

    prefix = 'sample' if self._sample else 'full'
    cache_manager.create((prefix, self._label,))

  def expand(self, pcoll):
    prefix = 'sample' if self._sample else 'full'

    if self._sample:
      pcoll |= 'Sample' >> (
          combiners.Sample.FixedSizeGlobally(self._sample_size)
          | beam.FlatMap(lambda sample: sample))
    # pylint: disable=expression-not-assigned
    return pcoll | 'Write' >> self._cache_manager.get(
        (prefix, self._label,)).writer()
