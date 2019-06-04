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
import urllib

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import filesystems
from apache_beam.transforms import combiners

try:                    # Python 3
  unquote_to_bytes = urllib.parse.unquote_to_bytes
  quote = urllib.parse.quote
except AttributeError:  # Python 2
  # pylint: disable=deprecated-urllib-function
  unquote_to_bytes = urllib.unquote
  quote = urllib.quote


class CacheManager(object):
  """Abstract class for caching PCollections.

  A PCollection cache is identified by labels, which consist of a prefix (either
  'full' or 'sample') and a cache_label which is a hash of the PCollection
  derivation.
  """

  def exists(self, *labels):
    """Returns if the PCollection cache exists."""
    raise NotImplementedError

  def is_latest_version(self, version, *labels):
    """Returns if the given version number is the latest."""
    return version == self._latest_version(*labels)

  def _latest_version(self, *labels):
    """Returns the latest version number of the PCollection cache."""
    raise NotImplementedError

  def read(self, *labels):
    """Return the PCollection as a list as well as the version number.

    Returns:
      (List[PCollection])
      (int) the version number

    It is possible that the version numbers from read() and_latest_version()
    are different. This usually means that the cache's been evicted (thus
    unavailable => read() returns version = -1), but it had reached version n
    before eviction.
    """
    raise NotImplementedError

  def source(self, *labels):
    """Returns a beam.io.Source that reads the PCollection cache."""
    raise NotImplementedError

  def sink(self, *labels):
    """Returns a beam.io.Sink that writes the PCollection cache."""
    raise NotImplementedError

  def cleanup(self):
    """Cleans up all the PCollection caches."""
    raise NotImplementedError


class FileBasedCacheManager(CacheManager):
  """Maps PCollections to local temp files for materialization."""

  def __init__(self, cache_dir=None):
    if cache_dir:
      self._cache_dir = filesystems.FileSystems.join(
          cache_dir,
          datetime.datetime.now().strftime("cache-%y-%m-%d-%H_%M_%S"))
    else:
      self._cache_dir = tempfile.mkdtemp(
          prefix='interactive-temp-', dir=os.environ.get('TEST_TMPDIR', None))
    self._versions = collections.defaultdict(lambda: self._CacheVersion())

  def exists(self, *labels):
    return bool(self._match(*labels))

  def _latest_version(self, *labels):
    timestamp = 0
    for path in self._match(*labels):
      timestamp = max(timestamp, filesystems.FileSystems.last_updated(path))
    result = self._versions["-".join(labels)].get_version(timestamp)
    return result

  def read(self, *labels):
    if not self.exists(*labels):
      return [], -1

    def _read_helper():
      coder = SafeFastPrimitivesCoder()
      for path in self._match(*labels):
        for line in filesystems.FileSystems.open(path):
          yield coder.decode(line.strip())
    result, version = list(_read_helper()), self._latest_version(*labels)
    return result, version

  def source(self, *labels):
    return beam.io.ReadFromText(self._glob_path(*labels),
                                coder=SafeFastPrimitivesCoder())._source

  def sink(self, *labels):
    return beam.io.WriteToText(self._path(*labels),
                               coder=SafeFastPrimitivesCoder())._sink

  def cleanup(self):
    if filesystems.FileSystems.exists(self._cache_dir):
      filesystems.FileSystems.delete([self._cache_dir])

  def _glob_path(self, *labels):
    return self._path(*labels) + '-*-of-*'

  def _path(self, *labels):
    return filesystems.FileSystems.join(self._cache_dir, *labels)

  def _match(self, *labels):
    match = filesystems.FileSystems.match([self._glob_path(*labels)])
    assert len(match) == 1
    return [metadata.path for metadata in match[0].metadata_list]

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
    return pbegin | 'Read' >> beam.io.Read(
        self._cache_manager.source('full', self._label))


class WriteCache(beam.PTransform):
  """A PTransform that writes the PCollections to the cache."""
  def __init__(self, cache_manager, label, sample=False, sample_size=0):
    self._cache_manager = cache_manager
    self._label = label
    self._sample = sample
    self._sample_size = sample_size

  def expand(self, pcoll):
    prefix = 'sample' if self._sample else 'full'
    if self._sample:
      pcoll |= 'Sample' >> (
          combiners.Sample.FixedSizeGlobally(self._sample_size)
          | beam.FlatMap(lambda sample: sample))
    # pylint: disable=expression-not-assigned
    return pcoll | 'Write' >> beam.io.Write(
        self._cache_manager.sink(prefix, self._label))


class SafeFastPrimitivesCoder(coders.Coder):
  """This class add an quote/unquote step to escape special characters."""
  # pylint: disable=deprecated-urllib-function

  def encode(self, value):
    return quote(coders.coders.FastPrimitivesCoder().encode(value)).encode(
        'utf-8')

  def decode(self, value):
    return coders.coders.FastPrimitivesCoder().decode(unquote_to_bytes(value))
