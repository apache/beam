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

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import datetime
import os
import sys
import tempfile
import urllib

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import filesystems
from apache_beam.io import textio
from apache_beam.io import tfrecordio
from apache_beam.transforms import combiners

if sys.version_info[0] > 2:
  unquote_to_bytes = urllib.parse.unquote_to_bytes
  quote = urllib.parse.quote
else:
  unquote_to_bytes = urllib.unquote  # pylint: disable=deprecated-urllib-function
  quote = urllib.quote  # pylint: disable=deprecated-urllib-function


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

    Args:
      *labels: List of labels for PCollection instance.

    Returns:
      Tuple[List[Any], int]: A tuple containing a list of items in the
        PCollection and the version number.

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

  def save_pcoder(self, pcoder, *labels):
    """Saves pcoder for given PCollection.

    Correct reading of PCollection from Cache requires PCoder to be known.
    This method saves desired PCoder for PCollection that will subsequently
    be used by sink(...), source(...), and, most importantly, read(...) method.
    The latter must be able to read a PCollection written by Beam using
    non-Beam IO.

    Args:
      pcoder: A PCoder to be used for reading and writing a PCollection.
      *labels: List of labels for PCollection instance.
    """
    raise NotImplementedError

  def load_pcoder(self, *labels):
    """Returns previously saved PCoder for reading and writing PCollection."""
    raise NotImplementedError

  def cleanup(self):
    """Cleans up all the PCollection caches."""
    raise NotImplementedError


class FileBasedCacheManager(CacheManager):
  """Maps PCollections to local temp files for materialization."""

  _available_formats = {
      'text': (textio.ReadFromText, textio.WriteToText),
      'tfrecord': (tfrecordio.ReadFromTFRecord, tfrecordio.WriteToTFRecord)
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
    self._reader_class, self._writer_class = self._available_formats[
        cache_format]
    self._default_pcoder = (
        SafeFastPrimitivesCoder() if cache_format == 'text' else None)

    # List of saved pcoders keyed by PCollection path. It is OK to keep this
    # list in memory because once FileBasedCacheManager object is
    # destroyed/re-created it loses the access to previously written cache
    # objects anyways even if cache_dir already exists. In other words,
    # it is not possible to resume execution of Beam pipeline from the
    # saved cache if FileBasedCacheManager has been reset.
    #
    # However, if we are to implement better cache persistence, one needs
    # to take care of keeping consistency between the cached PCollection
    # and its PCoder type.
    self._saved_pcoders = {}

  def exists(self, *labels):
    return bool(self._match(*labels))

  def _latest_version(self, *labels):
    timestamp = 0
    for path in self._match(*labels):
      timestamp = max(timestamp, filesystems.FileSystems.last_updated(path))
    result = self._versions["-".join(labels)].get_version(timestamp)
    return result

  def save_pcoder(self, pcoder, *labels):
    self._saved_pcoders[self._path(*labels)] = pcoder

  def load_pcoder(self, *labels):
    return (
        self._default_pcoder if self._default_pcoder is not None else
        self._saved_pcoders[self._path(*labels)])

  def read(self, *labels):
    if not self.exists(*labels):
      return [], -1

    source = self.source(*labels)
    range_tracker = source.get_range_tracker(None, None)
    result = list(source.read(range_tracker))
    version = self._latest_version(*labels)
    return result, version

  def source(self, *labels):
    return self._reader_class(
        self._glob_path(*labels), coder=self.load_pcoder(*labels))._source

  def sink(self, *labels):
    return self._writer_class(
        self._path(*labels), coder=self.load_pcoder(*labels))._sink

  def cleanup(self):
    if filesystems.FileSystems.exists(self._cache_dir):
      filesystems.FileSystems.delete([self._cache_dir])
    self._saved_pcoders = {}

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

    # We save pcoder that is necessary for proper reading of
    # cached PCollection. _cache_manager.sink(...) call below
    # should be using this saved pcoder.
    self._cache_manager.save_pcoder(
        coders.registry.get_coder(pcoll.element_type), prefix, self._label)

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
    return quote(
        coders.coders.FastPrimitivesCoder().encode(value)).encode('utf-8')

  def decode(self, value):
    return coders.coders.FastPrimitivesCoder().decode(unquote_to_bytes(value))
