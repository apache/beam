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

"""A framework for developing sources for new file types.

To create a source for a new file type a sub-class of ``FileBasedSource`` should
be created. Sub-classes of ``FileBasedSource`` must implement the method
``FileBasedSource.read_records()``. Please read the documentation of that method
for more details.

For an example implementation of ``FileBasedSource`` see ``avroio.AvroSource``.
"""

import bisect
from multiprocessing.pool import ThreadPool
import threading

from apache_beam.io import fileio
from apache_beam.io import iobase
from apache_beam.io import range_trackers

MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 25


class ConcatSource(iobase.BoundedSource):
  """A ``BoundedSource`` that can group a set of ``BoundedSources``."""

  class ConcatRangeTracker(iobase.RangeTracker):

    def __init__(self, start, end, sources, weights=None):
      super(ConcatSource.ConcatRangeTracker, self).__init__()
      self._start = start
      self._end = end
      self._lock = threading.RLock()
      self._sources = sources
      self._range_trackers = [None] * len(self._sources)
      self._claimed_source_ix = self._start[0]

      if weights is None:
        n = max(1, end[0] - start[0])
        self._cumulative_weights = [
          max(0, min(1, float(k) / n))
          for k in range(-start[0], len(self._sources) - start[0] + 1)]
      else:
        assert len(sources) == len(weights)
        relevant_weights = weights[start[0]:end[0] + 1]
        # TODO(robertwb): Implement fraction-at-position to properly scale
        # partial start and end sources.
        total = sum(relevant_weights)
        running_total = [0]
        for w in relevant_weights:
          running_total.append(max(1, running_total[-1] + w / total))
        running_total[-1] = 1  # In case of rounding error.
        self._cumulative_weights = (
          [0] * start[0]
          + running_total
          + [1] * (len(self._sources) - end[0]))

    def start_position(self):
      return self._start

    def stop_position(self):
      return self._end

    def try_claim(self, pos):
      source_ix, source_pos = pos
      with self._lock:
        if source_ix > self._end[0]:
          return False
        elif source_ix == self._end[0] and self._end[1] is None:
          return False
        else:
          self._claimed_source_ix = source_ix
          if source_pos is None:
            return True
          else:
            return self.sub_range_tracker(source_ix).try_claim(source_pos)

    def try_split(self, pos):
      source_ix, source_pos = pos
      with self._lock:
        if source_ix < self._claimed_source_ix:
          # Already claimed.
          return None
        elif source_ix > self._end[0]:
          # After end.
          return None
        elif source_ix == self._end[0] and self._end[1] is None:
          # At/after end.
          return None
        else:
          if source_ix > self._claimed_source_ix:
            # Prefer to split on even boundary.
            split_pos = None
            ratio = self._cumulative_weights[source_ix]
          else:
            # Split the current subsource.
            split = self.sub_range_tracker(source_ix).try_split(
                source_pos)
            if not split:
              return None
            split_pos, frac = split
            ratio = self.local_to_global(source_ix, frac)

          self._end = source_ix, split_pos
          self._cumulative_weights = [min(w / ratio, 1)
                                        for w in self._cumulative_weights]
          return (source_ix, split_pos), ratio

    def set_current_position(self, pos):
      raise NotImplementedError('Should only be called on sub-trackers')

    def position_at_fraction(self, fraction):
      source_ix, source_frac = self.global_to_local(fraction)
      if source_ix == len(self._sources):
        return (source_ix, None)
      else:
        return (source_ix,
                self.sub_range_tracker(source_ix).position_at_fraction(
                    source_frac))

    def fraction_consumed(self):
      with self._lock:
        return self.local_to_global(self._claimed_source_ix,
                                    self.sub_range_tracker(
                                        self._claimed_source_ix)
                                        .fraction_consumed())

    def local_to_global(self, source_ix, source_frac):
      cw = self._cumulative_weights
      return cw[source_ix] + source_frac * (cw[source_ix + 1] - cw[source_ix])

    def global_to_local(self, frac):
      if frac == 1:
        return (len(self._sources), 0)
      else:
        cw = self._cumulative_weights
        source_ix = bisect.bisect(cw, frac) - 1
        return (source_ix,
                (frac - cw[source_ix]) / (cw[source_ix + 1] - cw[source_ix]))

    def sub_range_tracker(self, source_ix):
      assert self._start[0] <= source_ix <= self._end[0]
      if self._range_trackers[source_ix] is None:
        with self._lock:
          if self._range_trackers[source_ix] is None:
            self._range_trackers[source_ix] = (
                self._sources[source_ix].get_range_tracker(
                    self._start[1] if source_ix == self._start[0] else None,
                    self._end[1] if source_ix == self._end[0] else None))
      return self._range_trackers[source_ix]

  def __init__(self, sources):
    self._sources = sources

  @property
  def sources(self):
    return self._sources

  def estimate_size(self):
    return sum(s.estimate_size() for s in self._sources)

  def split(
      self, desired_bundle_size=None, start_position=None, stop_position=None):
    if start_position or stop_position:
      raise ValueError(
          'Multi-level initial splitting is not supported. Expected start and '
          'stop positions to be None. Received %r and %r respectively.' %
          (start_position, stop_position))

    for source in self._sources:
      # We assume all sub-sources to produce bundles that specify weight using
      # the same unit. For example, all sub-sources may specify the size in
      # bytes as their weight.
      for bundle in source.split(desired_bundle_size, None, None):
        yield bundle

  def get_range_tracker(self, start_position=None, stop_position=None):
    if start_position is None:
      start_position = (0, None)
    if stop_position is None:
      stop_position = (len(self._sources), None)
    return self.ConcatRangeTracker(start_position, stop_position, self._sources)

  def read(self, range_tracker):
    start_source, _ = range_tracker.start_position()
    stop_source, stop_pos = range_tracker.stop_position()
    if stop_pos is not None:
      stop_source += 1
    for source_ix in range(start_source, stop_source):
      if not range_tracker.try_claim((source_ix, None)):
        break
      for record in self._sources[source_ix].read(
          range_tracker.sub_range_tracker(source_ix)):
        yield record

  def default_output_coder(self):
    if self._sources:
      # Getting coder from the first sub-sources. This assumes all sub-sources
      # to produce the same coder.
      return self._sources[0].default_output_coder()
    else:
      # Defaulting to PickleCoder.
      return super(ConcatSource, self).default_output_coder()


_ConcatSource = ConcatSource


class FileBasedSource(iobase.BoundedSource):
  """A ``BoundedSource`` for reading a file glob of a given type."""

  def __init__(self,
               file_pattern,
               min_bundle_size=0,
               # TODO(BEAM-614)
               compression_type=fileio.CompressionTypes.NO_COMPRESSION,
               splittable=True):
    """Initializes ``FileBasedSource``.

    Args:
      file_pattern: the file glob to read.
      min_bundle_size: minimum size of bundles that should be generated when
                       performing initial splitting on this source.
      compression_type: compression type to use
      splittable: whether FileBasedSource should try to logically split a single
                  file into data ranges so that different parts of the same file
                  can be read in parallel. If set to False, FileBasedSource will
                  prevent both initial and dynamic splitting of sources for
                  single files. File patterns that represent multiple files may
                  still get split into sources for individual files. Even if set
                  to True by the user, FileBasedSource may choose to not split
                  the file, for example, for compressed files where currently
                  it is not possible to efficiently read a data range without
                  decompressing the whole file.
    Raises:
      TypeError: when compression_type is not valid.
      ValueError: when compression and splittable files are specified.
    """
    self._pattern = file_pattern
    self._concat_source = None
    self._min_bundle_size = min_bundle_size
    if not fileio.CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError('compression_type must be CompressionType object but '
                      'was %s' % type(compression_type))
    self._compression_type = compression_type
    if compression_type != fileio.CompressionTypes.NO_COMPRESSION:
      # We can't split compressed files efficiently so turn off splitting.
      self._splittable = False
    else:
      self._splittable = splittable

  def _get_concat_source(self):
    if self._concat_source is None:
      single_file_sources = []
      file_names = [f for f in fileio.ChannelFactory.glob(self._pattern)]
      sizes = FileBasedSource._estimate_sizes_in_parallel(file_names)

      for index, file_name in enumerate(file_names):
        if sizes[index] == 0:
          continue  # Ignoring empty file.

        single_file_source = _SingleFileSource(
            self, file_name,
            0,
            sizes[index],
            min_bundle_size=self._min_bundle_size)
        single_file_sources.append(single_file_source)
      self._concat_source = ConcatSource(single_file_sources)
    return self._concat_source

  def open_file(self, file_name):
    raw_file = fileio.ChannelFactory.open(
        file_name, 'rb', 'application/octet-stream')
    if self._compression_type == fileio.CompressionTypes.NO_COMPRESSION:
      return raw_file
    else:
      return fileio._CompressedFile(  # pylint: disable=protected-access
          fileobj=raw_file,
          compression_type=self.compression_type)

  @staticmethod
  def _estimate_sizes_in_parallel(file_names):

    def _calculate_size_of_file(file_name):
      return fileio.ChannelFactory.size_in_bytes(file_name)

    return ThreadPool(MAX_NUM_THREADS_FOR_SIZE_ESTIMATION).map(
        _calculate_size_of_file, file_names)

  def split(
      self, desired_bundle_size=None, start_position=None, stop_position=None):
    return self._get_concat_source().split(
        desired_bundle_size=desired_bundle_size,
        start_position=start_position,
        stop_position=stop_position)

  def estimate_size(self):
    return self._get_concat_source().estimate_size()

  def read(self, range_tracker):
    return self._get_concat_source().read(range_tracker)

  def get_range_tracker(self, start_position, stop_position):
    return self._get_concat_source().get_range_tracker(start_position,
                                                       stop_position)

  def default_output_coder(self):
    return self._get_concat_source().default_output_coder()

  def read_records(self, file_name, offset_range_tracker):
    """Returns a generator of records created by reading file 'file_name'.

    Args:
      file_name: a ``string`` that gives the name of the file to be read. Method
                 ``FileBasedSource.open_file()`` must be used to open the file
                 and create a seekable file object.
      offset_range_tracker: a object of type ``OffsetRangeTracker``. This
                            defines the byte range of the file that should be
                            read. See documentation in
                            ``iobase.BoundedSource.read()`` for more information
                            on reading records while complying to the range
                            defined by a given ``RangeTracker``.

    Returns:
      a iterator that gives the records read from the given file.
    """
    raise NotImplementedError

  @property
  def splittable(self):
    return self._splittable


class _SingleFileSource(iobase.BoundedSource):
  """Denotes a source for a specific file type."""

  def __init__(self, file_based_source, file_name, start_offset, stop_offset,
               min_bundle_size=0):
    if not isinstance(start_offset, (int, long)):
      raise TypeError(
          'start_offset must be a number. Received: %r', start_offset)
    if not isinstance(stop_offset, (int, long)):
      raise TypeError(
          'stop_offset must be a number. Received: %r', stop_offset)
    if start_offset >= stop_offset:
      raise ValueError(
          'start_offset must be smaller than stop_offset. Received %d and %d '
          'for start and stop offsets respectively', start_offset, stop_offset)

    self._file_name = file_name
    self._is_gcs_file = file_name.startswith('gs://') if file_name else False
    self._start_offset = start_offset
    self._stop_offset = stop_offset
    self._min_bundle_size = min_bundle_size
    self._file_based_source = file_based_source

  def split(self, desired_bundle_size, start_offset=None, stop_offset=None):
    if start_offset is None:
      start_offset = self._start_offset
    if stop_offset is None:
      stop_offset = self._stop_offset

    if self._file_based_source.splittable:
      bundle_size = max(desired_bundle_size, self._min_bundle_size)

      bundle_start = start_offset
      while bundle_start < stop_offset:
        bundle_stop = min(bundle_start + bundle_size, stop_offset)
        yield iobase.SourceBundle(
            bundle_stop - bundle_start,
            _SingleFileSource(
                self._file_based_source,
                self._file_name,
                bundle_start,
                bundle_stop,
                min_bundle_size=self._min_bundle_size),
            bundle_start,
            bundle_stop)
        bundle_start = bundle_stop
    else:
      yield iobase.SourceBundle(
          stop_offset - start_offset,
          _SingleFileSource(
              self._file_based_source,
              self._file_name,
              start_offset,
              stop_offset,
              min_bundle_size=self._min_bundle_size
          ),
          start_offset,
          stop_offset
      )

  def estimate_size(self):
    return self._stop_offset - self._start_offset

  def get_range_tracker(self, start_position, stop_position):
    if start_position is None:
      start_position = self._start_offset
    if stop_position is None:
      stop_position = self._stop_offset

    range_tracker = range_trackers.OffsetRangeTracker(
        start_position, stop_position)
    if not self._file_based_source.splittable:
      range_tracker = range_trackers.UnsplittableRangeTracker(range_tracker)

    return range_tracker

  def read(self, range_tracker):
    return self._file_based_source.read_records(self._file_name, range_tracker)
