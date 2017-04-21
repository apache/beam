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

from apache_beam.internal import pickler
from apache_beam.io import concat_source
from apache_beam.io import iobase
from apache_beam.io import range_trackers
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems_util import get_filesystem
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.utils.value_provider import ValueProvider
from apache_beam.utils.value_provider import StaticValueProvider
from apache_beam.utils.value_provider import check_accessible

MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 25


class FileBasedSource(iobase.BoundedSource):
  """A ``BoundedSource`` for reading a file glob of a given type."""

  MIN_NUMBER_OF_FILES_TO_STAT = 100
  MIN_FRACTION_OF_FILES_TO_STAT = 0.01

  def __init__(self,
               file_pattern,
               min_bundle_size=0,
               compression_type=CompressionTypes.AUTO,
               splittable=True,
               validate=True):
    """Initializes ``FileBasedSource``.

    Args:
      file_pattern: the file glob to read a string or a ValueProvider
                    (placeholder to inject a runtime value).
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
      validate: Boolean flag to verify that the files exist during the pipeline
                creation time.
    Raises:
      TypeError: when compression_type is not valid or if file_pattern is not a
                 string or a ValueProvider.
      ValueError: when compression and splittable files are specified.
      IOError: when the file pattern specified yields an empty result.
    """

    if not isinstance(file_pattern, (basestring, ValueProvider)):
      raise TypeError('%s: file_pattern must be of type string'
                      ' or ValueProvider; got %r instead'
                      % (self.__class__.__name__, file_pattern))

    if isinstance(file_pattern, basestring):
      file_pattern = StaticValueProvider(str, file_pattern)
    self._pattern = file_pattern
    if file_pattern.is_accessible():
      self._file_system = get_filesystem(file_pattern.get())
    else:
      self._file_system = None

    self._concat_source = None
    self._min_bundle_size = min_bundle_size
    if not CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError('compression_type must be CompressionType object but '
                      'was %s' % type(compression_type))
    self._compression_type = compression_type
    if compression_type in (CompressionTypes.UNCOMPRESSED,
                            CompressionTypes.AUTO):
      self._splittable = splittable
    else:
      # We can't split compressed files efficiently so turn off splitting.
      self._splittable = False
    if validate and file_pattern.is_accessible():
      self._validate()

  def display_data(self):
    return {'file_pattern': DisplayDataItem(str(self._pattern),
                                            label="File Pattern"),
            'compression': DisplayDataItem(str(self._compression_type),
                                           label='Compression Type')}

  @check_accessible(['_pattern'])
  def _get_concat_source(self):
    if self._concat_source is None:
      pattern = self._pattern.get()

      single_file_sources = []
      if self._file_system is None:
        self._file_system = get_filesystem(pattern)
      match_result = self._file_system.match([pattern])[0]
      files_metadata = match_result.metadata_list

      # We create a reference for FileBasedSource that will be serialized along
      # with each _SingleFileSource. To prevent this FileBasedSource from having
      # a reference to ConcatSource (resulting in quadratic space complexity)
      # we clone it here.
      file_based_source_ref = pickler.loads(pickler.dumps(self))

      for file_metadata in files_metadata:
        file_name = file_metadata.path
        file_size = file_metadata.size_in_bytes
        if file_size == 0:
          continue  # Ignoring empty file.

        # We determine splittability of this specific file.
        splittable = self.splittable
        if (splittable and
            self._compression_type == CompressionTypes.AUTO):
          compression_type = CompressionTypes.detect_compression_type(
              file_name)
          if compression_type != CompressionTypes.UNCOMPRESSED:
            splittable = False

        single_file_source = _SingleFileSource(
            file_based_source_ref, file_name,
            0,
            file_size,
            min_bundle_size=self._min_bundle_size,
            splittable=splittable)
        single_file_sources.append(single_file_source)
      self._concat_source = concat_source.ConcatSource(single_file_sources)
    return self._concat_source

  def open_file(self, file_name):
    return get_filesystem(file_name).open(
        file_name, 'application/octet-stream',
        compression_type=self._compression_type)

  @check_accessible(['_pattern'])
  def _validate(self):
    """Validate if there are actual files in the specified glob pattern
    """
    pattern = self._pattern.get()
    if self._file_system is None:
      self._file_system = get_filesystem(pattern)

    # Limit the responses as we only want to check if something exists
    match_result = self._file_system.match([pattern], limits=[1])[0]
    if len(match_result.metadata_list) <= 0:
      raise IOError(
          'No files found based on the file pattern %s' % pattern)

  def split(
      self, desired_bundle_size=None, start_position=None, stop_position=None):
    return self._get_concat_source().split(
        desired_bundle_size=desired_bundle_size,
        start_position=start_position,
        stop_position=stop_position)

  @check_accessible(['_pattern'])
  def estimate_size(self):
    pattern = self._pattern.get()
    if self._file_system is None:
      self._file_system = get_filesystem(pattern)
    match_result = self._file_system.match([pattern])[0]
    return sum([f.size_in_bytes for f in match_result.metadata_list])

  def read(self, range_tracker):
    return self._get_concat_source().read(range_tracker)

  def get_range_tracker(self, start_position, stop_position):
    return self._get_concat_source().get_range_tracker(start_position,
                                                       stop_position)

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
      an iterator that gives the records read from the given file.
    """
    raise NotImplementedError

  @property
  def splittable(self):
    return self._splittable


class _SingleFileSource(iobase.BoundedSource):
  """Denotes a source for a specific file type."""

  def __init__(self, file_based_source, file_name, start_offset, stop_offset,
               min_bundle_size=0, splittable=True):
    if not isinstance(start_offset, (int, long)):
      raise TypeError(
          'start_offset must be a number. Received: %r' % start_offset)
    if stop_offset != range_trackers.OffsetRangeTracker.OFFSET_INFINITY:
      if not isinstance(stop_offset, (int, long)):
        raise TypeError(
            'stop_offset must be a number. Received: %r' % stop_offset)
      if start_offset >= stop_offset:
        raise ValueError(
            'start_offset must be smaller than stop_offset. Received %d and %d '
            'for start and stop offsets respectively' %
            (start_offset, stop_offset))

    self._file_name = file_name
    self._is_gcs_file = file_name.startswith('gs://') if file_name else False
    self._start_offset = start_offset
    self._stop_offset = stop_offset
    self._min_bundle_size = min_bundle_size
    self._file_based_source = file_based_source
    self._splittable = splittable

  def split(self, desired_bundle_size, start_offset=None, stop_offset=None):
    if start_offset is None:
      start_offset = self._start_offset
    if stop_offset is None:
      stop_offset = self._stop_offset

    if self._splittable:
      bundle_size = max(desired_bundle_size, self._min_bundle_size)

      bundle_start = start_offset
      while bundle_start < stop_offset:
        bundle_stop = min(bundle_start + bundle_size, stop_offset)
        yield iobase.SourceBundle(
            bundle_stop - bundle_start,
            _SingleFileSource(
                # Copying this so that each sub-source gets a fresh instance.
                pickler.loads(pickler.dumps(self._file_based_source)),
                self._file_name,
                bundle_start,
                bundle_stop,
                min_bundle_size=self._min_bundle_size,
                splittable=self._splittable),
            bundle_start,
            bundle_stop)
        bundle_start = bundle_stop
    else:
      # Returning a single sub-source with end offset set to OFFSET_INFINITY (so
      # that all data of the source gets read) since this source is
      # unsplittable. Choosing size of the file as end offset will be wrong for
      # certain unsplittable source, e.g., compressed sources.
      yield iobase.SourceBundle(
          stop_offset - start_offset,
          _SingleFileSource(
              self._file_based_source,
              self._file_name,
              start_offset,
              range_trackers.OffsetRangeTracker.OFFSET_INFINITY,
              min_bundle_size=self._min_bundle_size,
              splittable=self._splittable
          ),
          start_offset,
          range_trackers.OffsetRangeTracker.OFFSET_INFINITY
      )

  def estimate_size(self):
    return self._stop_offset - self._start_offset

  def get_range_tracker(self, start_position, stop_position):
    if start_position is None:
      start_position = self._start_offset
    if stop_position is None:
      # If file is unsplittable we choose OFFSET_INFINITY as the default end
      # offset so that all data of the source gets read. Choosing size of the
      # file as end offset will be wrong for certain unsplittable source, for
      # e.g., compressed sources.
      stop_position = (
          self._stop_offset if self._splittable
          else range_trackers.OffsetRangeTracker.OFFSET_INFINITY)

    range_tracker = range_trackers.OffsetRangeTracker(
        start_position, stop_position)
    if not self._splittable:
      range_tracker = range_trackers.UnsplittableRangeTracker(range_tracker)

    return range_tracker

  def read(self, range_tracker):
    return self._file_based_source.read_records(self._file_name, range_tracker)

  def default_output_coder(self):
    return self._file_based_source.default_output_coder()
