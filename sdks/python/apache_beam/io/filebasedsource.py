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

To create a source for a new file type a sub-class of :class:`FileBasedSource`
should be created. Sub-classes of :class:`FileBasedSource` must implement the
method :meth:`FileBasedSource.read_records()`. Please read the documentation of
that method for more details.

For an example implementation of :class:`FileBasedSource` see
:class:`~apache_beam.io._AvroSource`.
"""

# pytype: skip-file

from typing import Callable
from typing import Iterable
from typing import Tuple
from typing import Union

from apache_beam.internal import pickler
from apache_beam.io import concat_source
from apache_beam.io import iobase
from apache_beam.io import range_trackers
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.options.value_provider import ValueProvider
from apache_beam.options.value_provider import check_accessible
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.core import ParDo
from apache_beam.transforms.core import PTransform
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.transforms.util import Reshuffle

MAX_NUM_THREADS_FOR_SIZE_ESTIMATION = 25

__all__ = ['FileBasedSource']


class FileBasedSource(iobase.BoundedSource):
  """A :class:`~apache_beam.io.iobase.BoundedSource` for reading a file glob of
  a given type."""

  MIN_NUMBER_OF_FILES_TO_STAT = 100
  MIN_FRACTION_OF_FILES_TO_STAT = 0.01

  def __init__(
      self,
      file_pattern,
      min_bundle_size=0,
      compression_type=CompressionTypes.AUTO,
      splittable=True,
      validate=True):
    """Initializes :class:`FileBasedSource`.

    Args:
      file_pattern (str): the file glob to read a string or a
        :class:`~apache_beam.options.value_provider.ValueProvider`
        (placeholder to inject a runtime value).
      min_bundle_size (int): minimum size of bundles that should be generated
        when performing initial splitting on this source.
      compression_type (str): Used to handle compressed output files.
        Typical value is :attr:`CompressionTypes.AUTO
        <apache_beam.io.filesystem.CompressionTypes.AUTO>`,
        in which case the final file path's extension will be used to detect
        the compression.
      splittable (bool): whether :class:`FileBasedSource` should try to
        logically split a single file into data ranges so that different parts
        of the same file can be read in parallel. If set to :data:`False`,
        :class:`FileBasedSource` will prevent both initial and dynamic splitting
        of sources for single files. File patterns that represent multiple files
        may still get split into sources for individual files. Even if set to
        :data:`True` by the user, :class:`FileBasedSource` may choose to not
        split the file, for example, for compressed files where currently it is
        not possible to efficiently read a data range without decompressing the
        whole file.
      validate (bool): Boolean flag to verify that the files exist during the
        pipeline creation time.

    Raises:
      TypeError: when **compression_type** is not valid or if
        **file_pattern** is not a :class:`str` or a
        :class:`~apache_beam.options.value_provider.ValueProvider`.
      ValueError: when compression and splittable files are
        specified.
      IOError: when the file pattern specified yields an empty
        result.
    """

    if not isinstance(file_pattern, (str, ValueProvider)):
      raise TypeError(
          '%s: file_pattern must be of type string'
          ' or ValueProvider; got %r instead' %
          (self.__class__.__name__, file_pattern))

    if isinstance(file_pattern, str):
      file_pattern = StaticValueProvider(str, file_pattern)
    self._pattern = file_pattern

    self._concat_source = None
    self._min_bundle_size = min_bundle_size
    if not CompressionTypes.is_valid_compression_type(compression_type):
      raise TypeError(
          'compression_type must be CompressionType object but '
          'was %s' % type(compression_type))
    self._compression_type = compression_type
    self._splittable = splittable
    if validate and file_pattern.is_accessible():
      self._validate()

  def display_data(self):
    return {
        'file_pattern': DisplayDataItem(
            str(self._pattern), label="File Pattern"),
        'compression': DisplayDataItem(
            str(self._compression_type), label='Compression Type')
    }

  @check_accessible(['_pattern'])
  def _get_concat_source(self) -> concat_source.ConcatSource:
    if self._concat_source is None:
      pattern = self._pattern.get()

      single_file_sources = []
      match_result = FileSystems.match([pattern])[0]
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
        splittable = (
            self.splittable and _determine_splittability_from_compression_type(
                file_name, self._compression_type))

        single_file_source = _SingleFileSource(
            file_based_source_ref,
            file_name,
            0,
            file_size,
            min_bundle_size=self._min_bundle_size,
            splittable=splittable)
        single_file_sources.append(single_file_source)

      self._concat_source = concat_source.ConcatSource(single_file_sources)

      # Report source Lineage. depend on the number of files, report full file
      # name or only dir
      if len(files_metadata) <= 100:
        for file_metadata in files_metadata:
          FileSystems.report_source_lineage(file_metadata.path)
      else:
        for file_metadata in files_metadata:
          try:
            base, _ = FileSystems.split(file_metadata.path)
          except ValueError:
            pass
          else:
            FileSystems.report_source_lineage(base)

    return self._concat_source

  def open_file(self, file_name):
    return FileSystems.open(
        file_name,
        'application/octet-stream',
        compression_type=self._compression_type)

  @check_accessible(['_pattern'])
  def _validate(self):
    """Validate if there are actual files in the specified glob pattern
    """
    pattern = self._pattern.get()

    # Limit the responses as we only want to check if something exists
    match_result = FileSystems.match([pattern], limits=[1])[0]
    if len(match_result.metadata_list) <= 0:
      raise IOError('No files found based on the file pattern %s' % pattern)

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
    return self._get_concat_source().get_range_tracker(
        start_position, stop_position)

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


def _determine_splittability_from_compression_type(file_path, compression_type):
  if compression_type == CompressionTypes.AUTO:
    compression_type = CompressionTypes.detect_compression_type(file_path)

  return compression_type == CompressionTypes.UNCOMPRESSED


class _SingleFileSource(iobase.BoundedSource):
  """Denotes a source for a specific file type."""
  def __init__(
      self,
      file_based_source,
      file_name,
      start_offset,
      stop_offset,
      min_bundle_size=0,
      splittable=True):
    if not isinstance(start_offset, int):
      raise TypeError(
          'start_offset must be a number. Received: %r' % start_offset)
    if stop_offset != range_trackers.OffsetRangeTracker.OFFSET_INFINITY:
      if not isinstance(stop_offset, int):
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
      splits = OffsetRange(start_offset, stop_offset).split(
          desired_bundle_size, self._min_bundle_size)
      for split in splits:
        yield iobase.SourceBundle(
            split.stop - split.start,
            _SingleFileSource(
                # Copying this so that each sub-source gets a fresh instance.
                pickler.loads(pickler.dumps(self._file_based_source)),
                self._file_name,
                split.start,
                split.stop,
                min_bundle_size=self._min_bundle_size,
                splittable=self._splittable),
            split.start,
            split.stop)
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
              splittable=self._splittable),
          start_offset,
          range_trackers.OffsetRangeTracker.OFFSET_INFINITY)

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
          self._stop_offset if self._splittable else
          range_trackers.OffsetRangeTracker.OFFSET_INFINITY)

    range_tracker = range_trackers.OffsetRangeTracker(
        start_position, stop_position)
    if not self._splittable:
      range_tracker = range_trackers.UnsplittableRangeTracker(range_tracker)

    return range_tracker

  def read(self, range_tracker):
    return self._file_based_source.read_records(self._file_name, range_tracker)

  def default_output_coder(self):
    return self._file_based_source.default_output_coder()


class _ExpandIntoRanges(DoFn):
  def __init__(
      self, splittable, compression_type, desired_bundle_size, min_bundle_size):
    self._desired_bundle_size = desired_bundle_size
    self._min_bundle_size = min_bundle_size
    self._splittable = splittable
    self._compression_type = compression_type

  def process(self, element: Union[str, FileMetadata], *args,
              **kwargs) -> Iterable[Tuple[FileMetadata, OffsetRange]]:
    if isinstance(element, FileMetadata):
      metadata_list = [element]
    else:
      match_results = FileSystems.match([element])
      metadata_list = match_results[0].metadata_list
    for metadata in metadata_list:
      # fail-safely report source lineage
      try:
        base, _ = FileSystems.split(metadata.path)
      except ValueError:
        pass
      else:
        FileSystems.report_source_lineage(base)

      splittable = (
          self._splittable and _determine_splittability_from_compression_type(
              metadata.path, self._compression_type))

      if splittable:
        for split in OffsetRange(0, metadata.size_in_bytes).split(
            self._desired_bundle_size, self._min_bundle_size):
          yield (metadata, split)
      else:
        yield (
            metadata,
            OffsetRange(0, range_trackers.OffsetRangeTracker.OFFSET_INFINITY))


class _ReadRange(DoFn):
  def __init__(
      self,
      source_from_file: Union[str, iobase.BoundedSource],
      with_filename: bool = False) -> None:
    self._source_from_file = source_from_file
    self._with_filename = with_filename

  def process(self, element, *args, **kwargs):
    metadata, range = element
    source = self._source_from_file(metadata.path)
    # Following split() operation has to be performed to create a proper
    # _SingleFileSource. Otherwise what we have is a ConcatSource that contains
    # a single _SingleFileSource. ConcatSource.read() expects a RangeTracker for
    # sub-source range and reads full sub-sources (not byte ranges).
    source_list = list(source.split(float('inf')))
    # Handle the case of an empty source.
    if not source_list:
      return
    source = source_list[0].source

    for record in source.read(range.new_tracker()):
      if self._with_filename:
        yield (metadata.path, record)
      else:
        yield record


class ReadAllFiles(PTransform):
  """A Read transform that reads a PCollection of files.

  Pipeline authors should not use this directly. This is to be used by Read
  PTransform authors who wishes to implement file-based Read transforms that
  read a PCollection of files.
  """
  def __init__(
      self,
      splittable: bool,
      compression_type,
      desired_bundle_size: int,
      min_bundle_size: int,
      source_from_file: Callable[[str], iobase.BoundedSource],
      with_filename: bool = False):
    """
    Args:
      splittable: If False, files won't be split into sub-ranges. If True,
                  files may or may not be split into data ranges.
      compression_type: A ``CompressionType`` object that specifies the
                  compression type of the files that will be processed. If
                  ``CompressionType.AUTO``, system will try to automatically
                  determine the compression type based on the extension of
                  files.
      desired_bundle_size: the desired size of data ranges that should be
                           generated when splitting a file into data ranges.
      min_bundle_size: minimum size of data ranges that should be generated when
                           splitting a file into data ranges.
      source_from_file: a function that produces a ``BoundedSource`` given a
                        file name. System will use this function to generate
                        ``BoundedSource`` objects for file paths. Note that file
                        paths passed to this will be for individual files, not
                        for file patterns even if the ``PCollection`` of files
                        processed by the transform consist of file patterns.
      with_filename: If True, returns a Key Value with the key being the file
        name and the value being the actual data. If False, it only returns
        the data.
    """
    self._splittable = splittable
    self._compression_type = compression_type
    self._desired_bundle_size = desired_bundle_size
    self._min_bundle_size = min_bundle_size
    self._source_from_file = source_from_file
    self._with_filename = with_filename
    # TODO(BEAM-14497) always reshuffle once gbk always trigger works.
    self._is_reshuffle = True

  def _disable_reshuffle(self):
    # TODO(BEAM-14497) Remove this private method once gbk always trigger works.
    #
    # Currently Reshuffle() holds elements until the stage is completed. When
    # ReadRange is needed instantly after match (like read continuously), the
    # reshard is temporarily disabled. However, the read then does not scale and
    # is deemed experimental.
    self._is_reshuffle = False
    return self

  def expand(self, pvalue):
    pvalue = (
        pvalue
        | 'ExpandIntoRanges' >> ParDo(
            _ExpandIntoRanges(
                self._splittable,
                self._compression_type,
                self._desired_bundle_size,
                self._min_bundle_size)))
    if self._is_reshuffle:
      pvalue = pvalue | 'Reshard' >> Reshuffle()
    return (
        pvalue
        | 'ReadRange' >> ParDo(
            _ReadRange(
                self._source_from_file, with_filename=self._with_filename)))
