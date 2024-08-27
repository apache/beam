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

"""``PTransforms`` for manipulating files in Apache Beam.

Provides reading ``PTransform``\\s, ``MatchFiles``,
``MatchAll``, that produces a ``PCollection`` of records representing a file
and its metadata; and ``ReadMatches``, which takes in a ``PCollection`` of file
metadata records, and produces a ``PCollection`` of ``ReadableFile`` objects.
These transforms currently do not support splitting by themselves.

Writing to Files
================

The transforms in this file include ``WriteToFiles``, which allows you to write
a ``beam.PCollection`` to files, and gives you many options to customize how to
do this.

The ``WriteToFiles`` transform supports bounded and unbounded PCollections
(i.e. it can be used both batch and streaming pipelines). For streaming
pipelines, it currently does not have support for multiple trigger firings
on the same window.

File Naming
-----------
One of the parameters received by ``WriteToFiles`` is a function specifying how
to name the files that are written. This is a function that takes in the
following parameters:

- window
- pane
- shard_index
- total_shards
- compression
- destination

It should return a file name that is unique for a combination of these
parameters.

The default naming strategy is to name files
in the format
`$prefix-$start-$end-$pane-$shard-of-$numShards$suffix$compressionSuffix`,
where:

- `$prefix` is, by default, `"output"`.
- `$start` and `$end` are the boundaries of the window for the data being
  written. These are omitted if we're using the Global window.
- `$pane` is the index for the number of firing for a window.
- `$shard` and `$numShards` are the current shard number, and the total number
  of shards for this window firing.
- `$suffix` is, by default, an empty string, but it can be set by the user via
  ``default_file_naming``.

Dynamic Destinations
--------------------
If the elements in the input ``beam.PCollection`` can be partitioned into groups
that should be treated differently (e.g. some events are to be stored as CSV,
while some others are to be stored as Avro files), it is possible to do this
by passing a `destination` parameter to ``WriteToFiles``. Something like the
following::

    my_pcollection | beam.io.fileio.WriteToFiles(
          path='/my/file/path',
          destination=lambda record: 'avro' if record['type'] == 'A' else 'csv',
          sink=lambda dest: AvroSink() if dest == 'avro' else CsvSink(),
          file_naming=beam.io.fileio.destination_prefix_naming())

In this transform, depending on the type of a record, it will be written down to
a destination named `'avro'`, or `'csv'`. The value returned by the
`destination` call is then passed to the `sink` call, to determine what sort of
sink will be used for each destination. The return type of the `destination`
parameter can be anything, as long as elements can be grouped by it.
"""

# pytype: skip-file

import collections
import logging
import random
import uuid
from collections import namedtuple
from functools import partial
from typing import TYPE_CHECKING
from typing import Any
from typing import BinaryIO  # pylint: disable=unused-import
from typing import Callable
from typing import DefaultDict
from typing import Dict
from typing import Iterable
from typing import List
from typing import Tuple
from typing import Union

import apache_beam as beam
from apache_beam.io import filesystem
from apache_beam.io import filesystems
from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.options.value_provider import ValueProvider
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.userstate import CombiningValueStateSpec
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import IntervalWindow
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

if TYPE_CHECKING:
  from apache_beam.transforms.window import BoundedWindow

__all__ = [
    'EmptyMatchTreatment',
    'MatchFiles',
    'MatchAll',
    'MatchContinuously',
    'ReadableFile',
    'ReadMatches',
    'WriteToFiles'
]

_LOGGER = logging.getLogger(__name__)

FileMetadata = namedtuple("FileMetadata", "mime_type compression_type")

CreateFileMetadataFn = Callable[[str, str], FileMetadata]


class EmptyMatchTreatment(object):
  """How to treat empty matches in ``MatchAll`` and ``MatchFiles`` transforms.

  If empty matches are disallowed, an error will be thrown if a pattern does not
  match any files."""

  ALLOW = 'ALLOW'
  DISALLOW = 'DISALLOW'
  ALLOW_IF_WILDCARD = 'ALLOW_IF_WILDCARD'

  @staticmethod
  def allow_empty_match(pattern, setting):
    if setting == EmptyMatchTreatment.ALLOW:
      return True
    elif setting == EmptyMatchTreatment.ALLOW_IF_WILDCARD and '*' in pattern:
      return True
    elif setting == EmptyMatchTreatment.DISALLOW:
      return False
    else:
      raise ValueError(setting)


class _MatchAllFn(beam.DoFn):
  def __init__(self, empty_match_treatment):
    self._empty_match_treatment = empty_match_treatment

  def process(self, file_pattern: str) -> List[filesystem.FileMetadata]:
    # TODO: Should we batch the lookups?
    match_results = filesystems.FileSystems.match([file_pattern])
    match_result = match_results[0]

    if (not match_result.metadata_list and
        not EmptyMatchTreatment.allow_empty_match(file_pattern,
                                                  self._empty_match_treatment)):
      raise BeamIOError(
          'Empty match for pattern %s. Disallowed.' % file_pattern)

    return match_result.metadata_list


class MatchFiles(beam.PTransform):
  """Matches a file pattern using ``FileSystems.match``.

  This ``PTransform`` returns a ``PCollection`` of matching files in the form
  of ``FileMetadata`` objects."""
  def __init__(
      self,
      file_pattern: str,
      empty_match_treatment=EmptyMatchTreatment.ALLOW_IF_WILDCARD):
    self._file_pattern = file_pattern
    self._empty_match_treatment = empty_match_treatment

  def expand(self, pcoll) -> beam.PCollection[filesystem.FileMetadata]:
    return pcoll.pipeline | beam.Create([self._file_pattern]) | MatchAll(
        empty_match_treatment=self._empty_match_treatment)


class MatchAll(beam.PTransform):
  """Matches file patterns from the input PCollection via ``FileSystems.match``.

  This ``PTransform`` returns a ``PCollection`` of matching files in the form
  of ``FileMetadata`` objects."""
  def __init__(self, empty_match_treatment=EmptyMatchTreatment.ALLOW):
    self._empty_match_treatment = empty_match_treatment

  def expand(
      self,
      pcoll: beam.PCollection,
  ) -> beam.PCollection[filesystem.FileMetadata]:
    return pcoll | beam.ParDo(_MatchAllFn(self._empty_match_treatment))


class ReadableFile(object):
  """A utility class for accessing files."""
  def __init__(self, metadata, compression=None):
    self.metadata = metadata
    self._compression = compression

  def open(self, mime_type='text/plain', compression_type=None):
    compression = (
        compression_type or self._compression or
        filesystems.CompressionTypes.AUTO)
    return filesystems.FileSystems.open(
        self.metadata.path, mime_type=mime_type, compression_type=compression)

  def read(self, mime_type='application/octet-stream'):
    return self.open(mime_type).read()

  def read_utf8(self):
    return self.open().read().decode('utf-8')


class _ReadMatchesFn(beam.DoFn):
  def __init__(self, compression, skip_directories):
    self._compression = compression
    self._skip_directories = skip_directories

  def process(
      self,
      file_metadata: Union[str, filesystem.FileMetadata],
  ) -> Iterable[ReadableFile]:
    metadata = (
        filesystem.FileMetadata(file_metadata, 0) if isinstance(
            file_metadata, str) else file_metadata)

    if ((metadata.path.endswith('/') or metadata.path.endswith('\\')) and
        self._skip_directories):
      return
    elif metadata.path.endswith('/') or metadata.path.endswith('\\'):
      raise BeamIOError(
          'Directories are not allowed in ReadMatches transform.'
          'Found %s.' % metadata.path)

    # TODO: Mime type? Other arguments? Maybe arguments passed in to transform?
    yield ReadableFile(metadata, self._compression)


class MatchContinuously(beam.PTransform):
  """Checks for new files for a given pattern every interval.

  This ``PTransform`` returns a ``PCollection`` of matching files in the form
  of ``FileMetadata`` objects.

  MatchContinuously is experimental.  No backwards-compatibility
  guarantees.

  Matching continuously scales poorly, as it is stateful, and requires storing
  file ids in memory. In addition, because it is memory-only, if a pipeline is
  restarted, already processed files will be reprocessed. Consider an alternate
  technique, such as Pub/Sub Notifications
  (https://cloud.google.com/storage/docs/pubsub-notifications)
  when using GCS if possible.
  """
  def __init__(
      self,
      file_pattern,
      interval=360.0,
      has_deduplication=True,
      start_timestamp=Timestamp.now(),
      stop_timestamp=MAX_TIMESTAMP,
      match_updated_files=False,
      apply_windowing=False,
      empty_match_treatment=EmptyMatchTreatment.ALLOW):
    """Initializes a MatchContinuously transform.

    Args:
      file_pattern: The file path to read from.
      interval: Interval at which to check for files in seconds.
      has_deduplication: Whether files already read are discarded or not.
      start_timestamp: Timestamp for start file checking.
      stop_timestamp: Timestamp after which no more files will be checked.
      match_updated_files: (When has_deduplication is set to True) whether match
        file with timestamp changes.
      apply_windowing: Whether each element should be assigned to
        individual window. If false, all elements will reside in global window.
    """

    self.file_pattern = file_pattern
    self.interval = interval
    self.has_deduplication = has_deduplication
    self.start_ts = start_timestamp
    self.stop_ts = stop_timestamp
    self.match_upd = match_updated_files
    self.apply_windowing = apply_windowing
    self.empty_match_treatment = empty_match_treatment
    _LOGGER.warning(
        'Matching Continuously is stateful, and can scale poorly. '
        'Consider using Pub/Sub Notifications '
        '(https://cloud.google.com/storage/docs/pubsub-notifications) '
        'if possible')

  def expand(self, pbegin) -> beam.PCollection[filesystem.FileMetadata]:
    # invoke periodic impulse
    impulse = pbegin | PeriodicImpulse(
        start_timestamp=self.start_ts,
        stop_timestamp=self.stop_ts,
        fire_interval=self.interval)

    # match file pattern periodically
    match_files = (
        impulse
        | 'GetFilePattern' >> beam.Map(lambda x: self.file_pattern)
        | MatchAll(self.empty_match_treatment))

    # apply deduplication strategy if required
    if self.has_deduplication:
      # Making a Key Value so each file has its own state.
      match_files = match_files | 'ToKV' >> beam.Map(lambda x: (x.path, x))
      if self.match_upd:
        match_files = match_files | 'RemoveOldAlreadyRead' >> beam.ParDo(
            _RemoveOldDuplicates())
      else:
        match_files = match_files | 'RemoveAlreadyRead' >> beam.ParDo(
            _RemoveDuplicates())

    # apply windowing if required. Apply at last because deduplication relies on
    # the global window.
    if self.apply_windowing:
      match_files = match_files | beam.WindowInto(FixedWindows(self.interval))

    return match_files


class ReadMatches(beam.PTransform):
  """Converts each result of MatchFiles() or MatchAll() to a ReadableFile.

   This helps read in a file's contents or obtain a file descriptor."""
  def __init__(self, compression=None, skip_directories=True):
    self._compression = compression
    self._skip_directories = skip_directories

  def expand(
      self,
      pcoll: beam.PCollection[Union[str, filesystem.FileMetadata]],
  ) -> beam.PCollection[ReadableFile]:
    return pcoll | beam.ParDo(
        _ReadMatchesFn(self._compression, self._skip_directories))


class FileSink(object):
  """Specifies how to write elements to individual files in ``WriteToFiles``.

  A Sink class must implement the following:

   - The ``open`` method, which initializes writing to a file handler (it is not
     responsible for opening the file handler itself).
   - The ``write`` method, which writes an element to the file that was passed
     in ``open``.
   - The ``flush`` method, which flushes any buffered state. This is most often
     called before closing a file (but not exclusively called in that
     situation). The sink is not responsible for closing the file handler.
  A Sink class can override the following:
   - The ``create_metadata`` method, which creates all metadata passed to
     Filesystems.create.
   """
  def create_metadata(
      self, destination: str, full_file_name: str) -> FileMetadata:
    return FileMetadata(
        mime_type="application/octet-stream",
        compression_type=CompressionTypes.AUTO)

  def open(self, fh):
    # type: (BinaryIO) -> None
    raise NotImplementedError

  def write(self, record):
    raise NotImplementedError

  def flush(self):
    raise NotImplementedError


@beam.typehints.with_input_types(str)
class TextSink(FileSink):
  """A sink that encodes utf8 elements, and writes to file handlers.

  This sink simply calls file_handler.write(record.encode('utf8') + '\n') on all
  records that come into it.
  """
  def open(self, fh):
    self._fh = fh

  def write(self, record):
    self._fh.write(record.encode('utf8'))
    self._fh.write(b'\n')

  def flush(self):
    self._fh.flush()


def prefix_naming(prefix):
  return default_file_naming(prefix)


_DEFAULT_FILE_NAME_TEMPLATE = (
    '{prefix}-{start}-{end}-{pane}-'
    '{shard:05d}-of-{total_shards:05d}'
    '{suffix}{compression}')


def _format_shard(
    window, pane, shard_index, total_shards, compression, prefix, suffix):
  kwargs = {
      'prefix': prefix,
      'start': '',
      'end': '',
      'pane': '',
      'shard': 0,
      'total_shards': 0,
      'suffix': '',
      'compression': ''
  }

  if total_shards is not None and shard_index is not None:
    kwargs['shard'] = int(shard_index)
    kwargs['total_shards'] = int(total_shards)

  if window != GlobalWindow():
    kwargs['start'] = window.start.to_utc_datetime().isoformat()
    kwargs['end'] = window.end.to_utc_datetime().isoformat()

  # TODO(https://github.com/apache/beam/issues/18721): Add support for PaneInfo
  # If the PANE is the ONLY firing in the window, we don't add it.
  #if pane and not (pane.is_first and pane.is_last):
  #  kwargs['pane'] = pane.index

  if suffix:
    kwargs['suffix'] = suffix

  if compression:
    kwargs['compression'] = '.%s' % compression

  # Remove separators for unused template parts.
  format = _DEFAULT_FILE_NAME_TEMPLATE
  if shard_index is None:
    format = format.replace('-{shard:05d}', '')
  if total_shards is None:
    format = format.replace('-of-{total_shards:05d}', '')
  for name, value in kwargs.items():
    if value in (None, ''):
      format = format.replace('-{%s}' % name, '')

  return format.format(**kwargs)


FileNaming = Callable[[Any, Any, int, int, Any, str, str], str]


def destination_prefix_naming(suffix=None) -> FileNaming:
  def _inner(window, pane, shard_index, total_shards, compression, destination):
    prefix = str(destination)
    return _format_shard(
        window, pane, shard_index, total_shards, compression, prefix, suffix)

  return _inner


def default_file_naming(prefix, suffix=None) -> FileNaming:
  def _inner(window, pane, shard_index, total_shards, compression, destination):
    return _format_shard(
        window, pane, shard_index, total_shards, compression, prefix, suffix)

  return _inner


def single_file_naming(prefix, suffix=None) -> FileNaming:
  def _inner(window, pane, shard_index, total_shards, compression, destination):
    assert shard_index in (0, None), shard_index
    assert total_shards in (1, None), total_shards
    return _format_shard(window, pane, None, None, compression, prefix, suffix)

  return _inner


_FileResult = collections.namedtuple(
    'FileResult', [
        'file_name',
        'shard_index',
        'total_shards',
        'window',
        'pane',
        'destination'
    ])


# Adding a class to contain PyDoc.
class FileResult(_FileResult):
  """A descriptor of a file that has been written."""
  pass


class WriteToFiles(beam.PTransform):
  r"""Write the incoming PCollection to a set of output files.

  The incoming ``PCollection`` may be bounded or unbounded.

  **Note:** For unbounded ``PCollection``\s, this transform does not support
  multiple firings per Window (due to the fact that files are named only by
  their destination, and window, at the moment).
  """

  # We allow up to 20 different destinations to be written in a single bundle.
  # Too many files will add memory pressure to the worker, so we let it be 20.
  MAX_NUM_WRITERS_PER_BUNDLE = 20

  DEFAULT_SHARDING = 5

  def __init__(
      self,
      path,
      file_naming=None,
      destination=None,
      temp_directory=None,
      sink=None,
      shards=None,
      output_fn=None,
      max_writers_per_bundle=MAX_NUM_WRITERS_PER_BUNDLE):
    """Initializes a WriteToFiles transform.

    Args:
      path (str, ValueProvider): The directory to write files into.
      file_naming (callable): A callable that takes in a window, pane,
        shard_index, total_shards and compression; and returns a file name.
      destination (callable): If this argument is provided, the sink parameter
        must also be a callable.
      temp_directory (str, ValueProvider): To ensure atomicity in the transform,
        the output is written into temporary files, which are written to a
        directory that is meant to be temporary as well. Once the whole output
        has been written, the files are moved into their final destination, and
        given their final names. By default, the temporary directory will be
        within the temp_location of your pipeline.
      sink (callable, ~apache_beam.io.fileio.FileSink): The sink to use to write
        into a file. It should implement the methods of a ``FileSink``. Pass a
        class signature or an instance of FileSink to this parameter. If none is
        provided, a ``TextSink`` is used.
      shards (int): The number of shards per destination and trigger firing.
      max_writers_per_bundle (int): The number of writers that can be open
        concurrently in a single worker that's processing one bundle.
    """
    self.path = (
        path if isinstance(path, ValueProvider) else StaticValueProvider(
            str, path))
    self.file_naming_fn = file_naming or default_file_naming('output')
    self.destination_fn = self._get_destination_fn(destination)
    self._temp_directory = temp_directory
    self.sink_fn = self._get_sink_fn(sink)
    self.shards = shards or WriteToFiles.DEFAULT_SHARDING
    self.output_fn = output_fn or (lambda x: x)

    self._max_num_writers_per_bundle = max_writers_per_bundle

  @staticmethod
  def _get_sink_fn(input_sink):
    # type: (...) -> Callable[[Any], FileSink]
    if isinstance(input_sink, type) and issubclass(input_sink, FileSink):
      return lambda x: input_sink()
    elif isinstance(input_sink, FileSink):
      kls = input_sink.__class__
      return lambda x: kls()
    elif callable(input_sink):
      return input_sink
    else:
      return lambda x: TextSink()

  @staticmethod
  def _get_destination_fn(destination):
    # type: (...) -> Callable[[Any], str]
    if isinstance(destination, ValueProvider):
      return lambda elm: destination.get()
    elif callable(destination):
      return destination
    else:
      return lambda elm: destination

  def expand(self, pcoll):
    p = pcoll.pipeline

    if not self._temp_directory:
      temp_location = (
          p.options.view_as(GoogleCloudOptions).temp_location or
          self.path.get())
      dir_uid = str(uuid.uuid4())
      self._temp_directory = StaticValueProvider(
          str, filesystems.FileSystems.join(temp_location, '.temp%s' % dir_uid))
      _LOGGER.info('Added temporary directory %s', self._temp_directory.get())

    output = (
        pcoll
        | beam.ParDo(
            _WriteUnshardedRecordsFn(
                base_path=self._temp_directory,
                destination_fn=self.destination_fn,
                sink_fn=self.sink_fn,
                max_writers_per_bundle=self._max_num_writers_per_bundle)).
        with_outputs(
            _WriteUnshardedRecordsFn.SPILLED_RECORDS,
            _WriteUnshardedRecordsFn.WRITTEN_FILES))

    written_files_pc = output[_WriteUnshardedRecordsFn.WRITTEN_FILES]
    spilled_records_pc = output[_WriteUnshardedRecordsFn.SPILLED_RECORDS]

    more_written_files_pc = (
        spilled_records_pc
        | beam.ParDo(
            _AppendShardedDestination(self.destination_fn, self.shards))
        | "GroupRecordsByDestinationAndShard" >> beam.GroupByKey()
        | beam.ParDo(
            _WriteShardedRecordsFn(
                self._temp_directory, self.sink_fn, self.shards)))

    files_by_destination_pc = (
        (written_files_pc, more_written_files_pc)
        | beam.Flatten()
        | beam.Map(lambda file_result: (file_result.destination, file_result))
        | "GroupTempFilesByDestination" >> beam.GroupByKey())

    # Now we should take the temporary files, and write them to the final
    # destination, with their proper names.

    file_results = (
        files_by_destination_pc
        | beam.ParDo(
            _MoveTempFilesIntoFinalDestinationFn(
                self.path, self.file_naming_fn, self._temp_directory)))

    return file_results


def _create_writer(
    base_path,
    writer_key: Tuple[str, IntervalWindow],
    create_metadata_fn: CreateFileMetadataFn,
):
  try:
    filesystems.FileSystems.mkdirs(base_path)
  except IOError:
    # Directory already exists.
    pass

  destination = writer_key[0]

  # The file name has a prefix determined by destination+window, along with
  # a random string. This allows us to retrieve orphaned files later on.
  file_name = '%s_%s' % (abs(hash(writer_key)), uuid.uuid4())
  full_file_name = filesystems.FileSystems.join(base_path, file_name)
  metadata = create_metadata_fn(destination, full_file_name)
  return full_file_name, filesystems.FileSystems.create(
      full_file_name,
      **metadata._asdict())


class _MoveTempFilesIntoFinalDestinationFn(beam.DoFn):
  def __init__(self, path, file_naming_fn, temp_dir):
    self.path = path
    self.file_naming_fn = file_naming_fn
    self.temporary_directory = temp_dir

  def process(self, element, w=beam.DoFn.WindowParam):
    destination = element[0]
    # list of FileResult objects for temp files
    temp_file_results = list(element[1])
    # list of FileResult objects for final files
    final_file_results = []

    for i, r in enumerate(temp_file_results):
      # TODO(pabloem): Handle compression for files.
      final_file_name = self.file_naming_fn(
          r.window, r.pane, i, len(temp_file_results), '', destination)

      final_file_results.append(
          FileResult(
              final_file_name,
              i,
              len(temp_file_results),
              r.window,
              r.pane,
              destination))

    move_from = [f.file_name for f in temp_file_results]
    move_to = [f.file_name for f in final_file_results]

    _LOGGER.info(
        'Moving %d temporary files to dir: %s as %s',
        len(move_from),
        self.path.get(),
        move_to)

    try:
      filesystems.FileSystems.mkdirs(self.path.get())
    except IOError as e:
      cause = repr(e)
      if 'FileExistsError' not in cause:
        # Usually harmless. Especially if see FileExistsError so no need to log
        _LOGGER.debug('Fail to create dir for final destination: %s', cause)

    try:
      filesystems.FileSystems.rename(
          move_from,
          [filesystems.FileSystems.join(self.path.get(), f) for f in move_to])
    except BeamIOError:
      # This error is not serious, because it may happen on a retry of the
      # bundle. We simply log it.
      _LOGGER.debug(
          'Exception occurred during moving files: %s. This may be due to a'
          ' bundle being retried.',
          move_from)

    yield from final_file_results

    _LOGGER.debug(
        'Checking orphaned temporary files for destination %s and window %s',
        destination,
        w)
    writer_key = (destination, w)
    self._check_orphaned_files(writer_key)

  def _check_orphaned_files(self, writer_key):
    try:
      prefix = filesystems.FileSystems.join(
          self.temporary_directory.get(), str(abs(hash(writer_key))))
      match_result = filesystems.FileSystems.match(['%s*' % prefix])
      orphaned_files = [m.path for m in match_result[0].metadata_list]

      if len(orphaned_files) > 0:
        _LOGGER.warning(
            'Some files may be left orphaned in the temporary folder: %s. '
            'This may be a result of retried work items or insufficient'
            'permissions to delete these temp files.',
            orphaned_files)
    except BeamIOError as e:
      _LOGGER.warning('Exceptions when checking orphaned files: %s', e)


class _WriteShardedRecordsFn(beam.DoFn):

  def __init__(self,
               base_path,
               sink_fn,  # type: Callable[[Any], FileSink]
               shards  # type: int
              ):
    self.base_path = base_path
    self.sink_fn = sink_fn
    self.shards = shards

  def process(
      self, element, w=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
    destination_and_shard = element[0]
    destination = destination_and_shard[0]
    shard = destination_and_shard[1]
    records = element[1]

    sink = self.sink_fn(destination)

    full_file_name, writer = _create_writer(
        base_path=self.base_path.get(),
        writer_key=(destination, w),
        create_metadata_fn=sink.create_metadata)

    sink.open(writer)

    for r in records:
      sink.write(r)

    sink.flush()
    writer.close()

    _LOGGER.info(
        'Writing file %s for destination %s and shard %s',
        full_file_name,
        destination,
        repr(shard))

    yield FileResult(
        full_file_name,
        shard_index=shard,
        total_shards=self.shards,
        window=w,
        pane=pane,
        destination=destination)


class _AppendShardedDestination(beam.DoFn):
  def __init__(
      self,
      destination,  # type: Callable[[Any], str]
      shards  # type: int
  ):
    self.destination_fn = destination
    self.shards = shards

    # We start the shards for a single destination at an arbitrary point.
    self._shard_counter = collections.defaultdict(
        lambda: random.randrange(self.shards))  # type: DefaultDict[str, int]

  def _next_shard_for_destination(self, destination):
    self._shard_counter[destination] = ((self._shard_counter[destination] + 1) %
                                        self.shards)

    return self._shard_counter[destination]

  def process(self, record):
    destination = self.destination_fn(record)
    shard = self._next_shard_for_destination(destination)

    yield ((destination, shard), record)


class _WriteUnshardedRecordsFn(beam.DoFn):

  SPILLED_RECORDS = 'spilled_records'
  WRITTEN_FILES = 'written_files'

  _writers_and_sinks = None  # type: Dict[Tuple[str, BoundedWindow], Tuple[BinaryIO, FileSink]]
  _file_names = None  # type: Dict[Tuple[str, BoundedWindow], str]

  def __init__(
      self,
      base_path,
      destination_fn,
      sink_fn,
      max_writers_per_bundle=WriteToFiles.MAX_NUM_WRITERS_PER_BUNDLE):
    self.base_path = base_path
    self.destination_fn = destination_fn
    self.sink_fn = sink_fn
    self.max_num_writers_per_bundle = max_writers_per_bundle

  def start_bundle(self):
    self._writers_and_sinks = {}
    self._file_names = {}

  def process(
      self, record, w=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
    destination = self.destination_fn(record)

    writer, sink = self._get_or_create_writer_and_sink(destination, w)

    if not writer:
      return [beam.pvalue.TaggedOutput(self.SPILLED_RECORDS, record)]
    else:
      sink.write(record)

  def _get_or_create_writer_and_sink(self, destination, window):
    """Returns a tuple of writer, sink."""
    writer_key = (destination, window)
    if writer_key in self._writers_and_sinks:
      return self._writers_and_sinks.get(writer_key)
    elif len(self._writers_and_sinks) >= self.max_num_writers_per_bundle:
      # The writer does not exist, and we have too many writers already.
      return None, None
    else:
      # The writer does not exist, but we can still create a new one.
      sink = self.sink_fn(destination)

      full_file_name, writer = _create_writer(
          base_path=self.base_path.get(),
          writer_key=writer_key,
          create_metadata_fn=sink.create_metadata)

      sink.open(writer)
      self._writers_and_sinks[writer_key] = (writer, sink)
      self._file_names[writer_key] = full_file_name
      return self._writers_and_sinks[writer_key]

  def finish_bundle(self):
    for key, (writer, sink) in self._writers_and_sinks.items():

      sink.flush()
      writer.close()

      file_result = FileResult(self._file_names[key],
                               shard_index=-1,
                               total_shards=0,
                               window=key[1],
                               pane=None,  # TODO(pabloem): get the pane info
                               destination=key[0])

      yield beam.pvalue.TaggedOutput(
          self.WRITTEN_FILES,
          beam.transforms.window.WindowedValue(
              file_result,
              timestamp=key[1].start,
              windows=[key[1]]  # TODO(pabloem) HOW DO WE GET THE PANE
          ))


class _RemoveDuplicates(beam.DoFn):
  """Internal DoFn that filters out filenames already seen (even though the file
  has updated)."""
  COUNT_STATE = CombiningValueStateSpec('count', combine_fn=sum)

  def process(
      self,
      element: Tuple[str, filesystem.FileMetadata],
      count_state=beam.DoFn.StateParam(COUNT_STATE)
  ) -> Iterable[filesystem.FileMetadata]:

    path = element[0]
    file_metadata = element[1]
    counter = count_state.read()

    if counter == 0:
      count_state.add(1)
      _LOGGER.debug('Generated entry for file %s', path)
      yield file_metadata
    else:
      _LOGGER.debug('File %s was already read, seen %d times', path, counter)


class _RemoveOldDuplicates(beam.DoFn):
  """Internal DoFn that filters out filenames already seen and timestamp
  unchanged."""
  TIME_STATE = CombiningValueStateSpec(
      'count', combine_fn=partial(max, default=0.0))

  def process(
      self,
      element: Tuple[str, filesystem.FileMetadata],
      time_state=beam.DoFn.StateParam(TIME_STATE)
  ) -> Iterable[filesystem.FileMetadata]:
    path = element[0]
    file_metadata = element[1]
    new_ts = file_metadata.last_updated_in_seconds
    old_ts = time_state.read()

    if old_ts < new_ts:
      time_state.add(new_ts)
      _LOGGER.debug('Generated entry for file %s', path)
      yield file_metadata
    else:
      _LOGGER.debug('File %s was already read', path)
