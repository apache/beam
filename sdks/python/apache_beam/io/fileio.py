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

No backward compatibility guarantees. Everything in this module is experimental.
"""

# pytype: skip-file

from __future__ import absolute_import

import collections
import logging
import random
import uuid
from typing import TYPE_CHECKING
from typing import Any
from typing import BinaryIO  # pylint: disable=unused-import
from typing import Callable
from typing import DefaultDict
from typing import Dict
from typing import Tuple

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import filesystem
from apache_beam.io import filesystems
from apache_beam.io.filesystem import BeamIOError
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.options.value_provider import ValueProvider
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils.annotations import experimental

if TYPE_CHECKING:
  from apache_beam.transforms.window import BoundedWindow

__all__ = [
    'EmptyMatchTreatment',
    'MatchFiles',
    'MatchAll',
    'ReadableFile',
    'ReadMatches'
]

_LOGGER = logging.getLogger(__name__)


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

  def process(self, file_pattern):
    # TODO: Should we batch the lookups?
    match_results = filesystems.FileSystems.match([file_pattern])
    match_result = match_results[0]

    if (not match_result.metadata_list and
        not EmptyMatchTreatment.allow_empty_match(file_pattern,
                                                  self._empty_match_treatment)):
      raise BeamIOError(
          'Empty match for pattern %s. Disallowed.' % file_pattern)

    return match_result.metadata_list


@experimental()
class MatchFiles(beam.PTransform):
  """Matches a file pattern using ``FileSystems.match``.

  This ``PTransform`` returns a ``PCollection`` of matching files in the form
  of ``FileMetadata`` objects."""
  def __init__(
      self,
      file_pattern,
      empty_match_treatment=EmptyMatchTreatment.ALLOW_IF_WILDCARD):
    self._file_pattern = file_pattern
    self._empty_match_treatment = empty_match_treatment

  def expand(self, pcoll):
    return pcoll.pipeline | beam.Create([self._file_pattern]) | MatchAll()


@experimental()
class MatchAll(beam.PTransform):
  """Matches file patterns from the input PCollection via ``FileSystems.match``.

  This ``PTransform`` returns a ``PCollection`` of matching files in the form
  of ``FileMetadata`` objects."""
  def __init__(self, empty_match_treatment=EmptyMatchTreatment.ALLOW):
    self._empty_match_treatment = empty_match_treatment

  def expand(self, pcoll):
    return pcoll | beam.ParDo(_MatchAllFn(self._empty_match_treatment))


class _ReadMatchesFn(beam.DoFn):
  def __init__(self, compression, skip_directories):
    self._compression = compression
    self._skip_directories = skip_directories

  def process(self, file_metadata):
    metadata = (
        filesystem.FileMetadata(file_metadata, 0) if isinstance(
            file_metadata, (str, unicode)) else file_metadata)

    if ((metadata.path.endswith('/') or metadata.path.endswith('\\')) and
        self._skip_directories):
      return
    elif metadata.path.endswith('/') or metadata.path.endswith('\\'):
      raise BeamIOError(
          'Directories are not allowed in ReadMatches transform.'
          'Found %s.' % metadata.path)

    # TODO: Mime type? Other arguments? Maybe arguments passed in to transform?
    yield ReadableFile(metadata, self._compression)


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


@experimental()
class ReadMatches(beam.PTransform):
  """Converts each result of MatchFiles() or MatchAll() to a ReadableFile.

   This helps read in a file's contents or obtain a file descriptor."""
  def __init__(self, compression=None, skip_directories=True):
    self._compression = compression
    self._skip_directories = skip_directories

  def expand(self, pcoll):
    return pcoll | beam.ParDo(
        _ReadMatchesFn(self._compression, self._skip_directories))


class FileSink(object):
  """Specifies how to write elements to individual files in ``WriteToFiles``.

  **NOTE: THIS CLASS IS EXPERIMENTAL.**

  A Sink class must implement the following:

   - The ``open`` method, which initializes writing to a file handler (it is not
     responsible for opening the file handler itself).
   - The ``write`` method, which writes an element to the file that was passed
     in ``open``.
   - The ``flush`` method, which flushes any buffered state. This is most often
     called before closing a file (but not exclusively called in that
     situation). The sink is not responsible for closing the file handler.
   """
  def open(self, fh: BinaryIO) -> None:
    raise NotImplementedError

  def write(self, record):
    raise NotImplementedError

  def flush(self):
    raise NotImplementedError


@beam.typehints.with_input_types(str)
class TextSink(FileSink):
  """A sink that encodes utf8 elements, and writes to file handlers.

  **NOTE: THIS CLASS IS EXPERIMENTAL.**

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
    '{shard:05d}-{total_shards:05d}'
    '{suffix}{compression}')


def destination_prefix_naming():
  def _inner(window, pane, shard_index, total_shards, compression, destination):
    kwargs = {
        'prefix': str(destination),
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

    # TODO(BEAM-3759): Add support for PaneInfo
    # If the PANE is the ONLY firing in the window, we don't add it.
    #if pane and not (pane.is_first and pane.is_last):
    #  kwargs['pane'] = pane.index

    if compression:
      kwargs['compression'] = '.%s' % compression

    return _DEFAULT_FILE_NAME_TEMPLATE.format(**kwargs)

  return _inner


def default_file_naming(prefix, suffix=None):
  def _inner(window, pane, shard_index, total_shards, compression, destination):
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

    # TODO(pabloem): Add support for PaneInfo
    # If the PANE is the ONLY firing in the window, we don't add it.
    #if pane and not (pane.is_first and pane.is_last):
    #  kwargs['pane'] = pane.index

    if compression:
      kwargs['compression'] = '.%s' % compression
    if suffix:
      kwargs['suffix'] = suffix

    return _DEFAULT_FILE_NAME_TEMPLATE.format(**kwargs)

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


@experimental()
class WriteToFiles(beam.PTransform):
  """Write the incoming PCollection to a set of output files.

  The incoming ``PCollection`` may be bounded or unbounded.

  **Note:** For unbounded ``PCollection``s, this transform does not support
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
      sink (callable, FileSink): The sink to use to write into a file. It should
        implement the methods of a ``FileSink``. If none is provided, a
        ``TextSink`` is used.
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
  def _get_sink_fn(input_sink) -> Callable[[Any], FileSink]:
    if isinstance(input_sink, FileSink):
      return lambda x: input_sink
    elif callable(input_sink):
      return input_sink
    else:
      return lambda x: TextSink()

  @staticmethod
  def _get_destination_fn(destination) -> Callable[[Any], str]:
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


def _create_writer(base_path, writer_key):
  try:
    filesystems.FileSystems.mkdirs(base_path)
  except IOError:
    # Directory already exists.
    pass

  # The file name has a prefix determined by destination+window, along with
  # a random string. This allows us to retrieve orphaned files later on.
  file_name = '%s_%s' % (abs(hash(writer_key)), uuid.uuid4())
  full_file_name = filesystems.FileSystems.join(base_path, file_name)
  return full_file_name, filesystems.FileSystems.create(full_file_name)


class _MoveTempFilesIntoFinalDestinationFn(beam.DoFn):
  def __init__(self, path, file_naming_fn, temp_dir):
    self.path = path
    self.file_naming_fn = file_naming_fn
    self.temporary_directory = temp_dir

  def process(self, element, w=beam.DoFn.WindowParam):
    destination = element[0]
    file_results = list(element[1])

    for i, r in enumerate(file_results):
      # TODO(pabloem): Handle compression for files.
      final_file_name = self.file_naming_fn(
          r.window, r.pane, i, len(file_results), '', destination)

      _LOGGER.info(
          'Moving temporary file %s to dir: %s as %s. Res: %s',
          r.file_name,
          self.path.get(),
          final_file_name,
          r)

      final_full_path = filesystems.FileSystems.join(
          self.path.get(), final_file_name)

      # TODO(pabloem): Batch rename requests?
      try:
        filesystems.FileSystems.rename([r.file_name], [final_full_path])
      except BeamIOError:
        # This error is not serious, because it may happen on a retry of the
        # bundle. We simply log it.
        _LOGGER.debug(
            'File %s failed to be copied. This may be due to a bundle'
            ' being retried.',
            r.file_name)

      yield FileResult(
          final_file_name, i, len(file_results), r.window, r.pane, destination)

    _LOGGER.info(
        'Cautiously removing temporary files for'
        ' destination %s and window %s',
        destination,
        w)
    writer_key = (destination, w)
    self._remove_temporary_files(writer_key)

  def _remove_temporary_files(self, writer_key):
    try:
      prefix = filesystems.FileSystems.join(
          self.temporary_directory.get(), str(abs(hash(writer_key))))
      match_result = filesystems.FileSystems.match(['%s*' % prefix])
      orphaned_files = [m.path for m in match_result[0].metadata_list]

      _LOGGER.debug('Deleting orphaned files: %s', orphaned_files)
      filesystems.FileSystems.delete(orphaned_files)
    except BeamIOError as e:
      _LOGGER.debug('Exceptions when deleting files: %s', e)


class _WriteShardedRecordsFn(beam.DoFn):

  def __init__(self,
               base_path,
               sink_fn: Callable[[Any], FileSink],
               shards: int
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

    full_file_name, writer = _create_writer(base_path=self.base_path.get(),
                                            writer_key=(destination, w))
    sink = self.sink_fn(destination)
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
      destination: Callable[[Any], str],
      shards: int
  ):
    self.destination_fn = destination
    self.shards = shards

    # We start the shards for a single destination at an arbitrary point.
    self._shard_counter: DefaultDict[str, int] = collections.defaultdict(
        lambda: random.randrange(self.shards))

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

  _writers_and_sinks: Dict[Tuple[str, BoundedWindow], Tuple[BinaryIO, FileSink]] = None
  _file_names: Dict[Tuple[str, BoundedWindow], str] = None

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
      full_file_name, writer = _create_writer(base_path=self.base_path.get(),
                                              writer_key=writer_key)
      sink = self.sink_fn(destination)

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
