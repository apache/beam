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

"""Sources and sinks for the Beam DataFrame API.

Sources
#######
This module provides analogs for pandas ``read`` methods, like
:func:`pandas.read_csv`. However Beam sources like :func:`read_csv`
create a Beam :class:`~apache_beam.PTransform`, and return a
:class:`~apache_beam.dataframe.frames.DeferredDataFrame` or
:class:`~apache_beam.dataframe.frames.DeferredSeries` representing the contents
of the referenced file(s) or data source.

The result of these methods must be applied to a :class:`~apache_beam.Pipeline`
object, for example::

    df = p | beam.dataframe.io.read_csv(...)

Sinks
#####
This module also defines analogs for pandas sink, or ``to``, methods that
generate a Beam :class:`~apache_beam.PTransform`. Users should prefer calling
these operations from :class:`~apache_beam.dataframe.frames.DeferredDataFrame`
instances (for example with
:meth:`DeferredDataFrame.to_csv
<apache_beam.dataframe.frames.DeferredDataFrame.to_csv>`).
"""

import itertools
import re
from io import BytesIO
from io import StringIO
from io import TextIOWrapper

import pandas as pd

import apache_beam as beam
from apache_beam import io
from apache_beam.dataframe import frame_base
from apache_beam.io import fileio

_DEFAULT_LINES_CHUNKSIZE = 10_000
_DEFAULT_BYTES_CHUNKSIZE = 1 << 20


@frame_base.with_docs_from(pd)
def read_csv(path, *args, splittable=False, **kwargs):
  """If your files are large and records do not contain quoted newlines, you may
  pass the extra argument ``splittable=True`` to enable dynamic splitting for
  this read on newlines. Using this option for records that do contain quoted
  newlines may result in partial records and data corruption."""
  if 'nrows' in kwargs:
    raise ValueError('nrows not yet supported')
  return _ReadFromPandas(
      pd.read_csv,
      path,
      args,
      kwargs,
      incremental=True,
      splitter=_CsvSplitter(args, kwargs) if splittable else None)


def _as_pc(df):
  from apache_beam.dataframe import convert  # avoid circular import
  # TODO(roberwb): Amortize the computation for multiple writes?
  return convert.to_pcollection(df, yield_elements='pandas')


@frame_base.with_docs_from(pd.DataFrame)
def to_csv(df, path, *args, **kwargs):

  return _as_pc(df) | _WriteToPandas(
      'to_csv', path, args, kwargs, incremental=True, binary=False)


@frame_base.with_docs_from(pd)
def read_fwf(path, *args, **kwargs):
  return _ReadFromPandas(pd.read_fwf, path, args, kwargs, incremental=True)


@frame_base.with_docs_from(pd)
def read_json(path, *args, **kwargs):
  if 'nrows' in kwargs:
    raise NotImplementedError('nrows not yet supported')
  elif kwargs.get('lines', False):
    # Work around https://github.com/pandas-dev/pandas/issues/34548.
    kwargs = dict(kwargs, nrows=1 << 63)
  return _ReadFromPandas(
      pd.read_json,
      path,
      args,
      kwargs,
      incremental=kwargs.get('lines', False),
      splitter=_DelimSplitter(b'\n', _DEFAULT_BYTES_CHUNKSIZE) if kwargs.get(
          'lines', False) else None,
      binary=False)


@frame_base.with_docs_from(pd.DataFrame)
def to_json(df, path, orient=None, *args, **kwargs):
  if orient is None:
    if isinstance(df._expr.proxy(), pd.DataFrame):
      orient = 'columns'
    elif isinstance(df._expr.proxy(), pd.Series):
      orient = 'index'
    else:
      raise frame_base.WontImplementError('not dataframes or series')
  kwargs['orient'] = orient
  return _as_pc(df) | _WriteToPandas(
      'to_json',
      path,
      args,
      kwargs,
      incremental=orient in ('index', 'records', 'values'),
      binary=False)


@frame_base.with_docs_from(pd)
def read_html(path, *args, **kwargs):
  return _ReadFromPandas(
      lambda *args,
      **kwargs: pd.read_html(*args, **kwargs)[0],
      path,
      args,
      kwargs)


@frame_base.with_docs_from(pd.DataFrame)
def to_html(df, path, *args, **kwargs):
  return _as_pc(df) | _WriteToPandas(
      'to_html',
      path,
      args,
      kwargs,
      incremental=(
          df._expr.proxy().index.nlevels == 1 or
          not kwargs.get('sparsify', True)),
      binary=False)


def _binary_reader(format):
  func = getattr(pd, 'read_%s' % format)
  result = lambda path, *args, **kwargs: _ReadFromPandas(func, path, args,
                                                         kwargs)
  result.__name__ = f'read_{format}'

  return result


def _binary_writer(format):
  result = (
      lambda df,
      path,
      *args,
      **kwargs: _as_pc(df) | _WriteToPandas(f'to_{format}', path, args, kwargs))
  result.__name__ = f'to_{format}'
  return result


for format in ('excel', 'feather', 'parquet', 'stata'):
  globals()['read_%s' % format] = frame_base.with_docs_from(pd)(
      _binary_reader(format))
  globals()['to_%s' % format] = frame_base.with_docs_from(pd.DataFrame)(
      _binary_writer(format))

for format in ('sas', 'spss'):
  if hasattr(pd, 'read_%s' % format):  # Depends on pandas version.
    globals()['read_%s' % format] = frame_base.with_docs_from(pd)(
        _binary_reader(format))

read_clipboard = frame_base.not_implemented_method(
    'read_clipboard', base_type=pd)
to_clipboard = frame_base.not_implemented_method(
    'to_clipboard', base_type=pd.DataFrame)
read_msgpack = frame_base.wont_implement_method(
    pd, 'read_msgpack', reason="deprecated")
to_msgpack = frame_base.wont_implement_method(
    pd.DataFrame, 'to_msgpack', reason="deprecated")
read_hdf = frame_base.wont_implement_method(
    pd, 'read_hdf', explanation="because HDF5 is a random access file format")
to_hdf = frame_base.wont_implement_method(
    pd.DataFrame,
    'to_hdf',
    explanation="because HDF5 is a random access file format")

for name in dir(pd):
  if name.startswith('read_') and name not in globals():
    globals()[name] = frame_base.not_implemented_method(name, base_type=pd)


def _prefix_range_index_with(prefix, df):
  if isinstance(df.index, pd.RangeIndex):
    return df.set_index(prefix + df.index.map(str).astype(str))
  else:
    return df


class _ReadFromPandas(beam.PTransform):
  def __init__(
      self,
      reader,
      path,
      args,
      kwargs,
      binary=True,
      incremental=False,
      splitter=False):
    if 'compression' in kwargs:
      raise NotImplementedError('compression')
    if not isinstance(path, str):
      raise frame_base.WontImplementError('non-deferred')
    self.reader = reader
    self.path = path
    self.args = args
    self.kwargs = kwargs
    self.binary = binary
    self.incremental = incremental
    self.splitter = splitter

  def expand(self, root):
    paths_pcoll = root | beam.Create([self.path])
    match = io.filesystems.FileSystems.match([self.path], limits=[1])[0]
    if not match.metadata_list:
      # TODO(BEAM-12031): This should be allowed for streaming pipelines if
      # user provides an explicit schema.
      raise FileNotFoundError(f"Found no files that match {self.path!r}")
    first_path = match.metadata_list[0].path
    with io.filesystems.FileSystems.open(first_path) as handle:
      if not self.binary:
        handle = TextIOWrapper(handle)
      if self.incremental:
        sample = next(
            self.reader(handle, *self.args, **dict(self.kwargs, chunksize=100)))
      else:
        sample = self.reader(handle, *self.args, **self.kwargs)

    pcoll = (
        paths_pcoll
        | fileio.MatchFiles(self.path)
        | beam.Reshuffle()
        | fileio.ReadMatches()
        | beam.ParDo(
            _ReadFromPandasDoFn(
                self.reader,
                self.args,
                self.kwargs,
                self.binary,
                self.incremental,
                self.splitter)))
    from apache_beam.dataframe import convert
    return convert.to_dataframe(
        pcoll, proxy=_prefix_range_index_with(':', sample[:0]))


class _Splitter:
  def empty_buffer(self):
    """Returns an empty buffer of the right type (string or bytes).
    """
    raise NotImplementedError(self)

  def read_header(self, handle):
    """Reads the header from handle, which points to the start of the file.

    Returns the pair (header, buffer) where buffer contains any part of the
    file that was "overread" from handle while seeking the end of header.
    """
    raise NotImplementedError(self)

  def read_to_record_boundary(self, buffered, handle):
    """Reads the given handle up to the end of the current record.

    The buffer argument represents bytes that were read previously; logically
    it's as if these were pushed back into handle for reading. If the
    record end is within buffered, it's possible that no more bytes will be read
    from handle at all.

    Returns the pair (remaining_record_bytes, buffer) where buffer contains
    any part of the file that was "overread" from handle while seeking the end
    of the record.
    """
    raise NotImplementedError(self)


class _DelimSplitter(_Splitter):
  """A _Splitter that splits on delimiters between records.

  This delimiter is assumed ot never occur within a record.
  """
  def __init__(self, delim, read_chunk_size=_DEFAULT_BYTES_CHUNKSIZE):
    # Multi-char delimiters would require more care across chunk boundaries.
    assert len(delim) == 1
    self._delim = delim
    self._empty = delim[:0]
    self._read_chunk_size = read_chunk_size

  def empty_buffer(self):
    return self._empty

  def read_header(self, handle):
    return self._empty, self._empty

  def read_to_record_boundary(self, buffered, handle):
    if self._delim in buffered:
      ix = buffered.index(self._delim) + len(self._delim)
      return buffered[:ix], buffered[ix:]
    else:
      while True:
        chunk = handle.read(self._read_chunk_size)
        if self._delim in chunk:
          ix = chunk.index(self._delim) + len(self._delim)
          return buffered + chunk[:ix], chunk[ix:]
        elif not chunk:
          return buffered, self._empty
        else:
          buffered += chunk


def _maybe_encode(str_or_bytes):
  if isinstance(str_or_bytes, str):
    return str_or_bytes.encode('utf-8')
  else:
    return str_or_bytes


class _CsvSplitter(_DelimSplitter):
  """Splitter for dynamically sharding CSV files and newline record boundaries.

  Currently does not handle quoted newlines, so is off by default, but such
  support could be added in the future.
  """
  def __init__(self, args, kwargs, read_chunk_size=_DEFAULT_BYTES_CHUNKSIZE):
    if args:
      # TODO(robertwb): Automatically populate kwargs as we do for df methods.
      raise ValueError(
          'Non-path arguments must be passed by keyword '
          'for splittable csv reads.')
    if kwargs.get('skipfooter', 0):
      raise ValueError('Splittablility incompatible with skipping footers.')
    super(_CsvSplitter, self).__init__(
        _maybe_encode(kwargs.get('lineterminator', b'\n')),
        _DEFAULT_BYTES_CHUNKSIZE)
    self._kwargs = kwargs

  def read_header(self, handle):
    if self._kwargs.get('header', 'infer') == 'infer':
      if 'names' in self._kwargs:
        header = None
      else:
        header = 0
    else:
      header = self._kwargs['header']

    if header is None:
      return self._empty, self._empty

    if isinstance(header, int):
      max_header = header
    else:
      max_header = max(header)

    skiprows = self._kwargs.get('skiprows', 0)
    if isinstance(skiprows, int):
      is_skiprow = lambda ix: ix < skiprows
    elif callable(skiprows):
      is_skiprow = skiprows
    elif skiprows is None:
      is_skiprow = lambda ix: False
    else:
      is_skiprow = lambda ix: ix in skiprows

    comment = _maybe_encode(self._kwargs.get('comment', None))
    if comment:
      is_comment = lambda line: line.startswith(comment)
    else:
      is_comment = lambda line: False

    skip_blank_lines = self._kwargs.get('skip_blank_lines', True)
    if skip_blank_lines:
      is_blank = lambda line: re.match(rb'^\s*$', line)
    else:
      is_blank = lambda line: False

    text_header = b''
    rest = b''
    skipped = 0
    for ix in itertools.count():
      line, rest = self.read_to_record_boundary(rest, handle)
      text_header += line
      if is_skiprow(ix) or is_blank(line) or is_comment(line):
        skipped += 1
        continue
      if ix - skipped == max_header:
        return text_header, rest


class _TruncatingFileHandle(object):
  """A wrapper of a file-like object representing the restriction of the
  underling handle according to the given SDF restriction tracker, breaking
  the file only after the given delimiter.

  For example, if the underling restriction is [103, 607) and each line were
  exactly 10 characters long (i.e. every 10th charcter was a newline), then this
  would give a view of a 500-byte file consisting of bytes bytes 110 to 609
  (inclusive) of the underlying file.

  As with all SDF trackers, the endpoint may change dynamically during reading.
  """
  def __init__(self, underlying, tracker, splitter):
    self._underlying = underlying
    self._tracker = tracker
    self._splitter = splitter

    self._empty = self._splitter.empty_buffer()
    self._done = False
    self._header, self._buffer = self._splitter.read_header(self._underlying)
    self._buffer_start_pos = len(self._header)
    start = self._tracker.current_restriction().start
    # Seek to first delimiter after the start position.
    if start > len(self._header):
      if start > len(self._header) + len(self._buffer):
        self._buffer_start_pos = start
        self._buffer = self._empty
        self._underlying.seek(start)
      else:
        self._buffer_start_pos = start
        self._buffer = self._buffer[start - len(self._header):]
      skip, self._buffer = self._splitter.read_to_record_boundary(
          self._buffer, self._underlying)
      self._buffer_start_pos += len(skip)

  def readable(self):
    return True

  def writable(self):
    return False

  def seekable(self):
    return False

  @property
  def closed(self):
    return False

  def __iter__(self):
    # For pandas is_file_like.
    raise NotImplementedError()

  def read(self, size=-1):
    if self._header:
      res = self._header
      self._header = None
      return res
    elif self._done:
      return self._empty
    elif size == -1:
      self._buffer += self._underlying.read()
    elif not self._buffer:
      self._buffer = self._underlying.read(size)

    if not self._buffer:
      self._done = True
      return self._empty

    if self._tracker.try_claim(self._buffer_start_pos + len(self._buffer)):
      res = self._buffer
      self._buffer = self._empty
      self._buffer_start_pos += len(res)
    else:
      offset = self._tracker.current_restriction().stop - self._buffer_start_pos
      if offset <= 0:
        res = self._empty
      else:
        rest, _ = self._splitter.read_to_record_boundary(
            self._buffer[offset:], self._underlying)
        res = self._buffer[:offset] + rest
      self._done = True
    return res


class _ReadFromPandasDoFn(beam.DoFn, beam.RestrictionProvider):
  def __init__(self, reader, args, kwargs, binary, incremental, splitter):
    # avoid pickling issues
    if reader.__module__.startswith('pandas.'):
      reader = reader.__name__
    self.reader = reader
    self.args = args
    self.kwargs = kwargs
    self.binary = binary
    self.incremental = incremental
    self.splitter = splitter

  def initial_restriction(self, readable_file):
    return beam.io.restriction_trackers.OffsetRange(
        0, readable_file.metadata.size_in_bytes)

  def restriction_size(self, readable_file, restriction):
    return restriction.size()

  def create_tracker(self, restriction):
    tracker = beam.io.restriction_trackers.OffsetRestrictionTracker(restriction)
    if self.splitter:
      return tracker
    else:
      return beam.io.restriction_trackers.UnsplittableRestrictionTracker(
          tracker)

  def process(self, readable_file, tracker=beam.DoFn.RestrictionParam()):
    reader = self.reader
    if isinstance(reader, str):
      reader = getattr(pd, self.reader)
    with readable_file.open() as handle:
      if self.incremental:
        # TODO(robertwb): We could consider trying to get progress for
        # non-incremental sources that are read linearly, as long as they
        # don't try to seek.  This could be deceptive as progress would
        # advance to 100% the instant the (large) read was done, discounting
        # any downstream processing.
        handle = _TruncatingFileHandle(
            handle,
            tracker,
            splitter=self.splitter or
            _DelimSplitter(b'\n', _DEFAULT_BYTES_CHUNKSIZE))
      if not self.binary:
        handle = TextIOWrapper(handle)
      if self.incremental:
        if 'chunksize' not in self.kwargs:
          self.kwargs['chunksize'] = _DEFAULT_LINES_CHUNKSIZE
        frames = reader(handle, *self.args, **self.kwargs)
      else:
        frames = [reader(handle, *self.args, **self.kwargs)]
      for df in frames:
        yield _prefix_range_index_with(readable_file.metadata.path + ':', df)
      if not self.incremental:
        # Satisfy the SDF contract by claiming the whole range.
        # Do this after emitting the frames to avoid advancing progress to 100%
        # prior to that.
        tracker.try_claim(tracker.current_restriction().stop)


class _WriteToPandas(beam.PTransform):
  def __init__(
      self, writer, path, args, kwargs, incremental=False, binary=True):
    self.writer = writer
    self.path = path
    self.args = args
    self.kwargs = kwargs
    self.incremental = incremental
    self.binary = binary

  def expand(self, pcoll):
    dir, name = io.filesystems.FileSystems.split(self.path)
    return pcoll | fileio.WriteToFiles(
        path=dir,
        file_naming=fileio.default_file_naming(name),
        sink=lambda _: _WriteToPandasFileSink(
            self.writer, self.args, self.kwargs, self.incremental, self.binary))


class _WriteToPandasFileSink(fileio.FileSink):
  def __init__(self, writer, args, kwargs, incremental, binary):
    if 'compression' in kwargs:
      raise NotImplementedError('compression')
    self.writer = writer
    self.args = args
    self.kwargs = kwargs
    self.incremental = incremental
    self.binary = binary
    self.StringOrBytesIO = BytesIO if binary else StringIO
    if incremental:
      self.write = self.write_record_incremental
      self.flush = self.close_incremental
    else:
      self.write = self.buffer_record
      self.flush = self.flush_buffer

  def open(self, file_handle):
    self.buffer = []
    self.empty = self.header = self.footer = None
    if not self.binary:
      file_handle = TextIOWrapper(file_handle)
    self.file_handle = file_handle

  def write_to(self, df, file_handle=None):
    non_none_handle = file_handle or self.StringOrBytesIO()
    getattr(df, self.writer)(non_none_handle, *self.args, **self.kwargs)
    if file_handle is None:
      return non_none_handle.getvalue()

  def write_record_incremental(self, value):
    if self.empty is None:
      self.empty = self.write_to(value[:0])
    if self.header is None and len(value):

      def new_value(ix):
        if isinstance(ix, tuple):
          return (new_value(ix[0]), ) + ix[1:]
        else:
          return str('x') + '_again'

      def change_index(df):
        df.index = df.index.map(new_value)
        return df

      one_row = self.write_to(value[:1])
      another_row = self.write_to(change_index(value[:1]))
      two_rows = self.write_to(pd.concat([value[:1], change_index(value[:1])]))
      for ix, c in enumerate(self.empty):
        if one_row[ix] != c:
          break
      else:
        ix = len(self.empty)
      self.header = self.empty[:ix]
      self.footer = self.empty[ix:]
      self.delimiter = two_rows[len(one_row) - len(self.footer):-(
          len(another_row) - len(self.header)) or None]
      self.file_handle.write(self.header)
      self.first = True

    if len(value):
      if self.first:
        self.first = False
      else:
        self.file_handle.write(self.delimiter)

      # IDEA(robertwb): Construct a "truncating" stream wrapper to avoid the
      # in-memory copy.
      rows = self.write_to(value)
      self.file_handle.write(rows[len(self.header):-len(self.footer) or None])

  def close_incremental(self):
    if self.footer is not None:
      self.file_handle.write(self.footer)
    elif self.empty is not None:
      self.file_handle.write(self.empty)
    self.file_handle.flush()

  def buffer_record(self, value):
    self.buffer.append(value)

  def flush_buffer(self):
    if self.buffer:
      self.write_to(pd.concat(self.buffer), self.file_handle)
      self.file_handle.flush()
