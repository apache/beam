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

from __future__ import absolute_import

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


def read_csv(path, *args, **kwargs):
  """Emulates `pd.read_csv` from Pandas, but as a Beam PTransform.

  Use this as

      df = p | beam.dataframe.io.read_csv(...)

  to get a deferred Beam dataframe representing the contents of the file.
  """
  return _ReadFromPandas(pd.read_csv, path, args, kwargs, incremental=True)


def _as_pc(df):
  from apache_beam.dataframe import convert  # avoid circular import
  # TODO(roberwb): Amortize the computation for multiple writes?
  return convert.to_pcollection(df, yield_elements='pandas')


def to_csv(df, path, *args, **kwargs):
  return _as_pc(df) | _WriteToPandas(
      'to_csv', path, args, kwargs, incremental=True, binary=False)


def read_fwf(path, *args, **kwargs):
  return _ReadFromPandas(pd.read_fwf, path, args, kwargs, incremental=True)


def read_json(path, *args, **kwargs):
  return _ReadFromPandas(
      pd.read_json,
      path,
      args,
      kwargs,
      incremental=kwargs.get('lines', False),
      splittable=kwargs.get('lines', False),
      binary=False)


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


def read_html(path, *args, **kwargs):
  return _ReadFromPandas(
      lambda *args,
      **kwargs: pd.read_html(*args, **kwargs)[0],
      path,
      args,
      kwargs)


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
  return lambda path, *args, **kwargs: _ReadFromPandas(func, path, args, kwargs)


def _binary_writer(format):
  return (
      lambda df,
      path,
      *args,
      **kwargs: _as_pc(df) | _WriteToPandas(f'to_{format}', path, args, kwargs))


for format in ('excel', 'feather', 'parquet', 'stata'):
  globals()['read_%s' % format] = _binary_reader(format)
  globals()['to_%s' % format] = _binary_writer(format)

for format in ('sas', 'spss'):
  if hasattr(pd, 'read_%s' % format):  # Depends on pandas version.
    globals()['read_%s' % format] = _binary_reader(format)

read_clipboard = to_clipboard = frame_base.wont_implement_method('clipboard')
read_msgpack = to_msgpack = frame_base.wont_implement_method('deprecated')
read_hdf = to_hdf = frame_base.wont_implement_method('random access files')

for name in dir(pd):
  if name.startswith('read_') and name not in globals():
    globals()[name] = frame_base.not_implemented_method(name)


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
      incremental=False,
      splittable=False,
      binary=True):
    if 'compression' in kwargs:
      raise NotImplementedError('compression')
    if not isinstance(path, str):
      raise frame_base.WontImplementError('non-deferred')
    self.reader = reader
    self.path = path
    self.args = args
    self.kwargs = kwargs
    self.incremental = incremental
    self.splittable = splittable
    self.binary = binary

  def expand(self, root):
    # TODO(robertwb): Handle streaming (with explicit schema).
    paths_pcoll = root | beam.Create([self.path])
    first = io.filesystems.FileSystems.match([self.path],
                                             limits=[1
                                                     ])[0].metadata_list[0].path
    with io.filesystems.FileSystems.open(first) as handle:
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
                self.incremental,
                self.splittable,
                self.binary)))
    from apache_beam.dataframe import convert
    return convert.to_dataframe(
        pcoll, proxy=_prefix_range_index_with(':', sample[:0]))


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
  def __init__(
      self,
      underlying,
      tracker,
      delim=b'\n',
      chunk_size=_DEFAULT_BYTES_CHUNKSIZE):
    self._underlying = underlying
    self._tracker = tracker
    self._buffer_start_pos = self._tracker.current_restriction().start
    self._delim = delim
    self._chunk_size = chunk_size

    self._buffer = self._empty = self._delim[:0]
    self._done = False
    if self._buffer_start_pos > 0:
      # Seek to first delimiter after the start position.
      self._underlying.seek(self._buffer_start_pos)
      if self.buffer_to_delim():
        line_start = self._buffer.index(self._delim) + len(self._delim)
        self._buffer_start_pos += line_start
        self._buffer = self._buffer[line_start:]
      else:
        self._done = True

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

  def buffer_to_delim(self, offset=0):
    """Read enough of the file such that the buffer contains the delimiter, or
    end-of-file is reached.
    """
    if self._delim in self._buffer[offset:]:
      return True
    while True:
      chunk = self._underlying.read(self._chunk_size)
      self._buffer += chunk
      if self._delim in chunk:
        return True
      elif not chunk:
        return False

  def read(self, size=-1):
    if self._done:
      return self._empty
    elif size == -1:
      self._buffer += self._underlying.read()
    elif not self._buffer:
      self._buffer = self._underlying.read(size)

    if self._tracker.try_claim(self._buffer_start_pos + len(self._buffer)):
      res = self._buffer
      self._buffer = self._empty
      self._buffer_start_pos += len(res)
    else:
      offset = self._tracker.current_restriction().stop - self._buffer_start_pos
      if self.buffer_to_delim(offset):
        end_of_line = self._buffer.index(self._delim, offset)
        res = self._buffer[:end_of_line + len(self._delim)]
      else:
        res = self._buffer
      self._done = True
    return res


class _ReadFromPandasDoFn(beam.DoFn, beam.RestrictionProvider):
  def __init__(self, reader, args, kwargs, incremental, splittable, binary):
    # avoid pickling issues
    if reader.__module__.startswith('pandas.'):
      reader = reader.__name__
    self.reader = reader
    self.args = args
    self.kwargs = kwargs
    self.incremental = incremental
    self.splittable = splittable
    self.binary = binary

  def initial_restriction(self, readable_file):
    return beam.io.restriction_trackers.OffsetRange(
        0, readable_file.metadata.size_in_bytes)

  def restriction_size(self, readable_file, restriction):
    return restriction.size()

  def create_tracker(self, restriction):
    tracker = beam.io.restriction_trackers.OffsetRestrictionTracker(restriction)
    if self.splittable:
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
        # We can get progress even if we can't split.
        # TODO(robertwb): We could consider trying to get progress for
        # non-incremental sources that are read linearly, as long as they
        # don't try to seek.  This could be deceptive as progress would
        # advance to 100% the instant the (large) read was done, discounting
        # any downstream processing.
        handle = _TruncatingFileHandle(handle, tracker)
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
        sink=_WriteToPandasFileSink(
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
