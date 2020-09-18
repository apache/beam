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


def read_csv(path, *args, **kwargs):
  """Emulates `pd.read_csv` from Pandas, but as a Beam PTransform.

  Use this as

      df = p | beam.dataframe.io.read_csv(...)

  to get a deferred Beam dataframe representing the contents of the file.
  """
  return _ReadFromPandas(pd.read_csv, path, args, kwargs)


def write_csv(df, path, *args, **kwargs):
  from apache_beam.dataframe import convert  # avoid circular import
  # TODO(roberwb): Amortize the computation for multiple writes?
  return convert.to_pcollection(df) | _WriteToPandas(
      pd.DataFrame.to_csv, path, args, kwargs, incremental=True, binary=False)


def _prefix_range_index_with(prefix, df):
  if isinstance(df.index, pd.RangeIndex):
    return df.set_index(prefix + df.index.map(str).astype(str))
  else:
    return df


class _ReadFromPandas(beam.PTransform):
  def __init__(self, reader, path, args, kwargs):
    if not isinstance(path, str):
      raise frame_base.WontImplementError('non-deferred')
    self.reader = reader
    self.path = path
    self.args = args
    self.kwargs = kwargs

  def expand(self, root):
    # TODO(robertwb): Handle streaming (with explicit schema).
    paths_pcoll = root | beam.Create([self.path])
    first = io.filesystems.FileSystems.match([self.path],
                                             limits=[1
                                                     ])[0].metadata_list[0].path
    with io.filesystems.FileSystems.open(first) as handle:
      df = next(self.reader(handle, *self.args, chunksize=100, **self.kwargs))

    pcoll = (
        paths_pcoll
        | fileio.MatchFiles(self.path)
        | fileio.ReadMatches()
        | beam.ParDo(_ReadFromPandasDoFn(self.reader, self.args, self.kwargs)))
    from apache_beam.dataframe import convert
    return convert.to_dataframe(
        pcoll, proxy=_prefix_range_index_with(':', df[:0]))


# TODO(robertwb): Actually make an SDF.
class _ReadFromPandasDoFn(beam.DoFn):
  def __init__(self, reader, args, kwargs):
    # avoid pickling issues
    self.reader = reader.__name__
    self.args = args
    self.kwargs = kwargs

  def process(self, readable_file):
    reader = getattr(pd, self.reader)
    with readable_file.open() as handle:
      for df in reader(handle, *self.args, chunksize=100, **self.kwargs):
        yield _prefix_range_index_with(readable_file.metadata.path + ':', df)


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
    self.writer(df, non_none_handle, *self.args, **self.kwargs)
    if file_handle is None:
      return non_none_handle.getvalue()

  def write_record_incremental(self, value):
    if self.empty is None:
      self.empty = self.write_to(value[:0])
    if self.header is None and len(value):
      one_row = self.write_to(value[:1])
      for ix, c in enumerate(self.empty):
        if one_row[ix] != c:
          break
      else:
        ix = len(self.empty)
      self.header = self.empty[:ix]
      self.footer = self.empty[ix:]
      self.file_handle.write(self.header)

    if len(value):
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

  def flush_buffer(self, file_handle):
    if self.buffer:
      self.write_to(pd.concat(self.buffer), file_handle)
      self.file_handle.flush()
