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

import urllib

from apache_beam import coders
from apache_beam.io import textio
from apache_beam.io import tfrecordio
from apache_beam.io.filesystems import FileSystems
from apache_beam.runners.interactive.caching import PCollectionCache
from apache_beam.testing import datatype_inference
from apache_beam.transforms import PTransform

try:
  from weakref import finalize
except ImportError:
  from backports.weakref import finalize

try:  # Python 3
  unquote_to_bytes = urllib.parse.unquote_to_bytes
  quote = urllib.parse.quote
except AttributeError:  # Python 2
  # pylint: disable=deprecated-urllib-function
  unquote_to_bytes = urllib.unquote
  quote = urllib.quote

__all__ = [
    "FileBasedCache",
    "TextBasedCache",
    "TFRecordBasedCache",
    "SafeFastPrimitivesCoder",
]


class FileBasedCache(PCollectionCache):

  def __init__(self, location, mode="error", persist=False, **writer_kwargs):
    """Initialize a PCollectionCache object.

    Args:
      location (str): A string indicating the location where the cache should
          be stored. Typically, this includes the folder and the filename
          prefix for files that will be created.
      mode (str): Controls the behavior when one or more files matching location
          already exit. If "error", an IOError will be raised. If "overwrite",
          the existing files will be removed.
      persist (bool): A flag indicating whether the underlying data should be
          destroyed when the cache instance goes out of scope.
      writer_kwargs (Dict[str, Any]): A dictionary of key-value pairs
          to be passed to the underlying writer objects.

    Raises:
      ~exceptions.ValueError: If the specified mode is not supported.
      ~exceptions.IOError: If one or more files matching `location` already
          exist and mode == "error".
    """
    self.location = location
    self.element_type = None
    self.default_coder = writer_kwargs.pop("coder", None)
    self._writer_kwargs = writer_kwargs
    self._num_writes = 0
    self._persist = persist
    self._finalizer = self._configure_finalizer(persist)

    # TODO(ostrokach): Implement append mode.
    if mode not in ['error', 'overwrite']:
      raise ValueError("'mode' must be set to one of: ['error', 'overwrite'].")
    exitsting_files = list(glob_files(self.file_pattern))
    if mode == "error" and exitsting_files:
      raise IOError("The following cache files already exist: {}.".format(
          exitsting_files))
    if mode == "overwrite":
      self.truncate()

    root, _ = FileSystems.split(self._file_path_prefix)
    try:
      FileSystems.mkdirs(root)
    except IOError:
      pass
    # It is possible to read from am empty stream, so it should also be possible
    # to read from an empty file.
    FileSystems.create(self._file_path_prefix + ".empty").close()

  @property
  def persist(self):
    return self._persist

  @persist.setter
  def persist(self, persist):
    if self._persist == persist:
      return
    self._persist = persist
    if self._finalizer:
      self._finalizer.detach()
    self._finalizer = self._configure_finalizer(persist)

  @property
  def timestamp(self):
    timestamp = 0
    for path in glob_files(self.file_pattern):
      timestamp = max(timestamp, FileSystems.last_updated(path))
    return timestamp

  def reader(self, **kwargs):
    """Returns a reader ``PTransform`` to be used for reading the contents of
    the cache into a Beam Pipeline.

    Args:
      kwargs (Dict[str, Any]): Arguments to be passed to the underlying reader
          class.
    """
    reader_kwargs = self._reader_kwargs.copy()
    reader_kwargs.update(kwargs)
    reader = self._reader_class(self.file_pattern, **reader_kwargs)
    # Keep a reference to the parent object so that cache does not get garbage
    # collected while the pipeline is running.
    reader._cache = self
    return reader

  def writer(self):
    """Returns a writer object to be used for writing the contents of a
    PCollection from a Beam Pipeline into the cache.
    """
    self._num_writes += 1
    writer_kwargs = self._writer_kwargs.copy()

    if self.element_type is None:
      writer = PatchedWriter(self, self._writer_class,
                             (self._file_path_prefix,), writer_kwargs)
      return writer

    if ("coder" in self._reader_passthrough_arguments and
        "coder" not in writer_kwargs):
      writer_kwargs["coder"] = (
          self.default_coder if self.default_coder is not None else
          coders.registry.get_coder(self.element_type))
    writer = self._writer_class(self._file_path_prefix, **writer_kwargs)
    # Keep a reference to the parent object so that cache does not get garbage
    # collected while the pipeline is running.
    writer._cache = self
    return writer

  def read(self, **kwargs):
    """Returns an iterator over the contents of the cache.

    Args:
      kwargs (Dict[str, Any]): Arguments to be passed to the underlying reader
          class.

    Returns:
      An iterator over the elements in the cache.
    """
    reader_kwargs = self._reader_kwargs.copy()
    reader_kwargs.update(kwargs)
    source = self.reader(**reader_kwargs)._source
    range_tracker = source.get_range_tracker(None, None)
    for element in source.read(range_tracker):
      yield element

  def write(self, elements):
    """Writes a collection of elements into the cache.

    Args:
      elements (Iterable[Any]): A collection of elements to be written to cache.
    """
    self._num_writes += 1
    if self.element_type is None:
      # TODO(ostrokach): We might want to infer the element type from the first
      # N elements, rather than reading the entire iterator.
      elements = list(elements)
      self.element_type = datatype_inference.infer_element_type(elements)
    sink = self.writer()._sink
    handle = sink.open(self._file_path_prefix)
    try:
      for element in elements:
        sink.write_record(handle, element)
    finally:
      sink.close(handle)

  def truncate(self):
    """Removes all contents from the cache."""
    FileSystems.delete(list(glob_files(self.file_pattern)))
    FileSystems.create(self._file_path_prefix + ".empty").close()
    self.element_type = None

  def remove(self):
    """Deletes the cache, including all underlying data.

    The cache should not be used after this method is called.
    """
    self._finalizer()

  @property
  def removed(self):
    return not self._finalizer.alive

  def __del__(self):
    self.remove()

  @property
  def file_pattern(self):
    return self.location + '**'

  @property
  def _reader_kwargs(self):
    reader_kwargs = {
        k: v for k, v in self._writer_kwargs.items()
        if k in self._reader_passthrough_arguments
    }
    if ("coder" in self._reader_passthrough_arguments and
        "coder" not in reader_kwargs):
      if self.default_coder is not None:
        reader_kwargs["coder"] = self.default_coder
      elif self.element_type is None:
        reader_kwargs["coder"] = None
      else:
        reader_kwargs["coder"] = coders.registry.get_coder(self.element_type)
    return reader_kwargs

  @property
  def _file_path_prefix(self):
    return self.location + "-{:03d}".format(self._num_writes)

  def _configure_finalizer(self, persist):
    if persist:
      return finalize(self, lambda: None)
    else:
      return finalize(
          self, lambda pattern: FileSystems.delete(list(glob_files(pattern))),
          self.file_pattern)


class TextBasedCache(FileBasedCache):
  """A ``PCollectionCache`` object which uses a text-based file format to store
  the underlying data.
  """
  _reader_class = textio.ReadFromText
  _writer_class = textio.WriteToText
  _reader_passthrough_arguments = {"coder", "compression_type"}

  def __init__(self, location, **writer_kwargs):
    writer_kwargs["coder"] = SafeFastPrimitivesCoder()
    super(TextBasedCache, self).__init__(location, **writer_kwargs)


class TFRecordBasedCache(FileBasedCache):
  """A ``PCollectionCache`` object which uses the TFRecord file format to store
  the underlying data.
  """

  _reader_class = tfrecordio.ReadFromTFRecord
  _writer_class = tfrecordio.WriteToTFRecord
  _reader_passthrough_arguments = {"coder", "compression_type"}


class PatchedWriter(PTransform):
  """A wrapper over a write ``PTransform`` which sets the element_type of the
  parent cache when the writer is expanded into a pipeline.
  """

  def __init__(self, cache, writer_class, writer_args, writer_kwargs):
    self._cache = cache
    self._writer_class = writer_class
    self._writer_args = writer_args
    self._writer_kwargs = writer_kwargs

  def expand(self, pcoll):
    if self._cache.element_type is None:
      self._cache.element_type = pcoll.element_type
    writer_kwargs = self._writer_kwargs.copy()
    if "coder" in self._cache._reader_passthrough_arguments:
      writer_kwargs["coder"] = (
          self._cache.default_coder if self._cache.default_coder is not None
          else coders.registry.get_coder(self._cache.element_type))
    writer = self._writer_class(*self._writer_args, **writer_kwargs)
    return pcoll | writer


class SafeFastPrimitivesCoder(coders.Coder):
  """This class add an quote/unquote step to escape special characters."""

  def encode(self, value):
    return quote(
        coders.coders.FastPrimitivesCoder().encode(value)).encode('utf-8')

  def decode(self, value):
    return coders.coders.FastPrimitivesCoder().decode(unquote_to_bytes(value))


def glob_files(pattern):
  match = FileSystems.match([pattern])
  assert len(match) == 1
  for metadata in match[0].metadata_list:
    yield metadata.path
