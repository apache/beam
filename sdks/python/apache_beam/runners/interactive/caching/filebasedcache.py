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
import urllib

from apache_beam import coders
from apache_beam.io import avroio
from apache_beam.io import parquetio
from apache_beam.io import textio
from apache_beam.io import tfrecordio
from apache_beam.io.filesystems import FileSystems
from apache_beam.runners.interactive.caching import PCollectionCache
from apache_beam.typehints import trivial_inference
from apache_beam.typehints import typehints

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
    "SafeTextBasedCache",
    "TFRecordBasedCache",
    "ParquetBasedCache",
    "AvroBasedCache",
    "SafeFastPrimitivesCoder",
]


class FileBasedCache(PCollectionCache):

  def __init__(self, location, **writer_kwargs):
    self._file_path_prefix = location
    self._writer_kwargs = writer_kwargs
    self._coder_is_inferred = False

    if (not self._writer_kwargs.get("coder") and
        "coder" in self._reader_passthrough_arguments):
      self._writer_class = register_coder_patch(self._writer_class,
                                                self._writer_kwargs)
      self._coder_is_inferred = True

  def reader(self, **reader_kwargs):
    kwargs = {
        k: v
        for k, v in self._writer_kwargs.items()
        if k in self._reader_passthrough_arguments
    }
    kwargs.update(reader_kwargs)
    return self._reader_class(self._file_pattern, **kwargs)

  @property
  def _file_pattern(self):
    return self._file_path_prefix + '*'

  def writer(self):
    return self._writer_class(self._file_path_prefix, **self._writer_kwargs)

  def read(self, **reader_kwargs):
    source = self.reader(**reader_kwargs)._source
    range_tracker = source.get_range_tracker(None, None)
    for element in source.read(range_tracker):
      yield element

  def write(self, elements):
    writer = self.writer()
    if (not self._writer_kwargs.get("coder") and
        "coder" in self._reader_passthrough_arguments):
      # TODO(ostrokach): We may need to warn the user that we are loading
      # all elements in memory.
      elements = list(elements)
      element_type = typehints.Union[[
          trivial_inference.instance_to_type(e) for e in elements
      ]]
      register_coder(writer, self._writer_kwargs, element_type)
      self._coder_is_inferred = True
    handle = writer._sink.open(self._file_path_prefix)
    try:
      for element in elements:
        writer._sink.write_record(handle, element)
    finally:
      try:
        writer._sink.close(handle)
      except AttributeError:
        # Not all sinks implement the close() method
        pass

  def clear(self):
    paths = [
        match_meta.path
        for match_result in FileSystems.match([self._file_pattern])
        for match_meta in match_result.metadata_list
    ]
    FileSystems.delete(paths)
    if self._coder_is_inferred:
      del self._writer_kwargs["coder"]
      self._coder_is_inferred = False


class TextBasedCache(FileBasedCache):

  _reader_class = textio.ReadFromText
  _writer_class = textio.WriteToText
  _reader_passthrough_arguments = {"coder", "compression_type"}


class SafeTextBasedCache(FileBasedCache):

  _reader_class = textio.ReadFromText
  _writer_class = textio.WriteToText
  _reader_passthrough_arguments = {"coder", "compression_type"}

  def __init__(self, file_path_prefix, **writer_kwargs):
    writer_kwargs["coder"] = SafeFastPrimitivesCoder()
    super(SafeTextBasedCache, self).__init__(file_path_prefix, **writer_kwargs)


class TFRecordBasedCache(FileBasedCache):

  _reader_class = tfrecordio.ReadFromTFRecord
  _writer_class = tfrecordio.WriteToTFRecord
  _reader_passthrough_arguments = {"coder", "compression_type"}


class ParquetBasedCache(FileBasedCache):

  _reader_class = parquetio.ReadFromParquet
  _writer_class = parquetio.WriteToParquet
  _reader_passthrough_arguments = {}


class AvroBasedCache(FileBasedCache):

  _reader_class = avroio.ReadFromAvro
  _writer_class = avroio.WriteToAvro
  _reader_passthrough_arguments = {"use_fastavro"}


def register_coder(writer, writer_kwargs, element_type):
  coder = coders.registry.get_coder(element_type)
  writer_kwargs["coder"] = coder
  writer._sink.coder = coder


def register_coder_patch(writer_class, writer_kwargs):

  class PatchedWriter(writer_class):

    def expand(self, pcoll):
      register_coder(self, writer_kwargs, pcoll.element_type)
      return super(PatchedWriter, self).expand(pcoll)

  return PatchedWriter


class SafeFastPrimitivesCoder(coders.Coder):
  """This class add an quote/unquote step to escape special characters."""

  # pylint: disable=deprecated-urllib-function

  def encode(self, value):
    return quote(
        coders.coders.FastPrimitivesCoder().encode(value)).encode('utf-8')

  def decode(self, value):
    return coders.coders.FastPrimitivesCoder().decode(unquote_to_bytes(value))
