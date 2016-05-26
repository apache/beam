# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Worker utilities related to custom sources."""

from google.cloud.dataflow.internal import pickler
from google.cloud.dataflow.internal.json_value import from_json_value
from google.cloud.dataflow.io import iobase
from google.cloud.dataflow.utils import names

DEFAULT_DESIRED_BUNDLE_SIZE = 64 * (1 << 20)  # 64MB


class NativeBoundedSource(iobase.NativeSource):

  def __init__(self, bounded_source, start_position, stop_position):
    self.bounded_source = bounded_source
    self.start_position = start_position
    self.stop_position = stop_position

  def reader(self):
    return NativeBoundedSourceReader(self)


class NativeBoundedSourceReader(iobase.NativeSourceReader):
  """A native source reader for reading custom sources."""

  def __init__(self, source):
    self._source = source

  def __exit__(self, exception_type, exception_value, traceback):
    pass

  def __enter__(self):
    return self

  def __iter__(self):
    range_tracker = self._source.bounded_source.get_range_tracker(
        self._source.start_position, self._source.stop_position)
    return self._source.bounded_source.read(range_tracker) or iter([])


class SourceOperationSplitTask(object):

  def __init__(self, source_operation_split_proto):
    source_spec = {p.key: from_json_value(p.value) for p in
                   source_operation_split_proto.source.spec
                   .additionalProperties}
    if not source_spec.has_key(names.SERIALIZED_SOURCE_KEY):
      raise ValueError(
          'Source split spec must contain a serialized source. Received: %r',
          source_operation_split_proto)
    self.source = pickler.loads(
        source_spec[names.SERIALIZED_SOURCE_KEY]['value'])

    assert self.source is not None
    assert isinstance(self.source, iobase.BoundedSource)

    desired_bundle_size_bytes = (
        source_operation_split_proto.options.desiredBundleSizeBytes)
    if not desired_bundle_size_bytes:
      self.desired_bundle_size_bytes = DEFAULT_DESIRED_BUNDLE_SIZE
    else:
      self.desired_bundle_size_bytes = long(desired_bundle_size_bytes)
