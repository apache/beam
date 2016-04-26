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

"""In-memory input source."""

import itertools
import logging

from google.cloud.dataflow import coders
from google.cloud.dataflow.io import iobase
from google.cloud.dataflow.io import range_trackers


class InMemorySource(iobase.NativeSource):
  """In-memory input source."""

  def __init__(
      self, elements, coder=coders.Base64PickleCoder(), start_index=None,
      end_index=None):
    self.elements = elements
    self.coder = coder

    if start_index is None:
      self.start_index = 0
    else:
      self.start_index = start_index

    if end_index is None:
      self.end_index = len(elements)
    else:
      self.end_index = end_index

  def __eq__(self, other):
    return (self.elements == other.elements and
            self.coder == other.coder and
            self.start_index == other.start_index and
            self.end_index == other.end_index)

  def reader(self):
    return InMemoryReader(self)


class InMemoryReader(iobase.NativeSourceReader):
  """A reader for in-memory source."""

  def __init__(self, source):
    self._source = source

    # Index of the last item returned by InMemoryReader.
    # Initialized to None.
    self._current_index = None

    self._range_tracker = range_trackers.OffsetRangeTracker(
        self._source.start_index, self._source.end_index)

  def __enter__(self):
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    pass

  def __iter__(self):
    for value in itertools.islice(self._source.elements,
                                  self._source.start_index,
                                  self._source.end_index):
      claimed = False
      if self._current_index is None:
        claimed = self._range_tracker.try_return_record_at(
            True, self._source.start_index)
      else:
        claimed = self._range_tracker.try_return_record_at(
            True, self._current_index + 1)

      if claimed:
        if self._current_index is None:
          self._current_index = self._source.start_index
        else:
          self._current_index += 1

        yield self._source.coder.decode(value)
      else:
        return

  def get_progress(self):
    if self._current_index is None:
      return None

    return iobase.ReaderProgress(
        position=iobase.ReaderPosition(record_index=self._current_index))

  def request_dynamic_split(self, dynamic_split_request):
    assert dynamic_split_request is not None
    progress = dynamic_split_request.progress
    split_position = progress.position
    if split_position is None:
      logging.debug('InMemory reader only supports split requests that are '
                    'based on positions. Received : %r', dynamic_split_request)

    index_position = split_position.record_index
    if index_position is None:
      logging.debug('InMemory reader only supports split requests that are '
                    'based on index positions. Received : %r',
                    dynamic_split_request)

    if self._range_tracker.try_split_at_position(index_position):
      return iobase.DynamicSplitResultWithPosition(split_position)
