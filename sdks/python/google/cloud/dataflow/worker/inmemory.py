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

from google.cloud.dataflow import coders
from google.cloud.dataflow.io import iobase


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
    self.source = source

    # Index of the next item to be read by the InMemoryReader.
    # Starts at source.start_index.
    self.current_index = source.start_index

  def __enter__(self):
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    pass

  def __iter__(self):
    for value in itertools.islice(self.source.elements,
                                  self.source.start_index,
                                  self.source.end_index):
      self.current_index += 1
      yield self.source.coder.decode(value)

  def get_progress(self):
    if (self.current_index >= self.source.end_index or
        self.source.start_index >= self.source.end_index):
      percent_complete = 1
    elif self.current_index == self.source.start_index:
      percent_complete = 0
    else:
      percent_complete = (
          float(self.current_index - self.source.start_index) / (
              self.source.end_index - self.source.start_index))

    return iobase.ReaderProgress(percent_complete=percent_complete)
