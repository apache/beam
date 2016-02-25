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

"""A reader that encapsulates a set of other readers.

This is to be used for optimizing the execution of Dataflow jobs. Users should
not use this when developing Dataflow jobs.
"""

from __future__ import absolute_import

from google.cloud.dataflow.io import iobase


class ConcatSource(iobase.Source):
  """A wrapper source class for ConcatReader."""

  def __init__(self, sub_sources):
    self.sub_sources = sub_sources

  def reader(self):
    return ConcatReader(self)

  def __eq__(self, other):
    return self.sub_sources == other.sub_sources


class ConcatReader(iobase.SourceReader):
  """A reader that reads elements from a given set of encoded sources.

  Creates readers for sources lazily, i.e. only when elements
  from the particular reader are about to be read.

  This class does does not cache readers and instead creates new set of readers
  evertime it is iterated on. Because of this, multiple iterators created for
  the same ConcatReader will not be able to share any state between each other.
  This design was chosen since keeping a large number of reader objects alive
  within a single ConcatReader could be highly resource consuming.

  For progress reporting ConcatReader uses a position of type
  iobase.ConcatPosition.
  """

  def __init__(self, source):
    self.source = source
    self.current_reader = None
    self.current_reader_index = -1

  def __enter__(self):
    return self

  def __iter__(self):
    if self.source.sub_sources is None:
      return

    for sub_source in self.source.sub_sources:
      with sub_source.reader() as reader:
        self.current_reader_index += 1
        self.current_reader = reader
        for data in reader:
          yield data

  def __exit__(self, exception_type, exception_value, traceback):
    pass

  def get_progress(self):
    if self.current_reader_index < 0 or self.current_reader is None:
      return

    index = self.current_reader_index
    inner_position = None

    sub_reader_progress = self.current_reader.get_progress()
    if sub_reader_progress is not None:
      sub_reader_position = sub_reader_progress.position
      if sub_reader_position is not None:
        inner_position = sub_reader_position
      else:
        raise ValueError('A concat source should only be created with '
                         'sub-sources that create readers that perform '
                         'progress reporting and dynamic work rebalancing '
                         'using positions')
      return iobase.ReaderProgress(
          position=iobase.ReaderPosition(
              concat_position=iobase.ConcatPosition(index, inner_position)))
