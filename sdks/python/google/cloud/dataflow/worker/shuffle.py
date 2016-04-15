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

"""Shuffle sources and sinks.

The worker code communicates with the shuffler using a shuffle client library
(see shuffle_client below). The shuffle operates with entries consisting of a
4-tuple: position, key, secondary key (a.k.a. 2nd-key), value.
All values are just raw bytes. On the wire a shuffle entry is represented as a
sequence of length and bytes tuples in the order mentioned above. The length
is represented as a 4 byte big endian
integer.

The semantics when reading from shuffle is that values are grouped by key and
the values associated with a key are sorted by the secondary key. The opaque
position information returned for each shuffle entry can be used to reiterate
over values several times and in general to read in a non-sequential manner.

The shuffle source supports reiterating over values and values returned
have indefinite lifetimes, are stateless and immutable.
"""

from __future__ import absolute_import

import base64
import cStringIO as StringIO
import logging
import struct

from google.cloud.dataflow.coders import observable
from google.cloud.dataflow.io import iobase
from google.cloud.dataflow.io import range_trackers


# The following import works perfectly fine for the Dataflow SDK properly
# installed. However in the testing environment the module is not available
# since it is built elsewhere. The tests rely on the test_reader/test_writer
# arguments for shuffle readers and writers respectively to inject alternative
# implementations.
try:
  from google.cloud.dataflow.worker import shuffle_client  # pylint: disable=g-import-not-at-top
except ImportError:
  pass


def _shuffle_decode(parameter):
  """Decodes a shuffle parameter.

  The parameters used to initialize a shuffle source or shuffle sink are sent
  by the service as urlsafe_base64 Unicode strings. In addition, the encoding
  does not contain the '=' padding expected by the base64 library.

  The parameters using this encoding are: shuffle reader positions (start/end),
  and shuffle reader/writer configuration protobufs.

  Args:
    parameter: A Unicode string encoded using urlsafe_base64.

  Returns:
    Decoded string.
  """
  # Convert to str and compensate for the potential lack of padding.
  parameter = str(parameter)
  if len(parameter) % 4 != 0:
    parameter += '=' * (4 - len(parameter) % 4)
  return base64.urlsafe_b64decode(parameter)


class ShuffleEntry(object):
  """A (position, key, 2nd-key, value) tuple as used by the shuffle library."""

  def __init__(self, key, secondary_key, value, position):
    self.key = key
    self.secondary_key = secondary_key
    self.value = value
    self.position = position

  def __str__(self):
    return '<%s>' % self._str_internal()

  def __repr__(self):
    return '<%s at %s>' % (self._str_internal(), hex(id(self)))

  def _str_internal(self):
    return 'ShuffleEntry %s%s' % (self.key, '/%s' % self.secondary_key
                                  if self.secondary_key != self.key else '')

  def __eq__(self, other):
    return (self.key == other.key and
            self.secondary_key == other.secondary_key and
            self.value == other.value and
            self.position == other.position)

  @property
  def size(self):
    """Returns the size in bytes of the serialized entry."""
    return (16 + len(self.key) + len(self.secondary_key) + len(self.value) +
            (len(self.position) if self.position else 0))

  def to_bytes(self, stream, with_position=True):
    """Writes the serialized shuffle entry to the stream.

    Args:
       stream: A StringIO where the bytes are written to.
       with_position: True whenever reading from shuffle. False when we write
        an entry to the shuffle.
    """
    # The struct.pack '>I' specifier means 32 bit big endian integer.
    if with_position:
      stream.write(struct.pack('>I', len(self.position)))
      stream.write(self.position)
    stream.write(struct.pack('>I', len(self.key)))
    stream.write(self.key)
    stream.write(struct.pack('>I', len(self.secondary_key)))
    stream.write(self.secondary_key)
    stream.write(struct.pack('>I', len(self.value)))
    stream.write(self.value)

  @staticmethod
  def from_stream(stream, with_position=True):
    """Returns a shuffle entry read from a StringIO stream.

    Args:
      stream: StringIO stream to read the bytes from.
      with_position: False only for tests when we want to read something that
        was written to the shuffle without a position. During normal execution
        when reading the position is always there.

    Returns:
      A fully initialized shuffle entry read from the StringIO stream.
    """
    if with_position:
      position_length = struct.unpack('>I', stream.read(4))
      position = stream.read(position_length[0])
    else:
      position = None
    key_length = struct.unpack('>I', stream.read(4))
    key = stream.read(key_length[0])
    secondary_key_length = struct.unpack('>I', stream.read(4))
    secondary_key = stream.read(secondary_key_length[0])
    value_length = struct.unpack('>I', stream.read(4))
    value = stream.read(value_length[0])
    return ShuffleEntry(key, secondary_key, value, position)


class ShuffleEntriesIterable(object):
  """An iterable over all entries between two positions filtered by key.

  The method can be used to iterate over all values in the shuffle if key is
  None and start and nd positions are ''.
  """

  def __init__(self, reader, start_position='', end_position='', key=None):
    """Constructs an iterable for reading sequentially entries in a range.

    The iterable object can be used to get all the shuffle entries associated
    with a key (repeatedly) or simply iterating over all entries (if key is
    None).

    Args:
      reader: A shuffle reader object. These are shared among all iterables
        since there are networking costs associated to setting one up.
      start_position: The first shuffle position to read from.
      end_position: The shuffle position where reading will stop.
      key: The key to match for all shuffle entries if not None. The iteration
        stops when a record with a different key is encountered.

    """
    self.reader = reader
    self.start_position = start_position
    self.end_position = end_position
    self.key = key
    self._pushed_back_entry = None

  def push_back(self, entry):
    """Pushes back one entry to support simple look ahead scenarios."""
    if self._pushed_back_entry is not None:
      raise RuntimeError('There is already an entry pushed back.')
    self._pushed_back_entry = entry

  def __iter__(self):
    last_chunk_seen = False
    start_position = self.start_position
    end_position = self.end_position
    while not last_chunk_seen:
      chunk, next_position = self.reader.Read(start_position, end_position)
      if not next_position:  # An empty string signals the last chunk.
        last_chunk_seen = True
      # Yield records inside the chunk just read.
      read_bytes, total_bytes = 0, len(chunk)
      stream = StringIO.StringIO(chunk)
      while read_bytes < total_bytes:
        entry = ShuffleEntry.from_stream(stream)
        if self.key is not None and self.key != entry.key:
          return
        read_bytes += entry.size
        yield entry
        # Check if anything was pushed back. We do this until there is no
        # value pushed back since it is quite possible to have values pushed
        # back multiple times by the upper callers.
        while self._pushed_back_entry is not None:
          to_return, self._pushed_back_entry = self._pushed_back_entry, None
          yield to_return
      # Move on to the next chunk.
      start_position = next_position


class ShuffleEntriesIterator(object):
  """An iterator object for a ShuffleEntryIterable with push back support.

  The class supports also the iterable protocol (__iter__) and it is careful
  to not create a new iterator from the underlying iterable when iter() is
  called. This is important because shuffle entries iterators are passed
  around and we want to keep reading sequentially while the passing happens.
  More specifically they are kept as the underlying iterators for the key values
  iterables returned for each key.
  """

  def __init__(self, iterable):
    self.iterable = iterable
    self.iterator = iter(self.iterable)

  def __iter__(self):
    return self

  def push_back(self, entry):
    self.iterable.push_back(entry)

  def next(self):
    return next(self.iterator)

  def clone(self, start_position, end_position, key):
    """Clones the current iterator with a new key, start, and end position."""
    return ShuffleEntriesIterator(
        ShuffleEntriesIterable(
            self.iterable.reader, start_position, end_position, key))


class ShuffleKeyValuesIterable(observable.ObservableMixin):
  """An iterable over all values associated with a key.

  The class supports reiteration over the values by cloning the underlying
  iterables every time __iter__ gets called. This way the values can be
  reiterated. The first time __iter__ is called no cloning happens.
  This supports the very common case of going once over all values for all keys.
  """

  def __init__(self, entries_iterator, key, value_coder,
               start_position, end_position=''):
    super(ShuffleKeyValuesIterable, self).__init__()
    self.key = key
    self.value_coder = value_coder
    self.start_position = start_position
    self.end_position = end_position
    self.entries_iterator = entries_iterator
    self.first_values_iterator = None

  def __iter__(self):
    if self.first_values_iterator is None:
      # We safe the first values iterator returned because upper layers
      # can use it to drain the values in it. This is an optimization needed
      # to make efficient the very common case of iterating over all key values
      # available.
      self.first_values_iterator = self.values_iterator()
      return self.first_values_iterator
    else:
      # If this is not the first time __iter__ is called we will clone the
      # underlying iterables so that we can reiterate as many times as we
      # want over the key's values.
      return ShuffleKeyValuesIterable(
          self.entries_iterator.clone(
              self.start_position, self.end_position, self.key),
          self.key, self.value_coder,
          self.start_position, self.end_position).values_iterator()

  def values_iterator(self):
    for entry in self.entries_iterator:
      if self.key != entry.key:
        # Remember the end_position so that if we reiterate over the values
        # we can do that without reading too much beyond the key.
        self.end_position = entry.position
        self.entries_iterator.push_back(entry)
        break
      decoded_value = self.value_coder.decode(entry.value)
      self.notify_observers(entry.value, is_encoded=True)
      yield decoded_value

  def __str__(self):
    return '<%s>' % self._str_internal()

  def __repr__(self):
    return '<%s at %s>' % (self._str_internal(), hex(id(self)))

  def _str_internal(self):
    return '%s on %s' % (self.__class__.__name__, self.key)


class ShuffleReaderBase(iobase.SourceReader):
  """A base class for grouped and ungrouped shuffle readers."""

  def __init__(self, shuffle_source, reader=None):
    self.source = shuffle_source
    self.reader = reader
    self.entries_iterable = None
    self.key_coder = self.source.key_coder.get_impl()
    self.value_coder = self.source.value_coder.get_impl()

  def __enter__(self):
    if self.reader is None:
      self.reader = shuffle_client.PyShuffleReader(
          _shuffle_decode(self.source.config_bytes))
    # Initialize the shuffle entries iterable. For now we read from start to
    # end which is enough for plain GroupByKey operations.
    if self.entries_iterable is None:
      self.entries_iterable = ShuffleEntriesIterable(
          self.reader, self.source.start_position, self.source.end_position)
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    pass


class GroupedShuffleReader(ShuffleReaderBase):
  """A shuffle reader providing grouped reading."""

  def __init__(self, shuffle_source, reader=None):
    super(GroupedShuffleReader, self).__init__(shuffle_source, reader)
    self._range_tracker = range_trackers.GroupedShuffleRangeTracker(
        decoded_start_pos=shuffle_source.start_position,
        decoded_stop_pos=shuffle_source.end_position)

  def __iter__(self):
    entries_iterator = ShuffleEntriesIterator(self.entries_iterable)
    for entry in entries_iterator:
      entries_iterator.push_back(entry)
      key_values = ShuffleKeyValuesIterable(
          entries_iterator,
          entry.key, self.value_coder, entry.position)
      group_start = entry.position

      last_group_start = self._range_tracker.last_group_start
      is_at_split_point = (
          last_group_start is None or group_start != last_group_start)

      if not self._range_tracker.try_return_record_at(is_at_split_point,
                                                      group_start):
        # If an end position is defined, reader has read all records up to the
        # defined end position, otherwise, reader has read all records of the
        # source.
        return

      yield (self.key_coder.decode(entry.key), key_values)
      # We need to drain the iterator returned just in case this
      # was not done by the caller. Otherwise we will not properly advance
      # to the next key but rather return the next entry for the current
      # key (if there are multiple values).
      drain_iterator = key_values.first_values_iterator
      if drain_iterator is None:
        drain_iterator = iter(key_values)
      for _ in drain_iterator:
        pass

  def get_progress(self):
    last_group_start = self._range_tracker.last_group_start
    if last_group_start is None:
      return None
    reader_position = iobase.ReaderPosition(
        shuffle_position=base64.urlsafe_b64encode(last_group_start))
    return iobase.ReaderProgress(position=reader_position)

  def request_dynamic_split(self, dynamic_split_request):
    assert dynamic_split_request is not None
    split_request_progress = dynamic_split_request.progress
    if split_request_progress.position is None:
      logging.warning('GroupingShuffleReader only supports split at a Position.'
                      ' Requested: %r', dynamic_split_request)
      return
    encoded_shuffle_position = split_request_progress.position.shuffle_position
    if encoded_shuffle_position is None:
      logging.warning('GroupingShuffleReader only supports split at a shuffle'
                      ' position. Requested: %r'
                      , split_request_progress.position)
      return

    if self._range_tracker.try_split_at_position(
        _shuffle_decode(encoded_shuffle_position)):
      logging.info('Split GroupedShuffleReader at %s', encoded_shuffle_position)
      split_position = iobase.ReaderPosition(
          shuffle_position=encoded_shuffle_position)
      return iobase.DynamicSplitResultWithPosition(split_position)
    else:
      logging.info('Refusing to split GroupedShuffleReader %r at %s'
                   , self, encoded_shuffle_position)


class UngroupedShuffleReader(ShuffleReaderBase):
  """A shuffle reader providing ungrouped reading."""

  def __init__(self, shuffle_source, reader=None):
    super(UngroupedShuffleReader, self).__init__(shuffle_source, reader)

  def __iter__(self):
    for entry in self.entries_iterable:
      yield self.value_coder.decode(entry.value)


class ShuffleSourceBase(iobase.Source):
  """A base class for grouped and ungrouped shuffle sources."""

  def __init__(self, config_bytes, coder, start_position='', end_position=''):
    self.config_bytes = config_bytes
    self.key_coder, self.value_coder = (
        coder if isinstance(coder, tuple) else (coder, coder))
    self.start_position = (start_position if not start_position
                           else _shuffle_decode(start_position))
    self.end_position = (end_position if not end_position
                         else _shuffle_decode(end_position))


class GroupedShuffleSource(ShuffleSourceBase):
  """A source that reads from a shuffled dataset and yields key-grouped data.

  The value for each key will be an iterable object that will yield values.
  """

  def reader(self, test_reader=None):
    return GroupedShuffleReader(self, reader=test_reader)


class UngroupedShuffleSource(ShuffleSourceBase):
  """A source that reads from a shuffled dataset and yields values.

  This source will drop the keys of the key-value pairs and yield just the
  values. This source is used in resharding operations.
  """

  def reader(self, test_reader=None):
    return UngroupedShuffleReader(self, reader=test_reader)


class ShuffleSinkWriter(iobase.NativeSinkWriter):
  """A sink writer for ShuffleSink."""

  def __init__(self, shuffle_sink, writer=None):
    self.sink = shuffle_sink
    self.writer = writer
    self.stream = StringIO.StringIO()
    self.bytes_buffered = 0
    self.key_coder = self.sink.key_coder.get_impl()
    self.value_coder = self.sink.value_coder.get_impl()

  def __enter__(self):
    if self.writer is None:
      self.writer = shuffle_client.PyShuffleWriter(
          _shuffle_decode(self.sink.config_bytes))
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    value = self.stream.getvalue()
    if value:
      self.writer.Write(value)
      self.bytes_buffered = 0
    self.stream.close()
    self.writer.Close()

  def Write(self, key, secondary_key, value):
    entry = ShuffleEntry(
        self.key_coder.encode(key),
        secondary_key,
        self.value_coder.encode(value),
        position=None)
    entry.to_bytes(self.stream, with_position=False)
    self.bytes_buffered += entry.size
    if self.bytes_buffered > 10 << 20:
      self.writer.Write(self.stream.getvalue())
      self.stream.close()
      self.stream = StringIO.StringIO()
      self.bytes_buffered = 0


class ShuffleSink(iobase.NativeSink):
  """A sink that writes to a shuffled dataset."""

  def __init__(self, config_bytes, coder):
    self.config_bytes = config_bytes
    self.key_coder, self.value_coder = (
        coder if isinstance(coder, tuple) else (coder, coder))

  def writer(self, test_writer=None):
    return ShuffleSinkWriter(self, writer=test_writer)
