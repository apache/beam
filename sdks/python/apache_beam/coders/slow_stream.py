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

"""A pure Python implementation of stream.pyx."""

import struct


class OutputStream(object):
  """A pure Python implementation of stream.OutputStream."""

  def __init__(self):
    self.data = []

  def write(self, b, nested=False):
    assert isinstance(b, str)
    if nested:
      self.write_var_int64(len(b))
    self.data.append(b)

  def write_byte(self, val):
    self.data.append(chr(val))

  def write_var_int64(self, v):
    if v < 0:
      v += 1 << 64
      if v <= 0:
        raise ValueError('Value too large (negative).')
    while True:
      bits = v & 0x7F
      v >>= 7
      if v:
        bits |= 0x80
      self.write_byte(bits)
      if not v:
        break

  def write_bigendian_int64(self, v):
    self.write(struct.pack('>q', v))

  def write_bigendian_uint64(self, v):
    self.write(struct.pack('>Q', v))

  def write_bigendian_int32(self, v):
    self.write(struct.pack('>i', v))

  def write_bigendian_double(self, v):
    self.write(struct.pack('>d', v))

  def get(self):
    return ''.join(self.data)


class BufferedElementCountingOutputStream(OutputStream):
  """
  An output stream that provides efficient encoding for ``Iterables`` of
  unknown length and containing small values.

  The elements are buffered up to ``buffer_size`` bytes before prefixing the
  count of number of elements. Note that each element needs to be encoded in a
  nested context.

  To write to this stream::

    buffered_stream = BufferedElementCountingOutputStream(output_stream)
      for element in elements:
        buffered_stream.mark_element_start()
        buffered_stream.write(...)
    buffered_stream.finish()

  The resulting output stream is::

    countA element(0) element(1) ... element(countA - 1)
    countB element(0) element(1) ... element(countB - 1)
    ...
    countX element(0) element(1) ... element(countX - 1)
    countY

  To decode data encoded by this stream::

    in_stream = ...
    count = in_stream.read_var_int64()
    elements = []
    while count > 0:
      elements.append(element_coder.decode_from_stream(in_stream, nested=True))
      count -= 1
      if not count:
        count = in_stream.read_var_int64()

  The counts are encoded as variable length longs and the end of iterable is
  detected by reading a count of 0.
  """

  # Default buffer size of 64kB.
  _DEFAULT_BUFFER_SIZE = 64 * 1024

  def __init__(self, op_stream, buffer_size=_DEFAULT_BUFFER_SIZE):
    self._buffer_size = buffer_size
    self._underlying_op_stream = op_stream
    self._count = 0
    self._current_data = []
    self._current_data_size_in_bytes = 0
    self._finished = False

  def mark_element_start(self):
    """
    Marks that a new element is being output.

    This allows this output stream to use the buffer if it had previously
    overflowed marking the start of a new block of elements.
    """
    self._check_not_finished()
    self._count += 1

  def finish(self):
    """
    Finishes the encoding by flushing any buffered data, and outputting a
    final count of 0.
    """
    if self._finished:
      return
    self._output()
    self._underlying_op_stream.write_var_int64(0)
    self._finished = True

  def write(self, b, nested=False):
    assert isinstance(b, str), type(b)
    if nested:
      self.write_var_int64(len(b))
    self._write(b)

  def write_byte(self, val):
    self._write(chr(val))

  def _write(self, b):
    self._check_not_finished()
    if self._count == 0:
      self._underlying_op_stream.write(b)
      return

    if self._current_data_size_in_bytes + len(b) < self._buffer_size:
      self._current_data.append(b)
      self._current_data_size_in_bytes += len(b)
    else:
      self._output()
      self._underlying_op_stream.write(b)

  def _output(self):
    if self._count > 0:
      self._underlying_op_stream.write_var_int64(self._count)
      self._underlying_op_stream.write(''.join(self._current_data))
      self._current_data = []
      self._current_data_size_in_bytes = 0
      # The buffer has been flushed so we must write to the underlying stream
      # until we learn of the next element. We reset the count to zero marking
      # that we should not use the buffer.
      self._count = 0

  def _check_not_finished(self):
    if self._finished:
      raise ValueError("Stream has been finished. Can not write any more data.")

  def get(self):
    return self._underlying_op_stream.get()


class ByteCountingOutputStream(OutputStream):
  """A pure Python implementation of stream.ByteCountingOutputStream."""

  def __init__(self):
    # Note that we don't actually use any of the data initialized by our super.
    super(ByteCountingOutputStream, self).__init__()
    self.count = 0

  def write(self, byte_array, nested=False):
    blen = len(byte_array)
    if nested:
      self.write_var_int64(blen)
    self.count += blen

  def write_byte(self, _):
    self.count += 1

  def get_count(self):
    return self.count

  def get(self):
    raise NotImplementedError

  def __str__(self):
    return '<%s %s>' % (self.__class__.__name__, self.count)


class InputStream(object):
  """A pure Python implementation of stream.InputStream."""

  def __init__(self, data):
    self.data = data
    self.pos = 0

  def size(self):
    return len(self.data) - self.pos

  def read(self, size):
    self.pos += size
    return self.data[self.pos - size : self.pos]

  def read_all(self, nested):
    return self.read(self.read_var_int64() if nested else self.size())

  def read_byte(self):
    self.pos += 1
    return ord(self.data[self.pos - 1])

  def read_var_int64(self):
    shift = 0
    result = 0
    while True:
      byte = self.read_byte()
      if byte < 0:
        raise RuntimeError('VarLong not terminated.')

      bits = byte & 0x7F
      if shift >= 64 or (shift >= 63 and bits > 1):
        raise RuntimeError('VarLong too long.')
      result |= bits << shift
      shift += 7
      if not byte & 0x80:
        break
    if result >= 1 << 63:
      result -= 1 << 64
    return result

  def read_bigendian_int64(self):
    return struct.unpack('>q', self.read(8))[0]

  def read_bigendian_uint64(self):
    return struct.unpack('>Q', self.read(8))[0]

  def read_bigendian_int32(self):
    return struct.unpack('>i', self.read(4))[0]

  def read_bigendian_double(self):
    return struct.unpack('>d', self.read(8))[0]


def get_varint_size(v):
  """Returns the size of the given integer value when encode as a VarInt."""
  if v < 0:
    v += 1 << 64
    if v <= 0:
      raise ValueError('Value too large (negative).')
  varint_size = 0
  while True:
    varint_size += 1
    v >>= 7
    if not v:
      break
  return varint_size
