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

"""Compiled version of the Stream objects used by CoderImpl.

For internal use only; no backwards-compatibility guarantees.
"""

cimport libc.stdlib
cimport libc.string


cdef class OutputStream(object):
  """An output string stream implementation supporting write() and get()."""

  #TODO(robertwb): Consider using raw C++ streams.

  def __cinit__(self):
    self.buffer_size = 1024
    self.pos = 0
    self.data = <char*>libc.stdlib.malloc(self.buffer_size)
    assert self.data, "OutputStream malloc failed."

  def __dealloc__(self):
    if self.data:
      libc.stdlib.free(self.data)

  cpdef write(self, bytes b, bint nested=False):
    cdef size_t blen = len(b)
    if nested:
      self.write_var_int64(blen)
    if self.buffer_size < self.pos + blen:
      self.extend(blen)
    libc.string.memcpy(self.data + self.pos, <char*>b, blen)
    self.pos += blen

  cpdef write_byte(self, unsigned char val):
    if  self.buffer_size < self.pos + 1:
      self.extend(1)
    self.data[self.pos] = val
    self.pos += 1

  cpdef write_var_int64(self, libc.stdint.int64_t signed_v):
    """Encode a long using variable-length encoding to a stream."""
    cdef libc.stdint.uint64_t v = signed_v
    # Inline common case.
    if v <= 0x7F and self.pos < self.buffer_size - 1:
      self.data[self.pos] = v
      self.pos += 1
      return

    cdef long bits
    while True:
      bits = v & 0x7F
      v >>= 7
      if v:
        bits |= 0x80
      self.write_byte(<unsigned char>bits)
      if not v:
        break

  cpdef write_var_int32(self, libc.stdint.int64_t signed_v):
    """Encode an int using variable-length encoding to a stream."""
    # for backward compatibility, input type is int64_t thus tolerates overflow
    cdef libc.stdint.int64_t v = signed_v & 0xFFFFFFFF
    self.write_var_int64(v)

  cpdef write_bigendian_int64(self, libc.stdint.int64_t signed_v):
    self.write_bigendian_uint64(signed_v)

  cpdef write_bigendian_uint64(self, libc.stdint.uint64_t v):
    if  self.buffer_size < self.pos + 8:
      self.extend(8)
    self.data[self.pos    ] = <unsigned char>(v >> 56)
    self.data[self.pos + 1] = <unsigned char>(v >> 48)
    self.data[self.pos + 2] = <unsigned char>(v >> 40)
    self.data[self.pos + 3] = <unsigned char>(v >> 32)
    self.data[self.pos + 4] = <unsigned char>(v >> 24)
    self.data[self.pos + 5] = <unsigned char>(v >> 16)
    self.data[self.pos + 6] = <unsigned char>(v >>  8)
    self.data[self.pos + 7] = <unsigned char>(v      )
    self.pos += 8

  cpdef write_bigendian_int32(self, libc.stdint.int32_t signed_v):
    cdef libc.stdint.uint32_t v = signed_v
    if self.buffer_size < self.pos + 4:
      self.extend(4)
    self.data[self.pos    ] = <unsigned char>(v >> 24)
    self.data[self.pos + 1] = <unsigned char>(v >> 16)
    self.data[self.pos + 2] = <unsigned char>(v >>  8)
    self.data[self.pos + 3] = <unsigned char>(v      )
    self.pos += 4

  cpdef write_bigendian_int16(self, libc.stdint.int16_t signed_v):
    cdef libc.stdint.uint16_t v = signed_v
    if  self.buffer_size < self.pos + 2:
      self.extend(2)
    self.data[self.pos    ] = <unsigned char>(v >>  8)
    self.data[self.pos + 1] = <unsigned char>(v      )
    self.pos += 2

  cpdef write_bigendian_double(self, double d):
    self.write_bigendian_int64((<libc.stdint.int64_t*><char*>&d)[0])

  cpdef write_bigendian_float(self, float f):
    self.write_bigendian_int32((<libc.stdint.int32_t*><char*>&f)[0])

  cpdef bytes get(self):
    return self.data[:self.pos]

  cpdef size_t size(self) except? -1:
    return self.pos

  cdef extend(self, size_t missing):
    while missing > self.buffer_size - self.pos:
      self.buffer_size *= 2
    self.data = <char*>libc.stdlib.realloc(self.data, self.buffer_size)
    assert self.data, "OutputStream realloc failed."

  cpdef _clear(self):
    self.pos = 0


cdef class ByteCountingOutputStream(OutputStream):
  """An output string stream implementation that only counts the bytes.

  This implementation counts the number of bytes it "writes" but
  doesn't actually write them anyway.  Thus it has write() but not
  get().  get_count() returns how many bytes were written.

  This is useful for sizing an encoding.
  """

  def __cinit__(self):
    self.count = 0

  cpdef write(self, bytes b, bint nested=False):
    cdef size_t blen = len(b)
    if nested:
      self.write_var_int64(blen)
    self.count += blen

  cpdef write_var_int64(self, libc.stdint.int64_t signed_v):
    self.count += get_varint_size(signed_v)

  cpdef write_var_int32(self, libc.stdint.int64_t signed_v):
    if signed_v < 0:
      self.count += 5
    else:
      self.count += get_varint_size(signed_v)

  cpdef write_byte(self, unsigned char _):
    self.count += 1

  cpdef write_bigendian_int64(self, libc.stdint.int64_t _):
    self.count += 8

  cpdef write_bigendian_uint64(self, libc.stdint.uint64_t _):
    self.count += 8

  cpdef write_bigendian_int32(self, libc.stdint.int32_t _):
    self.count += 4

  cpdef write_bigendian_int16(self, libc.stdint.int16_t _):
    self.count += 2

  cpdef size_t get_count(self):
    return self.count

  cpdef bytes get(self):
    raise NotImplementedError

  def __str__(self):
    return '<%s %s>' % (self.__class__.__name__, self.count)


cdef class InputStream(object):
  """An input string stream implementation supporting read() and size()."""

  def __init__(self, all):
    self.allc = self.all = all

  cpdef bytes read(self, size_t size):
    self.pos += size
    return self.allc[self.pos - size : self.pos]

  cpdef long read_byte(self) except? -1:
    self.pos += 1
    # Note: Some C++ compilers treats the char array below as a signed char.
    # This causes incorrect coder behavior unless explicitly cast to an
    # unsigned char here.
    return <long>(<unsigned char> self.allc[self.pos - 1])

  cpdef ssize_t size(self) except? -1:
    return len(self.all) - self.pos

  cpdef bytes read_all(self, bint nested=False):
    return self.read(<ssize_t>self.read_var_int64() if nested else self.size())

  cpdef libc.stdint.int64_t read_var_int64(self) except? -1:
    """Decode a variable-length encoded long from a stream."""
    # Inline common case.
    cdef long byte = <unsigned char> self.allc[self.pos]
    self.pos += 1
    if byte <= 0x7F:
      return byte

    cdef libc.stdint.int64_t bits
    cdef long shift = 0
    cdef libc.stdint.int64_t result = 0
    while True:
      bits = byte & 0x7F
      if (shift >= sizeof(libc.stdint.int64_t) * 8 or
          (shift >= (sizeof(libc.stdint.int64_t) * 8 - 1) and bits > 1)):
        raise RuntimeError('VarLong too long.')
      result |= bits << shift
      shift += 7
      if not (byte & 0x80):
        break
      byte = self.read_byte()
      if byte < 0:
        raise RuntimeError('VarInt not terminated.')

    return result

  cpdef libc.stdint.int32_t read_var_int32(self) except? -1:
    """Decode a variable-length encoded int32 from a stream."""
    cdef libc.stdint.int64_t v = self.read_var_int64()
    return <libc.stdint.int32_t>(v);

  cpdef libc.stdint.int64_t read_bigendian_int64(self) except? -1:
    return self.read_bigendian_uint64()

  cpdef libc.stdint.uint64_t read_bigendian_uint64(self) except? -1:
    self.pos += 8
    return (<unsigned char>self.allc[self.pos - 1]
      | <libc.stdint.uint64_t><unsigned char>self.allc[self.pos - 2] <<  8
      | <libc.stdint.uint64_t><unsigned char>self.allc[self.pos - 3] << 16
      | <libc.stdint.uint64_t><unsigned char>self.allc[self.pos - 4] << 24
      | <libc.stdint.uint64_t><unsigned char>self.allc[self.pos - 5] << 32
      | <libc.stdint.uint64_t><unsigned char>self.allc[self.pos - 6] << 40
      | <libc.stdint.uint64_t><unsigned char>self.allc[self.pos - 7] << 48
      | <libc.stdint.uint64_t><unsigned char>self.allc[self.pos - 8] << 56)

  cpdef libc.stdint.int32_t read_bigendian_int32(self) except? -1:
    self.pos += 4
    return (<unsigned char>self.allc[self.pos - 1]
      | <libc.stdint.uint32_t><unsigned char>self.allc[self.pos - 2] <<  8
      | <libc.stdint.uint32_t><unsigned char>self.allc[self.pos - 3] << 16
      | <libc.stdint.uint32_t><unsigned char>self.allc[self.pos - 4] << 24)

  cpdef libc.stdint.int16_t read_bigendian_int16(self) except? -1:
    self.pos += 2
    return (<unsigned char>self.allc[self.pos - 1]
      | <libc.stdint.uint16_t><unsigned char>self.allc[self.pos - 2] <<  8)

  cpdef double read_bigendian_double(self) except? -1:
    cdef libc.stdint.int64_t as_long = self.read_bigendian_int64()
    return (<double*><char*>&as_long)[0]

  cpdef float read_bigendian_float(self) except? -1:
    cdef libc.stdint.int32_t as_int = self.read_bigendian_int32()
    return (<float*><char*>&as_int)[0]

cpdef libc.stdint.int64_t get_varint_size(libc.stdint.int64_t value):
  """Returns the size of the given integer value when encode as a VarInt."""
  cdef libc.stdint.int64_t varint_size = 0
  cdef libc.stdint.uint64_t bits = value
  while True:
    varint_size += 1
    bits >>= 7
    if not bits:
      break
  return varint_size
