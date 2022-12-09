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

"""Optimized implementations of various schema columner types."""

# pytype: skip-file

import numpy as np
cimport numpy as np

from apache_beam.coders import coder_impl
from apache_beam.coders.coder_impl cimport RowColumnEncoder, OutputStream, InputStream
from apache_beam.portability.api import schema_pb2


cdef class AtomicTypeRowColumnEncoder(RowColumnEncoder):
  cdef original
  cdef contiguous

  def __init__(self, column):
    self.original = column
    self.contiguous = np.ascontiguousarray(column)

  def null_flags(self):
    return None

  def finalize_write(self):
    if self.original is not self.contiguous:
      self.original[:] = self.contiguous


cdef class FloatFloat32RowColumnEncoder(AtomicTypeRowColumnEncoder):
  cdef np.float32_t* data

  def __init__(self, unused_coder, column):
    super(FloatFloat32RowColumnEncoder, self).__init__(column)
    cdef np.float32_t[::1] view = self.contiguous
    self.data = &view[0]

  cdef bint encode_to_stream(self, size_t index, OutputStream stream) except -1:
    stream.write_bigendian_float(self.data[index])

  cdef bint decode_from_stream(self, size_t index, InputStream stream) except -1:
    self.data[index] = stream.read_bigendian_float()

FloatFloat32RowColumnEncoder.register(schema_pb2.FLOAT, np.float32().dtype)


cdef class FloatFloat64RowColumnEncoder(AtomicTypeRowColumnEncoder):
  cdef np.float64_t* data

  def __init__(self, unused_coder, column):
    super(FloatFloat64RowColumnEncoder, self).__init__(column)
    cdef np.float64_t[::1] view = self.contiguous
    self.data = &view[0]

  cdef bint encode_to_stream(self, size_t index, OutputStream stream) except -1:
    stream.write_bigendian_float(self.data[index])

  cdef bint decode_from_stream(self, size_t index, InputStream stream) except -1:
    self.data[index] = stream.read_bigendian_float()

FloatFloat64RowColumnEncoder.register(schema_pb2.FLOAT, np.float64().dtype)


cdef class DoubleFloat32RowColumnEncoder(AtomicTypeRowColumnEncoder):
  cdef np.float32_t* data

  def __init__(self, unused_coder, column):
    super(DoubleFloat32RowColumnEncoder, self).__init__(column)
    cdef np.float32_t[::1] view = self.contiguous
    self.data = &view[0]

  cdef bint encode_to_stream(self, size_t index, OutputStream stream) except -1:
    stream.write_bigendian_double(self.data[index])

  cdef bint decode_from_stream(self, size_t index, InputStream stream) except -1:
    self.data[index] = stream.read_bigendian_double()

DoubleFloat32RowColumnEncoder.register(schema_pb2.DOUBLE, np.float32().dtype)


cdef class DoubleFloat64RowColumnEncoder(AtomicTypeRowColumnEncoder):
  cdef np.float64_t* data

  def __init__(self, unused_coder, column):
    super(DoubleFloat64RowColumnEncoder, self).__init__(column)
    cdef np.float64_t[::1] view = self.contiguous
    self.data = &view[0]

  cdef bint encode_to_stream(self, size_t index, OutputStream stream) except -1:
    stream.write_bigendian_double(self.data[index])

  cdef bint decode_from_stream(self, size_t index, InputStream stream) except -1:
    self.data[index] = stream.read_bigendian_double()

DoubleFloat64RowColumnEncoder.register(schema_pb2.DOUBLE, np.float64().dtype)


cdef class Int32Int32RowColumnEncoder(AtomicTypeRowColumnEncoder):
  cdef np.int32_t* data

  def __init__(self, unused_coder, column):
    super(Int32Int32RowColumnEncoder, self).__init__(column)
    cdef np.int32_t[::1] view = self.contiguous
    self.data = &view[0]

  cdef bint encode_to_stream(self, size_t index, OutputStream stream) except -1:
    stream.write_var_int64(self.data[index])

  cdef bint decode_from_stream(self, size_t index, InputStream stream) except -1:
    self.data[index] = stream.read_var_int64()

Int32Int32RowColumnEncoder.register(schema_pb2.INT32, np.int32().dtype)
Int32Int32RowColumnEncoder.register(schema_pb2.INT32, np.int64().dtype)


cdef class Int64Int64RowColumnEncoder(AtomicTypeRowColumnEncoder):
  cdef np.int64_t* data

  def __init__(self, unused_coder, column):
    super(Int64Int64RowColumnEncoder, self).__init__(column)
    cdef np.int64_t[::1] view = self.contiguous
    self.data = &view[0]

  cdef bint encode_to_stream(self, size_t index, OutputStream stream) except -1:
    stream.write_var_int64(self.data[index])

  cdef bint decode_from_stream(self, size_t index, InputStream stream) except -1:
    self.data[index] = stream.read_var_int64()

Int64Int64RowColumnEncoder.register(schema_pb2.INT64, np.int64().dtype)


cdef class BoolRowColumnEncoder(AtomicTypeRowColumnEncoder):
  cdef np.uint8_t* data

  def __init__(self, unused_coder, column):
    super(BoolRowColumnEncoder, self).__init__(column)
    self.contiguous = self.contiguous.astype(np.uint8)
    cdef np.uint8_t[::1] view = self.contiguous
    self.data = &view[0]

  cdef bint encode_to_stream(self, size_t index, OutputStream stream) except -1:
    stream.write_byte(self.data[index])

  cdef bint decode_from_stream(self, size_t index, InputStream stream) except -1:
    self.data[index] = stream.read_byte()

BoolRowColumnEncoder.register(schema_pb2.BOOLEAN, np.int8().dtype)
BoolRowColumnEncoder.register(schema_pb2.BOOLEAN, np.uint8().dtype)
BoolRowColumnEncoder.register(schema_pb2.BOOLEAN, np.bool_().dtype)


