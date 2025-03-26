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

cimport libc.stdint


cdef class OutputStream(object):
  cdef char* data
  cdef size_t buffer_size
  cdef size_t pos

  cpdef write(self, bytes b, bint nested=*)
  cpdef write_byte(self, unsigned char val)
  cpdef write_var_int64(self, libc.stdint.int64_t v)
  cpdef write_var_int32(self, libc.stdint.int64_t v)
  cpdef write_bigendian_int64(self, libc.stdint.int64_t signed_v)
  cpdef write_bigendian_uint64(self, libc.stdint.uint64_t signed_v)
  cpdef write_bigendian_int32(self, libc.stdint.int32_t signed_v)
  cpdef write_bigendian_int16(self, libc.stdint.int16_t signed_v)
  cpdef write_bigendian_double(self, double d)
  cpdef write_bigendian_float(self, float d)

  cpdef bytes get(self)
  cpdef size_t size(self) except? -1
  cdef extend(self, size_t missing)
  cpdef _clear(self)


cdef class ByteCountingOutputStream(OutputStream):
  cdef size_t count

  cpdef write(self, bytes b, bint nested=*)
  cpdef write_var_int64(self, libc.stdint.int64_t val)
  cpdef write_var_int32(self, libc.stdint.int64_t val)
  cpdef write_byte(self, unsigned char val)
  cpdef write_bigendian_int64(self, libc.stdint.int64_t val)
  cpdef write_bigendian_uint64(self, libc.stdint.uint64_t val)
  cpdef write_bigendian_int32(self, libc.stdint.int32_t val)
  cpdef write_bigendian_int16(self, libc.stdint.int16_t val)
  cpdef size_t get_count(self)
  cpdef bytes get(self)


cdef class InputStream(object):
  cdef size_t pos
  cdef bytes all
  cdef char* allc

  cpdef ssize_t size(self) except? -1
  cpdef bytes read(self, size_t len)
  cpdef long read_byte(self) except? -1
  cpdef libc.stdint.int64_t read_var_int64(self) except? -1
  cpdef libc.stdint.int32_t read_var_int32(self) except? -1
  cpdef libc.stdint.int64_t read_bigendian_int64(self) except? -1
  cpdef libc.stdint.uint64_t read_bigendian_uint64(self) except? -1
  cpdef libc.stdint.int32_t read_bigendian_int32(self) except? -1
  cpdef libc.stdint.int16_t read_bigendian_int16(self) except? -1
  cpdef double read_bigendian_double(self) except? -1
  cpdef float read_bigendian_float(self) except? -1
  cpdef bytes read_all(self, bint nested=*)

cpdef libc.stdint.int64_t get_varint_size(libc.stdint.int64_t value)
