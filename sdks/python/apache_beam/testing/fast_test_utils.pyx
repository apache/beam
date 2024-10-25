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
cimport libc.stdlib
cimport libc.string
from cpython.bytes cimport PyBytes_FromStringAndSize


cdef class LCGenerator(object):

  def __cinit__(self):
    self._a = 0x5DEECE66Dull
    self._c = 0xBull
    self._bits = 48
    self._mask = (1ull << self._bits) - 1
    self.seed(0)

  cpdef void seed(self, libc.stdint.uint64_t seed):
    self._seed = (seed * self._a + self._c) & self._mask

  cpdef void seed_jdk(self, libc.stdint.uint64_t seed):
    self._seed = (seed ^ self._a) & self._mask

  cpdef libc.stdint.int32_t next_int(self):
    self.seed(self._seed)
    return <libc.stdint.int32_t>(self._seed >> (self._bits - 32))

  cpdef libc.stdint.uint32_t next_uint(self):
    self.seed(self._seed)
    return <libc.stdint.uint32_t>(self._seed >> (self._bits - 32))

  cpdef bytes rand_bytes(self, int length):
    cdef libc.stdint.int32_t ints = (length + 3) // 4
    cdef char* data = <char*>libc.stdlib.malloc(ints * 4)
    cdef libc.stdint.uint32_t value
    cdef libc.stdint.int32_t i
    for i in range(0, ints, 1):
      value = self.next_uint()
      libc.string.memcpy(<void*>(data + i * 4), <void*>(& value), 4)
    retval = PyBytes_FromStringAndSize(data, length)
    libc.stdlib.free(data)
    return retval

  cpdef double random_sample(self):
    return <double>(self.next_uint() >> 8) / <double>(1 << 24)
