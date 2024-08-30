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

cdef class LCGenerator(object):
  cdef libc.stdint.uint64_t _a
  cdef libc.stdint.uint64_t _c
  cdef libc.stdint.int32_t _bits
  cdef libc.stdint.uint64_t _mask
  cdef libc.stdint.uint64_t _seed

  cpdef void seed(self, libc.stdint.uint64_t seed)
  cpdef void seed_jdk(self, libc.stdint.uint64_t seed)
  cpdef libc.stdint.int32_t next_int(self)
  cpdef libc.stdint.uint32_t next_uint(self)
  cpdef bytes rand_bytes(self, int length)
  cpdef double random_sample(self)