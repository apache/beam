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

# cython: overflowcheck=True

cdef class ByteLimitedQueue(object):
  cdef readonly Py_ssize_t max_elements
  cdef readonly Py_ssize_t max_bytes
  cdef readonly Py_ssize_t _byte_size
  cdef readonly object _mutex
  cdef readonly object _not_empty
  cdef readonly object _waiting_writers
  cdef readonly list _condition_pool
  cdef readonly object _queue
  cdef readonly Py_ssize_t _blocked_bytes

  cpdef bint _can_fit(self, Py_ssize_t item_bytes) except -1
