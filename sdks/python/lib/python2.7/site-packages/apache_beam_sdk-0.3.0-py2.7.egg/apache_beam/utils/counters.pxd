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

# cython: profile=True
# cython: overflowcheck=True

cdef class Counter(object):
  cdef readonly object name
  cdef readonly object combine_fn
  cdef readonly object accumulator
  cdef readonly object _add_input
  cpdef bint update(self, value) except -1


cdef class AccumulatorCombineFnCounter(Counter):
  cdef readonly object _fast_add_input
