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

cimport cython
cimport libc.stdint

from apache_beam.metrics.cells cimport MetricCell

cdef object get_current_tracker


cdef class _TypedMetricName(object):
  cdef readonly object cell_type
  cdef readonly object metric_name
  cdef readonly object fast_name
  cdef libc.stdint.int64_t _hash


cdef object _DEFAULT


cdef class MetricUpdater(object):
  cdef _TypedMetricName typed_metric_name
  cdef object default_value
  cdef bint process_wide  # bint is used to represent C++ bool.


cdef class MetricsContainer(object):
  cdef object step_name
  cdef object lock
  cdef public dict metrics
  cpdef MetricCell get_metric_cell(self, metric_key)
