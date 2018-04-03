# cython: profile=True

cimport cython
from libc.stdint cimport int64_t

cdef class DistributionAccumulator(object):
  cdef public int64_t min
  cdef public int64_t max
  cdef public int64_t count
  cdef public int64_t sum
  cdef public int64_t first_bucket_offset
  cdef int64_t* buckets
  cdef public int64_t buckets_per_10
  cpdef void add_input(self, int64_t element)
  cdef int64_t calculate_bucket_index(self, int64_t element)