# cython: profile=True

cimport cython
from libc.stdint cimport int64_t, INT64_MAX
from libc.stdlib cimport malloc, free

cdef int64_t* MAX_LONG_10_FOR_LEADING_ZEROS = [19, 18, 18, 18, 18, 17, 17, 17, 16, 16, 16, 15,
                                          15, 15, 15, 14, 14, 14, 13, 13, 13, 12, 12, 12,
                                          12, 11, 11, 11, 10, 10, 10, 9, 9, 9, 9, 8, 8,
                                          8, 7, 7, 7, 6, 6, 6, 6, 5, 5, 5, 4, 4, 4, 3, 3,
                                          3, 3, 2, 2, 2, 1, 1, 1, 0, 0, 0]

cdef unsigned long long* POWER_TEN = [10e-1, 10e0, 10e1, 10e2, 10e3, 10e4, 10e5, 10e6, 10e7, 10e8, 10e9, 10e10,
                      10e11, 10e12, 10e13, 10e14, 10e15, 10e16, 10e17, 10e18]

cdef inline bint compare_to(int64_t x, int64_t y):
  if x < y:
    return 1
  return 0

cdef int64_t bit_length(int64_t element):
  cdef int64_t bit_count = 0
  while element > 0:
    bit_count += 1
    element >>= 1
  return bit_count

cdef int64_t get_log10_round_to_floor(int64_t element):
  cdef int64_t number_of_leading_zeros = 64 - bit_length(element)
  cdef int64_t y = MAX_LONG_10_FOR_LEADING_ZEROS[number_of_leading_zeros]
  return y - compare_to(element, POWER_TEN[y])

cdef class DistributionAccumulator(object):
  def __init__(self):
    self.min = INT64_MAX
    self.max = 0
    self.count = 0
    self.sum = 0
    self.first_bucket_offset = 0
    self.buckets = <int64_t*> malloc(sizeof(int64_t) * 58)
    self.buckets_per_10 = 3

  cpdef void add_input(self, int64_t element):
    self.min = min(self.min, element)
    self.max = max(self.max, element)
    self.count += 1
    self.sum += element
    cdef int64_t bucket_index = self.calculate_bucket_index(element)
    self.buckets[bucket_index] += 1
    self.first_bucket_offset = min(self.first_bucket_offset, bucket_index)

  cdef int64_t calculate_bucket_index(self, int64_t element):
    if element == 0:
      return 0
    cdef int64_t log10_floor = get_log10_round_to_floor(element)
    cdef int64_t power_of_ten = POWER_TEN[log10_floor]
    cdef bucket_offset = 0
    if element <  power_of_ten<<1:
      bucket_offset = 0
    elif element < ((power_of_ten<<2) + power_of_ten):
      bucket_offset = 1
    else:
      bucket_offset = 2
    return 1 + (log10_floor << 1) + log10_floor + bucket_offset
