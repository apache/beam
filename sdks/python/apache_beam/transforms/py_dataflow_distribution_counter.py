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

"""For internal use only; no backwards-compatibility guarantees."""

# pytype: skip-file

globals()['INT64_MAX'] = 2**63 - 1
globals()['INT64_MIN'] = -2**63

POWER_TEN = [
    10e-1,
    10e0,
    10e1,
    10e2,
    10e3,
    10e4,
    10e5,
    10e6,
    10e7,
    10e8,
    10e9,
    10e10,
    10e11,
    10e12,
    10e13,
    10e14,
    10e15,
    10e16,
    10e17,
    10e18
]


def get_log10_round_to_floor(element):
  power = 0
  while element >= POWER_TEN[power]:
    power += 1
  return power - 1


class DataflowDistributionCounter(object):
  """Pure python DataflowDistributionCounter in case Cython not available.


  Please avoid using python mode if possible, since it's super slow
  Cythonized DatadflowDistributionCounter defined in
  apache_beam.transforms.cy_dataflow_distribution_counter.

  Currently using special bucketing strategy suitable for Dataflow

  Attributes:
    min: minimum value of all inputs.
    max: maximum value of all inputs.
    count: total count of all inputs.
    sum: sum of all inputs.
    buckets: histogram buckets of value counts for a
    distribution(1,2,5 bucketing). Max bucket_index is 58( sys.maxint as input).
    is_cythonized: mark whether DataflowDistributionCounter cythonized.
  """
  # Assume the max input is sys.maxint, then the possible max bucket size is 59
  MAX_BUCKET_SIZE = 59

  # 3 buckets for every power of ten -> 1, 2, 5
  BUCKET_PER_TEN = 3

  def __init__(self):
    global INT64_MAX  # pylint: disable=global-variable-not-assigned
    self.min = INT64_MAX
    self.max = 0
    self.count = 0
    self.sum = 0
    self.buckets = [0] * self.MAX_BUCKET_SIZE
    self.is_cythonized = False

  def add_input(self, element):
    if element < 0:
      raise ValueError('Distribution counters support only non-negative value')
    self.min = min(self.min, element)
    self.max = max(self.max, element)
    self.count += 1
    self.sum += element
    bucket_index = self.calculate_bucket_index(element)
    self.buckets[bucket_index] += 1

  def add_input_n(self, element, n):
    if element < 0:
      raise ValueError('Distribution counters support only non-negative value')
    self.min = min(self.min, element)
    self.max = max(self.max, element)
    self.count += n
    self.sum += element * n
    bucket_index = self.calculate_bucket_index(element)
    self.buckets[bucket_index] += n

  def calculate_bucket_index(self, element):
    """Calculate the bucket index for the given element."""
    if element == 0:
      return 0
    log10_floor = get_log10_round_to_floor(element)
    power_of_ten = POWER_TEN[log10_floor]
    if element < power_of_ten * 2:
      bucket_offset = 0
    elif element < power_of_ten * 5:
      bucket_offset = 1
    else:
      bucket_offset = 2
    return 1 + log10_floor * self.BUCKET_PER_TEN + bucket_offset

  def translate_to_histogram(self, histogram):
    """Translate buckets into Histogram.

    Args:
      histogram: apache_beam.runners.dataflow.internal.clents.dataflow.Histogram
      Ideally, only call this function when reporting counter to
      dataflow service.
    """
    first_bucket_offset = 0
    last_bucket_offset = 0
    for index in range(0, self.MAX_BUCKET_SIZE):
      if self.buckets[index] != 0:
        first_bucket_offset = index
        break
    for index in range(self.MAX_BUCKET_SIZE - 1, -1, -1):
      if self.buckets[index] != 0:
        last_bucket_offset = index
        break
    histogram.firstBucketOffset = first_bucket_offset
    histogram.bucketCounts = (
        self.buckets[first_bucket_offset:last_bucket_offset + 1])

  def extract_output(self):
    global INT64_MIN  # pylint: disable=global-variable-not-assigned
    global INT64_MAX  # pylint: disable=global-variable-not-assigned
    if not INT64_MIN <= self.sum <= INT64_MAX:
      self.sum %= 2**64
      if self.sum >= INT64_MAX:
        self.sum -= 2**64
    mean = self.sum // self.count if self.count else float('nan')
    return mean, self.sum, self.count, self.min, self.max

  def merge(self, accumulators):
    raise NotImplementedError()
