from apache_beam.transforms.cy_combiners import DistributionAccumulator

import unittest
import math


class DistributionAccumulatorTest(unittest.TestCase):

  def test_calculate_bucket_index_with_input_0(self):
    counter = DistributionAccumulator()
    index = counter.calculate_bucket_index(0)
    self.assertEquals(index, 0)

  def test_calculate_bucket_index_within_max_long(self):
    counter = DistributionAccumulator()
    bucket = 1
    power_of_ten = 1
    INT64_MAX = math.pow(2, 63) - 1
    while power_of_ten <= INT64_MAX:
      for multiplier in [1, 2, 5]:
        value = multiplier * power_of_ten
        actual_bucket = counter.calculate_bucket_index(value - 1)
        self.assertEquals(actual_bucket, bucket - 1)
        bucket += 1
      power_of_ten *= 10

  def test_add_input(self):
    counter = DistributionAccumulator()
    expected_buckets = [1, 3, 0, 0, 0, 0, 0, 0, 1, 1]
    for value in [1, 500, 2, 3, 1000, 4]:
      counter.add_input(value)
    print  counter.buckets
    self.assertEquals(counter.buckets, expected_buckets)


if __name__ == '__main__':
  unittest.main()
