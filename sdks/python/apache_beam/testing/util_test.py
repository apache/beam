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

"""Unit tests for testing utilities."""

import unittest

from apache_beam import Create
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import contains_in_any_order
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import is_empty


def equals_int(number):
  return lambda n: n == number


class UtilTest(unittest.TestCase):

  def test_assert_that_equal_to_passes(self):
    with TestPipeline() as p:
      assert_that(p | Create([1, 2, 3]), equal_to([1, 2, 3]))

  def test_assert_that_equal_to_fails(self):
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(p | Create([1, 10, 100]), equal_to([1, 2, 3]))

  def test_assert_that_equal_to_fails_on_empty_input(self):
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(p | Create([]), equal_to([1, 2, 3]))

  def test_assert_that_contains_in_any_order_passes(self):
    with TestPipeline() as p:
      assert_that(p | Create([2, 3, 1]), contains_in_any_order(
          [equals_int(1), equals_int(2), equals_int(3)]))

  def test_assert_that_contains_in_any_order_fails(self):
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(p | Create([1, 10, 100]), contains_in_any_order(
            [equals_int(1), equals_int(2), equals_int(3)]))

  def test_assert_that_is_empty_passes(self):
    with TestPipeline() as p:
      assert_that(p | Create([]), is_empty())

  def test_assert_that_is_empty_fails(self):
    with self.assertRaises(Exception):
      with TestPipeline() as p:
        assert_that(p | Create([1, 2, 3]), is_empty())


if __name__ == '__main__':
  unittest.main()
