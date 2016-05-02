# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for side input utilities."""

import logging
import unittest


from google.cloud.dataflow.worker import sideinputs


class EmulatedCollectionsTest(unittest.TestCase):

  def test_emulated_iterable(self):
    def _iterable_fn():
      for i in range(10):
        yield i
    iterable = sideinputs.EmulatedIterable(_iterable_fn)
    # Check that multiple iterations are supported.
    for _ in range(0, 5):
      for i, j in enumerate(iterable):
        self.assertEqual(i, j)

  def test_large_iterable_values(self):
    def _iterable_fn():
      for i in range(10):
        yield ('%d' % i) * (200 * 1024 * 1024)
    iterable = sideinputs.EmulatedIterable(_iterable_fn)
    # Check that multiple iterations are supported.
    for _ in range(0, 3):
      for i, j in enumerate(iterable):
        self.assertEqual(('%d' % i) * (200 * 1024 * 1024), j)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
