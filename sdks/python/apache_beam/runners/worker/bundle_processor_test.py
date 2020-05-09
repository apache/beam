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

"""Unit tests for bundle processing."""
# pytype: skip-file

from __future__ import absolute_import

import unittest

from apache_beam.runners.worker.bundle_processor import DataInputOperation


def simple_split(first_residual_index):
  return first_residual_index - 1, None, None, first_residual_index


def element_split(frac, index):
  return (
      index - 1,
      'Primary(%0.1f)' % frac,
      'Residual(%0.1f)' % (1 - frac),
      index + 1)


class SplitTest(unittest.TestCase):
  def split(
      self,
      index,
      current_element_progress,
      fraction_of_remainder,
      buffer_size,
      allowed=(),
      sdf=False):
    return DataInputOperation._compute_split(
        index,
        current_element_progress,
        float('inf'),
        fraction_of_remainder,
        buffer_size,
        allowed_split_points=allowed,
        try_split=lambda frac: element_split(frac, 0)[1:3] if sdf else None)

  def sdf_split(self, *args, **kwargs):
    return self.split(*args, sdf=True, **kwargs)

  def test_simple_split(self):
    self.assertEqual(self.split(0, 0, 0, 16), simple_split(1))
    self.assertEqual(self.split(0, 0, 0.24, 16), simple_split(4))
    self.assertEqual(self.split(0, 0, 0.25, 16), simple_split(4))
    self.assertEqual(self.split(0, 0, 0.26, 16), simple_split(4))
    self.assertEqual(self.split(0, 0, 0.5, 16), simple_split(8))
    self.assertEqual(self.split(2, 0, 0.5, 16), simple_split(9))
    self.assertEqual(self.split(6, 0, 0.5, 16), simple_split(11))

  def test_split_with_element_progres(self):
    self.assertEqual(self.split(0, 0.5, 0.25, 4), simple_split(1))
    self.assertEqual(self.split(0, 0.9, 0.25, 4), simple_split(2))
    self.assertEqual(self.split(1, 0.0, 0.25, 4), simple_split(2))
    self.assertEqual(self.split(1, 0.1, 0.25, 4), simple_split(2))

  def test_split_with_element_allowed_splits(self):
    self.assertEqual(
        self.split(0, 0, 0.25, 16, allowed=(2, 3, 4, 5)), simple_split(4))
    self.assertEqual(
        self.split(0, 0, 0.25, 16, allowed=(2, 3, 5)), simple_split(5))
    self.assertEqual(
        self.split(0, 0, 0.25, 16, allowed=(2, 3, 6)), simple_split(3))

    self.assertEqual(
        self.split(0, 0, 0.25, 16, allowed=(5, 6, 7)), simple_split(5))
    self.assertEqual(
        self.split(0, 0, 0.25, 16, allowed=(1, 2, 3)), simple_split(3))

    self.assertEqual(self.split(5, 0, 0.25, 16, allowed=(1, 2, 3)), None)

  def test_sdf_split(self):
    self.assertEqual(self.sdf_split(0, 0, 0.51, 4), simple_split(2))
    self.assertEqual(self.sdf_split(0, 0, 0.49, 4), simple_split(2))
    self.assertEqual(self.sdf_split(0, 0, 0.26, 4), simple_split(1))
    self.assertEqual(self.sdf_split(0, 0, 0.25, 4), simple_split(1))
    self.assertEqual(
        self.sdf_split(0, 0, 0.20, 4), (-1, 'Primary(0.8)', 'Residual(0.2)', 1))
    self.assertEqual(
        self.sdf_split(0, 0, 0.12, 4), (-1, 'Primary(0.5)', 'Residual(0.5)', 1))
    self.assertEqual(self.sdf_split(0, .5, 0.2, 4), simple_split(1))

    self.assertEqual(self.sdf_split(2, 0, 0.6, 4), simple_split(3))
    self.assertEqual(self.sdf_split(2, 0.9, 0.6, 4), simple_split(4))

  def test_sdf_split_with_allowed_splits(self):
    self.assertEqual(
        self.sdf_split(2, 0, 0.2, 5, allowed=(1, 2, 3, 4, 5)),
        (1, 'Primary(0.6)', 'Residual(0.4)', 3))
    self.assertEqual(
        self.sdf_split(2, 0, 0.2, 5, allowed=(1, 2, 4, 5)), simple_split(4))
    self.assertEqual(
        self.sdf_split(2, 0, 0.2, 5, allowed=(1, 2, 5)), simple_split(5))
    self.assertEqual(
        self.sdf_split(2, 0, 0.2, 5, allowed=(1, 3, 4, 5)), simple_split(3))


if __name__ == '__main__':
  unittest.main()
