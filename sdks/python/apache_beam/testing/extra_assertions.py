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
from __future__ import absolute_import

import itertools
import sys

import numpy as np
from past.builtins import unicode


class ExtraAssertionsMixin(object):

  if sys.version_info[0] < 3:

    def assertCountEqual(self, first, second, msg=None):
      """Assert that two containers have the same number of the same items in
      any order.
      """
      return self.assertItemsEqual(first, second, msg=msg)

  def assertArrayCountEqual(self, data1, data2):
    """Assert that two containers have the same items, with special treatment
    for numpy arrays.
    """
    try:
      self.assertEqual(type(data1), type(data2))
    except AssertionError:
      # Since 'a' == u'a', we should not automatically treat strings of
      # different type as different.
      self.assertTrue(isinstance(data1, (str, unicode)))
      self.assertTrue(isinstance(data2, (str, unicode)))

    # self.assertCountEqual is much faster, so try it first.
    try:
      return self.assertCountEqual(data1, data2)
    except (TypeError, ValueError):
      pass

    try:
      data1 = list(data1)
      data2 = list(data2)
    except TypeError:
      # Elements are not iterable.
      return self.assertEqual(data1, data2)
    else:
      # Containers must have the same length.
      self.assertEqual(len(data1), len(data2))

    # We can check for equality much faster for hashable objects
    data1_hashable, data1 = self._split_into_hash_nonhash(data1)
    data2_hashable, data2 = self._split_into_hash_nonhash(data2)
    self.assertCountEqual(data1_hashable, data2_hashable)

    if isinstance(data1, (str, bytes, unicode)):
      return self.assertEqual(data1, data2)

    if isinstance(data1, dict):
      self.assertCountEqual(list(data1.keys()), list(data2.keys()))
      for key in data1:
        self.assertArrayCountEqual(data1[key], data2[key])
      return

    if isinstance(data1, np.ndarray):
      return np.testing.assert_array_almost_equal(data1, data2)

    # Performance here is terrible: O(n!), and larger for nested containers.
    for data2_perm in itertools.permutations(data2):
      try:
        for d1, d2 in zip(data1, data2_perm):
          self.assertArrayCountEqual(d1, d2)
      except AssertionError:
        continue
      return
    raise AssertionError(
        "The two objects '{}' and '{}' do not contain the same elements.".
        format(data1, data2))

  def _split_into_hash_nonhash(self, data):
    data_hash = []
    data_nonhash = []
    for value in data:
      try:
        hash(value)
        data_hash.append(value)
      except TypeError:
        data_nonhash.append(value)
    return data_hash, data_nonhash
