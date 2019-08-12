# -*- coding: utf-8 -*-
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

import logging
import unittest

import numpy as np

from apache_beam.testing.extra_assertions import ExtraAssertionsMixin


class ExtraAssertionsMixinTest(ExtraAssertionsMixin, unittest.TestCase):

  def test_assert_array_count_equal_strings(self):
    data1 = [u"±♠Ωℑ", u"hello", "world"]
    data2 = ["hello", u"±♠Ωℑ", u"world"]
    self.assertArrayCountEqual(data1, data2)

  def test_assert_array_count_equal_mixed(self):
    # TODO(ostrokach): Add a timeout, since if assertArrayCountEqual is not
    # implemented efficiently, this test has the potential to run for a very
    # long time.
    data1 = [
        #
        {'a': 1, 123: 1.234},
        ['d', 1],
        u"±♠Ωℑ",
        np.zeros((3, 6)),
        (1, 2, 3, 'b'),
        'def',
        100,
        'abc',
        ('a', 'b', 'c'),
        None
    ]
    data2 = [
        #
        {'a': 1, 123: 1.234},
        ('a', 'b', 'c'),
        ['d', 1],
        None,
        'abc',
        'def',
        u"±♠Ωℑ",
        100,
        (1, 2, 3, 'b'),
        np.zeros((3, 6))
    ]
    self.assertArrayCountEqual(data1, data2)
    self.assertArrayCountEqual(data1 * 2, data2 * 2)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
