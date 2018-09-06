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

"""Tests for the Observable mixin class."""
from __future__ import absolute_import

import logging
import unittest

from apache_beam.coders import observable


class ObservableMixinTest(unittest.TestCase):
  observed_count = 0
  observed_sum = 0
  observed_keys = []

  def observer(self, value, key=None):
    self.observed_count += 1
    self.observed_sum += value
    self.observed_keys.append(key)

  def test_observable(self):
    class Watched(observable.ObservableMixin):

      def __iter__(self):
        for i in (1, 4, 3):
          self.notify_observers(i, key='a%d' % i)
          yield i

    watched = Watched()
    watched.register_observer(lambda v, key: self.observer(v, key=key))
    for _ in watched:
      pass

    self.assertEquals(3, self.observed_count)
    self.assertEquals(8, self.observed_sum)
    self.assertEquals(['a1', 'a3', 'a4'], sorted(self.observed_keys))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
