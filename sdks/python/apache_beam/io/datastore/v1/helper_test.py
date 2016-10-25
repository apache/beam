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

from apache_beam.io.datastore.v1 import helper
from google.datastore.v1.entity_pb2 import Key
import unittest


class HelperTest(unittest.TestCase):

  def test_compare_path_with_different_kind(self):
    p1 = Key.PathElement()
    p1.kind = 'dummy1'

    p2 = Key.PathElement()
    p2.kind = 'dummy2'

    self.assertLess(helper.compare_path(p1, p2), 0)

  def test_compare_path_with_different_id(self):
    p1 = Key.PathElement()
    p1.kind = 'dummy'
    p1.id = 10

    p2 = Key.PathElement()
    p2.kind = 'dummy'
    p2.id = 15

    self.assertLess(helper.compare_path(p1, p2), 0)

  def test_compare_path_with_different_name(self):
    p1 = Key.PathElement()
    p1.kind = 'dummy'
    p1.name = "dummy1"

    p2 = Key.PathElement()
    p2.kind = 'dummy'
    p2.name = 'dummy2'

    self.assertLess(helper.compare_path(p1, p2), 0)

  def test_compare_path_of_different_type(self):
    p1 = Key.PathElement()
    p1.kind = 'dummy'
    p1.id = 10

    p2 = Key.PathElement()
    p2.kind = 'dummy'
    p2.name = 'dummy'

    self.assertLess(helper.compare_path(p1, p2), 0)

  def test_key_comparator_with_different_partition(self):
    k1 = Key()
    k1.partition_id.namespace_id = 'dummy1'
    k2 = Key()
    k2.partition_id.namespace_id = 'dummy2'
    self.assertRaises(ValueError, helper.key_comparator, k1, k2)

  def test_key_comparator_with_single_path(self):
    k1 = Key()
    k2 = Key()
    p1 = k1.path.add()
    p2 = k2.path.add()
    p1.kind = p2.kind = 'dummy'
    self.assertEqual(helper.key_comparator(k1, k2), 0)

  def test_key_comparator_with_multiple_paths_1(self):
    k1 = Key()
    k2 = Key()
    p11 = k1.path.add()
    p12 = k1.path.add()
    p21 = k2.path.add()
    p11.kind = p12.kind = p21.kind = 'dummy'
    self.assertGreater(helper.key_comparator(k1, k2), 0)

  def test_key_comparator_with_multiple_paths_2(self):
    k1 = Key()
    k2 = Key()
    p11 = k1.path.add()
    p21 = k2.path.add()
    p22 = k2.path.add()
    p11.kind = p21.kind = p22.kind = 'dummy'
    self.assertLess(helper.key_comparator(k1, k2), 0)

  def test_key_comparator_with_multiple_paths_3(self):
    k1 = Key()
    k2 = Key()
    p11 = k1.path.add()
    p12 = k1.path.add()
    p21 = k2.path.add()
    p22 = k2.path.add()
    p11.kind = p12.kind = p21.kind = p22.kind = 'dummy'
    self.assertEqual(helper.key_comparator(k1, k2), 0)

  def test_key_comparator_with_multiple_paths_4(self):
    k1 = Key()
    k2 = Key()
    p11 = k1.path.add()
    p12 = k2.path.add()
    p21 = k2.path.add()
    p11.kind = p12.kind = 'dummy'
    # make path2 greater than path1
    p21.kind = 'dummy1'
    self.assertLess(helper.key_comparator(k1, k2), 0)

if __name__ == '__main__':
  unittest.main()
