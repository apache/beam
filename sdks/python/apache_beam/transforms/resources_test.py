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

import unittest

from parameterized import param
from parameterized import parameterized

from apache_beam import PTransform
from apache_beam.transforms.resources import ResourceHint


class ResourcesTest(unittest.TestCase):
  @parameterized.expand([
      param(
          name='min_ram',
          val='100 MiB',
          urn='beam:resources:min_ram_bytes:v1',
          bytestr=b'104857600'),
      param(
          name='minRam',
          val='100MB',
          urn='beam:resources:min_ram_bytes:v1',
          bytestr=b'100000000'),
      param(
          name='min_ram',
          val='6.5 GiB',
          urn='beam:resources:min_ram_bytes:v1',
          bytestr=b'6979321856'),
      param(
          name='accelerator',
          val='gpu',
          urn='beam:resources:accelerator:v1',
          bytestr=b'gpu'),
  ])
  def test_known_resource_hints(self, name, val, urn, bytestr):
    t = PTransform()
    t = t.with_resource_hints(**{name: val})
    self.assertTrue(ResourceHint.is_registered(name))
    self.assertEqual(t.get_resource_hints(), {urn: bytestr})

  @parameterized.expand([
      param(name='min_ram', val='3,500G'),
      param(name='accelerator', val=1),
      param(name='unknown_hint', val=1)
  ])
  def test_resource_hint_parsing_fails_early(self, name, val):
    t = PTransform()
    with self.assertRaises(ValueError):
      _ = t.with_resource_hints(**{name: val})


if __name__ == '__main__':
  unittest.main()
