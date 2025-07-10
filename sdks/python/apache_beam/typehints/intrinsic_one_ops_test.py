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

"""Tests for apache_beam.typehints.intrinsic_one_ops."""

# pytype: skip-file

import dis
import sys
import unittest

from apache_beam.typehints import intrinsic_one_ops


class IntrinsicOneOpsTest(unittest.TestCase):
  def test_unary_intrinsic_ops_are_in_the_same_order_as_in_cpython(self):
    if sys.version_info >= (3, 12):
      dis_order = dis.__dict__['_intrinsic_1_descs']
      beam_ops = [fn.__name__.upper() for fn in intrinsic_one_ops.INT_ONE_OPS]
      self.assertListEqual(dis_order, beam_ops)


if __name__ == '__main__':
  unittest.main()
