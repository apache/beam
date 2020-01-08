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

"""Tests for apache_beam.typehints.trivial_inference that use Python 3 syntax.
"""

# pytype: skip-file

from __future__ import absolute_import

import unittest

from apache_beam.typehints import trivial_inference
from apache_beam.typehints import typehints


class TrivialInferenceTest(unittest.TestCase):

  def assertReturnType(self, expected, f, inputs=(), depth=5):
    self.assertEqual(
        expected,
        trivial_inference.infer_return_type(f, inputs, debug=True, depth=depth))

  def testBuildListUnpack(self):
    # Lambda uses BUILD_LIST_UNPACK opcode in Python 3.
    self.assertReturnType(typehints.List[int],
                          lambda _list: [*_list, *_list, *_list],
                          [typehints.List[int]])

  def testBuildTupleUnpack(self):
    # Lambda uses BUILD_TUPLE_UNPACK opcode in Python 3.
    self.assertReturnType(typehints.Tuple[int, str, str],
                          lambda _list1, _list2: (*_list1, *_list2, *_list2),
                          [typehints.List[int], typehints.List[str]])


if __name__ == '__main__':
  unittest.main()
