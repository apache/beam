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

import unittest

from apache_beam.runners.common import DoFnSignature
from apache_beam.transforms.core import DoFn


class DoFnSignatureTest(unittest.TestCase):

  def test_dofn_validate_process_error(self):
    class MyDoFn(DoFn):
      def process(self, element, w1=DoFn.WindowParam, w2=DoFn.WindowParam):
        pass

    with self.assertRaises(ValueError):
      DoFnSignature(MyDoFn())

  def test_dofn_validate_start_bundle_error(self):
    class MyDoFn(DoFn):
      def process(self, element):
        pass

      def start_bundle(self, w1=DoFn.WindowParam):
        pass

    with self.assertRaises(ValueError):
      DoFnSignature(MyDoFn())

  def test_dofn_validate_finish_bundle_error(self):
    class MyDoFn(DoFn):
      def process(self, element):
        pass

      def finish_bundle(self, w1=DoFn.WindowParam):
        pass

    with self.assertRaises(ValueError):
      DoFnSignature(MyDoFn())


if __name__ == '__main__':
  unittest.main()
