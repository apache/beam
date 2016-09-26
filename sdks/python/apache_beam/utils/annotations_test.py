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
import warnings
from annotations import deprecated
from annotations import experimental


class AnnotationTests(unittest.TestCase):
  # Note: use different names for each of the the functions decorated
  # so that a warning is produced for each of them.
  def test_deprecated_with_since_current(self):
    with warnings.catch_warnings(record=True) as w:
      @deprecated(since='v.1', current='multiply')
      def fnc_test_deprecated_with_since_current():
        return 'lol'
      fnc_test_deprecated_with_since_current()
      self.assertEqual(1, len(w))
      self.assertTrue(issubclass(w[0].category, DeprecationWarning))
      self.assertIn('fnc_test_deprecated_with_since_current is deprecated', str(w[-1].message))
      self.assertIn('since', str(w[-1].message))
      self.assertIn('instead', str(w[-1].message))

  def test_deprecated_without_current(self):
    with warnings.catch_warnings(record=True) as w:
      @deprecated(since='v.1')
      def fnc_test_deprecated_without_current():
        return 'lol'
      fnc_test_deprecated_without_current()
      self.assertEqual(1, len(w))
      self.assertTrue(issubclass(w[0].category, DeprecationWarning))
      self.assertIn('fnc_test_deprecated_without_current is deprecated', str(w[-1].message))
      self.assertIn('since', str(w[-1].message))
      self.assertNotIn('instead', str(w[-1].message))

  def test_deprecated_without_since_should_fail(self):
    with warnings.catch_warnings(record=True) as w:
      with self.assertRaises(TypeError):

        @deprecated()
        def fnc_test_deprecated_without_since_should_fail():
          return 'lol'
        fnc_test_deprecated_without_since_should_fail()
      assert len(w) == 0

  def test_experimental_with_current(self):
    with warnings.catch_warnings(record=True) as w:
      @experimental(current='multiply')
      def fnc_test_experimental_with_current():
        return 'lol'
      fnc_test_experimental_with_current()
      self.assertEqual(1, len(w))
      self.assertTrue(issubclass(w[0].category, FutureWarning))
      self.assertIn('fnc_test_experimental_with_current is experimental', str(w[-1].message))
      self.assertIn('instead', str(w[-1].message))

  def test_experimental_without_current(self):
    with warnings.catch_warnings(record=True) as w:
      @experimental()
      def fnc_test_experimental_without_current():
        return 'lol'
      fnc_test_experimental_without_current()
      self.assertEqual(1, len(w))
      self.assertTrue(issubclass(w[0].category, FutureWarning))
      self.assertIn('fnc_test_experimental_without_current is experimental', str(w[-1].message))
      self.assertNotIn('instead', str(w[-1].message))

  def test_frequency(self):
    """Tests that the filter 'once' is sufficient to print once per
    warning independently of location."""
    with warnings.catch_warnings(record=True) as w:
      @experimental()
      def fnc_test_annotate_frequency():
        return 'lol'

      @experimental()
      def fnc2_test_annotate_frequency():
        return 'lol'
      fnc_test_annotate_frequency()
      fnc_test_annotate_frequency()
      fnc2_test_annotate_frequency()
      self.assertEqual(2, len(w))


if __name__ == '__main__': # It doesn't like these 2 lines
  unittest.main()
