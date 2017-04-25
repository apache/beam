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
from apache_beam.utils.annotations import deprecated
from apache_beam.utils.annotations import experimental


class AnnotationTests(unittest.TestCase):
  # Note: use different names for each of the the functions decorated
  # so that a warning is produced for each of them.
  def test_deprecated_with_since_current(self):
    with warnings.catch_warnings(record=True) as w:
      @deprecated(since='v.1', current='multiply')
      def fnc_test_deprecated_with_since_current():
        return 'lol'
      fnc_test_deprecated_with_since_current()
      self.check_annotation(warning=w, warning_size=1,
                            warning_type=DeprecationWarning,
                            fnc_name='fnc_test_deprecated_with_since_current',
                            annotation_type='deprecated',
                            label_check_list=[('since', True),
                                              ('instead', True)])

  def test_deprecated_without_current(self):
    with warnings.catch_warnings(record=True) as w:
      @deprecated(since='v.1')
      def fnc_test_deprecated_without_current():
        return 'lol'
      fnc_test_deprecated_without_current()
      self.check_annotation(warning=w, warning_size=1,
                            warning_type=DeprecationWarning,
                            fnc_name='fnc_test_deprecated_without_current',
                            annotation_type='deprecated',
                            label_check_list=[('since', True),
                                              ('instead', False)])

  def test_deprecated_without_since_should_fail(self):
    with warnings.catch_warnings(record=True) as w:
      with self.assertRaises(TypeError):

        @deprecated()
        def fnc_test_deprecated_without_since_should_fail():
          return 'lol'
        fnc_test_deprecated_without_since_should_fail()
      assert not w

  def test_experimental_with_current(self):
    with warnings.catch_warnings(record=True) as w:
      @experimental(current='multiply')
      def fnc_test_experimental_with_current():
        return 'lol'
      fnc_test_experimental_with_current()
      self.check_annotation(warning=w, warning_size=1,
                            warning_type=FutureWarning,
                            fnc_name='fnc_test_experimental_with_current',
                            annotation_type='experimental',
                            label_check_list=[('instead', True)])

  def test_experimental_without_current(self):
    with warnings.catch_warnings(record=True) as w:
      @experimental()
      def fnc_test_experimental_without_current():
        return 'lol'
      fnc_test_experimental_without_current()
      self.check_annotation(warning=w, warning_size=1,
                            warning_type=FutureWarning,
                            fnc_name='fnc_test_experimental_without_current',
                            annotation_type='experimental',
                            label_check_list=[('instead', False)])

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
      self.check_annotation(warning=[w[0]], warning_size=1,
                            warning_type=FutureWarning,
                            fnc_name='fnc_test_annotate_frequency',
                            annotation_type='experimental',
                            label_check_list=[])
      self.check_annotation(warning=[w[1]], warning_size=1,
                            warning_type=FutureWarning,
                            fnc_name='fnc2_test_annotate_frequency',
                            annotation_type='experimental',
                            label_check_list=[])

  # helper function
  def check_annotation(self, warning, warning_size, warning_type, fnc_name,
                       annotation_type, label_check_list):
    self.assertEqual(1, warning_size)
    self.assertTrue(issubclass(warning[-1].category, warning_type))
    self.assertIn(fnc_name + ' is ' + annotation_type, str(warning[-1].message))
    for label in label_check_list:
      if label[1] is True:
        self.assertIn(label[0], str(warning[-1].message))
      else:
        self.assertNotIn(label[0], str(warning[-1].message))


if __name__ == '__main__':
  unittest.main()
