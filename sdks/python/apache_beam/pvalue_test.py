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

"""Unit tests for the PValue and PCollection classes."""

from __future__ import absolute_import

import unittest

from apache_beam.pvalue import AsSingleton
from apache_beam.pvalue import PValue
from apache_beam.pvalue import TaggedOutput
from apache_beam.testing.test_pipeline import TestPipeline


class PValueTest(unittest.TestCase):

  def test_pvalue_expected_arguments(self):
    pipeline = TestPipeline()
    value = PValue(pipeline)
    self.assertEqual(pipeline, value.pipeline)

  def test_assingleton_multi_element(self):
    with self.assertRaisesRegexp(
        ValueError,
        'PCollection of size 2 with more than one element accessed as a '
        'singleton view. First two elements encountered are \"1\", \"2\".'):
      AsSingleton._from_runtime_iterable([1, 2], {})


class TaggedValueTest(unittest.TestCase):

  def test_passed_tuple_as_tag(self):
    with self.assertRaisesRegexp(
        TypeError,
        r'Attempting to create a TaggedOutput with non-string tag \(1, 2, 3\)'):
      TaggedOutput((1, 2, 3), 'value')


if __name__ == '__main__':
  unittest.main()
