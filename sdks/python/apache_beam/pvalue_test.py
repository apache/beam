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

import unittest

from apache_beam.pipeline import Pipeline
from apache_beam.pvalue import AsDict
from apache_beam.pvalue import AsIter
from apache_beam.pvalue import AsList
from apache_beam.pvalue import AsSingleton
from apache_beam.pvalue import PValue
from apache_beam.transforms import Create


class FakePipeline(Pipeline):
  """Fake pipeline object used to check if apply() receives correct args."""

  def apply(self, *args, **kwargs):
    self.args = args
    self.kwargs = kwargs


class PValueTest(unittest.TestCase):

  def test_pvalue_expected_arguments(self):
    pipeline = Pipeline('DirectPipelineRunner')
    value = PValue(pipeline)
    self.assertEqual(pipeline, value.pipeline)

  def test_pcollectionview_not_recreated(self):
    pipeline = Pipeline('DirectPipelineRunner')
    value = pipeline | 'create1' >> Create([1, 2, 3])
    value2 = pipeline | 'create2' >> Create([(1, 1), (2, 2), (3, 3)])
    value3 = pipeline | 'create3' >> Create([(1, 1), (2, 2), (3, 3)])
    self.assertEqual(AsSingleton(value), AsSingleton(value))
    self.assertEqual(AsSingleton('new', value, default_value=1),
                     AsSingleton('new', value, default_value=1))
    self.assertNotEqual(AsSingleton(value),
                        AsSingleton('new', value, default_value=1))
    self.assertEqual(AsIter(value), AsIter(value))
    self.assertEqual(AsList(value), AsList(value))
    self.assertEqual(AsDict(value2), AsDict(value2))

    self.assertNotEqual(AsSingleton(value), AsSingleton(value2))
    self.assertNotEqual(AsIter(value), AsIter(value2))
    self.assertNotEqual(AsList(value), AsList(value2))
    self.assertNotEqual(AsDict(value2), AsDict(value3))


if __name__ == '__main__':
  unittest.main()
