# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for the PValue and PCollection classes."""

import unittest

from google.cloud.dataflow.pipeline import Pipeline
from google.cloud.dataflow.pvalue import AsDict
from google.cloud.dataflow.pvalue import AsIter
from google.cloud.dataflow.pvalue import AsList
from google.cloud.dataflow.pvalue import AsSingleton
from google.cloud.dataflow.pvalue import PValue
from google.cloud.dataflow.transforms import Create


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
    value = pipeline | Create('create1', [1, 2, 3])
    value2 = pipeline | Create('create2', [(1, 1), (2, 2), (3, 3)])
    self.assertEqual(AsSingleton(value), AsSingleton(value))
    self.assertEqual(AsSingleton('new', value, default_value=1),
                     AsSingleton('new', value, default_value=1))
    self.assertNotEqual(AsSingleton(value),
                        AsSingleton('new', value, default_value=1))
    self.assertEqual(AsIter(value), AsIter(value))
    self.assertEqual(AsList(value), AsList(value))
    self.assertEqual(AsDict(value2), AsDict(value2))


if __name__ == '__main__':
  unittest.main()
