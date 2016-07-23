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

"""Tests for consumer_tracking_pipeline_visitor."""

import logging
import unittest

from apache_beam import pvalue
from apache_beam.io import Read
from apache_beam.io import TextFileSource
from apache_beam.pipeline import Pipeline
from apache_beam.pvalue import AsList
from apache_beam.runners.inprocess import InProcessPipelineRunner
from apache_beam.runners.inprocess.consumer_tracking_pipeline_visitor import ConsumerTrackingPipelineVisitor
from apache_beam.transforms import CoGroupByKey
from apache_beam.transforms import Create
from apache_beam.transforms import DoFn
from apache_beam.transforms import FlatMap
from apache_beam.transforms import Flatten
from apache_beam.transforms import ParDo

# Disable frequent lint warning due to pipe operator for chaining transforms.
# pylint: disable=expression-not-assigned
# pylint: disable=pointless-statement


class ConsumerTrackingPipelineVisitorTest(unittest.TestCase):

  def setUp(self):
    self.pipeline = Pipeline(InProcessPipelineRunner())
    self.visitor = ConsumerTrackingPipelineVisitor()

  def test_root_transforms(self):
    root_create = Create('create', [[1, 2, 3]])
    root_read = Read('read', TextFileSource('/tmp/somefile'))
    root_flatten = Flatten('flatten', pipeline=self.pipeline)

    pbegin = pvalue.PBegin(self.pipeline)
    pcoll_create = pbegin | root_create
    pbegin | root_read
    pcoll_create | FlatMap(lambda x: x)
    [] | root_flatten

    self.pipeline.visit(self.visitor)

    root_transforms = sorted(
        [t.transform for t in self.visitor.root_transforms])
    self.assertEqual(root_transforms, sorted(
        [root_read, root_create, root_flatten]))

    pbegin_consumers = sorted(
        [c.transform for c in self.visitor.value_to_consumers[pbegin]])
    self.assertEqual(pbegin_consumers, sorted([root_read, root_create]))
    self.assertEqual(len(self.visitor.step_names), 4)

  def test_side_inputs(self):

    class SplitNumbersFn(DoFn):

      def process(self, context):
        if context.element < 0:
          yield pvalue.SideOutputValue('tag_negative', context.element)
        else:
          yield context.element

    class ProcessNumbersFn(DoFn):

      def process(self, context, negatives):
        yield context.element

    root_create = Create('create', [[-1, 2, 3]])

    result = (self.pipeline
              | root_create
              | ParDo(SplitNumbersFn()).with_outputs('tag_negative',
                                                     main='positive'))
    positive, negative = result
    positive | ParDo(ProcessNumbersFn(), AsList(negative))

    self.pipeline.visit(self.visitor)

    root_transforms = sorted(
        [t.transform for t in self.visitor.root_transforms])
    self.assertEqual(root_transforms, sorted([root_create]))
    self.assertEqual(len(self.visitor.step_names), 4)
    self.assertEqual(len(self.visitor.views), 1)
    self.assertTrue(isinstance(self.visitor.views[0],
                               pvalue.ListPCollectionView))

  def test_co_group_by_key(self):
    emails = self.pipeline | 'email' >> Create([('joe', 'joe@example.com')])
    phones = self.pipeline | 'phone' >> Create([('mary', '111-222-3333')])
    {'emails': emails, 'phones': phones} | CoGroupByKey()

    self.pipeline.visit(self.visitor)

    root_transforms = sorted(
        [t.transform for t in self.visitor.root_transforms])
    self.assertEqual(len(root_transforms), 2)
    self.assertGreater(
        len(self.visitor.step_names), 3)  # 2 creates + expanded CoGBK
    self.assertEqual(len(self.visitor.views), 0)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
