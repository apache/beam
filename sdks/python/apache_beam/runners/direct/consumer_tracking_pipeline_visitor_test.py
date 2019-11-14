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
from __future__ import absolute_import

import logging
import unittest

from apache_beam import pvalue
from apache_beam.io import Read
from apache_beam.io import iobase
from apache_beam.pipeline import Pipeline
from apache_beam.pvalue import AsList
from apache_beam.runners.direct import DirectRunner
from apache_beam.runners.direct.consumer_tracking_pipeline_visitor import ConsumerTrackingPipelineVisitor
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
    self.pipeline = Pipeline(DirectRunner())
    self.visitor = ConsumerTrackingPipelineVisitor()
    try:                    # Python 2
      self.assertCountEqual = self.assertItemsEqual
    except AttributeError:  # Python 3
      pass

  def test_root_transforms(self):
    class DummySource(iobase.BoundedSource):
      pass

    root_read = Read(DummySource())
    root_flatten = Flatten(pipeline=self.pipeline)

    pbegin = pvalue.PBegin(self.pipeline)
    pcoll_read = pbegin | 'read' >> root_read
    pcoll_read | FlatMap(lambda x: x)
    [] | 'flatten' >> root_flatten

    self.pipeline.visit(self.visitor)

    root_transforms = [t.transform for t in self.visitor.root_transforms]

    self.assertCountEqual(root_transforms, [root_read, root_flatten])

    pbegin_consumers = [c.transform
                        for c in self.visitor.value_to_consumers[pbegin]]
    self.assertCountEqual(pbegin_consumers, [root_read])
    self.assertEqual(len(self.visitor.step_names), 3)

  def test_side_inputs(self):

    class SplitNumbersFn(DoFn):

      def process(self, element):
        if element < 0:
          yield pvalue.TaggedOutput('tag_negative', element)
        else:
          yield element

    class ProcessNumbersFn(DoFn):

      def process(self, element, negatives):
        yield element

    class DummySource(iobase.BoundedSource):
      pass

    root_read = Read(DummySource())

    result = (self.pipeline
              | 'read' >> root_read
              | ParDo(SplitNumbersFn()).with_outputs('tag_negative',
                                                     main='positive'))
    positive, negative = result
    positive | ParDo(ProcessNumbersFn(), AsList(negative))

    self.pipeline.visit(self.visitor)

    root_transforms = [t.transform for t in self.visitor.root_transforms]
    self.assertEqual(root_transforms, [root_read])
    self.assertEqual(len(self.visitor.step_names), 3)
    self.assertEqual(len(self.visitor.views), 1)
    self.assertTrue(isinstance(self.visitor.views[0],
                               pvalue.AsList))

  def test_co_group_by_key(self):
    emails = self.pipeline | 'email' >> Create([('joe', 'joe@example.com')])
    phones = self.pipeline | 'phone' >> Create([('mary', '111-222-3333')])
    {'emails': emails, 'phones': phones} | CoGroupByKey()

    self.pipeline.visit(self.visitor)

    root_transforms = [t.transform for t in self.visitor.root_transforms]
    self.assertEqual(len(root_transforms), 2)
    self.assertGreater(
        len(self.visitor.step_names), 3)  # 2 creates + expanded CoGBK
    self.assertEqual(len(self.visitor.views), 0)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
