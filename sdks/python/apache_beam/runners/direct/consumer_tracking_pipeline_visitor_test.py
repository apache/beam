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
# pytype: skip-file

import logging
import unittest

import apache_beam as beam
from apache_beam import pvalue
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

  def test_root_transforms(self):
    root_read = beam.Impulse()
    root_flatten = Flatten(pipeline=self.pipeline)

    pbegin = pvalue.PBegin(self.pipeline)
    pcoll_read = pbegin | 'read' >> root_read
    pcoll_read | FlatMap(lambda x: x)
    [] | 'flatten' >> root_flatten

    self.pipeline.visit(self.visitor)

    root_transforms = [t.transform for t in self.visitor.root_transforms]

    self.assertCountEqual(root_transforms, [root_read, root_flatten])

    pbegin_consumers = [
        c.transform for c in self.visitor.value_to_consumers[pbegin]
    ]
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

    def _process_numbers(pcoll, negatives):
      first_output = (
          pcoll
          | 'process numbers step 1' >> ParDo(ProcessNumbersFn(), negatives))

      second_output = (
          first_output
          | 'process numbers step 2' >> ParDo(ProcessNumbersFn(), negatives))

      output_pc = ((first_output, second_output)
                   | 'flatten results' >> beam.Flatten())
      return output_pc

    root_read = beam.Impulse()

    result = (
        self.pipeline
        | 'read' >> root_read
        | ParDo(SplitNumbersFn()).with_outputs('tag_negative', main='positive'))
    positive, negative = result
    _process_numbers(positive, AsList(negative))

    self.pipeline.visit(self.visitor)

    root_transforms = [t.transform for t in self.visitor.root_transforms]
    self.assertEqual(root_transforms, [root_read])
    self.assertEqual(len(self.visitor.step_names), 5)
    self.assertEqual(len(self.visitor.views), 1)
    self.assertTrue(isinstance(self.visitor.views[0], pvalue.AsList))

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

  def test_visitor_not_sorted(self):
    p = Pipeline()
    # pylint: disable=expression-not-assigned
    from apache_beam.testing.test_stream import TestStream
    p | TestStream().add_elements(['']) | beam.Map(lambda _: _)

    original_graph = p.to_runner_api(return_context=False)
    out_of_order_graph = p.to_runner_api(return_context=False)

    root_id = out_of_order_graph.root_transform_ids[0]
    root = out_of_order_graph.components.transforms[root_id]
    tmp = root.subtransforms[0]
    root.subtransforms[0] = root.subtransforms[1]
    root.subtransforms[1] = tmp

    p = beam.Pipeline().from_runner_api(
        out_of_order_graph, runner='BundleBasedDirectRunner', options=None)
    v_out_of_order = ConsumerTrackingPipelineVisitor()
    p.visit(v_out_of_order)

    p = beam.Pipeline().from_runner_api(
        original_graph, runner='BundleBasedDirectRunner', options=None)
    v_original = ConsumerTrackingPipelineVisitor()
    p.visit(v_original)

    # Convert to string to assert they are equal.
    out_of_order_labels = {
        str(k): [str(t) for t in value_to_consumer]
        for k,
        value_to_consumer in v_out_of_order.value_to_consumers.items()
    }

    original_labels = {
        str(k): [str(t) for t in value_to_consumer]
        for k,
        value_to_consumer in v_original.value_to_consumers.items()
    }
    self.assertDictEqual(out_of_order_labels, original_labels)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
