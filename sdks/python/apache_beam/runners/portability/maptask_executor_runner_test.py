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

import logging
import os
import tempfile
import unittest

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.metricbase import MetricName
from apache_beam.pvalue import AsList
from apache_beam.runners.portability import maptask_executor_runner
from apache_beam.testing.util import BeamAssertException
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.window import TimestampedValue


class MapTaskExecutorRunnerTest(unittest.TestCase):

  def create_pipeline(self):
    return beam.Pipeline(runner=maptask_executor_runner.MapTaskExecutorRunner())

  def test_assert_that(self):
    with self.assertRaisesRegexp(BeamAssertException, 'bad_assert'):
      with self.create_pipeline() as p:
        assert_that(p | beam.Create(['a', 'b']), equal_to(['a']), 'bad_assert')

  def test_create(self):
    with self.create_pipeline() as p:
      assert_that(p | beam.Create(['a', 'b']), equal_to(['a', 'b']))

  def test_pardo(self):
    with self.create_pipeline() as p:
      res = (p
             | beam.Create(['a', 'bc'])
             | beam.Map(lambda e: e * 2)
             | beam.Map(lambda e: e + 'x'))
      assert_that(res, equal_to(['aax', 'bcbcx']))

  def test_pardo_metrics(self):

    class MyDoFn(beam.DoFn):

      def start_bundle(self):
        self.count = Metrics.counter(self.__class__, 'elements')

      def process(self, element):
        self.count.inc(element)
        return [element]

    class MyOtherDoFn(beam.DoFn):

      def start_bundle(self):
        self.count = Metrics.counter(self.__class__, 'elementsplusone')

      def process(self, element):
        self.count.inc(element + 1)
        return [element]

    with self.create_pipeline() as p:
      res = (p | beam.Create([1, 2, 3])
             | 'mydofn' >> beam.ParDo(MyDoFn())
             | 'myotherdofn' >> beam.ParDo(MyOtherDoFn()))
      p.run()
      if not MetricsEnvironment.METRICS_SUPPORTED:
        self.skipTest('Metrics are not supported.')

      counter_updates = [{'key': key, 'value': val}
                         for container in p.runner.metrics_containers()
                         for key, val in
                         container.get_updates().counters.items()]
      counter_values = [update['value'] for update in counter_updates]
      counter_keys = [update['key'] for update in counter_updates]
      assert_that(res, equal_to([1, 2, 3]))
      self.assertEqual(counter_values, [6, 9])
      self.assertEqual(counter_keys, [
          MetricKey('mydofn',
                    MetricName(__name__ + '.MyDoFn', 'elements')),
          MetricKey('myotherdofn',
                    MetricName(__name__ + '.MyOtherDoFn', 'elementsplusone'))])

  def test_pardo_side_outputs(self):
    def tee(elem, *tags):
      for tag in tags:
        if tag in elem:
          yield beam.pvalue.TaggedOutput(tag, elem)
    with self.create_pipeline() as p:
      xy = (p
            | 'Create' >> beam.Create(['x', 'y', 'xy'])
            | beam.FlatMap(tee, 'x', 'y').with_outputs())
      assert_that(xy.x, equal_to(['x', 'xy']), label='x')
      assert_that(xy.y, equal_to(['y', 'xy']), label='y')

  def test_pardo_side_and_main_outputs(self):
    def even_odd(elem):
      yield elem
      yield beam.pvalue.TaggedOutput('odd' if elem % 2 else 'even', elem)
    with self.create_pipeline() as p:
      ints = p | beam.Create([1, 2, 3])
      named = ints | 'named' >> beam.FlatMap(
          even_odd).with_outputs('even', 'odd', main='all')
      assert_that(named.all, equal_to([1, 2, 3]), label='named.all')
      assert_that(named.even, equal_to([2]), label='named.even')
      assert_that(named.odd, equal_to([1, 3]), label='named.odd')

      unnamed = ints | 'unnamed' >> beam.FlatMap(even_odd).with_outputs()
      unnamed[None] | beam.Map(id)  # pylint: disable=expression-not-assigned
      assert_that(unnamed[None], equal_to([1, 2, 3]), label='unnamed.all')
      assert_that(unnamed.even, equal_to([2]), label='unnamed.even')
      assert_that(unnamed.odd, equal_to([1, 3]), label='unnamed.odd')

  def test_pardo_side_inputs(self):
    def cross_product(elem, sides):
      for side in sides:
        yield elem, side
    with self.create_pipeline() as p:
      main = p | 'main' >> beam.Create(['a', 'b', 'c'])
      side = p | 'side' >> beam.Create(['x', 'y'])
      assert_that(main | beam.FlatMap(cross_product, AsList(side)),
                  equal_to([('a', 'x'), ('b', 'x'), ('c', 'x'),
                            ('a', 'y'), ('b', 'y'), ('c', 'y')]))

  def test_pardo_unfusable_side_inputs(self):
    def cross_product(elem, sides):
      for side in sides:
        yield elem, side
    with self.create_pipeline() as p:
      pcoll = p | beam.Create(['a', 'b'])
      assert_that(pcoll | beam.FlatMap(cross_product, AsList(pcoll)),
                  equal_to([('a', 'a'), ('a', 'b'), ('b', 'a'), ('b', 'b')]))

    with self.create_pipeline() as p:
      pcoll = p | beam.Create(['a', 'b'])
      derived = ((pcoll,) | beam.Flatten()
                 | beam.Map(lambda x: (x, x))
                 | beam.GroupByKey()
                 | 'Unkey' >> beam.Map(lambda kv: kv[0]))
      assert_that(
          pcoll | beam.FlatMap(cross_product, AsList(derived)),
          equal_to([('a', 'a'), ('a', 'b'), ('b', 'a'), ('b', 'b')]))

  def test_group_by_key(self):
    with self.create_pipeline() as p:
      res = (p
             | beam.Create([('a', 1), ('a', 2), ('b', 3)])
             | beam.GroupByKey()
             | beam.Map(lambda k_vs: (k_vs[0], sorted(k_vs[1]))))
      assert_that(res, equal_to([('a', [1, 2]), ('b', [3])]))

  def test_flatten(self):
    with self.create_pipeline() as p:
      res = (p | 'a' >> beam.Create(['a']),
             p | 'bc' >> beam.Create(['b', 'c']),
             p | 'd' >> beam.Create(['d'])) | beam.Flatten()
      assert_that(res, equal_to(['a', 'b', 'c', 'd']))

  def test_combine_per_key(self):
    with self.create_pipeline() as p:
      res = (p
             | beam.Create([('a', 1), ('a', 2), ('b', 3)])
             | beam.CombinePerKey(beam.combiners.MeanCombineFn()))
      assert_that(res, equal_to([('a', 1.5), ('b', 3.0)]))

  def test_read(self):
    # Can't use NamedTemporaryFile as a context
    # due to https://bugs.python.org/issue14243
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    try:
      temp_file.write('a\nb\nc')
      temp_file.close()
      with self.create_pipeline() as p:
        assert_that(p | beam.io.ReadFromText(temp_file.name),
                    equal_to(['a', 'b', 'c']))
    finally:
      os.unlink(temp_file.name)

  def test_windowing(self):
    with self.create_pipeline() as p:
      res = (p
             | beam.Create([1, 2, 100, 101, 102])
             | beam.Map(lambda t: TimestampedValue(('k', t), t))
             | beam.WindowInto(beam.transforms.window.Sessions(10))
             | beam.GroupByKey()
             | beam.Map(lambda k_vs1: (k_vs1[0], sorted(k_vs1[1]))))
      assert_that(res, equal_to([('k', [1, 2]), ('k', [100, 101, 102])]))

  def test_errors(self):
    with self.assertRaises(BaseException) as e_cm:
      with self.create_pipeline() as p:
        def raise_error(x):
          raise RuntimeError('x')
        # pylint: disable=expression-not-assigned
        (p
         | beam.Create(['a', 'b'])
         | 'StageA' >> beam.Map(lambda x: x)
         | 'StageB' >> beam.Map(lambda x: x)
         | 'StageC' >> beam.Map(raise_error)
         | 'StageD' >> beam.Map(lambda x: x))
    self.assertIn('StageC', e_cm.exception.args[0])
    self.assertNotIn('StageB', e_cm.exception.args[0])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
