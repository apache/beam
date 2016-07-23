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

"""Unit tests for the PTransform and descendants."""

from __future__ import absolute_import

import operator
import re
import unittest


import apache_beam as beam
from apache_beam.pipeline import Pipeline
import apache_beam.pvalue as pvalue
import apache_beam.transforms.combiners as combine
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.util import assert_that, equal_to
import apache_beam.typehints as typehints
from apache_beam.typehints import with_input_types
from apache_beam.typehints import with_output_types
from apache_beam.typehints.typehints_test import TypeHintTestCase
from apache_beam.utils.options import PipelineOptions
from apache_beam.utils.options import TypeOptions


# Disable frequent lint warning due to pipe operator for chaining transforms.
# pylint: disable=expression-not-assigned


class PTransformTest(unittest.TestCase):

  def assertStartswith(self, msg, prefix):
    self.assertTrue(msg.startswith(prefix),
                    '"%s" does not start with "%s"' % (msg, prefix))

  def test_str(self):
    self.assertEqual('<PTransform(PTransform) label=[PTransform]>',
                     str(PTransform()))

    pa = Pipeline('DirectPipelineRunner')
    res = pa | 'a_label' >> beam.Create([1, 2])
    self.assertEqual('AppliedPTransform(a_label, Create)',
                     str(res.producer))

    pc = Pipeline('DirectPipelineRunner')
    res = pc | beam.Create([1, 2])
    inputs_tr = res.producer.transform
    inputs_tr.inputs = ('ci',)
    self.assertEqual(
        """<Create(PTransform) label=[Create] inputs=('ci',)>""",
        str(inputs_tr))

    pd = Pipeline('DirectPipelineRunner')
    res = pd | beam.Create([1, 2])
    side_tr = res.producer.transform
    side_tr.side_inputs = (4,)
    self.assertEqual(
        '<Create(PTransform) label=[Create] side_inputs=(4,)>',
        str(side_tr))

    inputs_tr.side_inputs = ('cs',)
    self.assertEqual(
        """<Create(PTransform) label=[Create] """
        """inputs=('ci',) side_inputs=('cs',)>""",
        str(inputs_tr))

  def test_parse_label_and_arg(self):

    def fun(*args, **kwargs):
      return PTransform().parse_label_and_arg(args, kwargs, 'name')

    self.assertEqual(('PTransform', 'value'), fun('value'))
    self.assertEqual(('PTransform', 'value'), fun(name='value'))
    self.assertEqual(('label', 'value'), fun('label', 'value'))
    self.assertEqual(('label', 'value'), fun('label', name='value'))
    self.assertEqual(('label', 'value'), fun('value', label='label'))
    self.assertEqual(('label', 'value'), fun(name='value', label='label'))

    self.assertRaises(ValueError, fun)
    self.assertRaises(ValueError, fun, 0, 'value')
    self.assertRaises(ValueError, fun, label=0, name='value')
    self.assertRaises(ValueError, fun, other='value')

    with self.assertRaises(ValueError) as cm:
      fun(0, name='value')
    self.assertEqual(
        cm.exception.message,
        'PTransform expects a (label, name) or (name) argument list '
        'instead of args=(0,), kwargs={\'name\': \'value\'}')

  def test_do_with_do_fn(self):
    class AddNDoFn(beam.DoFn):

      def process(self, context, addon):
        return [context.element + addon]

    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create([1, 2, 3])
    result = pcoll | 'do' >> beam.ParDo(AddNDoFn(), 10)
    assert_that(result, equal_to([11, 12, 13]))
    pipeline.run()

  def test_do_with_unconstructed_do_fn(self):
    class MyDoFn(beam.DoFn):

      def process(self, context):
        pass

    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create([1, 2, 3])
    with self.assertRaises(ValueError):
      pcoll | 'do' >> beam.ParDo(MyDoFn)  # Note the lack of ()'s

  def test_do_with_callable(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create([1, 2, 3])
    result = pcoll | 'do' >> beam.FlatMap(lambda x, addon: [x + addon], 10)
    assert_that(result, equal_to([11, 12, 13]))
    pipeline.run()

  def test_do_with_side_input_as_arg(self):
    pipeline = Pipeline('DirectPipelineRunner')
    side = pipeline | 'side' >> beam.Create([10])
    pcoll = pipeline | 'start' >> beam.Create([1, 2, 3])
    result = pcoll | beam.FlatMap(
        'do', lambda x, addon: [x + addon], pvalue.AsSingleton(side))
    assert_that(result, equal_to([11, 12, 13]))
    pipeline.run()

  def test_do_with_side_input_as_keyword_arg(self):
    pipeline = Pipeline('DirectPipelineRunner')
    side = pipeline | 'side' >> beam.Create([10])
    pcoll = pipeline | 'start' >> beam.Create([1, 2, 3])
    result = pcoll | beam.FlatMap(
        'do', lambda x, addon: [x + addon], addon=pvalue.AsSingleton(side))
    assert_that(result, equal_to([11, 12, 13]))
    pipeline.run()

  def test_do_with_do_fn_returning_string_raises_warning(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create(['2', '9', '3'])
    pcoll | 'do' >> beam.FlatMap(lambda x: x + '1')

    # Since the DoFn directly returns a string we should get an error warning
    # us.
    with self.assertRaises(typehints.TypeCheckError) as cm:
      pipeline.run()

    expected_error_prefix = ('Returning a str from a ParDo or FlatMap '
                             'is discouraged.')
    self.assertStartswith(cm.exception.message, expected_error_prefix)

  def test_do_with_do_fn_returning_dict_raises_warning(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create(['2', '9', '3'])
    pcoll | 'do' >> beam.FlatMap(lambda x: {x: '1'})

    # Since the DoFn directly returns a dict we should get an error warning
    # us.
    with self.assertRaises(typehints.TypeCheckError) as cm:
      pipeline.run()

    expected_error_prefix = ('Returning a dict from a ParDo or FlatMap '
                             'is discouraged.')
    self.assertStartswith(cm.exception.message, expected_error_prefix)

  def test_do_with_side_outputs_maintains_unique_name(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create([1, 2, 3])
    r1 = pcoll | 'a' >> beam.FlatMap(lambda x: [x + 1]).with_outputs(main='m')
    r2 = pcoll | 'b' >> beam.FlatMap(lambda x: [x + 2]).with_outputs(main='m')
    assert_that(r1.m, equal_to([2, 3, 4]), label='r1')
    assert_that(r2.m, equal_to([3, 4, 5]), label='r2')
    pipeline.run()

  def test_do_requires_do_fn_returning_iterable(self):
    # This function is incorrect because it returns an object that isn't an
    # iterable.
    def incorrect_par_do_fn(x):
      return x + 5
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create([2, 9, 3])
    pcoll | 'do' >> beam.FlatMap(incorrect_par_do_fn)
    # It's a requirement that all user-defined functions to a ParDo return
    # an iterable.
    with self.assertRaises(typehints.TypeCheckError) as cm:
      pipeline.run()

    expected_error_prefix = 'FlatMap and ParDo must return an iterable.'
    self.assertStartswith(cm.exception.message, expected_error_prefix)

  def test_do_fn_with_start_finish(self):
    class MyDoFn(beam.DoFn):
      def start_bundle(self, c):
        yield 'start'

      def process(self, c):
        pass

      def finish_bundle(self, c):
        yield 'finish'
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create([1, 2, 3])
    result = pcoll | 'do' >> beam.ParDo(MyDoFn())

    # May have many bundles, but each has a start and finish.
    def  matcher():
      def match(actual):
        equal_to(['start', 'finish'])(list(set(actual)))
        equal_to([actual.count('start')])([actual.count('finish')])
      return match

    assert_that(result, matcher())
    pipeline.run()

  def test_filter(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create([1, 2, 3, 4])
    result = pcoll | beam.Filter(
        'filter', lambda x: x % 2 == 0)
    assert_that(result, equal_to([2, 4]))
    pipeline.run()

  class _MeanCombineFn(beam.CombineFn):

    def create_accumulator(self):
      return (0, 0)

    def add_input(self, (sum_, count), element):
      return sum_ + element, count + 1

    def merge_accumulators(self, accumulators):
      sums, counts = zip(*accumulators)
      return sum(sums), sum(counts)

    def extract_output(self, (sum_, count)):
      if not count:
        return float('nan')
      return sum_ / float(count)

  def test_combine_with_combine_fn(self):
    vals = [1, 2, 3, 4, 5, 6, 7]
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create(vals)
    result = pcoll | 'mean' >> beam.CombineGlobally(self._MeanCombineFn())
    assert_that(result, equal_to([sum(vals) / len(vals)]))
    pipeline.run()

  def test_combine_with_callable(self):
    vals = [1, 2, 3, 4, 5, 6, 7]
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create(vals)
    result = pcoll | beam.CombineGlobally(sum)
    assert_that(result, equal_to([sum(vals)]))
    pipeline.run()

  def test_combine_with_side_input_as_arg(self):
    values = [1, 2, 3, 4, 5, 6, 7]
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create(values)
    divisor = pipeline | 'divisor' >> beam.Create([2])
    result = pcoll | beam.CombineGlobally(
        'max',
        # Multiples of divisor only.
        lambda vals, d: max(v for v in vals if v % d == 0),
        pvalue.AsSingleton(divisor)).without_defaults()
    filt_vals = [v for v in values if v % 2 == 0]
    assert_that(result, equal_to([max(filt_vals)]))
    pipeline.run()

  def test_combine_per_key_with_combine_fn(self):
    vals_1 = [1, 2, 3, 4, 5, 6, 7]
    vals_2 = [2, 4, 6, 8, 10, 12, 14]
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create(([('a', x) for x in vals_1] +
                                               [('b', x) for x in vals_2]))
    result = pcoll | 'mean' >> beam.CombinePerKey(self._MeanCombineFn())
    assert_that(result, equal_to([('a', sum(vals_1) / len(vals_1)),
                                  ('b', sum(vals_2) / len(vals_2))]))
    pipeline.run()

  def test_combine_per_key_with_callable(self):
    vals_1 = [1, 2, 3, 4, 5, 6, 7]
    vals_2 = [2, 4, 6, 8, 10, 12, 14]
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create(([('a', x) for x in vals_1] +
                                               [('b', x) for x in vals_2]))
    result = pcoll | beam.CombinePerKey(sum)
    assert_that(result, equal_to([('a', sum(vals_1)), ('b', sum(vals_2))]))
    pipeline.run()

  def test_combine_per_key_with_side_input_as_arg(self):
    vals_1 = [1, 2, 3, 4, 5, 6, 7]
    vals_2 = [2, 4, 6, 8, 10, 12, 14]
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create(([('a', x) for x in vals_1] +
                                               [('b', x) for x in vals_2]))
    divisor = pipeline | 'divisor' >> beam.Create([2])
    result = pcoll | beam.CombinePerKey(
        lambda vals, d: max(v for v in vals if v % d == 0),
        pvalue.AsSingleton(divisor))  # Multiples of divisor only.
    m_1 = max(v for v in vals_1 if v % 2 == 0)
    m_2 = max(v for v in vals_2 if v % 2 == 0)
    assert_that(result, equal_to([('a', m_1), ('b', m_2)]))
    pipeline.run()

  def test_group_by_key(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | beam.Create(
        'start', [(1, 1), (2, 1), (3, 1), (1, 2), (2, 2), (1, 3)])
    result = pcoll | 'group' >> beam.GroupByKey()
    assert_that(result, equal_to([(1, [1, 2, 3]), (2, [1, 2]), (3, [1])]))
    pipeline.run()

  def test_partition_with_partition_fn(self):

    class SomePartitionFn(beam.PartitionFn):

      def partition_for(self, context, num_partitions, offset):
        return (context.element % 3) + offset

    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create([0, 1, 2, 3, 4, 5, 6, 7, 8])
    # Attempt nominal partition operation.
    partitions = pcoll | 'part1' >> beam.Partition(SomePartitionFn(), 4, 1)
    assert_that(partitions[0], equal_to([]))
    assert_that(partitions[1], equal_to([0, 3, 6]), label='p1')
    assert_that(partitions[2], equal_to([1, 4, 7]), label='p2')
    assert_that(partitions[3], equal_to([2, 5, 8]), label='p3')
    pipeline.run()

    # Check that a bad partition label will yield an error. For the
    # DirectPipelineRunner, this error manifests as an exception.
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create([0, 1, 2, 3, 4, 5, 6, 7, 8])
    partitions = pcoll | 'part2' >> beam.Partition(SomePartitionFn(), 4, 10000)
    with self.assertRaises(ValueError):
      pipeline.run()

  def test_partition_with_callable(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create([0, 1, 2, 3, 4, 5, 6, 7, 8])
    partitions = (
        pcoll | beam.Partition(
            'part',
            lambda e, n, offset: (e % 3) + offset, 4,
            1))
    assert_that(partitions[0], equal_to([]))
    assert_that(partitions[1], equal_to([0, 3, 6]), label='p1')
    assert_that(partitions[2], equal_to([1, 4, 7]), label='p2')
    assert_that(partitions[3], equal_to([2, 5, 8]), label='p3')
    pipeline.run()

  def test_partition_followed_by_flatten_and_groupbykey(self):
    """Regression test for an issue with how partitions are handled."""
    pipeline = Pipeline('DirectPipelineRunner')
    contents = [('aa', 1), ('bb', 2), ('aa', 2)]
    created = pipeline | 'A' >> beam.Create(contents)
    partitioned = created | 'B' >> beam.Partition(lambda x, n: len(x) % n, 3)
    flattened = partitioned | 'C' >> beam.Flatten()
    grouped = flattened | 'D' >> beam.GroupByKey()
    assert_that(grouped, equal_to([('aa', [1, 2]), ('bb', [2])]))
    pipeline.run()

  def test_flatten_pcollections(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll_1 = pipeline | 'start_1' >> beam.Create([0, 1, 2, 3])
    pcoll_2 = pipeline | 'start_2' >> beam.Create([4, 5, 6, 7])
    result = (pcoll_1, pcoll_2) | 'flatten' >> beam.Flatten()
    assert_that(result, equal_to([0, 1, 2, 3, 4, 5, 6, 7]))
    pipeline.run()

  def test_flatten_no_pcollections(self):
    pipeline = Pipeline('DirectPipelineRunner')
    with self.assertRaises(ValueError):
      () | 'pipeline arg missing' >> beam.Flatten()
    result = () | 'empty' >> beam.Flatten(pipeline=pipeline)
    assert_that(result, equal_to([]))
    pipeline.run()

  def test_flatten_pcollections_in_iterable(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll_1 = pipeline | 'start_1' >> beam.Create([0, 1, 2, 3])
    pcoll_2 = pipeline | 'start_2' >> beam.Create([4, 5, 6, 7])
    result = ([pcoll for pcoll in (pcoll_1, pcoll_2)]
              | 'flatten' >> beam.Flatten())
    assert_that(result, equal_to([0, 1, 2, 3, 4, 5, 6, 7]))
    pipeline.run()

  def test_flatten_input_type_must_be_iterable(self):
    # Inputs to flatten *must* be an iterable.
    with self.assertRaises(ValueError):
      4 | 'flatten' >> beam.Flatten()

  def test_flatten_input_type_must_be_iterable_of_pcolls(self):
    # Inputs to flatten *must* be an iterable of PCollections.
    with self.assertRaises(TypeError):
      {'l': 'test'} | 'flatten' >> beam.Flatten()
    with self.assertRaises(TypeError):
      set([1, 2, 3]) | 'flatten' >> beam.Flatten()

  def test_co_group_by_key_on_list(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll_1 = pipeline | beam.Create(
        'start_1', [('a', 1), ('a', 2), ('b', 3), ('c', 4)])
    pcoll_2 = pipeline | beam.Create(
        'start_2', [('a', 5), ('a', 6), ('c', 7), ('c', 8)])
    result = (pcoll_1, pcoll_2) | 'cgbk' >> beam.CoGroupByKey()
    assert_that(result, equal_to([('a', ([1, 2], [5, 6])),
                                  ('b', ([3], [])),
                                  ('c', ([4], [7, 8]))]))
    pipeline.run()

  def test_co_group_by_key_on_iterable(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll_1 = pipeline | beam.Create(
        'start_1', [('a', 1), ('a', 2), ('b', 3), ('c', 4)])
    pcoll_2 = pipeline | beam.Create(
        'start_2', [('a', 5), ('a', 6), ('c', 7), ('c', 8)])
    result = ([pc for pc in (pcoll_1, pcoll_2)]
              | 'cgbk' >> beam.CoGroupByKey())
    assert_that(result, equal_to([('a', ([1, 2], [5, 6])),
                                  ('b', ([3], [])),
                                  ('c', ([4], [7, 8]))]))
    pipeline.run()

  def test_co_group_by_key_on_dict(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll_1 = pipeline | beam.Create(
        'start_1', [('a', 1), ('a', 2), ('b', 3), ('c', 4)])
    pcoll_2 = pipeline | beam.Create(
        'start_2', [('a', 5), ('a', 6), ('c', 7), ('c', 8)])
    result = {'X': pcoll_1, 'Y': pcoll_2} | 'cgbk' >> beam.CoGroupByKey()
    assert_that(result, equal_to([('a', {'X': [1, 2], 'Y': [5, 6]}),
                                  ('b', {'X': [3], 'Y': []}),
                                  ('c', {'X': [4], 'Y': [7, 8]})]))
    pipeline.run()

  def test_group_by_key_input_must_be_kv_pairs(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcolls = pipeline | 'A' >> beam.Create([1, 2, 3, 4, 5])

    with self.assertRaises(typehints.TypeCheckError) as e:
      pcolls | 'D' >> beam.GroupByKey()
      pipeline.run()

    self.assertStartswith(
        e.exception.message,
        'Input type hint violation at D: expected '
        'Tuple[TypeVariable[K], TypeVariable[V]]')

  def test_group_by_key_only_input_must_be_kv_pairs(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcolls = pipeline | 'A' >> beam.Create(['a', 'b', 'f'])
    with self.assertRaises(typehints.TypeCheckError) as cm:
      pcolls | 'D' >> beam.GroupByKeyOnly()
      pipeline.run()

    expected_error_prefix = ('Input type hint violation at D: expected '
                             'Tuple[TypeVariable[K], TypeVariable[V]]')
    self.assertStartswith(cm.exception.message, expected_error_prefix)

  def test_keys_and_values(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | beam.Create(
        'start', [(3, 1), (2, 1), (1, 1), (3, 2), (2, 2), (3, 3)])
    keys = pcoll.apply('keys', beam.Keys())
    vals = pcoll.apply('vals', beam.Values())
    assert_that(keys, equal_to([1, 2, 2, 3, 3, 3]), label='assert:keys')
    assert_that(vals, equal_to([1, 1, 1, 2, 2, 3]), label='assert:vals')
    pipeline.run()

  def test_kv_swap(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | beam.Create(
        'start', [(6, 3), (1, 2), (7, 1), (5, 2), (3, 2)])
    result = pcoll.apply('swap', beam.KvSwap())
    assert_that(result, equal_to([(1, 7), (2, 1), (2, 3), (2, 5), (3, 6)]))
    pipeline.run()

  def test_remove_duplicates(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | beam.Create(
        'start', [6, 3, 1, 1, 9, 'pleat', 'pleat', 'kazoo', 'navel'])
    result = pcoll.apply('nodupes', beam.RemoveDuplicates())
    assert_that(result, equal_to([1, 3, 6, 9, 'pleat', 'kazoo', 'navel']))
    pipeline.run()

  def test_chained_ptransforms(self):
    pipeline = Pipeline('DirectPipelineRunner')
    t = (beam.Map(lambda x: (x, 1))
         | beam.GroupByKey()
         | beam.Map(lambda (x, ones): (x, sum(ones))))
    result = pipeline | 'start' >> beam.Create(['a', 'a', 'b']) | t
    assert_that(result, equal_to([('a', 2), ('b', 1)]))
    pipeline.run()

  def test_apply_to_list(self):
    self.assertItemsEqual(
        [1, 2, 3], [0, 1, 2] | 'add_one' >> beam.Map(lambda x: x + 1))
    self.assertItemsEqual([1],
                          [0, 1, 2] | 'odd' >> beam.Filter(lambda x: x % 2))
    self.assertItemsEqual([1, 2, 100, 3],
                          ([1, 2, 3], [100]) | beam.Flatten())
    join_input = ([('k', 'a')],
                  [('k', 'b'), ('k', 'c')])
    self.assertItemsEqual([('k', (['a'], ['b', 'c']))],
                          join_input | beam.CoGroupByKey())

  def test_multi_input_ptransform(self):
    class DisjointUnion(PTransform):
      def apply(self, pcollections):
        return (pcollections
                | beam.Flatten()
                | beam.Map(lambda x: (x, None))
                | beam.GroupByKey()
                | beam.Map(lambda (x, _): x))
    self.assertEqual([1, 2, 3], sorted(([1, 2], [2, 3]) | DisjointUnion()))

  def test_apply_to_crazy_pvaluish(self):
    class NestedFlatten(PTransform):
      """A PTransform taking and returning nested PValueish.

      Takes as input a list of dicts, and returns a dict with the corresponding
      values flattened.
      """
      def _extract_input_pvalues(self, pvalueish):
        pvalueish = list(pvalueish)
        return pvalueish, sum([list(p.values()) for p in pvalueish], [])

      def apply(self, pcoll_dicts):
        keys = reduce(operator.or_, [set(p.keys()) for p in pcoll_dicts])
        res = {}
        for k in keys:
          res[k] = [p[k] for p in pcoll_dicts if k in p] | beam.Flatten(k)
        return res
    res = [{'a': [1, 2, 3]},
           {'a': [4, 5, 6], 'b': ['x', 'y', 'z']},
           {'a': [7, 8], 'b': ['x', 'y'], 'c': []}] | NestedFlatten()
    self.assertEqual(3, len(res))
    self.assertEqual([1, 2, 3, 4, 5, 6, 7, 8], sorted(res['a']))
    self.assertEqual(['x', 'x', 'y', 'y', 'z'], sorted(res['b']))
    self.assertEqual([], sorted(res['c']))


@beam.ptransform_fn
def SamplePTransform(pcoll):
  """Sample transform using the @ptransform_fn decorator."""
  map_transform = beam.Map('ToPairs', lambda v: (v, None))
  combine_transform = beam.CombinePerKey('Group', lambda vs: None)
  keys_transform = beam.Keys('RemoveDuplicates')
  return pcoll | map_transform | combine_transform | keys_transform


class PTransformLabelsTest(unittest.TestCase):

  class CustomTransform(beam.PTransform):

    pardo = None

    def apply(self, pcoll):
      self.pardo = beam.FlatMap('*do*', lambda x: [x + 1])
      return pcoll | self.pardo

  def test_chained_ptransforms(self):
    """Tests that chaining gets proper nesting."""
    pipeline = Pipeline('DirectPipelineRunner')
    map1 = beam.Map('map1', lambda x: (x, 1))
    gbk = beam.GroupByKey('gbk')
    map2 = beam.Map('map2', lambda (x, ones): (x, sum(ones)))
    t = (map1 | gbk | map2)
    result = pipeline | 'start' >> beam.Create(['a', 'a', 'b']) | t
    self.assertTrue('map1|gbk|map2/map1' in pipeline.applied_labels)
    self.assertTrue('map1|gbk|map2/gbk' in pipeline.applied_labels)
    self.assertTrue('map1|gbk|map2/map2' in pipeline.applied_labels)
    assert_that(result, equal_to([('a', 2), ('b', 1)]))
    pipeline.run()

  def test_apply_custom_transform_without_label(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'pcoll' >> beam.Create([1, 2, 3])
    custom = PTransformLabelsTest.CustomTransform()
    result = pipeline.apply(custom, pcoll)
    self.assertTrue('CustomTransform' in pipeline.applied_labels)
    self.assertTrue('CustomTransform/*do*' in pipeline.applied_labels)
    assert_that(result, equal_to([2, 3, 4]))
    pipeline.run()

  def test_apply_custom_transform_with_label(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'pcoll' >> beam.Create([1, 2, 3])
    custom = PTransformLabelsTest.CustomTransform('*custom*')
    result = pipeline.apply(custom, pcoll)
    self.assertTrue('*custom*' in pipeline.applied_labels)
    self.assertTrue('*custom*/*do*' in pipeline.applied_labels)
    assert_that(result, equal_to([2, 3, 4]))
    pipeline.run()

  def test_combine_without_label(self):
    vals = [1, 2, 3, 4, 5, 6, 7]
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create(vals)
    combine = beam.CombineGlobally(sum)
    result = pcoll | combine
    self.assertTrue('CombineGlobally(sum)' in pipeline.applied_labels)
    assert_that(result, equal_to([sum(vals)]))
    pipeline.run()

  def test_apply_ptransform_using_decorator(self):
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'pcoll' >> beam.Create([1, 2, 3])
    sample = SamplePTransform('*sample*')
    _ = pcoll | sample
    self.assertTrue('*sample*' in pipeline.applied_labels)
    self.assertTrue('*sample*/ToPairs' in pipeline.applied_labels)
    self.assertTrue('*sample*/Group' in pipeline.applied_labels)
    self.assertTrue('*sample*/RemoveDuplicates' in pipeline.applied_labels)

  def test_combine_with_label(self):
    vals = [1, 2, 3, 4, 5, 6, 7]
    pipeline = Pipeline('DirectPipelineRunner')
    pcoll = pipeline | 'start' >> beam.Create(vals)
    combine = beam.CombineGlobally('*sum*', sum)
    result = pcoll | combine
    self.assertTrue('*sum*' in pipeline.applied_labels)
    assert_that(result, equal_to([sum(vals)]))
    pipeline.run()

  def check_label(self, ptransform, expected_label):
    pipeline = Pipeline('DirectPipelineRunner')
    pipeline | 'start' >> beam.Create([('a', 1)]) | ptransform
    actual_label = sorted(pipeline.applied_labels - {'start'})[0]
    self.assertEqual(expected_label, re.sub(r'\d{3,}', '#', actual_label))

  def test_default_labels(self):
    self.check_label(beam.Map(len), r'Map(len)')
    self.check_label(beam.Map(lambda x: x),
                     r'Map(<lambda at ptransform_test.py:#>)')
    self.check_label(beam.FlatMap(list), r'FlatMap(list)')
    self.check_label(beam.Filter(sum), r'Filter(sum)')
    self.check_label(beam.CombineGlobally(sum), r'CombineGlobally(sum)')
    self.check_label(beam.CombinePerKey(sum), r'CombinePerKey(sum)')

    class MyDoFn(beam.DoFn):
      def process(self, context):
        pass

    self.check_label(beam.ParDo(MyDoFn()), r'ParDo(MyDoFn)')


class PTransformTypeCheckTestCase(TypeHintTestCase):

  def assertStartswith(self, msg, prefix):
    self.assertTrue(msg.startswith(prefix),
                    '"%s" does not start with "%s"' % (msg, prefix))

  def setUp(self):
    self.p = Pipeline(options=PipelineOptions([]))

  def test_do_fn_pipeline_pipeline_type_check_satisfied(self):
    @with_input_types(int, int)
    @with_output_types(typehints.List[int])
    class AddWithFive(beam.DoFn):
      def process(self, context, five):
        return [context.element + five]

    d = (self.p
         | 't' >> beam.Create([1, 2, 3]).with_output_types(int)
         | 'add' >> beam.ParDo(AddWithFive(), 5))

    assert_that(d, equal_to([6, 7, 8]))
    self.p.run()

  def test_do_fn_pipeline_pipeline_type_check_violated(self):
    @with_input_types(str, str)
    @with_output_types(typehints.List[str])
    class ToUpperCaseWithPrefix(beam.DoFn):
      def process(self, context, prefix):
        return [prefix + context.element.upper()]

    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | 't' >> beam.Create([1, 2, 3]).with_output_types(int)
       | 'upper' >> beam.ParDo(ToUpperCaseWithPrefix(), 'hello'))

    self.assertEqual("Type hint violation for 'upper': "
                     "requires <type 'str'> but got <type 'int'> for context",
                     e.exception.message)

  def test_do_fn_pipeline_runtime_type_check_satisfied(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    @with_input_types(int, int)
    @with_output_types(int)
    class AddWithNum(beam.DoFn):
      def process(self, context, num):
        return [context.element + num]

    d = (self.p
         | 't' >> beam.Create([1, 2, 3]).with_output_types(int)
         | 'add' >> beam.ParDo(AddWithNum(), 5))

    assert_that(d, equal_to([6, 7, 8]))
    self.p.run()

  def test_do_fn_pipeline_runtime_type_check_violated(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    @with_input_types(int, int)
    @with_output_types(typehints.List[int])
    class AddWithNum(beam.DoFn):
      def process(self, context, num):
        return [context.element + num]

    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | 't' >> beam.Create(['1', '2', '3']).with_output_types(str)
       | 'add' >> beam.ParDo(AddWithNum(), 5))
      self.p.run()

    self.assertEqual("Type hint violation for 'add': "
                     "requires <type 'int'> but got <type 'str'> for context",
                     e.exception.message)

  def test_pardo_does_not_type_check_using_type_hint_decorators(self):
    @with_input_types(a=int)
    @with_output_types(typehints.List[str])
    def int_to_str(a):
      return [str(a)]

    # The function above is expecting an int for its only parameter. However, it
    # will receive a str instead, which should result in a raised exception.
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | 's' >> beam.Create(['b', 'a', 'r']).with_output_types(str)
       | 'to str' >> beam.FlatMap(int_to_str))

    self.assertEqual("Type hint violation for 'to str': "
                     "requires <type 'int'> but got <type 'str'> for a",
                     e.exception.message)

  def test_pardo_properly_type_checks_using_type_hint_decorators(self):
    @with_input_types(a=str)
    @with_output_types(typehints.List[str])
    def to_all_upper_case(a):
      return [a.upper()]

    # If this type-checks than no error should be raised.
    d = (self.p
         | 't' >> beam.Create(['t', 'e', 's', 't']).with_output_types(str)
         | 'case' >> beam.FlatMap(to_all_upper_case))
    assert_that(d, equal_to(['T', 'E', 'S', 'T']))
    self.p.run()

    # Output type should have been recognized as 'str' rather than List[str] to
    # do the flatten part of FlatMap.
    self.assertEqual(str, d.element_type)

  def test_pardo_does_not_type_check_using_type_hint_methods(self):
    # The first ParDo outputs pcoll's of type int, however the second ParDo is
    # expecting pcoll's of type str instead.
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | 's' >> beam.Create(['t', 'e', 's', 't']).with_output_types(str)
       | ('score' >> beam.FlatMap(lambda x: [1] if x == 't' else [2])
          .with_input_types(str).with_output_types(int))
       | ('upper' >> beam.FlatMap(lambda x: [x.upper()])
          .with_input_types(str).with_output_types(str)))

    self.assertEqual("Type hint violation for 'upper': "
                     "requires <type 'str'> but got <type 'int'> for x",
                     e.exception.message)

  def test_pardo_properly_type_checks_using_type_hint_methods(self):
    # Pipeline should be created successfully without an error
    d = (self.p
         | 's' >> beam.Create(['t', 'e', 's', 't']).with_output_types(str)
         | 'dup' >> beam.FlatMap(lambda x: [x + x])
         .with_input_types(str).with_output_types(str)
         | 'upper' >> beam.FlatMap(lambda x: [x.upper()])
         .with_input_types(str).with_output_types(str))

    assert_that(d, equal_to(['TT', 'EE', 'SS', 'TT']))
    self.p.run()

  def test_map_does_not_type_check_using_type_hints_methods(self):
    # The transform before 'Map' has indicated that it outputs PCollections with
    # int's, while Map is expecting one of str.
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | 's' >> beam.Create([1, 2, 3, 4]).with_output_types(int)
       | 'upper' >> beam.Map(lambda x: x.upper())
       .with_input_types(str).with_output_types(str))

    self.assertEqual("Type hint violation for 'upper': "
                     "requires <type 'str'> but got <type 'int'> for x",
                     e.exception.message)

  def test_map_properly_type_checks_using_type_hints_methods(self):
    # No error should be raised if this type-checks properly.
    d = (self.p
         | 's' >> beam.Create([1, 2, 3, 4]).with_output_types(int)
         | 'to_str' >> beam.Map(lambda x: str(x))
         .with_input_types(int).with_output_types(str))
    assert_that(d, equal_to(['1', '2', '3', '4']))
    self.p.run()

  def test_map_does_not_type_check_using_type_hints_decorator(self):
    @with_input_types(s=str)
    @with_output_types(str)
    def upper(s):
      return s.upper()

    # Hinted function above expects a str at pipeline construction.
    # However, 'Map' should detect that Create has hinted an int instead.
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | 's' >> beam.Create([1, 2, 3, 4]).with_output_types(int)
       | 'upper' >> beam.Map(upper))

    self.assertEqual("Type hint violation for 'upper': "
                     "requires <type 'str'> but got <type 'int'> for s",
                     e.exception.message)

  def test_map_properly_type_checks_using_type_hints_decorator(self):
    @with_input_types(a=bool)
    @with_output_types(int)
    def bool_to_int(a):
      return int(a)

    # If this type-checks than no error should be raised.
    d = (self.p
         | 'bools' >> beam.Create([True, False, True]).with_output_types(bool)
         | 'to_ints' >> beam.Map(bool_to_int))
    assert_that(d, equal_to([1, 0, 1]))
    self.p.run()

  def test_filter_does_not_type_check_using_type_hints_method(self):
    # Filter is expecting an int but instead looks to the 'left' and sees a str
    # incoming.
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | 'strs' >> beam.Create(['1', '2', '3', '4', '5']).with_output_types(str)
       | 'lower' >> beam.Map(lambda x: x.lower())
       .with_input_types(str).with_output_types(str)
       | 'below 3' >> beam.Filter(lambda x: x < 3).with_input_types(int))

    self.assertEqual("Type hint violation for 'below 3': "
                     "requires <type 'int'> but got <type 'str'> for x",
                     e.exception.message)

  def test_filter_type_checks_using_type_hints_method(self):
    # No error should be raised if this type-checks properly.
    d = (self.p
         | beam.Create(['1', '2', '3', '4', '5']).with_output_types(str)
         | 'to int' >> beam.Map(lambda x: int(x))
         .with_input_types(str).with_output_types(int)
         | 'below 3' >> beam.Filter(lambda x: x < 3).with_input_types(int))
    assert_that(d, equal_to([1, 2]))
    self.p.run()

  def test_filter_does_not_type_check_using_type_hints_decorator(self):
    @with_input_types(a=float)
    def more_than_half(a):
      return a > 0.50

    # Func above was hinted to only take a float, yet an int will be passed.
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | 'ints' >> beam.Create([1, 2, 3, 4]).with_output_types(int)
       | 'half' >> beam.Filter(more_than_half))

    self.assertEqual("Type hint violation for 'half': "
                     "requires <type 'float'> but got <type 'int'> for a",
                     e.exception.message)

  def test_filter_type_checks_using_type_hints_decorator(self):
    @with_input_types(b=int)
    def half(b):
      import random
      return bool(random.choice([0, 1]))

    # Filter should deduce that it returns the same type that it takes.
    (self.p
     | 'str' >> beam.Create(range(5)).with_output_types(int)
     | 'half' >> beam.Filter(half)
     | 'to bool' >> beam.Map(lambda x: bool(x))
     .with_input_types(int).with_output_types(bool))

  def test_group_by_key_only_output_type_deduction(self):
    d = (self.p
         | 'str' >> beam.Create(['t', 'e', 's', 't']).with_output_types(str)
         | ('pair' >> beam.Map(lambda x: (x, ord(x)))
            .with_output_types(typehints.KV[str, str]))
         | beam.GroupByKeyOnly())

    # Output type should correctly be deduced.
    # GBK-only should deduce that KV[A, B] is turned into KV[A, Iterable[B]].
    self.assertCompatible(typehints.KV[str, typehints.Iterable[str]],
                          d.element_type)

  def test_group_by_key_output_type_deduction(self):
    d = (self.p
         | 'str' >> beam.Create(range(20)).with_output_types(int)
         | ('pair negative' >> beam.Map(lambda x: (x % 5, -x))
            .with_output_types(typehints.KV[int, int]))
         | beam.GroupByKey())

    # Output type should correctly be deduced.
    # GBK should deduce that KV[A, B] is turned into KV[A, Iterable[B]].
    self.assertCompatible(typehints.KV[int, typehints.Iterable[int]],
                          d.element_type)

  def test_group_by_key_only_does_not_type_check(self):
    # GBK will be passed raw int's here instead of some form of KV[A, B].
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | beam.Create([1, 2, 3]).with_output_types(int)
       | 'F' >> beam.GroupByKeyOnly())

    self.assertEqual("Input type hint violation at F: "
                     "expected Tuple[TypeVariable[K], TypeVariable[V]], "
                     "got <type 'int'>",
                     e.exception.message)

  def test_group_by_does_not_type_check(self):
    # Create is returning a List[int, str], rather than a KV[int, str] that is
    # aliased to Tuple[int, str].
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | (beam.Create(range(5))
          .with_output_types(typehints.Iterable[int]))
       | 'T' >> beam.GroupByKey())

    self.assertEqual("Input type hint violation at T: "
                     "expected Tuple[TypeVariable[K], TypeVariable[V]], "
                     "got Iterable[int]",
                     e.exception.message)

  def test_pipeline_checking_pardo_insufficient_type_information(self):
    self.p.options.view_as(TypeOptions).type_check_strictness = 'ALL_REQUIRED'

    # Type checking is enabled, but 'Create' doesn't pass on any relevant type
    # information to the ParDo.
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | 'nums' >> beam.Create(range(5))
       | 'mod dup' >> beam.FlatMap(lambda x: (x % 2, x)))

    self.assertEqual('Pipeline type checking is enabled, however no output '
                     'type-hint was found for the PTransform Create(nums)',
                     e.exception.message)

  def test_pipeline_checking_gbk_insufficient_type_information(self):
    self.p.options.view_as(TypeOptions).type_check_strictness = 'ALL_REQUIRED'
    # Type checking is enabled, but 'Map' doesn't pass on any relevant type
    # information to GBK-only.
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | 'nums' >> beam.Create(range(5)).with_output_types(int)
       | 'mod dup' >> beam.Map(lambda x: (x % 2, x))
       | beam.GroupByKeyOnly())

    self.assertEqual('Pipeline type checking is enabled, however no output '
                     'type-hint was found for the PTransform '
                     'ParDo(mod dup)',
                     e.exception.message)

  def test_disable_pipeline_type_check(self):
    self.p.options.view_as(TypeOptions).pipeline_type_check = False

    # The pipeline below should raise a TypeError, however pipeline type
    # checking was disabled above.
    (self.p
     | 't' >> beam.Create([1, 2, 3]).with_output_types(int)
     | 'lower' >> beam.Map(lambda x: x.lower())
     .with_input_types(str).with_output_types(str))

  def test_run_time_type_checking_enabled_type_violation(self):
    self.p.options.view_as(TypeOptions).pipeline_type_check = False
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    @with_output_types(str)
    @with_input_types(x=int)
    def int_to_string(x):
      return str(x)

    # Function above has been type-hinted to only accept an int. But in the
    # pipeline execution it'll be passed a string due to the output of Create.
    (self.p
     | 't' >> beam.Create(['some_string'])
     | 'to str' >> beam.Map(int_to_string))
    with self.assertRaises(typehints.TypeCheckError) as e:
      self.p.run()

    self.assertStartswith(
        e.exception.message,
        "Runtime type violation detected within ParDo(to str): "
        "Type-hint for argument: 'x' violated. "
        "Expected an instance of <type 'int'>, "
        "instead found some_string, an instance of <type 'str'>.")

  def test_run_time_type_checking_enabled_types_satisfied(self):
    self.p.options.view_as(TypeOptions).pipeline_type_check = False
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    @with_output_types(typehints.KV[int, str])
    @with_input_types(x=str)
    def group_with_upper_ord(x):
      return (ord(x.upper()) % 5, x)

    # Pipeline checking is off, but the above function should satisfy types at
    # run-time.
    result = (self.p
              | 't' >> beam.Create(['t', 'e', 's', 't', 'i', 'n', 'g'])
              .with_output_types(str)
              | 'gen keys' >> beam.Map(group_with_upper_ord)
              | 'O' >> beam.GroupByKey())

    assert_that(result, equal_to([(1, ['g']),
                                  (3, ['s', 'i', 'n']),
                                  (4, ['t', 'e', 't'])]))
    self.p.run()

  def test_pipeline_checking_satisfied_but_run_time_types_violate(self):
    self.p.options.view_as(TypeOptions).pipeline_type_check = False
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    @with_output_types(typehints.KV[bool, int])
    @with_input_types(a=int)
    def is_even_as_key(a):
      # Simulate a programming error, should be: return (a % 2 == 0, a)
      # However this returns KV[int, int]
      return (a % 2, a)

    (self.p
     | 'nums' >> beam.Create(range(5)).with_output_types(int)
     | 'is even' >> beam.Map(is_even_as_key)
     | 'parity' >> beam.GroupByKey())

    # Although all the types appear to be correct when checked at pipeline
    # construction. Runtime type-checking should detect the 'is_even_as_key' is
    # returning Tuple[int, int], instead of Tuple[bool, int].
    with self.assertRaises(typehints.TypeCheckError) as e:
      self.p.run()

    self.assertStartswith(
        e.exception.message,
        "Runtime type violation detected within ParDo(is even): "
        "Tuple[bool, int] hint type-constraint violated. "
        "The type of element #0 in the passed tuple is incorrect. "
        "Expected an instance of type bool, "
        "instead received an instance of type int.")

  def test_pipeline_checking_satisfied_run_time_checking_satisfied(self):
    self.p.options.view_as(TypeOptions).pipeline_type_check = False

    @with_output_types(typehints.KV[bool, int])
    @with_input_types(a=int)
    def is_even_as_key(a):
      # The programming error in the above test-case has now been fixed.
      # Everything should properly type-check.
      return (a % 2 == 0, a)

    result = (self.p
              | 'nums' >> beam.Create(range(5)).with_output_types(int)
              | 'is even' >> beam.Map(is_even_as_key)
              | 'parity' >> beam.GroupByKey())

    assert_that(result, equal_to([(False, [1, 3]), (True, [0, 2, 4])]))
    self.p.run()

  def test_pipeline_runtime_checking_violation_simple_type_input(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True
    self.p.options.view_as(TypeOptions).pipeline_type_check = False

    # The type-hinted applied via the 'with_input_types()' method indicates the
    # ParDo should receive an instance of type 'str', however an 'int' will be
    # passed instead.
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | beam.Create([1, 2, 3])
       | ('to int' >> beam.FlatMap(lambda x: [int(x)])
          .with_input_types(str).with_output_types(int)))
      self.p.run()

    self.assertStartswith(
        e.exception.message,
        "Runtime type violation detected within ParDo(to int): "
        "Type-hint for argument: 'x' violated. "
        "Expected an instance of <type 'str'>, "
        "instead found 1, an instance of <type 'int'>.")

  def test_pipeline_runtime_checking_violation_composite_type_input(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True
    self.p.options.view_as(TypeOptions).pipeline_type_check = False

    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | beam.Create([(1, 3.0), (2, 4.9), (3, 9.5)])
       | ('add' >> beam.FlatMap(lambda (x, y): [x + y])
          .with_input_types(typehints.Tuple[int, int]).with_output_types(int))
      )
      self.p.run()

    self.assertStartswith(
        e.exception.message,
        "Runtime type violation detected within ParDo(add): "
        "Type-hint for argument: 'y' violated. "
        "Expected an instance of <type 'int'>, "
        "instead found 3.0, an instance of <type 'float'>.")

  def test_pipeline_runtime_checking_violation_simple_type_output(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True
    self.p.options.view_as(TypeOptions).pipeline_type_check = False

    # The type-hinted applied via the 'returns()' method indicates the ParDo
    # should output an instance of type 'int', however a 'float' will be
    # generated instead.
    print "HINTS", beam.FlatMap(
        'to int',
        lambda x: [float(x)]).with_input_types(int).with_output_types(
            int).get_type_hints()
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | beam.Create([1, 2, 3])
       | ('to int' >> beam.FlatMap(lambda x: [float(x)])
          .with_input_types(int).with_output_types(int))
      )
      self.p.run()

    self.assertStartswith(
        e.exception.message,
        "Runtime type violation detected within "
        "ParDo(to int): "
        "According to type-hint expected output should be "
        "of type <type 'int'>. Instead, received '1.0', "
        "an instance of type <type 'float'>.")

  def test_pipeline_runtime_checking_violation_composite_type_output(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True
    self.p.options.view_as(TypeOptions).pipeline_type_check = False

    # The type-hinted applied via the 'returns()' method indicates the ParDo
    # should return an instance of type: Tuple[float, int]. However, an instance
    # of 'int' will be generated instead.
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | beam.Create([(1, 3.0), (2, 4.9), (3, 9.5)])
       | ('swap' >> beam.FlatMap(lambda (x, y): [x + y])
          .with_input_types(typehints.Tuple[int, float])
          .with_output_types(typehints.Tuple[float, int]))
      )
      self.p.run()

    self.assertStartswith(
        e.exception.message,
        "Runtime type violation detected within "
        "ParDo(swap): Tuple type constraint violated. "
        "Valid object instance must be of type 'tuple'. Instead, "
        "an instance of 'float' was received.")

  def test_pipline_runtime_checking_violation_with_side_inputs_decorator(self):
    self.p.options.view_as(TypeOptions).pipeline_type_check = False
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    @with_output_types(int)
    @with_input_types(a=int, b=int)
    def add(a, b):
      return a + b

    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p | beam.Create([1, 2, 3, 4]) | 'add 1' >> beam.Map(add, 1.0))
      self.p.run()

    self.assertStartswith(
        e.exception.message,
        "Runtime type violation detected within ParDo(add 1): "
        "Type-hint for argument: 'b' violated. "
        "Expected an instance of <type 'int'>, "
        "instead found 1.0, an instance of <type 'float'>.")

  def test_pipline_runtime_checking_violation_with_side_inputs_via_method(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True
    self.p.options.view_as(TypeOptions).pipeline_type_check = False

    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | beam.Create([1, 2, 3, 4])
       | ('add 1' >> beam.Map(lambda x, one: x + one, 1.0)
          .with_input_types(int, int)
          .with_output_types(float)))
      self.p.run()

    self.assertStartswith(
        e.exception.message,
        "Runtime type violation detected within ParDo(add 1): "
        "Type-hint for argument: 'one' violated. "
        "Expected an instance of <type 'int'>, "
        "instead found 1.0, an instance of <type 'float'>.")

  def test_combine_properly_pipeline_type_checks_using_decorator(self):
    @with_output_types(int)
    @with_input_types(ints=typehints.Iterable[int])
    def sum_ints(ints):
      return sum(ints)

    d = (self.p
         | 't' >> beam.Create([1, 2, 3]).with_output_types(int)
         | 'sum' >> beam.CombineGlobally(sum_ints))

    self.assertEqual(int, d.element_type)
    assert_that(d, equal_to([6]))
    self.p.run()

  def test_combine_func_type_hint_does_not_take_iterable_using_decorator(self):
    @with_output_types(int)
    @with_input_types(a=int)
    def bad_combine(a):
      5 + a

    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | 'm' >> beam.Create([1, 2, 3]).with_output_types(int)
       | 'add' >> beam.CombineGlobally(bad_combine))

    self.assertEqual(
        "All functions for a Combine PTransform must accept a "
        "single argument compatible with: Iterable[Any]. "
        "Instead a function with input type: <type 'int'> was received.",
        e.exception.message)

  def test_combine_pipeline_type_propagation_using_decorators(self):
    @with_output_types(int)
    @with_input_types(ints=typehints.Iterable[int])
    def sum_ints(ints):
      return sum(ints)

    @with_output_types(typehints.List[int])
    @with_input_types(n=int)
    def range_from_zero(n):
      return list(range(n+1))

    d = (self.p
         | 't' >> beam.Create([1, 2, 3]).with_output_types(int)
         | 'sum' >> beam.CombineGlobally(sum_ints)
         | 'range' >> beam.ParDo(range_from_zero))

    self.assertEqual(int, d.element_type)
    assert_that(d, equal_to([0, 1, 2, 3, 4, 5, 6]))
    self.p.run()

  def test_combine_runtime_type_check_satisfied_using_decorators(self):
    self.p.options.view_as(TypeOptions).pipeline_type_check = False

    @with_output_types(int)
    @with_input_types(ints=typehints.Iterable[int])
    def iter_mul(ints):
      return reduce(operator.mul, ints, 1)

    d = (self.p
         | 'k' >> beam.Create([5, 5, 5, 5]).with_output_types(int)
         | 'mul' >> beam.CombineGlobally(iter_mul))

    assert_that(d, equal_to([625]))
    self.p.run()

  def test_combine_runtime_type_check_violation_using_decorators(self):
    self.p.options.view_as(TypeOptions).pipeline_type_check = False
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    # Combine fn is returning the incorrect type
    @with_output_types(int)
    @with_input_types(ints=typehints.Iterable[int])
    def iter_mul(ints):
      return str(reduce(operator.mul, ints, 1))

    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | 'k' >> beam.Create([5, 5, 5, 5]).with_output_types(int)
       | 'mul' >> beam.CombineGlobally(iter_mul))
      self.p.run()

    self.assertStartswith(
        e.exception.message,
        "Runtime type violation detected within "
        "ParDo(mul/CombinePerKey/Combine/ParDo(CombineValuesDoFn)): "
        "Tuple[TypeVariable[K], int] hint type-constraint violated. "
        "The type of element #1 in the passed tuple is incorrect. "
        "Expected an instance of type int, "
        "instead received an instance of type str.")

  def test_combine_pipeline_type_check_using_methods(self):
    d = (self.p
         | beam.Create(['t', 'e', 's', 't']).with_output_types(str)
         | ('concat' >> beam.CombineGlobally(lambda s: ''.join(s))
            .with_input_types(str).with_output_types(str)))

    def matcher(expected):
      def match(actual):
        equal_to(expected)(list(actual[0]))
      return match
    assert_that(d, matcher('estt'))
    self.p.run()

  def test_combine_runtime_type_check_using_methods(self):
    self.p.options.view_as(TypeOptions).pipeline_type_check = False
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    d = (self.p
         | beam.Create(range(5)).with_output_types(int)
         | ('sum' >> beam.CombineGlobally(lambda s: sum(s))
            .with_input_types(int).with_output_types(int)))

    assert_that(d, equal_to([10]))
    self.p.run()

  def test_combine_pipeline_type_check_violation_using_methods(self):
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | beam.Create(range(3)).with_output_types(int)
       | ('sort join' >> beam.CombineGlobally(lambda s: ''.join(sorted(s)))
          .with_input_types(str).with_output_types(str)))

    self.assertEqual("Input type hint violation at sort join: "
                     "expected <type 'str'>, got <type 'int'>",
                     e.exception.message)

  def test_combine_runtime_type_check_violation_using_methods(self):
    self.p.options.view_as(TypeOptions).pipeline_type_check = False
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | beam.Create(range(3)).with_output_types(int)
       | ('sort join' >> beam.CombineGlobally(lambda s: ''.join(sorted(s)))
          .with_input_types(str).with_output_types(str)))
      self.p.run()

    self.assertStartswith(
        e.exception.message,
        "Runtime type violation detected within "
        "ParDo(sort join/KeyWithVoid): "
        "Type-hint for argument: 'v' violated. "
        "Expected an instance of <type 'str'>, "
        "instead found 0, an instance of <type 'int'>.")

  def test_combine_insufficient_type_hint_information(self):
    self.p.options.view_as(TypeOptions).type_check_strictness = 'ALL_REQUIRED'

    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | 'e' >> beam.Create(range(3)).with_output_types(int)
       | 'sort join' >> beam.CombineGlobally(lambda s: ''.join(sorted(s)))
       | 'f' >> beam.Map(lambda x: x + 1))

    self.assertEqual(
        'Pipeline type checking is enabled, '
        'however no output type-hint was found for the PTransform '
        'ParDo(sort join/CombinePerKey/Combine/ParDo(CombineValuesDoFn))',
        e.exception.message)

  def test_mean_globally_pipeline_checking_satisfied(self):
    d = (self.p
         | 'c' >> beam.Create(range(5)).with_output_types(int)
         | 'mean' >> combine.Mean.Globally())

    self.assertTrue(d.element_type is float)
    assert_that(d, equal_to([2.0]))
    self.p.run()

  def test_mean_globally_pipeline_checking_violated(self):
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | 'c' >> beam.Create(['test']).with_output_types(str)
       | 'mean' >> combine.Mean.Globally())

    self.assertEqual("Type hint violation for 'ParDo(CombineValuesDoFn)': "
                     "requires Tuple[TypeVariable[K], "
                     "Iterable[Union[float, int, long]]] "
                     "but got Tuple[None, Iterable[str]] for p_context",
                     e.exception.message)

  def test_mean_globally_runtime_checking_satisfied(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    d = (self.p
         | 'c' >> beam.Create(range(5)).with_output_types(int)
         | 'mean' >> combine.Mean.Globally())

    self.assertTrue(d.element_type is float)
    assert_that(d, equal_to([2.0]))
    self.p.run()

  def test_mean_globally_runtime_checking_violated(self):
    self.p.options.view_as(TypeOptions).pipeline_type_check = False
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | 'c' >> beam.Create(['t', 'e', 's', 't']).with_output_types(str)
       | 'mean' >> combine.Mean.Globally())
      self.p.run()
      self.assertEqual("Runtime type violation detected for transform input "
                       "when executing ParDoFlatMap(Combine): Tuple[Any, "
                       "Iterable[Union[int, float]]] hint type-constraint "
                       "violated. The type of element #1 in the passed tuple "
                       "is incorrect. Iterable[Union[int, float]] hint "
                       "type-constraint violated. The type of element #0 in "
                       "the passed Iterable is incorrect: Union[int, float] "
                       "type-constraint violated. Expected an instance of one "
                       "of: ('int', 'float'), received str instead.",
                       e.exception.message)

  def test_mean_per_key_pipeline_checking_satisfied(self):
    d = (self.p
         | beam.Create(range(5)).with_output_types(int)
         | ('even group' >> beam.Map(lambda x: (not x % 2, x))
            .with_output_types(typehints.KV[bool, int]))
         | 'even mean' >> combine.Mean.PerKey())

    self.assertCompatible(typehints.KV[bool, float], d.element_type)
    assert_that(d, equal_to([(False, 2.0), (True, 2.0)]))
    self.p.run()

  def test_mean_per_key_pipeline_checking_violated(self):
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | beam.Create(map(str, range(5))).with_output_types(str)
       | ('upper pair' >> beam.Map(lambda x: (x.upper(), x))
          .with_output_types(typehints.KV[str, str]))
       | 'even mean' >> combine.Mean.PerKey())
      self.p.run()

    self.assertEqual("Type hint violation for 'ParDo(CombineValuesDoFn)': "
                     "requires Tuple[TypeVariable[K], "
                     "Iterable[Union[float, int, long]]] "
                     "but got Tuple[str, Iterable[str]] for p_context",
                     e.exception.message)

  def test_mean_per_key_runtime_checking_satisfied(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    d = (self.p
         | beam.Create(range(5)).with_output_types(int)
         | ('odd group' >> beam.Map(lambda x: (bool(x % 2), x))
            .with_output_types(typehints.KV[bool, int]))
         | 'odd mean' >> combine.Mean.PerKey())

    self.assertCompatible(typehints.KV[bool, float], d.element_type)
    assert_that(d, equal_to([(False, 2.0), (True, 2.0)]))
    self.p.run()

  def test_mean_per_key_runtime_checking_violated(self):
    self.p.options.view_as(TypeOptions).pipeline_type_check = False
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | beam.Create(range(5)).with_output_types(int)
       | ('odd group' >> beam.Map(lambda x: (x, str(bool(x % 2))))
          .with_output_types(typehints.KV[int, str]))
       | 'odd mean' >> combine.Mean.PerKey())
      self.p.run()

    self.assertStartswith(
        e.exception.message,
        "Runtime type violation detected within "
        "ParDo(odd mean/CombinePerKey(MeanCombineFn)/"
        "Combine/ParDo(CombineValuesDoFn)): "
        "Type-hint for argument: 'p_context' violated: "
        "Tuple[TypeVariable[K], Iterable[Union[float, int, long]]]"
        " hint type-constraint violated. "
        "The type of element #1 in the passed tuple is incorrect. "
        "Iterable[Union[float, int, long]] "
        "hint type-constraint violated. The type of element #0 "
        "in the passed Iterable is incorrect: "
        "Union[float, int, long] type-constraint violated. "
        "Expected an instance of one of: "
        "('float', 'int', 'long'), received str instead.")

  def test_count_globally_pipeline_type_checking_satisfied(self):
    d = (self.p
         | 'p' >> beam.Create(range(5)).with_output_types(int)
         | 'count int' >> combine.Count.Globally())

    self.assertTrue(d.element_type is int)
    assert_that(d, equal_to([5]))
    self.p.run()

  def test_count_globally_runtime_type_checking_satisfied(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    d = (self.p
         | 'p' >> beam.Create(range(5)).with_output_types(int)
         | 'count int' >> combine.Count.Globally())

    self.assertTrue(d.element_type is int)
    assert_that(d, equal_to([5]))
    self.p.run()

  def test_count_perkey_pipeline_type_checking_satisfied(self):
    d = (self.p
         | beam.Create(range(5)).with_output_types(int)
         | ('even group' >> beam.Map(lambda x: (not x % 2, x))
            .with_output_types(typehints.KV[bool, int]))
         | 'count int' >> combine.Count.PerKey())

    self.assertCompatible(typehints.KV[bool, int], d.element_type)
    assert_that(d, equal_to([(False, 2), (True, 3)]))
    self.p.run()

  def test_count_perkey_pipeline_type_checking_violated(self):
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | beam.Create(range(5)).with_output_types(int)
       | 'count int' >> combine.Count.PerKey())

    self.assertEqual("Input type hint violation at GroupByKey: "
                     "expected Tuple[TypeVariable[K], TypeVariable[V]], "
                     "got <type 'int'>",
                     e.exception.message)

  def test_count_perkey_runtime_type_checking_satisfied(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    d = (self.p
         | beam.Create(['t', 'e', 's', 't']).with_output_types(str)
         | 'dup key' >> beam.Map(lambda x: (x, x))
         .with_output_types(typehints.KV[str, str])
         | 'count dups' >> combine.Count.PerKey())

    self.assertCompatible(typehints.KV[str, int], d.element_type)
    assert_that(d, equal_to([('e', 1), ('s', 1), ('t', 2)]))
    self.p.run()

  def test_count_perelement_pipeline_type_checking_satisfied(self):
    d = (self.p
         | beam.Create([1, 1, 2, 3]).with_output_types(int)
         | 'count elems' >> combine.Count.PerElement())

    self.assertCompatible(typehints.KV[int, int], d.element_type)
    assert_that(d, equal_to([(1, 2), (2, 1), (3, 1)]))
    self.p.run()

  def test_count_perelement_pipeline_type_checking_violated(self):
    self.p.options.view_as(TypeOptions).type_check_strictness = 'ALL_REQUIRED'

    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | 'f' >> beam.Create([1, 1, 2, 3])
       | 'count elems' >> combine.Count.PerElement())

    self.assertEqual('Pipeline type checking is enabled, however no output '
                     'type-hint was found for the PTransform '
                     'Create(f)',
                     e.exception.message)

  def test_count_perelement_runtime_type_checking_satisfied(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    d = (self.p
         | beam.Create([True, True, False, True, True])
         .with_output_types(bool)
         | 'count elems' >> combine.Count.PerElement())

    self.assertCompatible(typehints.KV[bool, int], d.element_type)
    assert_that(d, equal_to([(False, 1), (True, 4)]))
    self.p.run()

  def test_top_of_pipeline_checking_satisfied(self):
    d = (self.p
         | beam.Create(range(5, 11)).with_output_types(int)
         | 'top 3' >> combine.Top.Of(3, lambda x, y: x < y))

    self.assertCompatible(typehints.Iterable[int],
                          d.element_type)
    assert_that(d, equal_to([[10, 9, 8]]))
    self.p.run()

  def test_top_of_runtime_checking_satisfied(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    d = (self.p
         | beam.Create(list('testing')).with_output_types(str)
         | 'acii top' >> combine.Top.Of(3, lambda x, y: x < y))

    self.assertCompatible(typehints.Iterable[str], d.element_type)
    assert_that(d, equal_to([['t', 't', 's']]))
    self.p.run()

  def test_per_key_pipeline_checking_violated(self):
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | beam.Create(range(100)).with_output_types(int)
       | 'num + 1' >> beam.Map(lambda x: x + 1).with_output_types(int)
       | 'top mod' >> combine.Top.PerKey(1, lambda a, b: a < b))

    self.assertEqual("Input type hint violation at GroupByKey: "
                     "expected Tuple[TypeVariable[K], TypeVariable[V]], "
                     "got <type 'int'>",
                     e.exception.message)

  def test_per_key_pipeline_checking_satisfied(self):
    d = (self.p
         | beam.Create(range(100)).with_output_types(int)
         | ('group mod 3' >> beam.Map(lambda x: (x % 3, x))
            .with_output_types(typehints.KV[int, int]))
         | 'top mod' >> combine.Top.PerKey(1, lambda a, b: a < b))

    self.assertCompatible(typehints.Tuple[int, typehints.Iterable[int]],
                          d.element_type)
    assert_that(d, equal_to([(0, [99]), (1, [97]), (2, [98])]))
    self.p.run()

  def test_per_key_runtime_checking_satisfied(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    d = (self.p
         | beam.Create(range(21))
         | ('group mod 3' >> beam.Map(lambda x: (x % 3, x))
            .with_output_types(typehints.KV[int, int]))
         | 'top mod' >> combine.Top.PerKey(1, lambda a, b: a < b))

    self.assertCompatible(typehints.KV[int, typehints.Iterable[int]],
                          d.element_type)
    assert_that(d, equal_to([(0, [18]), (1, [19]), (2, [20])]))
    self.p.run()

  def test_sample_globally_pipeline_satisfied(self):
    d = (self.p
         | beam.Create([2, 2, 3, 3]).with_output_types(int)
         | 'sample' >> combine.Sample.FixedSizeGlobally(3))

    self.assertCompatible(typehints.Iterable[int], d.element_type)

    def matcher(expected_len):
      def match(actual):
        equal_to([expected_len])([len(actual[0])])
      return match
    assert_that(d, matcher(3))
    self.p.run()

  def test_sample_globally_runtime_satisfied(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    d = (self.p
         | beam.Create([2, 2, 3, 3]).with_output_types(int)
         | 'sample' >> combine.Sample.FixedSizeGlobally(2))

    self.assertCompatible(typehints.Iterable[int], d.element_type)

    def matcher(expected_len):
      def match(actual):
        equal_to([expected_len])([len(actual[0])])
      return match
    assert_that(d, matcher(2))
    self.p.run()

  def test_sample_per_key_pipeline_satisfied(self):
    d = (self.p
         | (beam.Create([(1, 2), (1, 2), (2, 3), (2, 3)])
            .with_output_types(typehints.KV[int, int]))
         | 'sample' >> combine.Sample.FixedSizePerKey(2))

    self.assertCompatible(typehints.KV[int, typehints.Iterable[int]],
                          d.element_type)

    def matcher(expected_len):
      def match(actual):
        for _, sample in actual:
          equal_to([expected_len])([len(sample)])
      return match
    assert_that(d, matcher(2))
    self.p.run()

  def test_sample_per_key_runtime_satisfied(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    d = (self.p
         | (beam.Create([(1, 2), (1, 2), (2, 3), (2, 3)])
            .with_output_types(typehints.KV[int, int]))
         | 'sample' >> combine.Sample.FixedSizePerKey(1))

    self.assertCompatible(typehints.KV[int, typehints.Iterable[int]],
                          d.element_type)

    def matcher(expected_len):
      def match(actual):
        for _, sample in actual:
          equal_to([expected_len])([len(sample)])
      return match
    assert_that(d, matcher(1))
    self.p.run()

  def test_to_list_pipeline_check_satisfied(self):
    d = (self.p
         | beam.Create((1, 2, 3, 4)).with_output_types(int)
         | combine.ToList())

    self.assertCompatible(typehints.List[int], d.element_type)

    def matcher(expected):
      def match(actual):
        equal_to(expected)(actual[0])
      return match
    assert_that(d, matcher([1, 2, 3, 4]))
    self.p.run()

  def test_to_list_runtime_check_satisfied(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    d = (self.p
         | beam.Create(list('test')).with_output_types(str)
         | combine.ToList())

    self.assertCompatible(typehints.List[str], d.element_type)

    def matcher(expected):
      def match(actual):
        equal_to(expected)(actual[0])
      return match
    assert_that(d, matcher(['e', 's', 't', 't']))
    self.p.run()

  def test_to_dict_pipeline_check_violated(self):
    with self.assertRaises(typehints.TypeCheckError) as e:
      (self.p
       | beam.Create([1, 2, 3, 4]).with_output_types(int)
       | combine.ToDict())

    self.assertEqual("Type hint violation for 'ParDo(CombineValuesDoFn)': "
                     "requires Tuple[TypeVariable[K], "
                     "Iterable[Tuple[TypeVariable[K], TypeVariable[V]]]] "
                     "but got Tuple[None, Iterable[int]] for p_context",
                     e.exception.message)

  def test_to_dict_pipeline_check_satisfied(self):
    d = (self.p
         | beam.Create(
             [(1, 2), (3, 4)]).with_output_types(typehints.Tuple[int, int])
         | combine.ToDict())

    self.assertCompatible(typehints.Dict[int, int], d.element_type)
    assert_that(d, equal_to([{1: 2, 3: 4}]))
    self.p.run()

  def test_to_dict_runtime_check_satisfied(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    d = (self.p
         | (beam.Create([('1', 2), ('3', 4)])
            .with_output_types(typehints.Tuple[str, int]))
         | combine.ToDict())

    self.assertCompatible(typehints.Dict[str, int], d.element_type)
    assert_that(d, equal_to([{'1': 2, '3': 4}]))
    self.p.run()

  def test_runtime_type_check_python_type_error(self):
    self.p.options.view_as(TypeOptions).runtime_type_check = True

    with self.assertRaises(TypeError) as e:
      (self.p
       | beam.Create([1, 2, 3]).with_output_types(int)
       | 'len' >> beam.Map(lambda x: len(x)).with_output_types(int))
      self.p.run()

    # Our special type-checking related TypeError shouldn't have been raised.
    # Instead the above pipeline should have triggered a regular Python runtime
    # TypeError.
    self.assertEqual("object of type 'int' has no len() [while running 'len']",
                     e.exception.message)
    self.assertFalse(isinstance(e, typehints.TypeCheckError))

  def test_pardo_type_inference(self):
    self.assertEqual(int,
                     beam.Filter(lambda x: False).infer_output_type(int))
    self.assertEqual(typehints.Tuple[str, int],
                     beam.Map(lambda x: (x, 1)).infer_output_type(str))

  def test_gbk_type_inference(self):
    self.assertEqual(
        typehints.Tuple[str, typehints.Iterable[int]],
        beam.core.GroupByKeyOnly().infer_output_type(typehints.KV[str, int]))

  def test_pipeline_inference(self):
    created = self.p | beam.Create(['a', 'b', 'c'])
    mapped = created | 'pair with 1' >> beam.Map(lambda x: (x, 1))
    grouped = mapped | beam.GroupByKey()
    self.assertEqual(str, created.element_type)
    self.assertEqual(typehints.KV[str, int], mapped.element_type)
    self.assertEqual(typehints.KV[str, typehints.Iterable[int]],
                     grouped.element_type)

  def test_inferred_bad_kv_type(self):
    with self.assertRaises(typehints.TypeCheckError) as e:
      _ = (self.p
           | beam.Create(['a', 'b', 'c'])
           | 'ungroupable' >> beam.Map(lambda x: (x, 0, 1.0))
           | beam.GroupByKey())

    self.assertEqual('Input type hint violation at GroupByKey: '
                     'expected Tuple[TypeVariable[K], TypeVariable[V]], '
                     'got Tuple[str, int, float]',
                     e.exception.message)

  def test_type_inference_command_line_flag_toggle(self):
    self.p.options.view_as(TypeOptions).pipeline_type_check = False
    x = self.p | 'c1' >> beam.Create([1, 2, 3, 4])
    self.assertIsNone(x.element_type)

    self.p.options.view_as(TypeOptions).pipeline_type_check = True
    x = self.p | 'c2' >> beam.Create([1, 2, 3, 4])
    self.assertEqual(int, x.element_type)


if __name__ == '__main__':
  unittest.main()
