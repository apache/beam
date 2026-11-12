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
import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml.yaml_join import _SqlJoinTransform
from apache_beam.yaml.yaml_join import _parse_fields
from apache_beam.yaml.yaml_transform import YamlTransform


class ToRow(beam.PTransform):
  def expand(self, pcoll):
    return pcoll | beam.Map(lambda row: beam.Row(**row._asdict()))


FRUITS = [
    beam.Row(id=1, name='raspberry'),
    beam.Row(id=2, name='blackberry'),
]

QUANTITIES = [
    beam.Row(name='raspberry', quantity=1),
    beam.Row(name='blackberry', quantity=2),
    beam.Row(name='blueberry', quantity=3),
]

CATEGORIES = [
    beam.Row(name='raspberry', category='juicy'),
    beam.Row(name='blackberry', category='dry'),
    beam.Row(name='blueberry', category='dry'),
    beam.Row(name='blueberry', category='juicy'),
]


@unittest.skipIf(
    TestPipeline().get_pipeline_options().view_as(StandardOptions).runner
    is None,
    'Do not run this test on precommit suites.')
class YamlJoinTest(unittest.TestCase):
  def test_basic_join(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      fruits = p | "fruits" >> beam.Create(FRUITS)
      quantities = p | "quantities" >> beam.Create(QUANTITIES)
      result = {
          "fruits": fruits, "quantities": quantities
      } | YamlTransform(
          '''
        type: Join
        input:
          f: fruits
          q: quantities
        config:
          type: inner
          equalities:
            - f: name
              q: name
        ''') | ToRow()
      assert_that(
          result,
          equal_to([
              beam.Row(id=1, name='raspberry', name0='raspberry', quantity=1),
              beam.Row(id=2, name='blackberry', name0='blackberry', quantity=2)
          ]))

  def test_join_three_inputs(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      fruits = p | "fruits" >> beam.Create(FRUITS)
      quantities = p | "quantities" >> beam.Create(QUANTITIES)
      categories = p | "categories" >> beam.Create(CATEGORIES)
      result = {
          "fruits": fruits, "quantities": quantities, "categories": categories
      } | YamlTransform(
          '''
        type: Join
        input:
          f: fruits
          q: quantities
          c: categories
        config:
          type: inner
          equalities:
            - f: name
              q: name
            - f: name
              c: name
        ''') | ToRow()
      assert_that(
          result,
          equal_to([
              beam.Row(
                  id=1,
                  name='raspberry',
                  name0='raspberry',
                  quantity=1,
                  name1='raspberry',
                  category='juicy'),
              beam.Row(
                  id=2,
                  name='blackberry',
                  name0='blackberry',
                  quantity=2,
                  name1='blackberry',
                  category='dry')
          ]))

  def test_join_three_inputs_arbitrary_order(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      fruits = p | "fruits" >> beam.Create(FRUITS)
      quantities = p | "quantities" >> beam.Create(QUANTITIES)
      categories = p | "categories" >> beam.Create(CATEGORIES)
      result = {
          "quantities": quantities, "categories": categories, "fruits": fruits
      } | YamlTransform(
          '''
        type: Join
        input:
          q: quantities
          c: categories
          f: fruits
        config:
          type: inner
          equalities:
            - q: name
              f: name
            - f: name
              c: name
        ''') | ToRow()
      assert_that(
          result,
          equal_to([
              beam.Row(
                  name='raspberry',
                  quantity=1,
                  id=1,
                  name0='raspberry',
                  name1='raspberry',
                  category='juicy'),
              beam.Row(
                  name='blackberry',
                  quantity=2,
                  id=2,
                  name0='blackberry',
                  name1='blackberry',
                  category='dry')
          ]))

  def test_join_with_fields(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      fruits = p | "fruits" >> beam.Create(FRUITS)
      quantities = p | "quantities" >> beam.Create(QUANTITIES)
      categories = p | "categories" >> beam.Create(CATEGORIES)
      result = {
          "fruits": fruits, "quantities": quantities, "categories": categories
      } | YamlTransform(
          '''
        type: Join
        input:
          f: fruits
          q: quantities
          c: categories
        config:
          type: inner
          equalities:
            - f: name
              q: name
            - f: name
              c: name
          fields:
            f:
              f_id: id
            q:
              - name
              - quantity    
        ''') | ToRow()

      assert_that(
          result,
          equal_to([
              beam.Row(
                  f_id=1,
                  name='raspberry',
                  quantity=1,
                  name0='raspberry',
                  category='juicy'),
              beam.Row(
                  f_id=2,
                  name='blackberry',
                  quantity=2,
                  name0='blackberry',
                  category='dry')
          ]))

  def test_join_with_equalities_shorthand(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      fruits = p | "fruits" >> beam.Create(FRUITS)
      quantities = p | "quantities" >> beam.Create(QUANTITIES)
      categories = p | "categories" >> beam.Create(CATEGORIES)

      result = {
          "fruits": fruits, "quantities": quantities, "categories": categories
      } | YamlTransform(
          '''
        type: Join
        input:
          f: fruits
          q: quantities
          c: categories
        config:
          type: inner
          equalities: name
        ''') | ToRow()

      assert_that(
          result,
          equal_to([
              beam.Row(
                  id=1,
                  name='raspberry',
                  name0='raspberry',
                  quantity=1,
                  name1='raspberry',
                  category='juicy'),
              beam.Row(
                  id=2,
                  name='blackberry',
                  name0='blackberry',
                  quantity=2,
                  name1='blackberry',
                  category='dry')
          ]))


class SqlJoinQueryTest(unittest.TestCase):
  def _make_mock_pcoll(self, field_names):
    from unittest.mock import MagicMock
    from collections import namedtuple
    MockRow = namedtuple('MockRow', field_names)
    pcoll = MagicMock()
    pcoll.element_type = MockRow
    return pcoll

  def test_three_table_chain_arbitrary_order(self):
    p = self._make_mock_pcoll(['product_id', 'product_name'])
    o = self._make_mock_pcoll(['order_id', 'customer'])
    l = self._make_mock_pcoll(['order_id', 'product_id', 'qty'])

    pcolls = {'products': p, 'orders': o, 'line_items': l}
    captured_queries = []

    def mock_sql(query):
      captured_queries.append(query)
      return pcolls

    _ = _SqlJoinTransform(
        mock_sql,
        equalities=[{
            'orders': 'order_id', 'line_items': 'order_id'
        }, {
            'line_items': 'product_id', 'products': 'product_id'
        }],
        type='inner').expand(pcolls)

    self.assertEqual(len(captured_queries), 1)
    query = captured_queries[0]

    # Verify query structure
    self.assertIn('SELECT * FROM products', query)
    self.assertIn(
        'INNER JOIN line_items ON line_items.product_id = products.product_id',
        query)
    self.assertIn(
        'INNER JOIN orders ON orders.order_id = line_items.order_id', query)
    # Verify orders is not missing an ON clause
    self.assertNotIn('INNER JOIN orders  INNER JOIN', query)

  def test_three_table_chain_no_forward_reference(self):
    p = self._make_mock_pcoll(['product_id', 'product_name'])
    o = self._make_mock_pcoll(['order_id', 'customer'])
    l = self._make_mock_pcoll(['order_id', 'product_id', 'qty'])

    pcolls = {'orders': o, 'products': p, 'line_items': l}
    captured_queries = []

    def mock_sql(query):
      captured_queries.append(query)
      return pcolls

    _ = _SqlJoinTransform(
        mock_sql,
        equalities=[{
            'orders': 'order_id', 'line_items': 'order_id'
        }, {
            'line_items': 'product_id', 'products': 'product_id'
        }],
        type='inner').expand(pcolls)

    self.assertEqual(len(captured_queries), 1)
    query = captured_queries[0]

    # Verify line_items is joined before products
    line_items_pos = query.find('JOIN line_items')
    products_pos = query.find('JOIN products')
    self.assertNotEqual(line_items_pos, -1)
    self.assertNotEqual(products_pos, -1)
    self.assertLess(line_items_pos, products_pos)

  def test_triangle_join_composite_predicates(self):
    a = self._make_mock_pcoll(['id'])
    b = self._make_mock_pcoll(['id'])
    c = self._make_mock_pcoll(['id'])

    pcolls = {'a': a, 'b': b, 'c': c}
    captured_queries = []

    def mock_sql(query):
      captured_queries.append(query)
      return pcolls

    _ = _SqlJoinTransform(
        mock_sql,
        equalities=[{
            'a': 'id', 'b': 'id'
        }, {
            'b': 'id', 'c': 'id'
        }, {
            'a': 'id', 'c': 'id'
        }],
        type='inner').expand(pcolls)

    self.assertEqual(len(captured_queries), 1)
    query = captured_queries[0]
    self.assertIn('INNER JOIN b ON a.id = b.id', query)
    self.assertIn('INNER JOIN c ON a.id = c.id AND b.id = c.id', query)

  def test_parse_fields_wildcard(self):
    tables = ['products', 'orders', 'line_items']
    fields = {'products': '*', 'orders': ['customer']}
    parsed = _parse_fields(tables, fields)
    self.assertEqual(
        parsed, ['products.*', 'orders.customer AS customer', 'line_items.*'])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
