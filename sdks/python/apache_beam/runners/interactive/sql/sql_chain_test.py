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

"""Tests for sql_chain module."""

# pytype: skip-file

import unittest
from unittest.mock import patch

import pytest

import apache_beam as beam
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive.sql.sql_chain import SqlChain
from apache_beam.runners.interactive.sql.sql_chain import SqlNode
from apache_beam.runners.interactive.testing.mock_ipython import mock_get_ipython


class SqlChainTest(unittest.TestCase):
  def test_init(self):
    chain = SqlChain()
    self.assertEqual({}, chain.nodes)
    self.assertIsNone(chain.root)
    self.assertIsNone(chain.current)
    self.assertIsNone(chain.user_pipeline)

  def test_append_first_node(self):
    node = SqlNode(output_name='first', source='a', query='q1')
    chain = SqlChain().append(node)
    self.assertIs(node, chain.get(node.output_name))
    self.assertIs(node, chain.root)
    self.assertIs(node, chain.current)

  def test_append_non_root_node(self):
    chain = SqlChain().append(
        SqlNode(output_name='root', source='root', query='q1'))
    self.assertIsNone(chain.root.next)
    node = SqlNode(output_name='next_node', source='root', query='q2')
    chain.append(node)
    self.assertIs(node, chain.root.next)
    self.assertIs(node, chain.get(node.output_name))

  @patch(
      'apache_beam.runners.interactive.sql.sql_chain.SchemaLoadedSqlTransform.'
      '__rrshift__')
  def test_to_pipeline_only_evaluate_once_per_pipeline_and_node(
      self, mocked_sql_transform):
    p = beam.Pipeline()
    ie.current_env().watch({'p': p})
    pcoll_1 = p | 'create pcoll_1' >> beam.Create([1, 2, 3])
    pcoll_2 = p | 'create pcoll_2' >> beam.Create([4, 5, 6])
    ie.current_env().watch({'pcoll_1': pcoll_1, 'pcoll_2': pcoll_2})
    node = SqlNode(
        output_name='root', source={'pcoll_1', 'pcoll_2'}, query='q1')
    chain = SqlChain(user_pipeline=p).append(node)
    _ = chain.to_pipeline()
    mocked_sql_transform.assert_called_once()
    _ = chain.to_pipeline()
    mocked_sql_transform.assert_called_once()

  @unittest.skipIf(
      not ie.current_env().is_interactive_ready,
      '[interactive] dependency is not installed.')
  @pytest.mark.skipif(
      not ie.current_env().is_interactive_ready,
      reason='[interactive] dependency is not installed.')
  @patch(
      'apache_beam.runners.interactive.sql.sql_chain.SchemaLoadedSqlTransform.'
      '__rrshift__')
  def test_nodes_with_same_outputs(self, mocked_sql_transform):
    p = beam.Pipeline()
    ie.current_env().watch({'p_nodes_with_same_output': p})
    pcoll = p | 'create pcoll' >> beam.Create([1, 2, 3])
    ie.current_env().watch({'pcoll': pcoll})
    chain = SqlChain(user_pipeline=p)
    output_name = 'output'

    with patch('IPython.get_ipython', new_callable=mock_get_ipython) as cell:
      with cell:
        node_cell_1 = SqlNode(output_name, source='pcoll', query='q1')
        chain.append(node_cell_1)
        _ = chain.to_pipeline()
        mocked_sql_transform.assert_called_with(
            'schema_loaded_beam_sql_output_1')
      with cell:
        node_cell_2 = SqlNode(output_name, source='pcoll', query='q2')
        chain.append(node_cell_2)
        _ = chain.to_pipeline()
        mocked_sql_transform.assert_called_with(
            'schema_loaded_beam_sql_output_2')


if __name__ == '__main__':
  unittest.main()
