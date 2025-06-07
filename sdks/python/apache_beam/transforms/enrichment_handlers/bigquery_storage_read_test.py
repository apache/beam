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

import unittest
from unittest import mock
from apache_beam.pvalue import Row as BeamRow
from apache_beam.transforms.enrichment_handlers import bigquery_storage_read

class TestBigQueryStorageEnrichmentHandler(unittest.TestCase):
    def setUp(self):
        self.project = 'test-project'
        self.table_name = 'test-project.test_dataset.test_table'
        self.fields = ['id']
        self.row_restriction_template = 'id = "{id}"'
        self.column_names = ['id', 'value']

    def make_handler(self, **kwargs):
        handler_kwargs = {
            'project': self.project,
            'table_name': self.table_name,
            'row_restriction_template': self.row_restriction_template,
            'fields': self.fields,
            'column_names': self.column_names,
        }
        handler_kwargs.update(kwargs)  # Override defaults with provided kwargs
        return bigquery_storage_read.BigQueryStorageEnrichmentHandler(**handler_kwargs)

    def test_init_invalid_args(self):
        # Both row_restriction_template and row_restriction_template_fn
        with self.assertRaises(ValueError):
            bigquery_storage_read.BigQueryStorageEnrichmentHandler(
                project=self.project,
                table_name=self.table_name,
                row_restriction_template='foo',
                row_restriction_template_fn=lambda d, p, r: 'bar',
                fields=self.fields
            )
        # Neither row_restriction_template nor row_restriction_template_fn
        with self.assertRaises(ValueError):
            bigquery_storage_read.BigQueryStorageEnrichmentHandler(
                project=self.project,
                table_name=self.table_name,
                fields=self.fields
            )
        # Both fields and condition_value_fn
        with self.assertRaises(ValueError):
            bigquery_storage_read.BigQueryStorageEnrichmentHandler(
                project=self.project,
                table_name=self.table_name,
                row_restriction_template='foo',
                fields=self.fields,
                condition_value_fn=lambda r: {'id': 1}
            )
        # Neither fields nor condition_value_fn
        with self.assertRaises(ValueError):
            bigquery_storage_read.BigQueryStorageEnrichmentHandler(
                project=self.project,
                table_name=self.table_name,
                row_restriction_template='foo'
            )

    def test_get_condition_values_dict_fields(self):
        handler = self.make_handler()
        row = BeamRow(id=1, value='a')
        self.assertEqual(handler._get_condition_values_dict(row), {'id': 1})

    def test_get_condition_values_dict_missing_field(self):
        handler = self.make_handler()
        row = BeamRow(value='a')
        self.assertIsNone(handler._get_condition_values_dict(row))

    def test_get_condition_values_dict_condition_value_fn(self):
        handler = self.make_handler(fields=None, condition_value_fn=lambda r: {'id': 2})
        row = BeamRow(id=2, value='b')
        self.assertEqual(handler._get_condition_values_dict(row), {'id': 2})

    def test_build_single_row_filter_template(self):
        handler = self.make_handler()
        row = BeamRow(id=3, value='c')
        cond = {'id': 3}
        self.assertEqual(handler._build_single_row_filter(row, cond), 'id = "3"')

    def test_build_single_row_filter_fn(self):
        fn = lambda d, p, r: f"id = '{d['id']}'"
        handler = self.make_handler(row_restriction_template=None, row_restriction_template_fn=fn)
        row = BeamRow(id=4, value='d')
        cond = {'id': 4}
        self.assertEqual(handler._build_single_row_filter(row, cond), "id = '4'")

    def test_apply_renaming(self):
        handler = self.make_handler(column_names=['id as new_id', 'value'])
        bq_row = {'id': 1, 'value': 'foo'}
        self.assertEqual(handler._apply_renaming(bq_row), {'new_id': 1, 'value': 'foo'})

    def test_create_row_key(self):
        handler = self.make_handler()
        row = BeamRow(id=5, value='e')
        self.assertEqual(handler.create_row_key(row), (('id', 5),))

    @mock.patch.object(bigquery_storage_read.BigQueryStorageEnrichmentHandler, '_execute_storage_read')
    def test_call_single_match(self, mock_exec):
        handler = self.make_handler()
        row = BeamRow(id=6, value='f')
        mock_exec.return_value = [{'id': 6, 'value': 'fetched'}]
        req, resp = handler(row)
        self.assertEqual(req, row)
        self.assertEqual(resp.id, 6)
        self.assertEqual(resp.value, 'fetched')

    @mock.patch.object(bigquery_storage_read.BigQueryStorageEnrichmentHandler, '_execute_storage_read')
    def test_call_single_no_match(self, mock_exec):
        handler = self.make_handler()
        row = BeamRow(id=7, value='g')
        mock_exec.return_value = []
        req, resp = handler(row)
        self.assertEqual(req, row)
        self.assertEqual(resp, BeamRow())

    @mock.patch.object(bigquery_storage_read.BigQueryStorageEnrichmentHandler, '_execute_storage_read')
    def test_call_batch(self, mock_exec):
        handler = self.make_handler()
        rows = [BeamRow(id=8, value='h'), BeamRow(id=9, value='i')]
        mock_exec.return_value = [
            {'id': 8, 'value': 'h_bq'},
            {'id': 9, 'value': 'i_bq'}
        ]
        result = handler(rows)
        self.assertEqual(result[0][0], rows[0])
        self.assertEqual(result[0][1].id, 8)
        self.assertEqual(result[0][1].value, 'h_bq')
        self.assertEqual(result[1][0], rows[1])
        self.assertEqual(result[1][1].id, 9)
        self.assertEqual(result[1][1].value, 'i_bq')

    @mock.patch.object(bigquery_storage_read.BigQueryStorageEnrichmentHandler, '_execute_storage_read')
    def test_call_batch_no_match(self, mock_exec):
        handler = self.make_handler()
        rows = [BeamRow(id=10, value='j'), BeamRow(id=11, value='k')]
        mock_exec.return_value = []
        result = handler(rows)
        self.assertEqual(result[0][0], rows[0])
        self.assertEqual(result[0][1], BeamRow())
        self.assertEqual(result[1][0], rows[1])
        self.assertEqual(result[1][1], BeamRow())

    def test_get_cache_key(self):
        handler = self.make_handler()
        row = BeamRow(id=12, value='l')
        self.assertEqual(handler.get_cache_key(row), str((('id', 12),)))
        rows = [BeamRow(id=13, value='m'), BeamRow(id=14, value='n')]
        self.assertEqual(
            handler.get_cache_key(rows),
            [str((('id', 13),)), str((('id', 14),))]
        )

    def test_batch_elements_kwargs(self):
        handler = self.make_handler(min_batch_size=2, max_batch_size=5, max_batch_duration_secs=10)
        self.assertEqual(
            handler.batch_elements_kwargs(),
            {'min_batch_size': 2, 'max_batch_size': 5, 'max_batch_duration_secs': 10}
        )

if __name__ == '__main__':
    unittest.main()
