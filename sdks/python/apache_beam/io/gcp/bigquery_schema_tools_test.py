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

import logging
import typing
import unittest

import numpy as np

from apache_beam.io.gcp import bigquery_schema_tools
from apache_beam.io.gcp.bigquery_test import HttpError
from apache_beam.io.gcp.internal.clients import bigquery


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestBigQueryToSchema(unittest.TestCase):
  def test_produce_pcoll_with_schema(self):
    fields = [
        bigquery.TableFieldSchema(name='stn', type='STRING', mode="NULLABLE"),
        bigquery.TableFieldSchema(name='temp', type='FLOAT64', mode="REPEATED"),
        bigquery.TableFieldSchema(name='count', type='INTEGER', mode="None")
    ]
    schema = bigquery.TableSchema(fields=fields)

    usertype = bigquery_schema_tools.produce_pcoll_with_schema(
        the_table_schema=schema)
    self.assertEqual(
        usertype.__annotations__,
        {
            'stn': typing.Optional[str],
            'temp': typing.Sequence[np.float64],
            'count': np.int64
        })

  def test_produce_pcoll_with_empty_schema(self):
    fields = []
    schema = bigquery.TableSchema(fields=fields)

    usertype = bigquery_schema_tools.produce_pcoll_with_schema(
        the_table_schema=schema)
    self.assertEqual(usertype.__annotations__, {})

  def test_unsupported_type(self):
    fields = [
        bigquery.TableFieldSchema(
            name='number', type='DOUBLE', mode="NULLABLE"),
        bigquery.TableFieldSchema(name='temp', type='FLOAT64', mode="REPEATED"),
        bigquery.TableFieldSchema(name='count', type='INTEGER', mode="None")
    ]
    schema = bigquery.TableSchema(fields=fields)
    with self.assertRaisesRegex(KeyError,
                                "Encountered an unsupported type: 'DOUBLE'"):
      bigquery_schema_tools.produce_pcoll_with_schema(the_table_schema=schema)

  def test_unsupported_mode(self):
    fields = [
        bigquery.TableFieldSchema(name='number', type='INTEGER', mode="NESTED"),
        bigquery.TableFieldSchema(name='temp', type='FLOAT64', mode="REPEATED"),
        bigquery.TableFieldSchema(name='count', type='INTEGER', mode="NONE")
    ]
    schema = bigquery.TableSchema(fields=fields)
    with self.assertRaisesRegex(ValueError,
                                "Encountered an unsupported mode: 'NESTED'"):
      bigquery_schema_tools.produce_pcoll_with_schema(the_table_schema=schema)

  if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
