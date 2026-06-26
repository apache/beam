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

"""A workflow that writes to a BigQuery table with nested and repeated fields.

Demonstrates how to build a bigquery.TableSchema object with nested and repeated
fields. Also, shows how to generate data to be written to a BigQuery table with
nested and repeated fields.
"""

# pytype: skip-file

import argparse
import logging

import apache_beam as beam


def run(argv=None):
  """Run the workflow."""
  parser = argparse.ArgumentParser()

  parser.add_argument(
      '--output',
      required=True,
      help=(
          'Output BigQuery table for results specified as: '
          'PROJECT:DATASET.TABLE or DATASET.TABLE.'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline(argv=pipeline_args) as p:

    # pylint: disable=wrong-import-order, wrong-import-position

    table_schema = {
        'fields': [{
            'name': 'kind', 'type': 'STRING', 'mode': 'NULLABLE'
        }, {
            'name': 'fullName', 'type': 'STRING', 'mode': 'REQUIRED'
        }, {
            'name': 'age', 'type': 'INTEGER', 'mode': 'NULLABLE'
        }, {
            'name': 'gender', 'type': 'STRING', 'mode': 'NULLABLE'
        },
                   {
                       'name': 'phoneNumber',
                       'type': 'RECORD',
                       'mode': 'NULLABLE',
                       'fields': [{
                           'name': 'areaCode',
                           'type': 'INTEGER',
                           'mode': 'NULLABLE'
                       },
                                  {
                                      'name': 'number',
                                      'type': 'INTEGER',
                                      'mode': 'NULLABLE'
                                  }]
                   }, {
                       'name': 'children', 'type': 'STRING', 'mode': 'REPEATED'
                   }]
    }

    def create_random_record(record_id):
      return {
          'kind': 'kind' + record_id,
          'fullName': 'fullName' + record_id,
          'age': int(record_id) * 10,
          'gender': 'male',
          'phoneNumber': {
              'areaCode': int(record_id) * 100,
              'number': int(record_id) * 100000
          },
          'children': [
              'child' + record_id + '1',
              'child' + record_id + '2',
              'child' + record_id + '3'
          ]
      }

    # pylint: disable=expression-not-assigned
    record_ids = p | 'CreateIDs' >> beam.Create(['1', '2', '3', '4', '5'])
    records = record_ids | 'CreateRecords' >> beam.Map(create_random_record)
    records | 'write' >> beam.io.WriteToBigQuery(
        known_args.output,
        schema=table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

    # Run the pipeline (all operations are deferred until run() is called).


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
