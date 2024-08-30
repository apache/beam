#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# beam-playground:
#   name: rest-api-io
#   description: REST-API BigQueryIO example.
#   multifile: false
#   context_line: 34
#   categories:
#     - Quickstart
#   complexity: ADVANCED
#   tags:
#     - hellobeam

"""
The idea behind this code is to read data from a BigQuery table,
process it in some way (although the example provided doesn't perform any significant transformations beyond type conversion),
and then write the data back into BigQuery, but with a unique table for each user based on the user's "id".
"""

import argparse
import logging

from apache_beam.io.gcp.internal.clients import bigquery
import apache_beam as beam


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


def run(argv=None):
    with beam.Pipeline() as p:
      table_schema = bigquery.TableSchema()

      # Fields that use standard types.
      # The destination table schema is a list of three fields ("id", "name", and "age"), matching the fields of the User objects (as implemented in getSchema).
      kind_schema = bigquery.TableFieldSchema()
      kind_schema.name = 'kind'
      kind_schema.type = 'string'
      kind_schema.mode = 'nullable'
      table_schema.fields.append(kind_schema)

      full_name_schema = bigquery.TableFieldSchema()
      full_name_schema.name = 'fullName'
      full_name_schema.type = 'string'
      full_name_schema.mode = 'required'
      table_schema.fields.append(full_name_schema)

      # The write operation is configured to create the destination table if it does not already exist (CREATE_IF_NEEDED) and to replace any existing data in the destination table (WRITE_TRUNCATE).
      # pylint: disable=expression-not-assigned
      record_ids = p | 'CreateIDs' >> beam.Create(['1', '2', '3', '4', '5'])
      records = record_ids | 'CreateRecords' >> beam.Map(create_random_record)
      """
        records | 'write' >> beam.io.WriteToBigQuery(
          'output.txt',
          schema=table_schema,
          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
          write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
      """


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
