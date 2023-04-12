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
#   name: table-schema
#   description: TextIO table schema example.
#   multifile: false
#   context_line: 30
#   categories:
#     - Quickstart
#   complexity: ADVANCED
#   tags:
#     - hellobeam
import apache_beam as beam
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.internal.clients import bigquery

p = beam.Pipeline()

table_spec = bigquery.TableReference(
                 projectId='project-id',
                 datasetId='dataset',
                 tableId='table')

table_schema = {
    'fields': [{
        'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE'
    }, {
        'name': 'quote', 'type': 'STRING', 'mode': 'REQUIRED'
    }]
}

input = p | beam.Create([
    {
        'source': 'Mahatma Gandhi', 'quote': 'My life is my message.'
    },
    {
        'source': 'Yoda', 'quote': "Do, or do not. There is no 'try'."
    },
])

input | beam.io.WriteToBigQuery(
    table_spec,
    schema=table_schema,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
