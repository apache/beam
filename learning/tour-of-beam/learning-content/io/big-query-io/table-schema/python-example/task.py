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
#   name: write-table-schema
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

# It defines the schema (table_schema) of the table. The table has two fields: source and quote, both of type STRING.
# The source field is nullable, while the quote field is required.

# It creates the input data which is a collection of dictionaries. Each dictionary represents a row in the BigQuery table.

# Finally, it writes the data to the BigQuery table using the beam.io.WriteToBigQuery function. The write_disposition
# parameter is set to WRITE_TRUNCATE which means that if the table already exists, it will be replaced with the new
# data. The create_disposition parameter is set to CREATE_IF_NEEDED which means the table will be created if it does
# not exist.

input | beam.io.WriteToBigQuery(
    table_spec,
    schema=table_schema,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
