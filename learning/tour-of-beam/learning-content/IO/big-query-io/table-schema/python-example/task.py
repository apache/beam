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
#   description: TextIO write table with schema example.
#   multifile: false
#   context_line: 30
#   categories:
#     - Quickstart
#   complexity: ADVANCED
#   tags:
#     - hellobeam

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


fictional_characters_view = beam.pvalue.AsDict(
    pipeline | 'CreateCharacters' >> beam.Create([('Yoda', True),
                                                  ('Obi Wan Kenobi', True)]))

def table_fn(element, fictional_characters):
  if element in fictional_characters:
    return 'my_dataset.fictional_quotes'
  else:
    return 'my_dataset.real_quotes'

quotes | 'WriteWithDynamicDestination' >> beam.io.WriteToBigQuery(
    table_fn,
    schema=table_schema,
    table_side_inputs=(fictional_characters_view, ),
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
