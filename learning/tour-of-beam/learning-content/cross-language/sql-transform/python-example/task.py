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
#   name: sql-transform
#   description: Sql transform
#   multifile: false
#   context_line: 35
#   categories:
#     - Quickstart
#   complexity: BASIC
#   never_run: true
#   tags:
#     - hellobeam

import apache_beam as beam
from apache_beam.transforms.sql import SqlTransform

# Define a sample input data as a list of dictionaries
input_data = [
    {'id': 1, 'name': 'Alice'},
    {'id': 2, 'name': 'Bob'},
    {'id': 101, 'name': 'Carol'},
    {'id': 102, 'name': 'David'},
]

# Create a pipeline
with beam.Pipeline() as pipeline:
  # Read input data and convert it to a PCollection of Rows
  input_rows = (
            pipeline
            | 'Create input data' >> beam.Create(input_data)
            | 'Convert to Rows' >> beam.Map(lambda x: beam.Row(id=int(x['id']), name=str(x['name'])))
    )

  # Apply the SQL transform
  filtered_rows = input_rows | SqlTransform("SELECT id, name FROM PCOLLECTION WHERE id > 100")

  # Print the results
  filtered_rows | 'Print results' >> beam.Map(print)
