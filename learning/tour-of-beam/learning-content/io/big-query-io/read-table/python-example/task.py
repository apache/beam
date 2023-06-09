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

# beam-playground-broken:
#   name: read-table
#   description: TextIO read table example.
#   multifile: false
#   context_line: 34
#   categories:
#     - Quickstart
#   complexity: ADVANCED
#   tags:
#     - hellobeam

import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='gs://bucket',
                        help='Input file to process.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # ReadFromBigQuery: This operation reads from a BigQuery table and outputs a PCollection of dictionaries. Each
    # dictionary represents a row in the BigQuery table, where the keys are the BigQuery column names. beam.Map: This
    # operation applies a function to each element in the PCollection, here, it selects a specific field from each row.

    with beam.Pipeline(options=pipeline_options) as p:
      (p #| 'ReadTable' >> beam.io.ReadFromBigQuery(table='project-id.dataset.table')
         # Each row is a dictionary where the keys are the BigQuery columns
         #| beam.Map(lambda elem: elem['field'])
       )


if __name__ == '__main__':
  run()
