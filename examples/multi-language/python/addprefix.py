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

"""A Python multi-language pipeline that adds prefixes to a given set of strings.

This pipeline reads an input text file and adds two prefixes to every line read from the file.
* Prefix 'java'. This is added by a multi-language Java transform named 'JavaPrefix'.
* Prefix 'python'. This is added by a Python transform.

Example commands for executing the program:

DirectRunner:
$ python addprefix.py --runner DirectRunner --environment_type=DOCKER --input <INPUT FILE> --output output --expansion_service_port <PORT>

DataflowRunner:
$ python addprefix.py \
      --runner DataflowRunner \
      --temp_location $TEMP_LOCATION \
      --project $GCP_PROJECT \
      --region $GCP_REGION \
      --job_name $JOB_NAME \
      --num_workers $NUM_WORKERS \
      --input "gs://dataflow-samples/shakespeare/kinglear.txt" \
      --output "gs://$GCS_BUCKET/javaprefix/output" \
      --expansion_service_port <PORT>
"""
import logging
import re
import typing

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder
from apache_beam.options.pipeline_options import PipelineOptions


def run(input_path, output_path, expansion_service_port, pipeline_args):
  pipeline_options = PipelineOptions(pipeline_args)

  with beam.Pipeline(options=pipeline_options) as p:
    input = p | 'Read' >> ReadFromText(input_path).with_output_types(str)

    java_output = (
        input
        | 'JavaPrefix' >> beam.ExternalTransform(
              'beam:transform:org.apache.beam:javaprefix:v1',
              ImplicitSchemaPayloadBuilder({'prefix': 'java:'}),
              ('localhost:%s' % expansion_service_port)))

    def python_prefix(record):
      return 'python:%s' % record

    output = java_output | 'PythonPrefix' >> beam.Map(python_prefix)
    output | 'Write' >> WriteToText(output_path)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  import argparse

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='Input file')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file')
  parser.add_argument(
      '--expansion_service_port',
      dest='expansion_service_port',
      required=True,
      help='Expansion service port')
  known_args, pipeline_args = parser.parse_known_args()

  run(
      known_args.input,
      known_args.output,
      known_args.expansion_service_port,
      pipeline_args)
