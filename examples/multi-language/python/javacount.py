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

import logging
import re
import typing

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder
from apache_beam.options.pipeline_options import PipelineOptions

"""A Python multi-language pipeline that counts words.

This pipeline reads an input text file and counts the words using the Java SDK
transform `Count.perElement()`.

Example commands for executing the program:

DirectRunner:
$ python javacount.py --runner DirectRunner --environment_type=DOCKER --input <INPUT FILE> --output output --expansion_service_port <PORT>

DataflowRunner:
$ python javacount.py \
      --runner DataflowRunner \
      --temp_location $TEMP_LOCATION \
      --project $GCP_PROJECT \
      --region $GCP_REGION \
      --job_name $JOB_NAME \
      --num_workers $NUM_WORKERS \
      --input "gs://dataflow-samples/shakespeare/kinglear.txt" \
      --output "gs://$GCS_BUCKET/javacount/output" \
      --expansion_service_port <PORT>
"""

class WordExtractingDoFn(beam.DoFn):
  def process(self, element):
    return re.findall(r'[\w\']+', element, re.UNICODE)


def run(input_path, output_path, expansion_service_port, pipeline_args):
  pipeline_options = PipelineOptions(pipeline_args)

  with beam.Pipeline(options=pipeline_options) as p:
    lines = p | 'Read' >> ReadFromText(input_path).with_output_types(str)
    words = lines | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))

    java_output = (
        words
        | 'JavaCount' >> beam.ExternalTransform(
              'beam:transform:org.apache.beam:javacount:v1',
              None,
              ('localhost:%s' % expansion_service_port)))

    def format(kv):
      key, value = kv
      return '%s:%s' % (key, value)

    output = java_output | 'Format' >> beam.Map(format)
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
