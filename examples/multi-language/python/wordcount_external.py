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

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.external_transform_provider import ExternalTransformProvider
from apache_beam.typehints.row_type import RowTypeConstraint
"""A Python multi-language pipeline that counts words using multiple Java SchemaTransforms.

This pipeline reads an input text file then extracts the words, counts them, and writes the results Java 
SchemaTransforms. The transforms are listed below and can be found in 
src/main/java/org/apache/beam/examples/schematransforms/:
- `ExtractWordsProvider`
- `JavaCountProvider`
- `WriteWordsProvider`

These Java transforms are accessible to the Python pipeline via an expansion service. Check out the
[`README.md`](https://github.com/apache/beam/blob/master/examples/multi-language/README.md#1-start-the-expansion-service)
for instructions on how to download the jar and run this expansion service.

This example aims to demonstrate how to use the `ExternalTransformProvider` utility, which dynamically generates and
provides user-friendly wrappers for external transforms. 

Example commands for executing this program:

DirectRunner:
$ python wordcount_external.py \
      --runner DirectRunner \
      --input <INPUT FILE> \
      --output <OUTPUT FILE> \
      --expansion_service_port <PORT>

DataflowRunner:
$ python wordcount_external.py \
      --runner DataflowRunner \
      --temp_location $TEMP_LOCATION \
      --project $GCP_PROJECT \
      --region $GCP_REGION \
      --job_name $JOB_NAME \
      --num_workers $NUM_WORKERS \
      --input "gs://dataflow-samples/shakespeare/kinglear.txt" \
      --output "gs://$GCS_BUCKET/wordcount_external/output" \
      --expansion_service_port <PORT>
"""

# Original Java transform is in ExtractWordsProvider.java
EXTRACT_IDENTIFIER = "beam:schematransform:org.apache.beam:extract_words:v1"
# Original Java transform is in JavaCountProvider.java
COUNT_IDENTIFIER = "beam:schematransform:org.apache.beam:count:v1"
# Original Java transform is in WriteWordsProvider.java
WRITE_IDENTIFIER = "beam:schematransform:org.apache.beam:write_words:v1"


def run(input_path, output_path, expansion_service_port, pipeline_args):
    pipeline_options = PipelineOptions(pipeline_args)

    # Discover and get external transforms from this expansion service
    provider = ExternalTransformProvider("localhost:" + expansion_service_port)
    # Get transforms with identifiers, then use them as you would a regular
    # native PTransform
    Extract = provider.get_urn(EXTRACT_IDENTIFIER)
    Count = provider.get_urn(COUNT_IDENTIFIER)
    Write = provider.get_urn(WRITE_IDENTIFIER)

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | 'Read' >> ReadFromText(input_path)

        words = (lines
                 | 'Prepare Rows' >> beam.Map(lambda line: beam.Row(line=line))
                 | 'Extract Words' >> Extract())
        word_counts = words | 'Count Words' >> Count()
        formatted_words = (
            word_counts
            | 'Format Text' >> beam.Map(lambda row: beam.Row(line="%s: %s" % (
                row.word, row.count))).with_output_types(
                    RowTypeConstraint.from_fields([('line', str)])))

        formatted_words | 'Write' >> Write(file_path_prefix=output_path)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        required=True,
                        help='Input file')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file')
    parser.add_argument('--expansion_service_port',
                        dest='expansion_service_port',
                        required=True,
                        help='Expansion service port')
    known_args, pipeline_args = parser.parse_known_args()

    run(known_args.input, known_args.output, known_args.expansion_service_port,
        pipeline_args)
