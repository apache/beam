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
#   name: multi-pipeline
#   description: Multi pipeline example.
#   multifile: false
#   context_line: 35
#   categories:
#     - Quickstart
#   complexity: ADVANCED
#   never_run: true
#   tags:
#     - hellobeam

import logging
import re
import typing

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.transforms.external import ImplicitSchemaPayloadBuilder
from apache_beam.options.pipeline_options import PipelineOptions


class WordExtractingDoFn(beam.DoFn):
    """Parse each line of input text into words."""
    def process(self, element):
        """Returns an iterator over the words of this element.
        The element is a line of text.  If the line is blank, note that, too.
        Args:
          element: the element being processed
        Returns:
          The processed element.
        """
        return re.findall(r'[\w\']+', element, re.UNICODE)


def run(input_path, output_path, pipeline_args):
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
      lines = p | 'Read' >> ReadFromText(input_path).with_output_types(str)
      words = lines | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))

      java_output = (words
                | 'JavaCount' >> beam.ExternalTransform(
            'my.beam.transform.javacount',
            None,
            "localhost:12345"))

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
        default='input.txt',
        required=True,
        help='Input file')
    parser.add_argument(
        '--output',
        dest='output',
        default='output.txt',
        required=True,
        help='Output file')
    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input,
        known_args.output,
        pipeline_args)
