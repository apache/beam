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

"""A minimalist word-counting workflow that counts words in Shakespeare.

This is the first in a series of successively more detailed 'word count'
examples.

Next, see the wordcount pipeline, then the wordcount_debugging pipeline, for
more detailed examples that introduce additional concepts.

Concepts:

1. Reading data from text files
2. Specifying 'inline' transforms
3. Counting a PCollection
4. Writing data to Cloud Storage as text files

To execute this pipeline locally, first edit the code to specify the output
location. Output location could be a local file path or an output prefix
on GCS. (Only update the output location marked with the first CHANGE comment.)

To execute this pipeline remotely, first edit the code to set your project ID,
runner type, the staging location, the temp location, and the output location.
The specified GCS bucket(s) must already exist. (Update all the places marked
with a CHANGE comment.)

Then, run the pipeline as described in the README. It will be deployed and run
using the Google Cloud Dataflow Service. No args are required to run the
pipeline. You can see the results in your output bucket in the GCS browser.
"""

from __future__ import absolute_import

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.utils.options import PipelineOptions
from apache_beam.utils.options import SetupOptions


def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      # CHANGE 1/5: The Google Cloud Storage path is required
                      # for outputting the results.
                      default='gs://YOUR_OUTPUT_BUCKET/AND_OUTPUT_PREFIX',
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
      # CHANGE 2/5: (OPTIONAL) Change this to BlockingDataflowPipelineRunner to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--runner=DirectPipelineRunner',
      # CHANGE 3/5: Your project ID is required in order to run your pipeline on
      # the Google Cloud Dataflow Service.
      '--project=SET_YOUR_PROJECT_ID_HERE',
      # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
      # files.
      '--staging_location=gs://YOUR_BUCKET_NAME/AND_STAGING_DIRECTORY',
      # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
      # files.
      '--temp_location=gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY',
      '--job_name=your-wordcount-job',
  ])

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  # Read the text file[pattern] into a PCollection.
  lines = p | 'read' >> beam.io.Read(beam.io.TextFileSource(known_args.input))

  # Count the occurrences of each word.
  counts = (lines
            | 'split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
                          .with_output_types(unicode))
            | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
            | 'group' >> beam.GroupByKey()
            | 'count' >> beam.Map(lambda (word, ones): (word, sum(ones))))

  # Format the counts into a PCollection of strings.
  output = counts | 'format' >> beam.Map(lambda (word, c): '%s: %s' % (word, c))

  # Write the output using a "Write" transform that has side effects.
  # pylint: disable=expression-not-assigned
  output | 'write' >> beam.io.Write(beam.io.TextFileSink(known_args.output))

  # Actually run the pipeline (all operations above are deferred).
  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
