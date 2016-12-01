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


"""A workflow demonstrating a DoFn with multiple outputs.

DoFns may produce a main output and additional side outputs. These side outputs
are marked with a tag at output time and later the same tag will be used to get
the corresponding result (a PCollection) for that side output.

This is a slightly modified version of the basic wordcount example. In this
example words are divided into 2 buckets as shorts words (3 characters in length
or less) and words (all other words). There will be 3 output files:

  [OUTPUT]-chars        :   Character count for the input.
  [OUTPUT]-short-words  :   Word count for short words only.
  [OUTPUT]-words        :   Word count for all other words.

To execute this pipeline locally, specify a local output file or output prefix
on GCS:
  --output [YOUR_LOCAL_FILE | gs://YOUR_OUTPUT_PREFIX]

To execute this pipeline using the Google Cloud Dataflow service, specify
pipeline configuration:
  --project YOUR_PROJECT_ID
  --staging_location gs://YOUR_STAGING_DIRECTORY
  --temp_location gs://YOUR_TEMP_DIRECTORY
  --job_name YOUR_JOB_NAME
  --runner BlockingDataflowPipelineRunner

and an output prefix on GCS:
  --output gs://YOUR_OUTPUT_PREFIX
"""

from __future__ import absolute_import

import argparse
import logging
import re

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.utils.options import PipelineOptions
from apache_beam.utils.options import SetupOptions


class SplitLinesToWordsFn(beam.DoFn):
  """A transform to split a line of text into individual words.

  This transform will have 3 outputs:
    - main output: all words that are longer than 3 characters.
    - short words side output: all other words.
    - character count side output: Number of characters in each processed line.
  """

  # These tags will be used to tag the side outputs of this DoFn.
  SIDE_OUTPUT_TAG_SHORT_WORDS = 'tag_short_words'
  SIDE_OUTPUT_TAG_CHARACTER_COUNT = 'tag_character_count'

  def process(self, context):
    """Receives a single element (a line) and produces words and side outputs.

    Important things to note here:
      - For a single element you may produce multiple main outputs:
        words of a single line.
      - For that same input you may produce multiple side outputs, along with
        multiple main outputs.
      - Side outputs may have different types (count) or may share the same type
        (words) as with the main output.

    Args:
      context: processing context.

    Yields:
      words as main output, short words as side output, line character count as
      side output.
    """
    # yield a count (integer) to the SIDE_OUTPUT_TAG_CHARACTER_COUNT tagged
    # collection.
    yield pvalue.SideOutputValue(self.SIDE_OUTPUT_TAG_CHARACTER_COUNT,
                                 len(context.element))

    words = re.findall(r'[A-Za-z\']+', context.element)
    for word in words:
      if len(word) <= 3:
        # yield word as a side output to the SIDE_OUTPUT_TAG_SHORT_WORDS tagged
        # collection.
        yield pvalue.SideOutputValue(self.SIDE_OUTPUT_TAG_SHORT_WORDS, word)
      else:
        # yield word to add it to the main collection.
        yield word


class CountWords(beam.PTransform):
  """A transform to count the occurrences of each word.

  A PTransform that converts a PCollection containing words into a PCollection
  of "word: count" strings.
  """

  def apply(self, pcoll):
    return (pcoll
            | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
            | 'group' >> beam.GroupByKey()
            | 'count' >> beam.Map(lambda (word, ones): (word, sum(ones)))
            | 'format' >> beam.Map(lambda (word, c): '%s: %s' % (word, c)))


def run(argv=None):
  """Runs the workflow counting the long words and short words separately."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')
  parser.add_argument('--output',
                      required=True,
                      help='Output prefix for files to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  lines = p | beam.Read(beam.io.TextFileSource(known_args.input))

  # with_outputs allows accessing the side outputs of a DoFn.
  split_lines_result = (lines
                        | beam.ParDo(SplitLinesToWordsFn()).with_outputs(
                            SplitLinesToWordsFn.SIDE_OUTPUT_TAG_SHORT_WORDS,
                            SplitLinesToWordsFn.SIDE_OUTPUT_TAG_CHARACTER_COUNT,
                            main='words'))

  # split_lines_result is an object of type DoOutputsTuple. It supports
  # accessing result in alternative ways.
  words, _, _ = split_lines_result
  short_words = split_lines_result[
      SplitLinesToWordsFn.SIDE_OUTPUT_TAG_SHORT_WORDS]
  character_count = split_lines_result.tag_character_count

  # pylint: disable=expression-not-assigned
  (character_count
   | 'pair_with_key' >> beam.Map(lambda x: ('chars_temp_key', x))
   | beam.GroupByKey()
   | 'count chars' >> beam.Map(lambda (_, counts): sum(counts))
   | 'write chars' >> beam.Write(
       beam.io.TextFileSink(known_args.output + '-chars')))

  # pylint: disable=expression-not-assigned
  (short_words
   | 'count short words' >> CountWords()
   | 'write short words' >> beam.Write(
       beam.io.TextFileSink(known_args.output + '-short-words')))

  # pylint: disable=expression-not-assigned
  (words
   | 'count words' >> CountWords()
   | 'write words' >> beam.Write(
       beam.io.TextFileSink(known_args.output + '-words')))

  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
