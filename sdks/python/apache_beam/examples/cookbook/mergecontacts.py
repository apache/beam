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

"""Merge phone, email, and mailing address information.

A Dataflow pipeline that merges phone, email, and address information associated
with the same names. Each input "database" is a tab-delimited text file pairing
names with one phone number/email address/mailing address; multiple entries
associated with the same name are allowed. Outputs are a tab-delimited text file
with the merged information and another file containing some simple statistics.
See mergecontacts_test.py for example inputs and outputs.

A demonstration of:
  CoGroupByKey
  Non-linear pipelines (i.e., pipelines with branches)
"""

from __future__ import absolute_import

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.utils.options import PipelineOptions
from apache_beam.utils.options import SetupOptions


def run(argv=None, assert_results=None):

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_email',
      required=True,
      help='Email database, with each line formatted as "name<TAB>email".')
  parser.add_argument(
      '--input_phone',
      required=True,
      help='Phonebook, with each line formatted as "name<TAB>phone number".')
  parser.add_argument(
      '--input_snailmail',
      required=True,
      help='Address database, with each line formatted as "name<TAB>address".')
  parser.add_argument('--output_tsv',
                      required=True,
                      help='Tab-delimited output file.')
  parser.add_argument('--output_stats',
                      required=True,
                      help='Output file for statistics about the input.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  # Helper: read a tab-separated key-value mapping from a text file, escape all
  # quotes/backslashes, and convert it a PCollection of (key, value) pairs.
  def read_kv_textfile(label, textfile):
    return (p
            | beam.io.Read('read_%s' % label, textfile)
            | beam.Map('backslash_%s' % label,
                       lambda x: re.sub(r'\\', r'\\\\', x))
            | beam.Map('escape_quotes_%s' % label,
                       lambda x: re.sub(r'"', r'\"', x))
            | beam.Map('split_%s' % label, lambda x: re.split(r'\t+', x, 1)))

  # Read input databases.
  email = read_kv_textfile('email',
                           beam.io.TextFileSource(known_args.input_email))
  phone = read_kv_textfile('phone',
                           beam.io.TextFileSource(known_args.input_phone))
  snailmail = read_kv_textfile('snailmail', beam.io.TextFileSource(
      known_args.input_snailmail))

  # Group together all entries under the same name.
  grouped = (email, phone, snailmail) | 'group_by_name' >> beam.CoGroupByKey()

  # Prepare tab-delimited output; something like this:
  # "name"<TAB>"email_1,email_2"<TAB>"phone"<TAB>"first_snailmail_only"
  tsv_lines = grouped | beam.Map(
      lambda (name, (email, phone, snailmail)): '\t'.join(
          ['"%s"' % name,
           '"%s"' % ','.join(email),
           '"%s"' % ','.join(phone),
           '"%s"' % next(iter(snailmail), '')]))

  # Compute some stats about our database of people.
  luddites = grouped | beam.Filter(  # People without email.
      lambda (name, (email, phone, snailmail)): not next(iter(email), None))
  writers = grouped | beam.Filter(   # People without phones.
      lambda (name, (email, phone, snailmail)): not next(iter(phone), None))
  nomads = grouped | beam.Filter(    # People without addresses.
      lambda (name, (email, phone, snailmail)): not next(iter(snailmail), None))

  num_luddites = luddites | 'luddites' >> beam.combiners.Count.Globally()
  num_writers = writers | 'writers' >> beam.combiners.Count.Globally()
  num_nomads = nomads | 'nomads' >> beam.combiners.Count.Globally()

  # Write tab-delimited output.
  # pylint: disable=expression-not-assigned
  tsv_lines | beam.io.Write('write_tsv',
                            beam.io.TextFileSink(known_args.output_tsv))

  # TODO(silviuc): Move the assert_results logic to the unit test.
  if assert_results is not None:
    expected_luddites, expected_writers, expected_nomads = assert_results
    beam.assert_that(num_luddites, beam.equal_to([expected_luddites]),
                     label='assert:luddites')
    beam.assert_that(num_writers, beam.equal_to([expected_writers]),
                     label='assert:writers')
    beam.assert_that(num_nomads, beam.equal_to([expected_nomads]),
                     label='assert:nomads')
  # Execute pipeline.
  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
