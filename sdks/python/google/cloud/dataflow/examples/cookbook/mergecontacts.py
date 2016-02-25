# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

import logging
import re

import google.cloud.dataflow as df
from google.cloud.dataflow.utils.options import add_option
from google.cloud.dataflow.utils.options import get_options


def run(options=None):
  p = df.Pipeline(options=get_options(options))

  # Helper: read a tab-separated key-value mapping from a text file, escape all
  # quotes/backslashes, and convert it a PCollection of (key, value) pairs.
  def read_kv_textfile(label, textfile):
    return (p
            | df.io.Read('read_%s' % label, textfile)
            | df.Map('backslash_%s' % label,
                     lambda x: re.sub(r'\\', r'\\\\', x))
            | df.Map('escape_quotes_%s' % label,
                     lambda x: re.sub(r'"', r'\"', x))
            | df.Map('split_%s' % label, lambda x: re.split(r'\t+', x, 1)))

  # Read input databases.
  email = read_kv_textfile(
      'email', df.io.TextFileSource(p.options.input_email))
  phone = read_kv_textfile(
      'phone', df.io.TextFileSource(p.options.input_phone))
  snailmail = read_kv_textfile(
      'snailmail', df.io.TextFileSource(p.options.input_snailmail))

  # Group together all entries under the same name.
  grouped = (email, phone, snailmail) | df.CoGroupByKey('group_by_name')

  # Prepare tab-delimited output; something like this:
  # "name"<TAB>"email_1,email_2"<TAB>"phone"<TAB>"first_snailmail_only"
  tsv_lines = grouped | df.Map(
      lambda (name, (email, phone, snailmail)): '\t'.join(
          ['"%s"' % name,
           '"%s"' % ','.join(email),
           '"%s"' % ','.join(phone),
           '"%s"' % next(iter(snailmail), '')]))

  # Compute some stats about our database of people.
  luddites = grouped | df.Filter(  # People without email.
      lambda (name, (email, phone, snailmail)): not next(iter(email), None))
  writers = grouped | df.Filter(   # People without phones.
      lambda (name, (email, phone, snailmail)): not next(iter(phone), None))
  nomads = grouped | df.Filter(    # People without addresses.
      lambda (name, (email, phone, snailmail)): not next(iter(snailmail), None))

  num_luddites = luddites | df.combiners.Count.Globally('luddites')
  num_writers = writers | df.combiners.Count.Globally('writers')
  num_nomads = nomads | df.combiners.Count.Globally('nomads')

  # Write tab-delimited output.
  # pylint: disable=expression-not-assigned
  tsv_lines | df.io.Write(
      'write_tsv', df.io.TextFileSink(p.options.output_tsv))

  # Execute pipeline.
  p.run()

  # Write our statistics to a separate file.
  with open(p.options.output_stats, 'w') as f:
    f.write('%d luddites\n' % next(num_luddites.get()))
    f.write('%d writers\n' % next(num_writers.get()))
    f.write('%d nomads\n' % next(num_nomads.get()))


add_option(
    '--input_email', dest='input_email', required=True,
    help='Email database, with each line formatted as "name<TAB>email".')
add_option(
    '--input_phone', dest='input_phone', required=True,
    help='Phonebook, with each line formatted as "name<TAB>phone number".')
add_option(
    '--input_snailmail', dest='input_snailmail', required=True,
    help='Address database, with each line formatted as "name<TAB>address".')

add_option(
    '--output_tsv', dest='output_tsv', required=True,
    help='Tab-delimited output file.')
add_option(
    '--output_stats', dest='output_stats', required=True,
    help='Output file for statistics about the input.')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
