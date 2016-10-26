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

"""Test for the mergecontacts example."""

import logging
import re
import unittest

import apache_beam as beam


class MergeContactsTest(unittest.TestCase):

  CONTACTS_EMAIL = '\n'.join(['Nathan Nomad\tnathan@example.com',
                              'Nicky Nomad\tnicky@example.com',
                              'Noreen Nomad\tnoreen@example.com',
                              'Noreen Nomad\tnomad@example.com',
                              'Robert B\trobert@example.com',
                              'Silviu C\tsilviu@example.com',
                              'Tom S\ttom@example.com',
                              'Wally Writer\twally@example.com',
                              ''])

  CONTACTS_PHONE = '\n'.join(['Larry Luddite\t724-228-3529',
                              'Lisa Luddite\t304-277-3504',
                              'Nathan Nomad\t412-466-8968',
                              'Nicky Nomad\t724-379-5815',
                              'Noreen Nomad\t412-472-0145',
                              'Robert B\t814-865-8799',
                              'Silviu C\t724-537-0671',
                              'Tom S\t570-368-3420',
                              'Tom S\t814-793-9655',
                              ''])

  CONTACTS_SNAILMAIL = '\n'.join(
      ['Larry Luddite\t1949 Westcott St, Detroit, MI 48222',
       'Lisa Luddite\t1949 Westcott St, Detroit, MI 48222',
       'Robert B\t601 N 34th St, Seattle, WA 98103',
       'Silviu C\t1600 Amphitheatre Pkwy, Mountain View, CA 94043',
       'Tom S\t6425 Penn Ave Ste 700, Pittsburgh, PA 15206',
       'Wally Writer\t101 Ridge Rd, Chalkyitsik, AK 99788',
       ''])

  EXPECTED_TSV = '\n'.join(
      ['\t'.join(['"Larry Luddite"', '""', '"724-228-3529"',
                  '"1949 Westcott St, Detroit, MI 48222"']),
       '\t'.join(['"Lisa Luddite"', '""', '"304-277-3504"',
                  '"1949 Westcott St, Detroit, MI 48222"']),
       '\t'.join(['"Nathan Nomad"', '"nathan@example.com"', '"412-466-8968"',
                  '""']),
       '\t'.join(['"Nicky Nomad"', '"nicky@example.com"', '"724-379-5815"',
                  '""']),
       '\t'.join(['"Noreen Nomad"', '"nomad@example.com,noreen@example.com"',
                  '"412-472-0145"', '""']),
       '\t'.join(['"Robert B"', '"robert@example.com"', '"814-865-8799"',
                  '"601 N 34th St, Seattle, WA 98103"']),
       '\t'.join(['"Silviu C"', '"silviu@example.com"', '"724-537-0671"',
                  '"1600 Amphitheatre Pkwy, Mountain View, CA 94043"']),
       '\t'.join(['"Tom S"', '"tom@example.com"', '"570-368-3420,814-793-9655"',
                  '"6425 Penn Ave Ste 700, Pittsburgh, PA 15206"']),
       '\t'.join(['"Wally Writer"', '"wally@example.com"', '""',
                  '"101 Ridge Rd, Chalkyitsik, AK 99788"']),
       ''])

  EXPECTED_STATS = '\n'.join(['2 luddites',
                              '1 writers',
                              '3 nomads',
                              ''])
  EXPECTED_LUDDITES = 2
  EXPECTED_WRITERS = 1
  EXPECTED_NOMADS = 3

  def normalize_tsv_results(self, tsv_data):
    """Sort .tsv file data so we can compare it with expected output."""
    lines_in = tsv_data.strip().split('\n')
    lines_out = []
    for line in lines_in:
      name, email, phone, snailmail = line.split('\t')
      lines_out.append('\t'.join(
          [name,
           '"%s"' % ','.join(sorted(email.strip('"').split(','))),
           '"%s"' % ','.join(sorted(phone.strip('"').split(','))),
           snailmail]))
    return '\n'.join(sorted(lines_out)) + '\n'

  def test_mergecontacts(self):
    p = beam.Pipeline('DirectPipelineRunner')

    contacts_email = p | 'create_email' >>  beam.Create([self.CONTACTS_EMAIL])
    contacts_phone = p | 'create_phone' >> beam.Create([self.CONTACTS_PHONE])
    contacts_snailmail = (p | 'create_snail_mail' >>
                          beam.Create([self.CONTACTS_SNAILMAIL]))

    email = (contacts_email
             | beam.Map('backslash_email', lambda x: re.sub(r'\\', r'\\\\', x))
             | beam.Map('escape_quotes_email', lambda x: re.sub(r'"', r'\"', x))
             | beam.Map('split_email', lambda x: re.split(r'\t+', x, 1)))
    phone = (contacts_phone
             | beam.Map('backslash_phone', lambda x: re.sub(r'\\', r'\\\\', x))
             | beam.Map('escape_quotes_phone', lambda x: re.sub(r'"', r'\"', x))
             | beam.Map('split_phone', lambda x: re.split(r'\t+', x, 1)))
    snailmail = (contacts_snailmail
                 | beam.Map('backslash_snailmail',
                            lambda x: re.sub(r'\\', r'\\\\', x))
                 | beam.Map('escape_quotes_snailmail',
                            lambda x: re.sub(r'"', r'\"', x))
                 | beam.Map('split_snailmail',
                            lambda x: re.split(r'\t+', x, 1)))

    grouped = (email, phone, snailmail) | 'group_by_name' >> beam.CoGroupByKey()

    result_tsv_lines = (grouped | 'result_tsv' >> beam.Map(
        lambda (name, (email, phone, snailmail)): '\t'
        .join(['"%s"' % name,
               '"%s"' % ','.join(sorted(email.strip('"').split(','))),
               '"%s"' % ','.join(sorted(phone.strip('"').split(','))),
               '"%s"' % next(iter(snailmail), '')])))

    luddites = (grouped | 'filter_luddites' >> beam.Filter(
        lambda (name, (email, phone, snailmail)
               ): not next(iter(email), None)))
    writers = (grouped | 'filter_writers' >> beam.Filter(
        lambda (name, (email, phone, snailmail)
               ): not next(iter(phone), None)))
    nomads = (grouped | 'filter_nomads' >> beam.Filter(
        lambda (name, (email, phone, snailmail)
               ): not next(iter(snailmail), None)))

    num_luddites = luddites | 'luddites' >> beam.combiners.Count.Globally()
    num_writers = writers | 'writers' >> beam.combiners.Count.Globally()
    num_nomads = nomads | 'nomads' >> beam.combiners.Count.Globally()
    #(self.normalize_tsv_results('\n'.join(result_tsv_lines)),
    beam.assert_that(result_tsv_lines,
                     beam.equal_to([self.EXPECTED_TSV]),
                     label='assert:tag_tsv_results')
    beam.assert_that(num_luddites, beam.equal_to([self.EXPECTED_LUDDITES]),
                     label='assert:tag_num_luddites')
    beam.assert_that(num_writers, beam.equal_to([self.EXPECTED_WRITERS]),
                     label='assert:tag_num_writers')
    beam.assert_that(num_nomads, beam.equal_to([self.EXPECTED_NOMADS]),
                     label='assert:tag_num_nomads')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
