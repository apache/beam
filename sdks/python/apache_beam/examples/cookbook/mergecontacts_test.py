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

# pytype: skip-file

import logging
import unittest
import uuid

import pytest

from apache_beam.examples.cookbook import mergecontacts
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import create_file
from apache_beam.testing.test_utils import read_files_from_pattern


class MergeContactsTest(unittest.TestCase):

  CONTACTS_EMAIL = '\n'.join([
      'Nathan Nomad\tnathan@example.com',
      'Nicky Nomad\tnicky@example.com',
      'Noreen Nomad\tnoreen@example.com',
      'Noreen Nomad\tnomad@example.com',
      'Robert B\trobert@example.com',
      'Silviu C\tsilviu@example.com',
      'Tom S\ttom@example.com',
      'Wally Writer\twally@example.com',
      ''
  ])

  CONTACTS_PHONE = '\n'.join([
      'Larry Luddite\t724-228-3529',
      'Lisa Luddite\t304-277-3504',
      'Nathan Nomad\t412-466-8968',
      'Nicky Nomad\t724-379-5815',
      'Noreen Nomad\t412-472-0145',
      'Robert B\t814-865-8799',
      'Silviu C\t724-537-0671',
      'Tom S\t570-368-3420',
      'Tom S\t814-793-9655',
      ''
  ])

  CONTACTS_SNAILMAIL = '\n'.join([
      'Larry Luddite\t1949 Westcott St, Detroit, MI 48222',
      'Lisa Luddite\t1949 Westcott St, Detroit, MI 48222',
      'Robert B\t601 N 34th St, Seattle, WA 98103',
      'Silviu C\t1600 Amphitheatre Pkwy, Mountain View, CA 94043',
      'Tom S\t6425 Penn Ave Ste 700, Pittsburgh, PA 15206',
      'Wally Writer\t101 Ridge Rd, Chalkyitsik, AK 99788',
      ''
  ])

  EXPECTED_TSV = '\n'.join([
      '\t'.join([
          '"Larry Luddite"',
          '""',
          '"724-228-3529"',
          '"1949 Westcott St, Detroit, MI 48222"'
      ]),
      '\t'.join([
          '"Lisa Luddite"',
          '""',
          '"304-277-3504"',
          '"1949 Westcott St, Detroit, MI 48222"'
      ]),
      '\t'.join(
          ['"Nathan Nomad"', '"nathan@example.com"', '"412-466-8968"', '""']),
      '\t'.join(
          ['"Nicky Nomad"', '"nicky@example.com"', '"724-379-5815"', '""']),
      '\t'.join([
          '"Noreen Nomad"',
          '"nomad@example.com,noreen@example.com"',
          '"412-472-0145"',
          '""'
      ]),
      '\t'.join([
          '"Robert B"',
          '"robert@example.com"',
          '"814-865-8799"',
          '"601 N 34th St, Seattle, WA 98103"'
      ]),
      '\t'.join([
          '"Silviu C"',
          '"silviu@example.com"',
          '"724-537-0671"',
          '"1600 Amphitheatre Pkwy, Mountain View, CA 94043"'
      ]),
      '\t'.join([
          '"Tom S"',
          '"tom@example.com"',
          '"570-368-3420,814-793-9655"',
          '"6425 Penn Ave Ste 700, Pittsburgh, PA 15206"'
      ]),
      '\t'.join([
          '"Wally Writer"',
          '"wally@example.com"',
          '""',
          '"101 Ridge Rd, Chalkyitsik, AK 99788"'
      ]),
      ''
  ])

  EXPECTED_STATS = '\n'.join(['2 luddites', '1 writers', '3 nomads', ''])

  def normalize_tsv_results(self, tsv_data):
    """Sort .tsv file data so we can compare it with expected output."""
    lines_in = tsv_data.strip().split('\n')
    lines_out = []
    for line in lines_in:
      name, email, phone, snailmail = line.split('\t')
      lines_out.append(
          '\t'.join([
              name,
              '"%s"' % ','.join(sorted(email.strip('"').split(','))),
              '"%s"' % ','.join(sorted(phone.strip('"').split(','))),
              snailmail
          ]))
    return '\n'.join(sorted(lines_out)) + '\n'

  @pytest.mark.examples_postcommit
  @pytest.mark.sickbay_flink
  def test_mergecontacts(self):
    test_pipeline = TestPipeline(is_integration_test=True)

    # Setup the files with expected content.
    temp_location = test_pipeline.get_option('temp_location')
    input_folder = '/'.join([temp_location, str(uuid.uuid4())])
    path_email = create_file(
        '/'.join([input_folder, 'path_email.txt']), self.CONTACTS_EMAIL)
    path_phone = create_file(
        '/'.join([input_folder, 'path_phone.txt']), self.CONTACTS_PHONE)
    path_snailmail = create_file(
        '/'.join([input_folder, 'path_snailmail.txt']), self.CONTACTS_SNAILMAIL)

    result_prefix = '/'.join([temp_location, str(uuid.uuid4()), 'result'])
    extra_opts = {
        'input_email': path_email,
        'input_phone': path_phone,
        'input_snailmail': path_snailmail,
        'output_tsv': '%s.tsv' % result_prefix,
        'output_stats': '%s.stats' % result_prefix
    }

    pipeline_opts = test_pipeline.get_full_options_as_args(**extra_opts)
    # Prevent ambiguous option error between output in
    # args and expected output_tsv and output_stats
    output_arg = [i for i in pipeline_opts if i.startswith('--output=')]
    if output_arg:
      pipeline_opts.remove(output_arg[0])
    mergecontacts.run(
        pipeline_opts, assert_results=(2, 1, 3), save_main_session=False)

    contents = read_files_from_pattern('%s*' % result_prefix)
    self.assertEqual(self.EXPECTED_TSV, self.normalize_tsv_results(contents))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
