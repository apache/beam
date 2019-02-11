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

"""End-to-end test for Datastore Wordcount example."""

from __future__ import absolute_import

import logging
import os
import sys
import time
import unittest

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.examples.cookbook import datastore_wordcount
from apache_beam.testing.pipeline_verifiers import FileChecksumMatcher
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline


@unittest.skipIf(sys.version_info[0] == 3 and
                 os.environ.get('RUN_SKIPPED_PY3_TESTS') != '1',
                 'This test still needs to be fixed on Python 3'
                 'TODO: BEAM-4543')
class DatastoreWordCountIT(unittest.TestCase):

  DATASTORE_WORDCOUNT_KIND = "DatastoreWordCount"
  EXPECTED_CHECKSUM = '826f69ed0275858c2e098f1e8407d4e3ba5a4b3f'

  @attr('IT')
  def test_datastore_wordcount_it(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    dataset = test_pipeline.get_option("project")
    kind = self.DATASTORE_WORDCOUNT_KIND
    output = '/'.join([test_pipeline.get_option('output'),
                       str(int(time.time() * 1000)),
                       'datastore_wordcount_results'])

    arg_sleep_secs = test_pipeline.get_option('sleep_secs')
    sleep_secs = int(arg_sleep_secs) if arg_sleep_secs is not None else None
    pipeline_verifiers = [PipelineStateMatcher(),
                          FileChecksumMatcher(output + '*-of-*',
                                              self.EXPECTED_CHECKSUM,
                                              sleep_secs)]
    extra_opts = {'dataset': dataset,
                  'kind': kind,
                  'output': output,
                  'read_only': True,
                  'on_success_matcher': all_of(*pipeline_verifiers)}

    datastore_wordcount.run(test_pipeline.get_full_options_as_args(
        **extra_opts))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
