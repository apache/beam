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

# pytype: skip-file

from __future__ import absolute_import

import logging
import time
import unittest

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.testing.pipeline_verifiers import FileChecksumMatcher
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline

# Don't fail during pytest collection scan.
try:
  from apache_beam.examples.cookbook import datastore_wordcount
except ImportError:
  datastore_wordcount = None


class DatastoreWordCountIT(unittest.TestCase):

  DATASTORE_WORDCOUNT_KIND = "DatastoreWordCount"
  EXPECTED_CHECKSUM = '826f69ed0275858c2e098f1e8407d4e3ba5a4b3f'

  @attr('IT')
  def test_datastore_wordcount_it(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    kind = self.DATASTORE_WORDCOUNT_KIND
    output = '/'.join([
        test_pipeline.get_option('output'),
        str(int(time.time() * 1000)),
        'datastore_wordcount_results'
    ])

    arg_sleep_secs = test_pipeline.get_option('sleep_secs')
    sleep_secs = int(arg_sleep_secs) if arg_sleep_secs is not None else None
    pipeline_verifiers = [
        PipelineStateMatcher(),
        FileChecksumMatcher(
            output + '*-of-*', self.EXPECTED_CHECKSUM, sleep_secs)
    ]
    extra_opts = {
        'kind': kind,
        'output': output,
        # Comment this out to regenerate input data on Datastore (delete
        # existing data first using the bulk delete Dataflow template).
        'read_only': True,
        'on_success_matcher': all_of(*pipeline_verifiers)
    }

    datastore_wordcount.run(
        test_pipeline.get_full_options_as_args(**extra_opts))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
