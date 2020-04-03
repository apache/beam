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

"""End-to-end test for the wordcount example."""

# pytype: skip-file

from __future__ import absolute_import

import logging
import time
import unittest

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.examples import wordcount
from apache_beam.testing.pipeline_verifiers import FileChecksumMatcher
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import delete_files


class WordCountIT(unittest.TestCase):

  # Enable nose tests running in parallel
  _multiprocess_can_split_ = True

  # The default checksum is a SHA-1 hash generated from a sorted list of
  # lines read from expected output. This value corresponds to the default
  # input of WordCount example.
  DEFAULT_CHECKSUM = '33535a832b7db6d78389759577d4ff495980b9c0'

  @attr('IT')
  def test_wordcount_it(self):
    self._run_wordcount_it(wordcount.run)

  @attr('IT', 'ValidatesContainer')
  def test_wordcount_fnapi_it(self):
    self._run_wordcount_it(wordcount.run, experiment='beam_fn_api')

  def _run_wordcount_it(self, run_wordcount, **opts):
    test_pipeline = TestPipeline(is_integration_test=True)
    extra_opts = {}

    # Set extra options to the pipeline for test purpose
    test_output = '/'.join([
        test_pipeline.get_option('output'),
        str(int(time.time() * 1000)),
        'results'
    ])
    extra_opts['output'] = test_output

    test_input = test_pipeline.get_option('input')
    if test_input:
      extra_opts['input'] = test_input

    arg_sleep_secs = test_pipeline.get_option('sleep_secs')
    sleep_secs = int(arg_sleep_secs) if arg_sleep_secs is not None else None
    expect_checksum = (
        test_pipeline.get_option('expect_checksum') or self.DEFAULT_CHECKSUM)
    pipeline_verifiers = [
        PipelineStateMatcher(),
        FileChecksumMatcher(
            test_output + '*-of-*', expect_checksum, sleep_secs)
    ]
    extra_opts['on_success_matcher'] = all_of(*pipeline_verifiers)
    extra_opts.update(opts)

    # Register clean up before pipeline execution
    self.addCleanup(delete_files, [test_output + '*'])

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    run_wordcount(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
