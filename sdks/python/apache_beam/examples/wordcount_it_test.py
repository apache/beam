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

import logging
import time
import unittest

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.examples import wordcount
from apache_beam.examples import wordcount_fnapi
from apache_beam.testing.pipeline_verifiers import FileChecksumMatcher
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import delete_files


class WordCountIT(unittest.TestCase):

  # Enable nose tests running in parallel
  _multiprocess_can_split_ = True

  # The default checksum is a SHA-1 hash generated from a sorted list of
  # lines read from expected output.
  DEFAULT_CHECKSUM = '33535a832b7db6d78389759577d4ff495980b9c0'

  @attr('IT')
  def test_wordcount_it(self):
    test_pipeline = TestPipeline(is_integration_test=True)

    # Set extra options to the pipeline for test purpose
    output = '/'.join([test_pipeline.get_option('output'),
                       str(int(time.time())),
                       'results'])
    arg_sleep_secs = test_pipeline.get_option('sleep_secs')
    sleep_secs = int(arg_sleep_secs) if arg_sleep_secs is not None else None
    pipeline_verifiers = [PipelineStateMatcher(),
                          FileChecksumMatcher(output + '*-of-*',
                                              self.DEFAULT_CHECKSUM,
                                              sleep_secs)]
    extra_opts = {'output': output,
                  'on_success_matcher': all_of(*pipeline_verifiers)}

    # Register clean up before pipeline execution
    self.addCleanup(delete_files, [output + '*'])

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    wordcount.run(test_pipeline.get_full_options_as_args(**extra_opts))

  @attr('IT', 'ValidatesContainer')
  def test_wordcount_fnapi_it(self):
    test_pipeline = TestPipeline(is_integration_test=True)

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    wordcount_fnapi.run(
        test_pipeline.get_full_options_as_args(
            experiment='beam_fn_api',
            on_success_matcher=PipelineStateMatcher()))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
