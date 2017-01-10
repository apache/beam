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
import unittest

from datetime import datetime as dt
from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.examples import wordcount
from apache_beam.test_pipeline import TestPipeline
from apache_beam.tests.pipeline_verifiers import PipelineStateMatcher
from apache_beam.tests.pipeline_verifiers import FileChecksumMatcher


class WordCountIT(unittest.TestCase):

  DEFAULT_CHECKSUM = 'c780e9466b8635af1d11b74bbd35233a82908a02'

  @attr('IT')
  def test_wordcount_it(self):
    test_pipeline = TestPipeline(is_integration_test=True)

    # Set extra options to the pipeline for test purpose
    output = '/'.join([test_pipeline.get_option('output'),
                       dt.now().strftime('py-wordcount-%Y-%m-%d-%H-%M-%S'),
                       'results'])
    pipeline_verifiers = [PipelineStateMatcher(),
                          FileChecksumMatcher(output + '*-of-*',
                                              self.DEFAULT_CHECKSUM)]
    extra_opts = {'output': output,
                  'on_success_matcher': all_of(*pipeline_verifiers)}

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    wordcount.run(test_pipeline.get_full_options_as_args(**extra_opts))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
