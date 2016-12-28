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

from apache_beam.examples import wordcount
from apache_beam.test_pipeline import TestPipeline
from apache_beam.tests.pipeline_verifiers import PipelineStateMatcher
from nose.plugins.attrib import attr


class WordCountIT(unittest.TestCase):

  @attr('IT')
  def test_wordcount_it(self):
    # Set extra options to the pipeline for test purpose
    extra_opts = {'on_success_matcher': PipelineStateMatcher()}

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    test_pipeline = TestPipeline(is_it=True)
    wordcount.run(test_pipeline.get_test_option_args(**extra_opts))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
