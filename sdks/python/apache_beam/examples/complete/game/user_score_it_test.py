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

"""End-to-end test for the user score example.

Code: beam/sdks/python/apache_beam/examples/complete/game/user_score.py
Usage:

  python setup.py nosetests --test-pipeline-options=" \
      --runner=TestDataflowRunner \
      --project=... \
      --region=... \
      --staging_location=gs://... \
      --temp_location=gs://... \
      --output=gs://... \
      --sdk_location=... \

"""

# pytype: skip-file

from __future__ import absolute_import

import logging
import unittest
import uuid

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.examples.complete.game import user_score
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.pipeline_verifiers import FileChecksumMatcher
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import delete_files


class UserScoreIT(unittest.TestCase):

  DEFAULT_INPUT_FILE = 'gs://dataflow-samples/game/gaming_data*'
  DEFAULT_EXPECTED_CHECKSUM = '9f3bd81669607f0d98ec80ddd477f3277cfba0a2'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.uuid = str(uuid.uuid4())

    self.output = '/'.join(
        [self.test_pipeline.get_option('output'), self.uuid, 'results'])

  @attr('IT')
  def test_user_score_it(self):

    state_verifier = PipelineStateMatcher(PipelineState.DONE)
    arg_sleep_secs = self.test_pipeline.get_option('sleep_secs')
    sleep_secs = int(arg_sleep_secs) if arg_sleep_secs is not None else None
    file_verifier = FileChecksumMatcher(
        self.output + '/*-of-*', self.DEFAULT_EXPECTED_CHECKSUM, sleep_secs)

    extra_opts = {
        'input': self.DEFAULT_INPUT_FILE,
        'output': self.output + '/user-score',
        'on_success_matcher': all_of(state_verifier, file_verifier)
    }

    # Register clean up before pipeline execution
    self.addCleanup(delete_files, [self.output + '*'])

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    user_score.run(
        self.test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
