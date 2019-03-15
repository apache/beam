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

"""An integration test for datastore_write_it_pipeline

This test creates entities and writes them to Cloud Datastore. Subsequently,
these entities are read from Cloud Datastore, compared to the expected value
for the entity, and deleted.

There is no output; instead, we use `assert_that` transform to verify the
results in the pipeline.
"""

from __future__ import absolute_import

import logging
import os
import random
import sys
import unittest
from datetime import datetime

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.io.gcp import datastore_write_it_pipeline
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline


@unittest.skipIf(sys.version_info[0] == 3 and
                 os.environ.get('RUN_SKIPPED_PY3_TESTS') != '1',
                 'This test still needs to be fixed on Python 3'
                 'TODO: BEAM-4543')
class DatastoreWriteIT(unittest.TestCase):

  NUM_ENTITIES = 1001
  LIMIT = 500

  def run_datastore_write(self, limit=None):
    test_pipeline = TestPipeline(is_integration_test=True)
    current_time = datetime.now().strftime("%m%d%H%M%S")
    seed = random.randint(0, 100000)
    kind = 'testkind%s%d' % (current_time, seed)
    pipeline_verifiers = [PipelineStateMatcher()]
    extra_opts = {'kind': kind,
                  'num_entities': self.NUM_ENTITIES,
                  'on_success_matcher': all_of(*pipeline_verifiers)}
    if limit is not None:
      extra_opts['limit'] = limit

    datastore_write_it_pipeline.run(test_pipeline.get_full_options_as_args(
        **extra_opts))

  @attr('IT')
  def test_datastore_write_limit(self):
    self.run_datastore_write(limit=self.LIMIT)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
