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

import logging
import os
import shutil
import tempfile
import unittest
import uuid

import pytest

import apache_beam as beam
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

_LOGGER = logging.getLogger(__name__)


class BeamPluginsIT(unittest.TestCase):
  def setUp(self):
    self.temp_dir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.temp_dir)

  @pytest.mark.it_postcommit
  def test_beam_plugins_staging(self):
    pipeline = TestPipeline(is_integration_test=True)
    setup_options = pipeline.options.view_as(SetupOptions)

    plugin_name = f'beam_integration_plugin_{uuid.uuid4().hex[:8]}'
    plugin_file_path = os.path.join(self.temp_dir, f'{plugin_name}.py')

    with open(plugin_file_path, 'w') as f:
      f.write("import sys\nsys.beam_plugin_loaded_for_test = True\n")

    staged_files = setup_options.files_to_stage or []
    staged_files.append(plugin_file_path)
    setup_options.files_to_stage = staged_files
    setup_options.beam_plugins = [plugin_name]

    def check_plugin_loaded(_):
      import sys
      return getattr(sys, 'beam_plugin_loaded_for_test', False)

    with pipeline as p:
      res = (p | beam.Create([None]) | beam.Map(check_plugin_loaded))
      assert_that(res, equal_to([True]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
