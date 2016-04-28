# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for worker environment compatibility checking."""

import logging
import os
import tempfile
import unittest

from google.cloud.dataflow import version
from google.cloud.dataflow.worker import environment


class EnvironmentTest(unittest.TestCase):

  def create_temp_file(self, path, contents):
    with open(path, 'w') as f:
      f.write(contents)
      return f.name

  def test_basics(self):
    config_path = os.path.join(tempfile.mkdtemp(), 'config')
    self.create_temp_file(
        config_path,
        '{"language":"python", "version": "%s"}' % version.__version__)
    environment.check_sdk_compatibility(config_path)
    # If we get here the test passes since no exception was raised.

  def test_language_no_match(self):
    config_path = os.path.join(tempfile.mkdtemp(), 'config')
    self.create_temp_file(
        config_path,
        '{"language":"java", "version": "%s"}' % version.__version__)
    with self.assertRaises(RuntimeError) as exn:
      environment.check_sdk_compatibility(config_path)
    self.assertEqual(
        'SDK language \'python\' does not match container language \'java\'. '
        'Please rebuild the container using a matching language container.',
        exn.exception.message)

  def test_version_no_match(self):
    config_path = os.path.join(tempfile.mkdtemp(), 'config')
    self.create_temp_file(
        config_path, '{"language":"python", "version": "0.0.0"}')
    with self.assertRaises(RuntimeError) as exn:
      environment.check_sdk_compatibility(config_path)
    self.assertEqual(
        'SDK version %s does not match container version 0.0.0. '
        'Please rebuild the container or use a matching version '
        'of the SDK.' % (
            version.__version__),
        exn.exception.message)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()

