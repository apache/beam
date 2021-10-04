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

"""Integration tests for interactive beam."""
# pytype: skip-file

import unittest

import pytest

from apache_beam.runners.interactive.testing.integration.screen_diff import BaseTestCase


@pytest.mark.timeout(300)
class DataFramesTest(BaseTestCase):
  def __init__(self, *args, **kwargs):
    kwargs['golden_size'] = (1024, 10000)
    super().__init__(*args, **kwargs)

  def explicit_wait(self):
    try:
      from selenium.webdriver.common.by import By
      from selenium.webdriver.support import expected_conditions
      from selenium.webdriver.support.ui import WebDriverWait

      WebDriverWait(self.driver, 5).until(
          expected_conditions.presence_of_element_located((By.ID, 'test-done')))
    # pylint: disable=bare-except
    except:
      pass  # The test will be ignored.

  def test_dataframes(self):
    self.assert_notebook('dataframes')


@pytest.mark.timeout(300)
class InitSquareCubeTest(BaseTestCase):
  def __init__(self, *args, **kwargs):
    kwargs['golden_size'] = (1024, 10000)
    super().__init__(*args, **kwargs)

  def test_init_square_cube_notebook(self):
    self.assert_notebook('init_square_cube')


if __name__ == '__main__':
  unittest.main()
