# coding=utf-8
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

from __future__ import absolute_import
from __future__ import print_function

import platform
import sys
import unittest

import mock

from apache_beam.examples.snippets.transforms.element_wise.pardo import *
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


def check_plants(actual):
  # [START plants]
  plants = [
      '🍓Strawberry',
      '🥕Carrot',
      '🍆Eggplant',
      '🍅Tomato',
      '🥔Potato',
  ]
  # [END plants]
  assert_that(actual, equal_to(plants))


def check_dofn_params(actual):
  # pylint: disable=line-too-long
  # [START dofn_params]
  dofn_params = '''\
# timestamp
type(timestamp) -> <class 'apache_beam.utils.timestamp.Timestamp'>
timestamp.micros -> 1584675660000000
timestamp.to_rfc3339() -> '2020-03-20T03:41:00Z'
timestamp.to_utc_datetime() -> datetime.datetime(2020, 3, 20, 3, 41)

# window
type(window) -> <class 'apache_beam.transforms.window.IntervalWindow'>
window.start -> Timestamp(1584675660) (2020-03-20 03:41:00)
window.end -> Timestamp(1584675690) (2020-03-20 03:41:30)
window.max_timestamp() -> Timestamp(1584675689.999999) (2020-03-20 03:41:29.999999)'''
  # [END dofn_params]
  # pylint: enable=line-too-long
  assert_that(actual, equal_to([dofn_params]))


@mock.patch('apache_beam.Pipeline', TestPipeline)
# pylint: disable=line-too-long
@mock.patch('apache_beam.examples.snippets.transforms.element_wise.pardo.print', lambda elem: elem)
# pylint: enable=line-too-long
class ParDoTest(unittest.TestCase):
  def test_pardo_dofn(self):
    pardo_dofn(check_plants)

  @unittest.skipIf(sys.version_info[0] < 3 and platform.system() == 'Windows',
                   'Python 2 on Windows uses `long` rather than `int`')
  def test_pardo_dofn_params(self):
    pardo_dofn_params(check_dofn_params)


if __name__ == '__main__':
  unittest.main()
