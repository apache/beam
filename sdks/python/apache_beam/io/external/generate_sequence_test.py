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

"""Unit tests for cross-language generate sequence."""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import print_function

import logging
import os
import re
import unittest

from nose.plugins.attrib import attr

from apache_beam.io.external.generate_sequence import GenerateSequence
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


@attr('UsesCrossLanguageTransforms')
@unittest.skipUnless(
    os.environ.get('EXPANSION_PORT'),
    "EXPANSION_PORT environment var is not provided.")
class XlangGenerateSequenceTest(unittest.TestCase):
  def test_generate_sequence(self):
    port = os.environ.get('EXPANSION_PORT')
    address = 'localhost:%s' % port

    try:
      with TestPipeline() as p:
        res = (
            p
            | GenerateSequence(start=1, stop=10, expansion_service=address))

        assert_that(res, equal_to([i for i in range(1, 10)]))
    except RuntimeError as e:
      if re.search(GenerateSequence.URN, str(e)):
        print("looks like URN not implemented in expansion service, skipping.")
      else:
        raise e


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
