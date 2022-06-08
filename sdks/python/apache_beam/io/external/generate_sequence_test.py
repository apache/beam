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

import logging
import os
import re
import unittest

import pytest

from apache_beam.io.external.generate_sequence import GenerateSequence
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import JavaClassLookupPayloadBuilder
from apache_beam.transforms.external import JavaExternalTransform


@pytest.mark.uses_java_expansion_service
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

        assert_that(res, equal_to(list(range(1, 10))))
    except RuntimeError as e:
      if re.search(GenerateSequence.URN, str(e)):
        print("looks like URN not implemented in expansion service, skipping.")
      else:
        raise e

  def test_generate_sequence_java_class_lookup_payload_builder(self):
    port = os.environ.get('EXPANSION_PORT')
    address = 'localhost:%s' % port

    with TestPipeline() as p:
      payload_builder = JavaClassLookupPayloadBuilder(
          'org.apache.beam.sdk.io.GenerateSequence')
      payload_builder.with_constructor_method('from', 1)
      payload_builder.add_builder_method('to', 10)

      res = (
          p
          | ExternalTransform(None, payload_builder, expansion_service=address))
      assert_that(res, equal_to(list(range(1, 10))))

  def test_generate_sequence_java_external_transform(self):
    port = os.environ.get('EXPANSION_PORT')
    address = 'localhost:%s' % port

    with TestPipeline() as p:
      java_transform = JavaExternalTransform(
          'org.apache.beam.sdk.io.GenerateSequence', expansion_service=address)
      # We have to use 'getattr' below for builder method 'from' of Java
      # 'GenerateSequence' class since 'from' is a reserved keyword for Python.
      java_transform = getattr(java_transform, 'from')(1).to(10)
      res = (p | java_transform)

      assert_that(res, equal_to(list(range(1, 10))))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
