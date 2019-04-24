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

"""Unit tests for cross-language parquet io read/write."""

from __future__ import absolute_import
from __future__ import print_function

import logging
import os
import re
import unittest

from nose.plugins.attrib import attr
from past.builtins import unicode

import apache_beam as beam
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

KAFKA_WRITE_URN = "beam:transforms:xlang:kafka_write"
KAFKA_READ_URN = "beam:transforms:xlang:kafka_read"


@attr('UsesCrossLanguageTransforms')
class XlangKafkaIOTest(unittest.TestCase):
  def test_write_and_read(self):
    expansion_jar = os.environ.get('EXPANSION_JAR')
    if not expansion_jar:
      print("EXPANSION_JAR environment variable is not set.")
      return

    read_pipeline = TestPipeline(blocking=False)
    read_pipeline.get_pipeline_options().view_as(
        DebugOptions).experiments.append('jar_packages='+expansion_jar)

    port = read_pipeline.get_option("expansion_port")
    if not port:
      print("--expansion_port is not provided. skipping.")
      return
    address = 'localhost:%s' % port

    try:
      read = read_pipeline \
        | beam.ExternalTransform(
            KAFKA_READ_URN,
            b'localhost:9092', address)

      assert_that(read, equal_to(['abc', 'def', 'ghi']))

      read_result = read_pipeline.run(test_runner_api=False)

      write_pipeline = TestPipeline()
      write_pipeline.get_pipeline_options().view_as(
          DebugOptions).experiments.append('jar_packages='+expansion_jar)
      write_pipeline.not_use_test_runner_api = True

      with write_pipeline as p:
        _ = p \
          | beam.Create(['abc', 'def', 'ghi']).with_output_types(unicode) \
          | beam.ExternalTransform(
              KAFKA_WRITE_URN,
              b'localhost:9092', address)

      read_result.wait_until_finish()

    except RuntimeError as e:
      if re.search(
          '{}|{}'.format(KAFKA_WRITE_URN, KAFKA_READ_URN), str(e)):
        print("looks like URN not implemented in expansion service, skipping.")
      else:
        raise e


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
