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

"""Unit tests for the windowing classes."""

# pytype: skip-file

from __future__ import absolute_import

import unittest

from apache_beam import coders
from apache_beam.runners import pipeline_context


class PipelineContextTest(unittest.TestCase):
  def test_deduplication(self):
    context = pipeline_context.PipelineContext()
    bytes_coder_ref = context.coders.get_id(coders.BytesCoder())
    bytes_coder_ref2 = context.coders.get_id(coders.BytesCoder())
    self.assertEqual(bytes_coder_ref, bytes_coder_ref2)

  def test_deduplication_by_proto(self):
    context = pipeline_context.PipelineContext()
    bytes_coder_proto = coders.BytesCoder().to_runner_api(None)
    bytes_coder_ref = context.coders.get_by_proto(bytes_coder_proto)
    bytes_coder_ref2 = context.coders.get_by_proto(
        bytes_coder_proto, deduplicate=True)
    self.assertEqual(bytes_coder_ref, bytes_coder_ref2)

  def test_serialization(self):
    context = pipeline_context.PipelineContext()
    float_coder_ref = context.coders.get_id(coders.FloatCoder())
    bytes_coder_ref = context.coders.get_id(coders.BytesCoder())
    proto = context.to_runner_api()
    context2 = pipeline_context.PipelineContext.from_runner_api(proto)
    self.assertEqual(
        coders.FloatCoder(), context2.coders.get_by_id(float_coder_ref))
    self.assertEqual(
        coders.BytesCoder(), context2.coders.get_by_id(bytes_coder_ref))

  def test_common_id_assignment(self):
    context = pipeline_context.PipelineContext()
    float_coder_ref = context.coders.get_id(coders.FloatCoder())
    bytes_coder_ref = context.coders.get_id(coders.BytesCoder())
    context2 = pipeline_context.PipelineContext(
        component_id_map=context.component_id_map)

    bytes_coder_ref2 = context2.coders.get_id(coders.BytesCoder())
    float_coder_ref2 = context2.coders.get_id(coders.FloatCoder())

    self.assertEqual(bytes_coder_ref, bytes_coder_ref2)
    self.assertEqual(float_coder_ref, float_coder_ref2)


if __name__ == '__main__':
  unittest.main()
