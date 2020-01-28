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

# pytype: skip-file

from __future__ import absolute_import

import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.common import DoFnSignature
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.core import DoFn


class DoFnSignatureTest(unittest.TestCase):
  def test_dofn_validate_process_error(self):
    class MyDoFn(DoFn):
      def process(self, element, w1=DoFn.WindowParam, w2=DoFn.WindowParam):
        pass

    with self.assertRaises(ValueError):
      DoFnSignature(MyDoFn())

  def test_dofn_validate_start_bundle_error(self):
    class MyDoFn(DoFn):
      def process(self, element):
        pass

      def start_bundle(self, w1=DoFn.WindowParam):
        pass

    with self.assertRaises(ValueError):
      DoFnSignature(MyDoFn())

  def test_dofn_validate_finish_bundle_error(self):
    class MyDoFn(DoFn):
      def process(self, element):
        pass

      def finish_bundle(self, w1=DoFn.WindowParam):
        pass

    with self.assertRaises(ValueError):
      DoFnSignature(MyDoFn())


class DoFnProcessTest(unittest.TestCase):
  # pylint: disable=expression-not-assigned
  all_records = None

  def setUp(self):
    DoFnProcessTest.all_records = []

  def record_dofn(self):
    class RecordDoFn(DoFn):
      def process(self, element):
        DoFnProcessTest.all_records.append(element)

    return RecordDoFn()

  def test_dofn_process_keyparam(self):
    class DoFnProcessWithKeyparam(DoFn):
      def process(self, element, mykey=DoFn.KeyParam):
        yield "{key}-verify".format(key=mykey)

    pipeline_options = PipelineOptions()

    with TestPipeline(options=pipeline_options) as p:
      test_stream = (TestStream().advance_watermark_to(10).add_elements([1, 2]))
      (
          p
          | test_stream
          | beam.Map(lambda x: (x, "some-value"))
          | "window_into" >> beam.WindowInto(
              window.FixedWindows(5),
              accumulation_mode=trigger.AccumulationMode.DISCARDING)
          | beam.ParDo(DoFnProcessWithKeyparam())
          | beam.ParDo(self.record_dofn()))

    self.assertEqual(['1-verify', '2-verify'],
                     sorted(DoFnProcessTest.all_records))

  def test_dofn_process_keyparam_error_no_key(self):
    class DoFnProcessWithKeyparam(DoFn):
      def process(self, element, mykey=DoFn.KeyParam):
        yield "{key}-verify".format(key=mykey)

    pipeline_options = PipelineOptions()
    with self.assertRaises(ValueError),\
         TestPipeline(options=pipeline_options) as p:
      test_stream = (TestStream().advance_watermark_to(10).add_elements([1, 2]))
      (p | test_stream | beam.ParDo(DoFnProcessWithKeyparam()))


if __name__ == '__main__':
  unittest.main()
