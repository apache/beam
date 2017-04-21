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

import unittest

import apache_beam as beam
from apache_beam.utils.test_stream import *  # TODO
from apache_beam.runners.direct.direct_runner import EagerRunner
from apache_beam.runners.direct import DirectRunner
from apache_beam.transforms import window


class StreamingTest(unittest.TestCase):

  def test_test_stream(self):
    test_stream = (TestStream()
        .advance_watermark_to(10)
        .add_elements(['a', 'b', 'c'])
        .advance_watermark_to(20)
        .add_elements(['d'])
        .add_elements(['e'])
        .advance_processing_time(10)
        .advance_watermark_to(300)
        .add_elements([window.TimestampedValue('late', 12)]))
    p = beam.Pipeline(runner=DirectRunner())
    elements = p | test_stream
    groups = elements  | beam.Map(lambda x: ('k', x))| beam.WindowInto(window.FixedWindows(15)) | beam.GroupByKey()
    import sys
    class PrintFn(beam.DoFn):
      def process(self, element=beam.DoFn.ElementParam, timestamp=beam.DoFn.TimestampParam):
        sys.stderr.write('!!!!!%s [ts=%s\n' % (element, timestamp))
    groups | beam.ParDo(PrintFn())
    p.run(test_runner_api=False)


if __name__ == '__main__':
  unittest.main()
