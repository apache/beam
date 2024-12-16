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

import logging
import uuid
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.io.textio import WriteToText
from apache_beam.transforms.window import FixedWindows

"""End-to-End tests for iobase

Usage:

    cd sdks/python
    pip install build && python -m build --sdist

    DataflowRunner:

    python -m pytest -o log_cli=True -o log_level=Info \
        apache_beam/io/iobase_it_test.py::IOBaseITTest \
        --test-pipeline-options="--runner=TestDataflowRunner \
        --project=apache-beam-testing --region=us-central1 \
        --temp_location=gs://apache-beam-testing-temp/temp \
        --sdk_location=dist/apache_beam-2.62.0.dev0.tar.gz"
"""


class IOBaseITTest(unittest.TestCase):
  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__

  def test_unbounded_pcoll_without_gloabl_window(self):
    # https://github.com/apache/beam/issues/25598

    args = self.test_pipeline.get_full_options_as_args(
        streaming=True,
    )

    topic = 'projects/pubsub-public-data/topics/taxirides-realtime'
    unique_id = str(uuid.uuid4())
    output_file = f'gs://apache-beam-testing-integration-testing/iobase/test-{unique_id}'

    p = beam.Pipeline(argv=args)
    # Read from Pub/Sub with fixed windowing
    lines = (
        p
        | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=topic)
        | "WindowInto" >> beam.WindowInto(FixedWindows(10)))

    # Write to text file
    lines | 'WriteToText' >> WriteToText(output_file)

    result = p.run()
    result.wait_until_finish(duration=60 * 1000)


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
