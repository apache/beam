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
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.dataflow import dataflow_job_service
from apache_beam.runners.portability import local_job_service

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.runners.dataflow.internal import apiclient
except ImportError:
  apiclient = None  # type: ignore
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(apiclient is None, 'GCP dependencies are not installed')
class DirectPipelineResultTest(unittest.TestCase):
  def test_dry_run(self):
    # Not an integration test that actually runs on Dataflow,
    # but does exercise (most of) the translation and setup code,
    # as well as the connection.
    job_servicer = local_job_service.LocalJobServicer(
        None, beam_job_type=dataflow_job_service.DataflowBeamJob)
    port = job_servicer.start_grpc_server(0)
    try:
      options = PipelineOptions(
          runner='PortableRunner',
          job_endpoint=f'localhost:{port}',
          project='some_project',
          temp_location='gs://bucket/dir',
          region='us-central1',
          dry_run=True,
      )
      with beam.Pipeline(options=options) as p:
        _ = p | beam.Create([1, 2, 3]) | beam.Map(lambda x: x * x)
    finally:
      job_servicer.stop()


if __name__ == '__main__':
  unittest.main()
