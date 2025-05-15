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

import argparse
import logging
import sys

from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.runners.dataflow import dataflow_runner
from apache_beam.runners.portability import local_job_service
from apache_beam.runners.portability import local_job_service_main
from apache_beam.runners.portability import portable_runner


class DataflowBeamJob(local_job_service.BeamJob):
  """A representation of a single Beam job to be run on the Dataflow runner.
  """
  def _invoke_runner(self):
    """Actually calls Dataflow and waits for completion.
    """
    runner = dataflow_runner.DataflowRunner()
    self.result = runner.run_pipeline(
        None, self.pipeline_options(), self._pipeline_proto)
    # The result can be None if there is no need to send a request
    # to the service (e.g. template creation).
    if not getattr(self.result, 'has_job', None):
      self.set_state(beam_job_api_pb2.JobState.DONE)
      return self.result
    # Prefer this to result.wait_until_finish() to get state updates
    # and avoid creating an extra thread (which also messes with logging).
    dataflow_runner.DataflowRunner.poll_for_job_completion(
        runner,
        self.result,
        None, lambda dataflow_state: self.set_state(
            portable_runner.PipelineResult.pipeline_state_to_runner_api_state(
                self.result.api_jobstate_to_pipeline_state(dataflow_state))))
    return self.result

  def cancel(self):
    if not self.is_terminal_state(self.state):
      self.result.cancel()


def run(argv, beam_job_type=DataflowBeamJob):
  if argv[0] == __file__:
    argv = argv[1:]
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '-p',
      '--port',
      '--job_port',
      type=int,
      default=0,
      help='port on which to serve the job api')
  parser.add_argument('--staging_dir')
  options = parser.parse_args(argv)

  job_servicer = local_job_service.LocalJobServicer(
      options.staging_dir, beam_job_type=beam_job_type)
  port = job_servicer.start_grpc_server(options.port)
  try:
    local_job_service_main.serve("Listening for beam jobs on port %d." % port)
  finally:
    job_servicer.stop()


if __name__ == '__main__':
  logging.basicConfig()
  logging.getLogger().setLevel(logging.INFO)
  run(sys.argv)
