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

"""A job server submitting portable pipelines as uber jars to Flink."""

from __future__ import absolute_import
from __future__ import print_function

import json
import logging
import os
import shutil
import tempfile
import time
import zipfile
from concurrent import futures

import grpc
import requests
from google.protobuf import json_format

from apache_beam.options import pipeline_options
from apache_beam.portability.api import beam_artifact_api_pb2_grpc
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.portability import abstract_job_service
from apache_beam.runners.portability import artifact_service
from apache_beam.runners.portability import job_server

_LOGGER = logging.getLogger(__name__)


class FlinkUberJarJobServer(abstract_job_service.AbstractJobServiceServicer):
  """A Job server which submits a self-contained Jar to a Flink cluster.

  The jar contains the Beam pipeline definition, dependencies, and
  the pipeline artifacts.
  """

  def __init__(self, master_url, options):
    super(FlinkUberJarJobServer, self).__init__()
    self._master_url = master_url
    self._executable_jar = (options.view_as(pipeline_options.FlinkRunnerOptions)
                            .flink_job_server_jar)
    self._artifact_port = (options.view_as(pipeline_options.JobServerOptions)
                           .artifact_port)
    self._temp_dir = tempfile.mkdtemp(prefix='apache-beam-flink')

  def start(self):
    return self

  def stop(self):
    pass

  def executable_jar(self):
    url = (self._executable_jar or
           job_server.JavaJarJobServer.path_to_beam_jar(
               'runners:flink:%s:job-server:shadowJar' % self.flink_version()))
    return job_server.JavaJarJobServer.local_jar(url)

  def flink_version(self):
    full_version = requests.get(
        '%s/v1/config' % self._master_url).json()['flink-version']
    # Only return up to minor version.
    return '.'.join(full_version.split('.')[:2])

  def create_beam_job(self, job_id, job_name, pipeline, options):
    return FlinkBeamJob(
        self._master_url,
        self.executable_jar(),
        job_id,
        job_name,
        pipeline,
        options,
        artifact_port=self._artifact_port)


class FlinkBeamJob(abstract_job_service.AbstractBeamJob):
  """Runs a single Beam job on Flink by staging all contents into a Jar
  and uploading it via the Flink Rest API."""

  # These must agree with those defined in PortablePipelineJarUtils.java.
  PIPELINE_FOLDER = 'BEAM-PIPELINE'
  PIPELINE_MANIFEST = PIPELINE_FOLDER + '/pipeline-manifest.json'

  # We only stage a single pipeline in the jar.
  PIPELINE_NAME = 'pipeline'
  PIPELINE_PATH = '/'.join(
      [PIPELINE_FOLDER, PIPELINE_NAME, "pipeline.json"])
  PIPELINE_OPTIONS_PATH = '/'.join(
      [PIPELINE_FOLDER, PIPELINE_NAME, 'pipeline-options.json'])
  ARTIFACT_MANIFEST_PATH = '/'.join(
      [PIPELINE_FOLDER, PIPELINE_NAME, 'artifact-manifest.json'])
  ARTIFACT_FOLDER = '/'.join([PIPELINE_FOLDER, PIPELINE_NAME, 'artifacts'])

  def __init__(
      self, master_url, executable_jar, job_id, job_name, pipeline, options,
      artifact_port=0):
    super(FlinkBeamJob, self).__init__(job_id, job_name, pipeline, options)
    self._master_url = master_url
    self._executable_jar = executable_jar
    self._jar_uploaded = False
    self._artifact_port = artifact_port

  def prepare(self):
    # Copy the executable jar, injecting the pipeline and options as resources.
    with tempfile.NamedTemporaryFile(suffix='.jar') as tout:
      self._jar = tout.name
    shutil.copy(self._executable_jar, self._jar)
    with zipfile.ZipFile(self._jar, 'a', compression=zipfile.ZIP_DEFLATED) as z:
      with z.open(self.PIPELINE_PATH, 'w') as fout:
        fout.write(json_format.MessageToJson(
            self._pipeline_proto).encode('utf-8'))
      with z.open(self.PIPELINE_OPTIONS_PATH, 'w') as fout:
        fout.write(json_format.MessageToJson(
            self._pipeline_options).encode('utf-8'))
      with z.open(self.PIPELINE_MANIFEST, 'w') as fout:
        fout.write(json.dumps(
            {'defaultJobName': self.PIPELINE_NAME}).encode('utf-8'))
    self._start_artifact_service(self._jar, self._artifact_port)

  def _start_artifact_service(self, jar, requested_port):
    self._artifact_staging_service = artifact_service.ZipFileArtifactService(
        jar, self.ARTIFACT_FOLDER)
    self._artifact_staging_server = grpc.server(futures.ThreadPoolExecutor())
    port = self._artifact_staging_server.add_insecure_port(
        '[::]:%s' % requested_port)
    beam_artifact_api_pb2_grpc.add_ArtifactStagingServiceServicer_to_server(
        self._artifact_staging_service, self._artifact_staging_server)
    self._artifact_staging_endpoint = endpoints_pb2.ApiServiceDescriptor(
        url='localhost:%d' % port)
    self._artifact_staging_server.start()
    _LOGGER.info('Artifact server started on port %s', port)
    return port

  def _stop_artifact_service(self):
    self._artifact_staging_server.stop(1)
    self._artifact_staging_service.close()
    self._artifact_manifest_location = (
        self._artifact_staging_service.retrieval_token(self._job_id))

  def artifact_staging_endpoint(self):
    return self._artifact_staging_endpoint

  def request(self, method, path, expected_status=200, **kwargs):
    url = '%s/%s' % (self._master_url, path)
    response = method(url, **kwargs)
    if response.status_code != expected_status:
      raise RuntimeError("Request to %s failed with status %d: %s" %
                         (url, response.status_code, response.text))
    if response.text:
      return response.json()

  def get(self, path, **kwargs):
    return self.request(requests.get, path, **kwargs)

  def post(self, path, **kwargs):
    return self.request(requests.post, path, **kwargs)

  def delete(self, path, **kwargs):
    return self.request(requests.delete, path, **kwargs)

  def run(self):
    self._stop_artifact_service()
    # Move the artifact manifest to the expected location.
    with zipfile.ZipFile(self._jar, 'a', compression=zipfile.ZIP_DEFLATED) as z:
      with z.open(self._artifact_manifest_location) as fin:
        manifest_contents = fin.read()
      with z.open(self.ARTIFACT_MANIFEST_PATH, 'w') as fout:
        fout.write(manifest_contents)

    # Upload the jar and start the job.
    with open(self._jar, 'rb') as jar_file:
      self._flink_jar_id = self.post(
          'v1/jars/upload',
          files={'jarfile': ('beam.jar', jar_file)})['filename'].split('/')[-1]
    self._jar_uploaded = True
    self._flink_job_id = self.post(
        'v1/jars/%s/run' % self._flink_jar_id,
        json={
            'entryClass': 'org.apache.beam.runners.flink.FlinkPipelineRunner'
        })['jobid']
    os.unlink(self._jar)
    _LOGGER.info('Started Flink job as %s' % self._flink_job_id)

  def cancel(self):
    self.post('v1/%s/stop' % self._flink_job_id, expected_status=202)
    self.delete_jar()

  def delete_jar(self):
    if self._jar_uploaded:
      self._jar_uploaded = False
      try:
        self.delete('v1/jars/%s' % self._flink_jar_id)
      except Exception:
        _LOGGER.info(
            'Error deleting jar %s' % self._flink_jar_id, exc_info=True)

  def _get_state(self):
    """Query flink to get the current state.

    :return: tuple of int and Timestamp or None
      timestamp will be None if the state has not changed since the last query.
    """
    # For just getting the status, execution-result seems cheaper.
    flink_status = self.get(
        'v1/jobs/%s/execution-result' % self._flink_job_id)['status']['id']
    if flink_status == 'COMPLETED':
      flink_status = self.get('v1/jobs/%s' % self._flink_job_id)['state']
    beam_state = {
        'CREATED': beam_job_api_pb2.JobState.STARTING,
        'RUNNING': beam_job_api_pb2.JobState.RUNNING,
        'FAILING': beam_job_api_pb2.JobState.RUNNING,
        'FAILED': beam_job_api_pb2.JobState.FAILED,
        'CANCELLING': beam_job_api_pb2.JobState.CANCELLING,
        'CANCELED': beam_job_api_pb2.JobState.CANCELLED,
        'FINISHED': beam_job_api_pb2.JobState.DONE,
        'RESTARTING': beam_job_api_pb2.JobState.RUNNING,
        'SUSPENDED': beam_job_api_pb2.JobState.RUNNING,
        'RECONCILING': beam_job_api_pb2.JobState.RUNNING,
        'IN_PROGRESS': beam_job_api_pb2.JobState.RUNNING,
        'COMPLETED': beam_job_api_pb2.JobState.DONE,
    }.get(flink_status, beam_job_api_pb2.JobState.UNSPECIFIED)
    if self.is_terminal_state(beam_state):
      self.delete_jar()
    # update the state history if it has changed
    return beam_state, self.set_state(beam_state)

  def get_state(self):
    state, timestamp = self._get_state()
    if timestamp is None:
      # state has not changed since it was last checked: use previous timestamp
      return super(FlinkBeamJob, self).get_state()
    else:
      return state, timestamp

  def get_state_stream(self):
    def _state_iter():
      sleep_secs = 1.0
      while True:
        current_state, timestamp = self._get_state()
        if timestamp is not None:
          # non-None indicates that the state has changed
          yield current_state, timestamp
        sleep_secs = min(60, sleep_secs * 1.2)
        time.sleep(sleep_secs)

    for state, timestamp in self.with_state_history(_state_iter()):
      yield state, timestamp
      if self.is_terminal_state(state):
        break

  def get_message_stream(self):
    for state, timestamp in self.get_state_stream():
      if self.is_terminal_state(state):
        response = self.get('v1/jobs/%s/exceptions' % self._flink_job_id)
        for ix, exc in enumerate(response['all-exceptions']):
          yield beam_job_api_pb2.JobMessage(
              message_id='message%d' % ix,
              time=str(exc['timestamp']),
              importance=
              beam_job_api_pb2.JobMessage.MessageImportance.JOB_MESSAGE_ERROR,
              message_text=exc['exception'])
        yield state, timestamp
        break
      else:
        yield state, timestamp
