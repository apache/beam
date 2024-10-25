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

"""A job server submitting portable pipelines as uber jars to Spark."""

# pytype: skip-file

import itertools
import logging
import os
import tempfile
import time
import urllib
import zipfile

import requests

from apache_beam.options import pipeline_options
from apache_beam.portability.api import beam_job_api_pb2
from apache_beam.runners.portability import abstract_job_service
from apache_beam.runners.portability import job_server
from apache_beam.utils.timestamp import Timestamp

_LOGGER = logging.getLogger(__name__)


class SparkUberJarJobServer(abstract_job_service.AbstractJobServiceServicer):
  """A Job server which submits a self-contained Jar to a Spark cluster.

  The jar contains the Beam pipeline definition, dependencies, and
  the pipeline artifacts.
  """
  def __init__(self, rest_url, options):
    super().__init__()
    self._rest_url = rest_url
    self._artifact_port = (
        options.view_as(pipeline_options.JobServerOptions).artifact_port)
    self._jar_cache_dir = (
        options.view_as(pipeline_options.JobServerOptions).jar_cache_dir)
    self._temp_dir = tempfile.mkdtemp(prefix='apache-beam-spark')
    spark_options = options.view_as(pipeline_options.SparkRunnerOptions)
    self._executable_jar = spark_options.spark_job_server_jar
    self._spark_version = spark_options.spark_version

  def start(self):
    return self

  def stop(self):
    pass

  def executable_jar(self):
    if self._executable_jar:
      if not os.path.exists(self._executable_jar):
        parsed = urllib.parse.urlparse(self._executable_jar)
        if not parsed.scheme:
          raise ValueError(
              'Unable to parse jar URL "%s". If using a full URL, make sure '
              'the scheme is specified. If using a local file path, make sure '
              'the file exists; you may have to first build the job server '
              'using `./gradlew runners:spark:3:job-server:shadowJar`.' %
              self._executable_jar)
      url = self._executable_jar
    else:
      if self._spark_version == '2':
        raise ValueError('Support for Spark 2 was dropped.')
      else:
        url = job_server.JavaJarJobServer.path_to_beam_jar(
            ':runners:spark:3:job-server:shadowJar')
    return job_server.JavaJarJobServer.local_jar(url)

  def create_beam_job(self, job_id, job_name, pipeline, options):
    return SparkBeamJob(
        self._rest_url,
        self.executable_jar(),
        job_id,
        job_name,
        pipeline,
        options,
        artifact_port=self._artifact_port)


class SparkBeamJob(abstract_job_service.UberJarBeamJob):
  """Runs a single Beam job on Spark by staging all contents into a Jar
  and uploading it via the Spark Rest API.

  Note that the Spark Rest API is not enabled by default. It must be enabled by
  setting the configuration property spark.master.rest.enabled to true."""
  def __init__(
      self,
      rest_url,
      executable_jar,
      job_id,
      job_name,
      pipeline,
      options,
      artifact_port=0):
    super().__init__(
        executable_jar,
        job_id,
        job_name,
        pipeline,
        options,
        artifact_port=artifact_port)
    self._rest_url = rest_url
    # Message history is a superset of state history.
    self._message_history = self._state_history[:]

  def request(self, method, path, expected_status=200, **kwargs):
    url = '%s/%s' % (self._rest_url, path)
    response = method(url, **kwargs)
    if response.status_code != expected_status:
      raise RuntimeError(
          "Request to %s failed with status %d: %s" %
          (url, response.status_code, response.text))
    if response.text:
      return response.json()

  def get(self, path, **kwargs):
    return self.request(requests.get, path, **kwargs)

  def post(self, path, **kwargs):
    return self.request(requests.post, path, **kwargs)

  def delete(self, path, **kwargs):
    return self.request(requests.delete, path, **kwargs)

  def _get_server_spark_version(self):
    # Spark REST API doesn't seem to offer a dedicated endpoint for getting the
    # version, but it does include the version in all responses, even errors.
    return self.get('', expected_status=400)['serverSparkVersion']

  def _get_client_spark_version_from_properties(self, jar):
    """Parse Spark version from spark-version-info.properties file in the jar.
    https://github.com/apache/spark/blob/dddfeca175bdce5294debe00d4a993daef92ca60/build/spark-build-info#L30
    """
    with zipfile.ZipFile(jar, 'a', compression=zipfile.ZIP_DEFLATED) as z:
      with z.open('spark-version-info.properties') as fin:
        for line in fin.read().decode('utf-8').splitlines():
          split = list(map(lambda s: s.strip(), line.split('=')))
          if len(split) == 2 and split[0] == 'version' and split[1] != '':
            return split[1]
        raise ValueError(
            'Property "version" not found in spark-version-info.properties.')

  def _get_client_spark_version(self, jar):
    try:
      return self._get_client_spark_version_from_properties(jar)
    except Exception as e:
      _LOGGER.debug(e)
      server_version = self._get_server_spark_version()
      _LOGGER.warning(
          'Unable to parse Spark version from '
          'spark-version-info.properties. Defaulting to %s' % server_version)
      return server_version

  def _create_submission_request(self, jar, job_name):
    jar_url = "file:%s" % jar
    return {
        "action": "CreateSubmissionRequest",
        "appArgs": [],
        "appResource": jar_url,
        "clientSparkVersion": self._get_client_spark_version(jar),
        "environmentVariables": {},
        "mainClass": "org.apache.beam.runners.spark.SparkPipelineRunner",
        "sparkProperties": {
            "spark.jars": jar_url,
            "spark.app.name": job_name,
            "spark.submit.deployMode": "cluster",
        }
    }

  def run(self):
    self._stop_artifact_service()

    # Upload the jar and start the job.
    self._spark_submission_id = self.post(
        'v1/submissions/create',
        json=self._create_submission_request(self._jar,
                                             self._job_name))['submissionId']
    _LOGGER.info('Submitted Spark job with ID %s' % self._spark_submission_id)

  def cancel(self):
    self.post('v1/submissions/kill/%s' % self._spark_submission_id)

  @staticmethod
  def _get_beam_state(spark_response):
    return {
        'SUBMITTED': beam_job_api_pb2.JobState.STARTING,
        'RUNNING': beam_job_api_pb2.JobState.RUNNING,
        'FINISHED': beam_job_api_pb2.JobState.DONE,
        'RELAUNCHING': beam_job_api_pb2.JobState.RUNNING,
        'UNKNOWN': beam_job_api_pb2.JobState.UNSPECIFIED,
        'KILLED': beam_job_api_pb2.JobState.CANCELLED,
        'FAILED': beam_job_api_pb2.JobState.FAILED,
        'ERROR': beam_job_api_pb2.JobState.FAILED,
    }.get(spark_response['driverState'], beam_job_api_pb2.JobState.UNSPECIFIED)

  def _get_spark_status(self):
    return self.get('v1/submissions/status/%s' % self._spark_submission_id)

  def get_state(self):
    response = self._get_spark_status()
    state = self._get_beam_state(response)
    timestamp = self.set_state(state)
    if timestamp is None:
      # State has not changed since last check. Use previous timestamp.
      return super().get_state()
    else:
      return state, timestamp

  def _with_message_history(self, message_stream):
    return itertools.chain(self._message_history[:], message_stream)

  def _get_message_iter(self):
    """Returns an iterator of messages from the Spark server.
    Note that while message history is de-duped, this function's returned
    iterator may contain duplicate values."""
    sleep_secs = 1.0
    message_ix = 0
    while True:
      response = self._get_spark_status()
      state = self._get_beam_state(response)
      timestamp = Timestamp.now()
      message = None
      if 'message' in response:
        importance = (
            beam_job_api_pb2.JobMessage.MessageImportance.JOB_MESSAGE_ERROR
            if state == beam_job_api_pb2.JobState.FAILED else
            beam_job_api_pb2.JobMessage.MessageImportance.JOB_MESSAGE_BASIC)
        message = beam_job_api_pb2.JobMessage(
            message_id='message%d' % message_ix,
            time=str(int(timestamp)),
            importance=importance,
            message_text=response['message'])
        yield message
        message_ix += 1
        # TODO(https://github.com/apache/beam/issues/20019) In the event of a
        #  failure, query additional info from Spark master and/or workers.
      check_timestamp = self.set_state(state)
      if check_timestamp is not None:
        if message:
          self._message_history.append(message)
        self._message_history.append((state, check_timestamp))
      yield state, timestamp
      sleep_secs = min(60, sleep_secs * 1.2)
      time.sleep(sleep_secs)

  def get_state_stream(self):
    for msg in self._with_message_history(self._get_message_iter()):
      if isinstance(msg, tuple):
        state, timestamp = msg
        yield state, timestamp
        if self.is_terminal_state(state):
          break

  def get_message_stream(self):
    for msg in self._with_message_history(self._get_message_iter()):
      yield msg
      if isinstance(msg, tuple):
        state, _ = msg
        if self.is_terminal_state(state):
          break
