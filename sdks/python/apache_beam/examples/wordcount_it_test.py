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

"""End-to-end test for the wordcount example."""

# pytype: skip-file

import logging
import os
import time
import unittest

import pytest
from hamcrest.core.core.allof import all_of

from apache_beam.examples import wordcount
from apache_beam.internal.gcp import auth
from apache_beam.testing.load_tests.load_test_metrics_utils import InfluxDBMetricsPublisherOptions
from apache_beam.testing.load_tests.load_test_metrics_utils import MetricsReader
from apache_beam.testing.pipeline_verifiers import FileChecksumMatcher
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_utils import delete_files


class WordCountIT(unittest.TestCase):

  # The default checksum is a SHA-1 hash generated from a sorted list of
  # lines read from expected output. This value corresponds to the default
  # input of WordCount example.
  DEFAULT_CHECKSUM = '33535a832b7db6d78389759577d4ff495980b9c0'

  @pytest.mark.it_postcommit
  def test_wordcount_it(self):
    self._run_wordcount_it(wordcount.run)

  @pytest.mark.it_postcommit
  @pytest.mark.sickbay_direct
  @pytest.mark.sickbay_spark
  @pytest.mark.sickbay_flink
  def test_wordcount_impersonation_it(self):
    """Tests impersonation on dataflow.

    For testing impersonation, we use three ingredients:
    - a principal to impersonate
    - a dataflow service account that only that principal is
      allowed to launch jobs as
    - a temp root that only the above two accounts have access to

    Jenkins and Dataflow workers both run as GCE default service account.
    So we remove that account from all the above.
    """
    # Credentials need to be reset or this test will fail and credentials
    # from a previous test will be used.
    with auth._Credentials._credentials_lock:
      auth._Credentials._credentials_init = False
    try:
      ACCOUNT_TO_IMPERSONATE = (
          'allows-impersonation@apache-'
          'beam-testing.iam.gserviceaccount.com')
      RUNNER_ACCOUNT = (
          'impersonation-dataflow-worker@'
          'apache-beam-testing.iam.gserviceaccount.com')
      TEMP_DIR = 'gs://impersonation-test-bucket/temp-it'
      STAGING_LOCATION = 'gs://impersonation-test-bucket/staging-it'
      extra_options = {
          'impersonate_service_account': ACCOUNT_TO_IMPERSONATE,
          'service_account_email': RUNNER_ACCOUNT,
          'temp_location': TEMP_DIR,
          'staging_location': STAGING_LOCATION
      }
      self._run_wordcount_it(wordcount.run, **extra_options)
    finally:
      # Reset credentials for future tests.
      with auth._Credentials._credentials_lock:
        auth._Credentials._credentials_init = False

  @pytest.mark.it_postcommit
  @pytest.mark.it_validatescontainer
  def test_wordcount_fnapi_it(self):
    self._run_wordcount_it(wordcount.run, experiment='beam_fn_api')

  @pytest.mark.it_validatescontainer
  def test_wordcount_it_with_prebuilt_sdk_container_local_docker(self):
    self._run_wordcount_it(
        wordcount.run,
        experiment='beam_fn_api',
        prebuild_sdk_container_engine='local_docker')

  @pytest.mark.it_validatescontainer
  def test_wordcount_it_with_prebuilt_sdk_container_cloud_build(self):
    self._run_wordcount_it(
        wordcount.run,
        experiment='beam_fn_api',
        prebuild_sdk_container_engine='cloud_build')

  def _run_wordcount_it(self, run_wordcount, **opts):
    test_pipeline = TestPipeline(is_integration_test=True)
    extra_opts = {}

    # Set extra options to the pipeline for test purpose
    test_output = '/'.join([
        test_pipeline.get_option('output'),
        str(int(time.time() * 1000)),
        'results'
    ])
    extra_opts['output'] = test_output

    test_input = test_pipeline.get_option('input')
    if test_input:
      extra_opts['input'] = test_input

    arg_sleep_secs = test_pipeline.get_option('sleep_secs')
    sleep_secs = int(arg_sleep_secs) if arg_sleep_secs is not None else None
    expect_checksum = (
        test_pipeline.get_option('expect_checksum') or self.DEFAULT_CHECKSUM)
    pipeline_verifiers = [
        PipelineStateMatcher(),
        FileChecksumMatcher(
            test_output + '*-of-*', expect_checksum, sleep_secs)
    ]
    extra_opts['on_success_matcher'] = all_of(*pipeline_verifiers)
    extra_opts.update(opts)

    # Register clean up before pipeline execution
    self.addCleanup(delete_files, [test_output + '*'])

    publish_to_bq = bool(test_pipeline.get_option('publish_to_big_query'))

    # Start measure time for performance test
    start_time = time.time()

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    run_wordcount(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False,
    )

    end_time = time.time()
    run_time = end_time - start_time

    if publish_to_bq:
      self._publish_metrics(test_pipeline, run_time)

  def _publish_metrics(self, pipeline, metric_value):
    influx_options = InfluxDBMetricsPublisherOptions(
        pipeline.get_option('influx_measurement'),
        pipeline.get_option('influx_db_name'),
        pipeline.get_option('influx_hostname'),
        os.getenv('INFLUXDB_USER'),
        os.getenv('INFLUXDB_USER_PASSWORD'),
    )
    metric_reader = MetricsReader(
        project_name=pipeline.get_option('project'),
        bq_table=pipeline.get_option('metrics_table'),
        bq_dataset=pipeline.get_option('metrics_dataset'),
        publish_to_bq=True,
        influxdb_options=influx_options,
    )

    metric_reader.publish_values([(
        'runtime',
        metric_value,
    )])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
