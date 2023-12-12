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

# pylint: skip-file

import logging
import os
# import time
import unittest
import uuid

import pytest

try:
  import apache_beam.testing.benchmarks.cloudml.cloudml_benchmark_constants_lib as constants
  #   from apache_beam.examples.ml_transform import vocab_tfidf_processing
  from apache_beam.testing.load_tests.load_test_metrics_utils import InfluxDBMetricsPublisherOptions
  from apache_beam.testing.load_tests.load_test_metrics_utils import MetricsReader
  from apache_beam.testing.test_pipeline import TestPipeline
  from apache_beam.examples.ml_transform import criteo
except ImportError:  # pylint: disable=bare-except
  raise unittest.SkipTest('tensorflow_transform is not installed.')

_INPUT_GCS_BUCKET_ROOT = 'gs://apache-beam-ml/datasets/cloudml/criteo'
_OUTPUT_GCS_BUCKET_ROOT = 'gs://temp-storage-for-end-to-end-tests/tft/'
_DISK_SIZE = 150


@pytest.mark.uses_tft
class CriteoTest(unittest.TestCase):
  def test_process_criteo_10GB_dataset(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    extra_opts = {}

    # beam pipeline options
    extra_opts['input'] = os.path.join(
        _INPUT_GCS_BUCKET_ROOT, constants.INPUT_CRITEO_10GB)
    extra_opts['artifact_location'] = os.path.join(
        _OUTPUT_GCS_BUCKET_ROOT, uuid.uuid4().hex)

    extra_opts['frequency_threshold'] = 0

    # dataflow pipeliens options
    extra_opts['disk_size_gb'] = _DISK_SIZE
    extra_opts['machine_type'] = 'e2-highmem-2'
    extra_opts['job_name'] = (
        'mltransform-criteo-dataset-{}-10'.format(uuid.uuid4().hex))
    # start_time = time.time()
    criteo.run(
        test_pipeline.get_full_options_as_args(
            **extra_opts, save_main_session=False))
    # end_time = time.time()


if __name__ == '__main__':
  unittest.main()
