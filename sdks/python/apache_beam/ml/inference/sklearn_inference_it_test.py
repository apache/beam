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

"""End-to-End test for Sklearn Inference"""

import logging
import pytest
import unittest
import uuid

from apache_beam.io.filesystems import FileSystems
from apache_beam.examples.inference import sklearn_inference_example
from apache_beam.testing.test_pipeline import TestPipeline


class SklearnInference(unittest.TestCase):
  @pytest.mark.it_postcommit
  def test_predictions_output_file(self):
    test_pipeline = TestPipeline(is_integration_test=False)
    input_file = 'gs://apache-beam-ml/datasets/mnist/train.csv'
    output_file_dir = 'gs://apache-beam-ml/temp_storage_end_to_end_testing/outputs'  # pylint: disable=line-too-long
    output_file = '/'.join(
        [output_file_dir, str(uuid.uuid4()), 'sklearn_result.txt'])
    model_path = 'gs://apache-beam-ml/models/mnist_model_svm.pickle'  # pylint: disable=line-too-long
    extra_opts = {
        'input': input_file,
        'output': output_file,
        'model_path': model_path,
    }

    sklearn_inference_example.run(
        test_pipeline.get_full_options_as_args(**extra_opts),
        save_main_session=False)
    self.assertEqual(FileSystems().exists(output_file), True)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
