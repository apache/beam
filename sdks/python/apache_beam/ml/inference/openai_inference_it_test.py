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

"""End-to-End test for OpenAI Remote Inference"""

import logging
import os
import unittest
import uuid

import apache_beam as beam
import pytest
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=ungrouped-imports
try:
  from apache_beam.ml.inference.openai_inference import OpenAIModelHandler
except ImportError:
  raise unittest.SkipTest('OpenAI dependencies are not installed')

# Default output directory for test results
_OUTPUT_DIR_DEFAULT = "gs://apache-beam-ml/testing/outputs/openai"
# Placeholder for API key. Users must set OPENAI_API_KEY environment variable.
_OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# Models for testing - one completion, one chat
_COMPLETION_MODEL = "gpt-3.5-turbo-instruct"  # A smaller, faster completion model
_CHAT_MODEL = "gpt-3.5-turbo"


@unittest.skipIf(not _OPENAI_API_KEY, "OPENAI_API_KEY is not set.")
class OpenAIInferenceIT(unittest.TestCase):
  def setUp(self):
    self.output_dir = os.environ.get("BEAM_ML_OUTPUT_DIR", _OUTPUT_DIR_DEFAULT)
    self.project = os.environ.get(
        "BEAM_ML_PROJECT")  # Not used by OpenAI but common in tests

  def run_pipeline(
      self, model_handler, test_data, output_path_suffix, inference_args=None):
    output_file = '/'.join(
        [self.output_dir, str(uuid.uuid4()), output_path_suffix])

    pipeline_options = {
        'output': output_file,
    }
    # Add project if available, for consistency with other IT tests,
    # though OpenAI handler doesn't directly use it.
    if self.project:
      pipeline_options['project'] = self.project

    test_pipeline = TestPipeline(
        is_integration_test=True, options=pipeline_options)

    with test_pipeline as p:
      results = (
          p
          | "CreateInputs" >> beam.Create(test_data)
          | "RunInference" >> RunInference(
              model_handler, inference_args=inference_args)
          | "SaveResults" >> beam.Map(
              lambda x: str(x))  # Convert PredictionResult to string for output
          | beam.io.WriteToText(output_file))

    self.assertTrue(FileSystems().exists(output_file))
    # Further checks could involve reading the output and verifying content,
    # but for now, we just check if the pipeline runs and produces output.

    # Basic check for content in the output file to ensure it's not empty
    # and contains expected PredictionResult structure.
    # This part can be flaky if API responses change slightly.
    # For a more robust check, one might mock the API in an IT setting or
    # use a very deterministic, simple prompt.
    match_results = []

    def process_output_file(readable_file):
      for line in readable_file:
        match_results.append(line)

    FileSystems.read_files(output_file, process_file_fn=process_output_file)
    self.assertGreater(len(match_results), 0)
    # Example: check if output contains 'PredictionResult(example=' or similar
    self.assertTrue(
        any("PredictionResult(example=" in line for line in match_results))

  @pytest.mark.postcommit  # Mark as postcommit as it makes external calls.
  def test_openai_completion_model(self):
    model_handler = OpenAIModelHandler(
        api_key=_OPENAI_API_KEY, model=_COMPLETION_MODEL)
    test_data = [
        "What is the capital of France?", "Translate 'hello' to Spanish."
    ]
    inference_args = {"max_tokens": 50, "temperature": 0.7}
    self.run_pipeline(
        model_handler,
        test_data,
        "output_completion.txt",
        inference_args=inference_args)

  @pytest.mark.postcommit
  def test_openai_chat_model(self):
    model_handler = OpenAIModelHandler(
        api_key=_OPENAI_API_KEY, model=_CHAT_MODEL)
    # Chat models expect a list of messages or a single string (handled as user message)
    test_data = [
        "What is 2+2?",  # Single string prompt
        [{
            "role": "user", "content": "Describe a perfect day."
        }]  # Message list prompt
    ]
    inference_args = {"max_tokens": 100, "temperature": 0.5}
    self.run_pipeline(
        model_handler,
        test_data,
        "output_chat.txt",
        inference_args=inference_args)

  @pytest.mark.postcommit
  def test_openai_chat_model_with_system_message(self):
    model_handler = OpenAIModelHandler(
        api_key=_OPENAI_API_KEY, model=_CHAT_MODEL)
    # Chat models expect a list of messages or a single string (handled as user message)
    test_data = [
        # This requires the OpenAIModelHandler's generate_completion to correctly
        # handle list of messages if the input element itself is a list of dicts.
        [{
            "role": "system",
            "content": "You are a helpful assistant that speaks like a pirate."
        }, {
            "role": "user", "content": "How are you?"
        }]
    ]
    inference_args = {"max_tokens": 50}
    self.run_pipeline(
        model_handler,
        test_data,
        "output_chat_system.txt",
        inference_args=inference_args)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
