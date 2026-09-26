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

import pytest

try:
  from openai import OpenAI

  from apache_beam.ml.inference.openai_inference import OpenAIModelHandler
  from apache_beam.ml.inference.openai_inference import chat_completion_from_conversation
  from apache_beam.ml.inference.openai_inference import chat_completion_from_string
  from apache_beam.ml.inference.openai_inference import embedding_from_string
except ImportError:
  raise unittest.SkipTest("OpenAI dependencies are not installed")

import apache_beam as beam
from apache_beam.ml.inference.base import RunInference
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import is_not_empty

_OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', None)
_TEST_CHAT_MODEL = 'gpt-4o-mini'
_TEST_EMBEDDING_MODEL = 'text-embedding-3-small'


def _extract_chat_text(prediction_result):
  return prediction_result.inference.choices[0].message.content


def _extract_embedding(prediction_result):
  return prediction_result.inference.embedding


@pytest.mark.openai_postcommit
class OpenAIInferenceIT(unittest.TestCase):
  @unittest.skipIf(
      _OPENAI_API_KEY is None, 'OPENAI_API_KEY environment variable is not set')
  def test_openai_chat_text_generation(self):
    handler = OpenAIModelHandler(
        model_name=_TEST_CHAT_MODEL,
        request_fn=chat_completion_from_string,
        api_key=_OPENAI_API_KEY,
        max_batch_size=1,
    )

    prompts = [
        'What is Apache Beam in one sentence?',
        'Name three distributed computing frameworks.',
    ]

    with TestPipeline() as p:
      results = (
          p
          | beam.Create(prompts)
          | RunInference(handler)
          | beam.Map(_extract_chat_text))
      assert_that(results, is_not_empty())

  @unittest.skipIf(
      _OPENAI_API_KEY is None, 'OPENAI_API_KEY environment variable is not set')
  def test_openai_conversation(self):
    handler = OpenAIModelHandler(
        model_name=_TEST_CHAT_MODEL,
        request_fn=chat_completion_from_conversation,
        api_key=_OPENAI_API_KEY,
        max_batch_size=1,
    )

    conversations = [
        [
            {
                "role": "user", "content": "What is 2 + 2?"
            },
            {
                "role": "assistant", "content": "4"
            },
            {
                "role": "user", "content": "Add 3 to that."
            },
        ],
    ]

    with TestPipeline() as p:
      results = (
          p
          | beam.Create(conversations)
          | RunInference(handler)
          | beam.Map(_extract_chat_text))
      assert_that(results, is_not_empty())

  @unittest.skipIf(
      _OPENAI_API_KEY is None, 'OPENAI_API_KEY environment variable is not set')
  def test_openai_with_system_prompt(self):
    handler = OpenAIModelHandler(
        model_name=_TEST_CHAT_MODEL,
        request_fn=chat_completion_from_string,
        api_key=_OPENAI_API_KEY,
        system='You are a pirate. Respond only in pirate speak.',
        max_batch_size=1,
    )

    prompts = ['What is your name?']

    with TestPipeline() as p:
      results = (
          p
          | beam.Create(prompts)
          | RunInference(handler)
          | beam.Map(_extract_chat_text))
      assert_that(results, is_not_empty())

  @unittest.skipIf(
      _OPENAI_API_KEY is None, 'OPENAI_API_KEY environment variable is not set')
  def test_openai_system_prompt_with_structured_output(self):
    handler = OpenAIModelHandler(
        model_name=_TEST_CHAT_MODEL,
        request_fn=chat_completion_from_string,
        api_key=_OPENAI_API_KEY,
        system="You respond only with JSON conforming to the schema.",
        response_format={
            'type': 'json_schema',
            'json_schema': {
                'name': 'fizz_buzz_response',
                'schema': {
                    'type': 'object',
                    'properties': {
                        'items': {
                            'type': 'array',
                            'items': {
                                'type': 'object',
                                'properties': {
                                    'value': {
                                        'type': 'string'
                                    },
                                },
                                'required': ['value'],
                                'additionalProperties': False,
                            },
                        },
                    },
                    'required': ['items'],
                    'additionalProperties': False,
                },
                'strict': True,
            },
        },
        max_batch_size=1,
    )

    prompts = ['Count from 1 to 5.']

    with TestPipeline() as p:
      results = (
          p
          | beam.Create(prompts)
          | RunInference(handler)
          | beam.Map(_extract_chat_text))
      assert_that(results, is_not_empty())

  @unittest.skipIf(
      _OPENAI_API_KEY is None, 'OPENAI_API_KEY environment variable is not set')
  def test_openai_embeddings(self):
    handler = OpenAIModelHandler(
        model_name=_TEST_EMBEDDING_MODEL,
        request_fn=embedding_from_string,
        api_key=_OPENAI_API_KEY,
        max_batch_size=2,
    )

    prompts = [
        'What is Apache Beam?',
        'Distributed stream and batch processing.',
    ]

    with TestPipeline() as p:
      results = (
          p
          | beam.Create(prompts)
          | RunInference(handler)
          | beam.Map(_extract_embedding))
      assert_that(results, is_not_empty())


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
