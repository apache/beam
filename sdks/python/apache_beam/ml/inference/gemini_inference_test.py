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

import threading
import time
import unittest
from unittest import mock

try:
  from google.genai import errors
  from google.genai import types

  from apache_beam.ml.inference.gemini_inference import GeminiModelHandler
  from apache_beam.ml.inference.gemini_inference import _retry_on_appropriate_service_error
  from apache_beam.ml.inference.gemini_inference import generate_from_string
  from apache_beam.ml.inference.gemini_inference import generate_image_from_strings_and_images
except ImportError:
  raise unittest.SkipTest('Gemini dependencies are not installed')

import apache_beam as beam
from apache_beam.ml.inference.base import RunInference
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

_TEST_MODEL = 'gemini-model-123'


def _make_response(text):
  return types.GenerateContentResponse(
      candidates=[
          types.Candidate(
              content=types.Content(
                  role='model', parts=[types.Part(text=text)]))
      ])


def _make_mock_client() -> mock.MagicMock:
  client = mock.MagicMock()
  client.models.generate_content = mock.MagicMock()
  return client


def _echo_contents(**kwargs):
  """Returns a fake response derived from the request's contents.

  Requests within a batch are issued concurrently, so call order is
  nondeterministic; keying the response on the request contents lets tests
  check that results are returned in batch order.
  """
  return _make_response(f"answer for: {kwargs['contents']}")


def _fake_request_fn(model_name, batch, model, inference_args):
  return [_make_response(f'answer for: {prompt}') for prompt in batch]


class RetryOnClientErrorTest(unittest.TestCase):
  def test_retry_on_client_error_positive(self):
    e = errors.APIError(code=429, response_json={})
    self.assertTrue(_retry_on_appropriate_service_error(e))

  def test_retry_on_client_error_negative(self):
    e = errors.APIError(code=404, response_json={})
    self.assertFalse(_retry_on_appropriate_service_error(e))

  def test_retry_on_server_error(self):
    e = errors.APIError(code=501, response_json={})
    self.assertTrue(_retry_on_appropriate_service_error(e))


class ModelHandlerArgConditions(unittest.TestCase):
  def test_max_batch_weight_requires_element_size_fn(self):
    with self.assertRaisesRegex(ValueError,
                                "max_batch_weight requires element_size_fn"):
      GeminiModelHandler(
          model_name="gemini-model-123",
          request_fn=generate_from_string,
          api_key="123456789",
          max_batch_weight=100)

  def test_max_batch_weight_cannot_combine_with_length_batching(self):
    with self.assertRaisesRegex(
        ValueError, "cannot be combined with length-based batching"):
      GeminiModelHandler(
          model_name="gemini-model-123",
          request_fn=generate_from_string,
          api_key="123456789",
          max_batch_weight=100,
          element_size_fn=len,
          batch_length_fn=len)

  def test_batch_bucket_boundaries_require_batch_length_fn(self):
    with self.assertRaisesRegex(
        ValueError, "batch_bucket_boundaries requires batch_length_fn"):
      GeminiModelHandler(
          model_name="gemini-model-123",
          request_fn=generate_from_string,
          api_key="123456789",
          batch_bucket_boundaries=[16, 32])

  def test_all_params_set(self):
    self.assertRaises(
        ValueError,
        GeminiModelHandler,
        model_name="gemini-model-123",
        request_fn=generate_from_string,
        api_key="123456789",
        project="testproject",
        location="us-central1",
    )

  def test_missing_vertex_location_param(self):
    self.assertRaises(
        ValueError,
        GeminiModelHandler,
        model_name="gemini-model-123",
        request_fn=generate_from_string,
        project="testproject",
    )

  def test_missing_vertex_project_param(self):
    self.assertRaises(
        ValueError,
        GeminiModelHandler,
        model_name="gemini-model-123",
        request_fn=generate_from_string,
        location="us-central1",
    )

  def test_missing_all_params(self):
    self.assertRaises(
        ValueError,
        GeminiModelHandler,
        model_name="gemini-model-123",
        request_fn=generate_from_string,
    )


@mock.patch('apache_beam.ml.inference.gemini_inference.genai.Client')
@mock.patch('apache_beam.ml.inference.gemini_inference.HttpOptions')
class TestGeminiModelHandler(unittest.TestCase):
  def test_create_client_with_flex_api(
      self, mock_http_options, mock_genai_client):
    handler = GeminiModelHandler(
        model_name="gemini-pro",
        request_fn=generate_from_string,
        project="test-project",
        location="us-central1",
        use_vertex_flex_api=True)
    handler.create_client()
    mock_http_options.assert_called_with(
        api_version="v1",
        headers={"X-Vertex-AI-LLM-Request-Type": "flex"},
        timeout=600000,
    )
    mock_genai_client.assert_called_with(
        vertexai=True,
        project="test-project",
        location="us-central1",
        http_options=mock_http_options.return_value)


class GenerateFromStringTest(unittest.TestCase):
  def test_sends_each_prompt_as_its_own_request(self):
    client = _make_mock_client()
    client.models.generate_content.side_effect = _echo_contents
    results = generate_from_string(_TEST_MODEL, ['hello', 'world'], client, {})
    self.assertEqual([r.text for r in results],
                     ['answer for: hello', 'answer for: world'])
    self.assertEqual(client.models.generate_content.call_count, 2)
    calls = client.models.generate_content.call_args_list
    self.assertCountEqual([c.kwargs['contents'] for c in calls],
                          ['hello', 'world'])
    for c in calls:
      self.assertEqual(c.kwargs['model'], _TEST_MODEL)

  def test_requests_run_concurrently_and_preserve_order(self):
    lock = threading.Lock()
    state = {'in_flight': 0, 'max_in_flight': 0}
    completion_order = []
    prompts = [f'p{i}' for i in range(4)]

    def slow_generate(**kwargs):
      prompt = kwargs['contents']
      with lock:
        state['in_flight'] += 1
        state['max_in_flight'] = max(state['max_in_flight'], state['in_flight'])
      # Earlier prompts sleep longer so they finish last.
      time.sleep(0.05 * (len(prompts) - prompts.index(prompt)))
      with lock:
        state['in_flight'] -= 1
        completion_order.append(prompt)
      return _make_response(f'answer for: {prompt}')

    client = _make_mock_client()
    client.models.generate_content.side_effect = slow_generate
    results = generate_from_string(_TEST_MODEL, prompts, client, {})

    self.assertGreater(state['max_in_flight'], 1)
    self.assertNotEqual(completion_order, prompts)
    self.assertEqual([r.text for r in results],
                     [f'answer for: {p}' for p in prompts])

  def test_concurrency_is_capped(self):
    lock = threading.Lock()
    state = {'in_flight': 0, 'max_in_flight': 0}

    def slow_generate(**kwargs):
      with lock:
        state['in_flight'] += 1
        state['max_in_flight'] = max(state['max_in_flight'], state['in_flight'])
      time.sleep(0.02)
      with lock:
        state['in_flight'] -= 1
      return _make_response('ok')

    client = _make_mock_client()
    client.models.generate_content.side_effect = slow_generate
    with mock.patch('apache_beam.ml.inference.gemini_inference.'
                    '_MAX_CONCURRENT_REQUESTS_PER_BATCH',
                    2):
      results = generate_from_string(
          _TEST_MODEL, [f'p{i}' for i in range(6)], client, {})

    self.assertEqual(len(results), 6)
    self.assertLessEqual(state['max_in_flight'], 2)

  def test_propagates_request_error(self):
    def fail_on_second_prompt(**kwargs):
      if kwargs['contents'] == 'p1':
        raise errors.APIError(code=429, response_json={})
      return _make_response('ok')

    client = _make_mock_client()
    client.models.generate_content.side_effect = fail_on_second_prompt
    with self.assertRaises(errors.APIError):
      generate_from_string(_TEST_MODEL, ['p0', 'p1', 'p2'], client, {})


class GenerateImageFromStringsAndImagesTest(unittest.TestCase):
  def test_sends_each_element_as_its_own_request(self):
    client = _make_mock_client()
    client.models.generate_content.side_effect = _echo_contents
    elements = [['make it blue', 'context a'], ['make it red', 'context b']]
    results = generate_image_from_strings_and_images(
        _TEST_MODEL, elements, client, {})
    self.assertEqual([r.text for r in results],
                     [f'answer for: {e}' for e in elements])
    calls = client.models.generate_content.call_args_list
    self.assertCountEqual([c.kwargs['contents'] for c in calls], elements)


class GeminiModelHandlerRequestTest(unittest.TestCase):
  def _handler(self, request_fn):
    return GeminiModelHandler(
        model_name=_TEST_MODEL, request_fn=request_fn, api_key='123456789')

  def test_returns_one_result_per_input(self):
    client = _make_mock_client()
    client.models.generate_content.side_effect = _echo_contents
    prompts = ['p0', 'p1', 'p2']
    results = list(self._handler(generate_from_string).request(prompts, client))
    self.assertEqual([(r.example, r.inference.text) for r in results],
                     [(p, f'answer for: {p}') for p in prompts])
    self.assertEqual([r.model_id for r in results], [_TEST_MODEL] * 3)

  def test_accepts_single_response_for_single_input(self):
    def request_fn(model_name, batch, model, inference_args):
      return _make_response('only answer')

    results = list(self._handler(request_fn).request(['p0'], None))
    self.assertEqual([r.inference.text for r in results], ['only answer'])

  def test_rejects_single_response_for_multiple_inputs(self):
    def request_fn(model_name, batch, model, inference_args):
      return _make_response('one merged answer')

    with self.assertRaisesRegex(ValueError, 'one response per input'):
      self._handler(request_fn).request(['p0', 'p1', 'p2'], None)

  def test_rejects_wrong_number_of_responses(self):
    def request_fn(model_name, batch, model, inference_args):
      return [_make_response('a'), _make_response('b')]

    with self.assertRaisesRegex(ValueError, 'one response per input'):
      self._handler(request_fn).request(['p0', 'p1', 'p2'], None)


class GeminiModelHandlerPipelineTest(unittest.TestCase):
  def test_every_input_gets_its_own_result(self):
    handler = GeminiModelHandler(
        model_name=_TEST_MODEL,
        request_fn=_fake_request_fn,
        api_key='123456789',
        min_batch_size=5,
        max_batch_size=5)
    prompts = [f'prompt {i}' for i in range(20)]

    with TestPipeline() as p:
      results = (
          p
          | beam.Create(prompts)
          | RunInference(handler)
          | beam.Map(lambda r: (r.example, r.inference.text)))
      assert_that(results, equal_to([(x, f'answer for: {x}') for x in prompts]))


if __name__ == '__main__':
  unittest.main()
