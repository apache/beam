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
from dataclasses import dataclass
from unittest import mock

import httpx

try:
  from openai import APIConnectionError
  from openai import APIStatusError
  from openai import APITimeoutError

  from apache_beam.ml.inference.openai_inference import OpenAIModelHandler
  from apache_beam.ml.inference.openai_inference import _retry_on_appropriate_error
  from apache_beam.ml.inference.openai_inference import chat_completion_from_conversation
  from apache_beam.ml.inference.openai_inference import chat_completion_from_string
  from apache_beam.ml.inference.openai_inference import embedding_from_string
except ImportError:
  raise unittest.SkipTest('OpenAI dependencies are not installed')

import apache_beam as beam
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

_TEST_MODEL = 'gpt-4o-mini'
_TEST_EMBEDDING_MODEL = 'text-embedding-3-small'


@dataclass
class FakeChatMessage:
  content: str
  role: str = 'assistant'


@dataclass
class FakeChoice:
  message: FakeChatMessage
  finish_reason: str = 'stop'
  index: int = 0


@dataclass
class FakeChatCompletion:
  """Picklable stand-in for ChatCompletion."""
  choices: list
  model: str = _TEST_MODEL
  id: str = 'fake-id'


@dataclass
class FakeEmbedding:
  """Picklable stand-in for Embedding."""
  embedding: list
  index: int
  object: str = 'embedding'


@dataclass
class FakeEmbeddingResponse:
  """Picklable stand-in for CreateEmbeddingResponse."""
  data: list
  model: str = _TEST_EMBEDDING_MODEL


def _make_fake_chat_response(text):
  return FakeChatCompletion(
      choices=[FakeChoice(message=FakeChatMessage(content=text))])


def _make_fake_embedding_response(batch, embedding_dim=4):
  data = [
      FakeEmbedding(embedding=[0.1 * (i + 1)] * embedding_dim, index=i)
      for i in range(len(batch))
  ]
  return FakeEmbeddingResponse(data=data)


def _make_mock_http_response(status_code: int) -> httpx.Response:
  req = httpx.Request('POST', 'https://api.openai.com/v1/chat/completions')
  return httpx.Response(status_code=status_code, request=req)


def _make_mock_client() -> mock.MagicMock:
  client = mock.MagicMock()
  client.chat.completions.create = mock.MagicMock()
  client.embeddings.create = mock.MagicMock()
  return client


def _echo_last_message(**kwargs):
  """Returns a fake chat response derived from the request's last message.

  Chat requests within a batch are issued concurrently, so call order is
  nondeterministic; keying the response on the request contents lets tests
  check that results are returned in batch order.
  """
  return _make_fake_chat_response(
      f"answer for: {kwargs['messages'][-1]['content']}")


class RetryOnErrorTest(unittest.TestCase):
  def test_retry_on_rate_limit(self):
    e = APIStatusError(
        message="Rate limited",
        response=_make_mock_http_response(429),
        body=None)
    self.assertTrue(_retry_on_appropriate_error(e))

  def test_retry_on_server_error(self):
    e = APIStatusError(
        message="Internal server error",
        response=_make_mock_http_response(500),
        body=None)
    self.assertTrue(_retry_on_appropriate_error(e))

  def test_retry_on_503(self):
    e = APIStatusError(
        message="Service unavailable",
        response=_make_mock_http_response(503),
        body=None)
    self.assertTrue(_retry_on_appropriate_error(e))

  def test_retry_on_connection_error(self):
    req = httpx.Request('POST', 'https://api.openai.com/v1/chat/completions')
    e = APIConnectionError(request=req)
    self.assertTrue(_retry_on_appropriate_error(e))

  def test_retry_on_timeout_error(self):
    req = httpx.Request('POST', 'https://api.openai.com/v1/chat/completions')
    e = APITimeoutError(request=req)
    self.assertTrue(_retry_on_appropriate_error(e))

  def test_no_retry_on_400(self):
    e = APIStatusError(
        message="Bad request",
        response=_make_mock_http_response(400),
        body=None)
    self.assertFalse(_retry_on_appropriate_error(e))

  def test_no_retry_on_401(self):
    e = APIStatusError(
        message="Unauthorized",
        response=_make_mock_http_response(401),
        body=None)
    self.assertFalse(_retry_on_appropriate_error(e))

  def test_no_retry_on_403(self):
    e = APIStatusError(
        message="Forbidden", response=_make_mock_http_response(403), body=None)
    self.assertFalse(_retry_on_appropriate_error(e))

  def test_no_retry_on_non_api_error(self):
    self.assertFalse(_retry_on_appropriate_error(ValueError("oops")))
    self.assertFalse(_retry_on_appropriate_error(RuntimeError("fail")))


class ChatCompletionFromStringTest(unittest.TestCase):
  def test_sends_each_prompt(self):
    client = _make_mock_client()
    client.chat.completions.create.side_effect = _echo_last_message
    results = chat_completion_from_string(
        _TEST_MODEL, ['hello', 'world'], client, {})
    self.assertEqual(len(results), 2)
    self.assertEqual(results[0].choices[0].message.content, "answer for: hello")
    self.assertEqual(results[1].choices[0].message.content, "answer for: world")
    self.assertEqual(client.chat.completions.create.call_count, 2)

    calls = client.chat.completions.create.call_args_list
    sent_messages = sorted((c.kwargs['messages'] for c in calls),
                           key=lambda m: m[0]['content'])
    self.assertEqual(
        sent_messages,
        [[{
            "role": "user", "content": "hello"
        }], [{
            "role": "user", "content": "world"
        }]])
    for c in calls:
      self.assertEqual(c.kwargs['model'], _TEST_MODEL)

  def test_requests_run_concurrently_and_preserve_order(self):
    lock = threading.Lock()
    state = {'in_flight': 0, 'max_in_flight': 0}
    completion_order = []
    prompts = [f'p{i}' for i in range(4)]

    def slow_create(**kwargs):
      prompt = kwargs['messages'][-1]['content']
      with lock:
        state['in_flight'] += 1
        state['max_in_flight'] = max(state['max_in_flight'], state['in_flight'])
      # Earlier prompts sleep longer so they finish last.
      time.sleep(0.05 * (len(prompts) - prompts.index(prompt)))
      with lock:
        state['in_flight'] -= 1
        completion_order.append(prompt)
      return _make_fake_chat_response(f"answer for: {prompt}")

    client = _make_mock_client()
    client.chat.completions.create.side_effect = slow_create
    results = chat_completion_from_string(_TEST_MODEL, prompts, client, {})

    self.assertGreater(state['max_in_flight'], 1)
    self.assertNotEqual(completion_order, prompts)
    self.assertEqual([r.choices[0].message.content for r in results],
                     [f"answer for: {p}" for p in prompts])

  def test_concurrency_is_capped(self):
    lock = threading.Lock()
    state = {'in_flight': 0, 'max_in_flight': 0}

    def slow_create(**kwargs):
      with lock:
        state['in_flight'] += 1
        state['max_in_flight'] = max(state['max_in_flight'], state['in_flight'])
      time.sleep(0.02)
      with lock:
        state['in_flight'] -= 1
      return _make_fake_chat_response("ok")

    client = _make_mock_client()
    client.chat.completions.create.side_effect = slow_create
    with mock.patch('apache_beam.ml.inference.openai_inference.'
                    '_MAX_CONCURRENT_REQUESTS_PER_BATCH',
                    2):
      results = chat_completion_from_string(
          _TEST_MODEL, [f'p{i}' for i in range(6)], client, {})

    self.assertEqual(len(results), 6)
    self.assertLessEqual(state['max_in_flight'], 2)

  def test_propagates_request_error(self):
    rate_limited = APIStatusError(
        message="Rate limited",
        response=_make_mock_http_response(429),
        body=None)

    def create(**kwargs):
      if kwargs['messages'][-1]['content'] == 'bad':
        raise rate_limited
      return _make_fake_chat_response("ok")

    client = _make_mock_client()
    client.chat.completions.create.side_effect = create
    with self.assertRaises(APIStatusError) as ctx:
      chat_completion_from_string(
          _TEST_MODEL, ['good', 'bad', 'good'], client, {})
    self.assertIs(ctx.exception, rate_limited)
    self.assertTrue(_retry_on_appropriate_error(ctx.exception))

  def test_passes_inference_args(self):
    client = _make_mock_client()
    client.chat.completions.create.return_value = _make_fake_chat_response("ok")
    chat_completion_from_string(
        _TEST_MODEL, ['test'], client, {
            'max_tokens': 2048, 'temperature': 0.5
        })
    call_args = client.chat.completions.create.call_args
    self.assertEqual(call_args.kwargs['max_tokens'], 2048)
    self.assertEqual(call_args.kwargs['temperature'], 0.5)

  def test_prepends_system_prompt(self):
    client = _make_mock_client()
    client.chat.completions.create.return_value = _make_fake_chat_response("ok")
    chat_completion_from_string(
        _TEST_MODEL, ['test'], client, {'system': 'You are helpful.'})
    call_args = client.chat.completions.create.call_args
    self.assertEqual(
        call_args.kwargs['messages'],
        [
            {
                "role": "system", "content": "You are helpful."
            },
            {
                "role": "user", "content": "test"
            },
        ])
    self.assertNotIn('system', call_args.kwargs)


class ChatCompletionFromConversationTest(unittest.TestCase):
  def test_sends_conversation(self):
    client = _make_mock_client()
    client.chat.completions.create.return_value = (
        _make_fake_chat_response("Paris!"))
    convo = [
        {
            "role": "user", "content": "What is the capital of France?"
        },
    ]
    results = chat_completion_from_conversation(
        _TEST_MODEL, [convo], client, {})
    self.assertEqual(len(results), 1)
    call_args = client.chat.completions.create.call_args
    self.assertEqual(call_args.kwargs['messages'], convo)

  def test_prepends_system_prompt_to_conversation(self):
    client = _make_mock_client()
    client.chat.completions.create.return_value = _make_fake_chat_response("ok")
    convo = [
        {
            "role": "user", "content": "Hello"
        },
    ]
    chat_completion_from_conversation(
        _TEST_MODEL, [convo], client, {'system': 'Be concise.'})
    call_args = client.chat.completions.create.call_args
    self.assertEqual(
        call_args.kwargs['messages'],
        [
            {
                "role": "system", "content": "Be concise."
            },
            {
                "role": "user", "content": "Hello"
            },
        ])
    self.assertNotIn('system', call_args.kwargs)


class EmbeddingFromStringTest(unittest.TestCase):
  def test_sends_batch_to_embeddings(self):
    client = _make_mock_client()
    fake_resp = _make_fake_embedding_response(['hello', 'world'])
    client.embeddings.create.return_value = fake_resp

    results = embedding_from_string(
        _TEST_EMBEDDING_MODEL, ['hello', 'world'], client, {})
    self.assertEqual(len(results), 2)
    client.embeddings.create.assert_called_once_with(
        model=_TEST_EMBEDDING_MODEL, input=['hello', 'world'])

  def test_preserves_order_via_index(self):
    client = _make_mock_client()
    # Out of order data
    fake_resp = FakeEmbeddingResponse(
        data=[
            FakeEmbedding(embedding=[0.2], index=1),
            FakeEmbedding(embedding=[0.1], index=0),
        ])
    client.embeddings.create.return_value = fake_resp

    results = embedding_from_string(
        _TEST_EMBEDDING_MODEL, ['first', 'second'], client, {})
    self.assertEqual(results[0].index, 0)
    self.assertEqual(results[0].embedding, [0.1])
    self.assertEqual(results[1].index, 1)
    self.assertEqual(results[1].embedding, [0.2])

  def test_passes_inference_args(self):
    client = _make_mock_client()
    client.embeddings.create.return_value = _make_fake_embedding_response(['a'])
    embedding_from_string(
        _TEST_EMBEDDING_MODEL, ['a'], client, {'dimensions': 256})
    call_args = client.embeddings.create.call_args
    self.assertEqual(call_args.kwargs['dimensions'], 256)


class OpenAIModelHandlerTest(unittest.TestCase):
  @mock.patch('apache_beam.ml.inference.openai_inference.OpenAI')
  def test_create_client_with_api_key(self, mock_openai):
    handler = OpenAIModelHandler(
        model_name=_TEST_MODEL,
        request_fn=chat_completion_from_string,
        api_key='test-key-123')
    handler.create_client()
    mock_openai.assert_called_once_with(api_key='test-key-123', max_retries=0)

  @mock.patch('apache_beam.ml.inference.openai_inference.OpenAI')
  def test_create_client_from_env(self, mock_openai):
    handler = OpenAIModelHandler(
        model_name=_TEST_MODEL, request_fn=chat_completion_from_string)
    handler.create_client()
    mock_openai.assert_called_once_with(max_retries=0)

  @mock.patch('apache_beam.ml.inference.openai_inference.OpenAI')
  def test_create_client_with_all_options(self, mock_openai):
    handler = OpenAIModelHandler(
        model_name=_TEST_MODEL,
        request_fn=chat_completion_from_string,
        api_key='test-key-123',
        organization='test-org',
        project='test-proj',
        base_url='https://custom.endpoint.com/v1',
        client_args={'timeout': 30.0})
    handler.create_client()
    mock_openai.assert_called_once_with(
        api_key='test-key-123',
        organization='test-org',
        project='test-proj',
        base_url='https://custom.endpoint.com/v1',
        max_retries=0,
        timeout=30.0)

  def test_request_returns_prediction_results(self):
    handler = OpenAIModelHandler(
        model_name=_TEST_MODEL,
        request_fn=chat_completion_from_string,
        api_key='fake')
    mock_client = _make_mock_client()
    responses = {
        'q1': _make_fake_chat_response("answer 1"),
        'q2': _make_fake_chat_response("answer 2"),
    }
    mock_client.chat.completions.create.side_effect = (
        lambda **kwargs: responses[kwargs['messages'][-1]['content']])

    results = list(handler.request(['q1', 'q2'], mock_client, {}))

    self.assertEqual(len(results), 2)
    self.assertIsInstance(results[0], PredictionResult)
    self.assertEqual(results[0].example, 'q1')
    self.assertEqual(results[0].inference, responses['q1'])
    self.assertEqual(results[0].model_id, _TEST_MODEL)
    self.assertEqual(results[1].example, 'q2')
    self.assertEqual(results[1].inference, responses['q2'])

  def test_batch_elements_kwargs(self):
    handler = OpenAIModelHandler(
        model_name=_TEST_MODEL,
        request_fn=chat_completion_from_string,
        api_key='fake',
        min_batch_size=2,
        max_batch_size=10,
        max_batch_duration_secs=5,
        max_batch_weight=100)
    kwargs = handler.batch_elements_kwargs()
    self.assertEqual(kwargs['min_batch_size'], 2)
    self.assertEqual(kwargs['max_batch_size'], 10)
    self.assertEqual(kwargs['max_batch_duration_secs'], 5)
    self.assertEqual(kwargs['max_batch_weight'], 100)

  def test_custom_retry_filter(self):
    custom_filter = lambda e: False
    handler = OpenAIModelHandler(
        model_name=_TEST_MODEL,
        request_fn=chat_completion_from_string,
        retry_filter=custom_filter)
    self.assertEqual(handler.retry_filter, custom_filter)


class SystemPromptTest(unittest.TestCase):
  def test_system_prompt_passed_via_inference_args(self):
    handler = OpenAIModelHandler(
        model_name=_TEST_MODEL,
        request_fn=chat_completion_from_string,
        api_key='fake')
    mock_client = _make_mock_client()
    mock_client.chat.completions.create.return_value = (
        _make_fake_chat_response("ok"))

    handler.request(['test'], mock_client, {'system': 'Be concise.'})

    call_args = mock_client.chat.completions.create.call_args
    self.assertEqual(
        call_args.kwargs['messages'][0], {
            "role": "system", "content": "Be concise."
        })
    self.assertNotIn('system', call_args.kwargs)

  def test_no_system_prompt_when_omitted(self):
    handler = OpenAIModelHandler(
        model_name=_TEST_MODEL,
        request_fn=chat_completion_from_string,
        api_key='fake')
    mock_client = _make_mock_client()
    mock_client.chat.completions.create.return_value = (
        _make_fake_chat_response("ok"))

    handler.request(['test'], mock_client, {})

    call_args = mock_client.chat.completions.create.call_args
    self.assertEqual(
        call_args.kwargs['messages'], [{
            "role": "user", "content": "test"
        }])


class ResponseFormatTest(unittest.TestCase):
  _SCHEMA = {
      'type': 'json_schema',
      'json_schema': {
          'name': 'answer_schema',
          'schema': {
              'type': 'object',
              'properties': {
                  'answer': {
                      'type': 'string'
                  }
              },
              'required': ['answer'],
              'additionalProperties': False,
          },
          'strict': True,
      },
  }

  def test_response_format_passed_via_inference_args(self):
    handler = OpenAIModelHandler(
        model_name=_TEST_MODEL,
        request_fn=chat_completion_from_string,
        api_key='fake')
    mock_client = _make_mock_client()
    mock_client.chat.completions.create.return_value = (
        _make_fake_chat_response('{"answer":"ok"}'))

    handler.request(['test'], mock_client, {'response_format': self._SCHEMA})

    call_args = mock_client.chat.completions.create.call_args
    self.assertEqual(call_args.kwargs['response_format'], self._SCHEMA)

  def test_no_response_format_when_omitted(self):
    handler = OpenAIModelHandler(
        model_name=_TEST_MODEL,
        request_fn=chat_completion_from_string,
        api_key='fake')
    mock_client = _make_mock_client()
    mock_client.chat.completions.create.return_value = (
        _make_fake_chat_response("ok"))

    handler.request(['test'], mock_client, {})

    call_args = mock_client.chat.completions.create.call_args
    self.assertNotIn('response_format', call_args.kwargs)


def _fake_chat_request_fn(model_name, batch, client, inference_args):
  """A picklable request function that returns fake chat responses."""
  return [
      FakeChatCompletion(
          choices=[
              FakeChoice(message=FakeChatMessage(content=f'answer for: {p}'))
          ]) for p in batch
  ]


def _fake_embedding_request_fn(model_name, batch, client, inference_args):
  """A picklable request function that returns fake embedding responses."""
  return [
      FakeEmbedding(embedding=[0.1, 0.2], index=i) for i, _ in enumerate(batch)
  ]


class OpenAIRunInferencePipelineTest(unittest.TestCase):
  def test_pipeline_chat_e2e(self):
    handler = OpenAIModelHandler(
        model_name=_TEST_MODEL,
        request_fn=_fake_chat_request_fn,
        api_key='fake-key',
        max_batch_size=5,
    )

    prompts = ['What is Beam?', 'What is MapReduce?']

    with TestPipeline() as p:
      results = (
          p
          | beam.Create(prompts)
          | RunInference(handler)
          | beam.Map(lambda r: r.example))
      assert_that(results, equal_to(prompts))

  def test_pipeline_with_system_prompt(self):
    handler = OpenAIModelHandler(
        model_name=_TEST_MODEL,
        request_fn=_fake_chat_request_fn,
        api_key='fake-key',
        max_batch_size=5,
    )

    prompts = ['Tell me about Beam.']

    with TestPipeline() as p:
      results = (
          p
          | beam.Create(prompts)
          | RunInference(
              handler, inference_args={'system': 'You respond in haiku form.'})
          | beam.Map(lambda r: r.example))
      assert_that(results, equal_to(prompts))

  def test_pipeline_embeddings_e2e(self):
    handler = OpenAIModelHandler(
        model_name=_TEST_EMBEDDING_MODEL,
        request_fn=_fake_embedding_request_fn,
        api_key='fake-key',
        max_batch_size=5,
    )

    inputs = ['text one', 'text two']

    with TestPipeline() as p:
      results = (
          p
          | beam.Create(inputs)
          | RunInference(handler)
          | beam.Map(lambda r: r.example))
      assert_that(results, equal_to(inputs))


if __name__ == '__main__':
  unittest.main()
