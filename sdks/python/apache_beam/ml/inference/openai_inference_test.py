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

import httpx
import logging
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.ml.inference.openai_inference import OpenAIModelHandler
  from apache_beam.ml.inference.openai_inference import _retry_on_appropriate_openai_error
  from openai import APIError
  from openai import RateLimitError
  from openai.types.chat.chat_completion import ChatCompletion
  from openai.types.chat.chat_completion import Choice as ChatChoice
  from openai.types.chat.chat_completion_message import ChatCompletionMessage
  from openai.types.completion import Completion
  from openai.types.completion_choice import CompletionChoice
except ImportError:
  raise unittest.SkipTest('OpenAI dependencies are not installed')

from apache_beam.ml.inference.base import PredictionResult

# Configure logger for debugging tests related to _retry_on_appropriate_openai_error
# This gets the logger instance used in openai_inference.py
logger_to_debug = logging.getLogger("OpenAIModelHandler")
logger_to_debug.setLevel(logging.DEBUG)
# Add a handler to see the output during tests, e.g., stream to stderr
# Check if a handler already exists to avoid duplicate messages if tests are run multiple times
if not any(isinstance(h, logging.StreamHandler)
           for h in logger_to_debug.handlers):
  stream_handler = logging.StreamHandler()
  stream_handler.setLevel(logging.DEBUG)
  formatter = logging.Formatter(
      '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  stream_handler.setFormatter(formatter)
  logger_to_debug.addHandler(stream_handler)


class RetryOnAPIErrorTest(unittest.TestCase):
  def _create_mock_error_with_status(self, status_code, error_class=APIError):
    """
    Helper to create a mock error object (APIError or RateLimitError)
    with a given status code.
    The key is to ensure that `getattr(err, 'status_code', None)` works as expected.
    For real OpenAI errors:
    - RateLimitError (and other APIStatusErrors) have `err.status_code` as a direct attribute.
    - APIError (the base) has `err.status_code` as a property that inspects `err.request.response.status_code`.
    """
    mock_response = MagicMock(spec=httpx.Response)
    # mock_response.status_code will be set below.
    # Ensure headers is a mock that can handle .get() for RateLimitError
    mock_response.headers = MagicMock(spec=httpx.Headers)
    mock_response.headers.get.return_value = "test-request-id"  # For x-request-id
    mock_response.content = b"{}"
    mock_response.text = "{}"

    mock_request_obj = MagicMock(spec=httpx.Request)
    mock_request_obj.method = "POST"
    mock_request_obj.url = httpx.URL("https://api.openai.com/v1/completions")

    mock_response.request = mock_request_obj

    if error_class == RateLimitError:
      mock_response.status_code = status_code
      err = RateLimitError("rate limited", response=mock_response, body=None)
    else:  # Generic APIError
      mock_request_that_failed = MagicMock(spec=httpx.Request)
      mock_request_that_failed.method = "POST"
      mock_request_that_failed.url = httpx.URL(
          "https://api.openai.com/v1/completions")

      # This is the response that APIError.status_code property will look for
      response_for_api_error_property = MagicMock(spec=httpx.Response)
      response_for_api_error_property.status_code = status_code
      mock_request_that_failed.response = response_for_api_error_property

      err = APIError("API error", request=mock_request_that_failed, body=None)
      # Directly set status_code on the instance for getattr in the retry function to pick up.
      # This is simpler than ensuring the nested property mock works perfectly.
      # Note: This shadows the property for this instance.
      err.status_code = status_code
    return err

  def test_retry_on_rate_limit_error(self):
    err = self._create_mock_error_with_status(429, error_class=RateLimitError)
    self.assertTrue(_retry_on_appropriate_openai_error(err))

  def test_retry_on_server_error_500(self):
    err = self._create_mock_error_with_status(500, error_class=APIError)
    self.assertTrue(_retry_on_appropriate_openai_error(err))

  def test_retry_on_server_error_503(self):
    err = self._create_mock_error_with_status(503, error_class=APIError)
    self.assertTrue(_retry_on_appropriate_openai_error(err))

  def test_no_retry_on_client_error_400(self):
    err = self._create_mock_error_with_status(400, error_class=APIError)
    self.assertFalse(_retry_on_appropriate_openai_error(err))

  def test_no_retry_on_client_error_401(self):
    err = self._create_mock_error_with_status(401, error_class=APIError)
    self.assertFalse(_retry_on_appropriate_openai_error(err))

  def test_no_retry_on_non_openai_error(self):
    self.assertFalse(
        _retry_on_appropriate_openai_error(ValueError("some other error")))


class OpenAIModelHandlerTest(unittest.TestCase):
  def setUp(self):
    self.api_key = "test_api_key"
    self.model_name = "gpt-3.5-turbo-instruct"  # A completion model
    self.chat_model_name = "gpt-3.5-turbo"  # A chat model

  @patch('openai.OpenAI')
  def test_create_client(self, mock_openai_client_constructor):
    mock_client_instance = MagicMock()
    mock_openai_client_constructor.return_value = mock_client_instance

    handler = OpenAIModelHandler(api_key=self.api_key, model=self.model_name)
    client = handler.create_client()

    mock_openai_client_constructor.assert_called_once_with(api_key=self.api_key)
    self.assertEqual(client, mock_client_instance)
    # Test if client is cached
    client2 = handler.create_client()
    mock_openai_client_constructor.assert_called_once(
    )  # Should still be called only once
    self.assertEqual(client2, mock_client_instance)

  @patch('openai.OpenAI')
  def test_request_completion_model_success(
      self, mock_openai_client_constructor):
    mock_openai_client = MagicMock()
    mock_openai_client_constructor.return_value = mock_openai_client

    # Mock the response from client.completions.create
    mock_completion_response = Completion(
        id="cmpl-test",
        object="text_completion",
        created=12345,
        model=self.model_name,
        choices=[
            CompletionChoice(
                text=" World!", index=0, finish_reason="length", logprobs=None)
        ])
    mock_openai_client.completions.create.return_value = mock_completion_response

    handler = OpenAIModelHandler(api_key=self.api_key, model=self.model_name)
    # Initialize client by calling create_client or load_model
    client = handler.load_model()
    prompts = ["Hello", "Hi"]
    results_generator = handler.request(prompts, client, {})
    results = list(results_generator)

    self.assertEqual(len(results), 2)
    self.assertIsInstance(results[0], PredictionResult)
    self.assertEqual(results[0].example, "Hello")
    self.assertEqual(results[0].inference, " World!")
    self.assertEqual(results[0].model_id, self.model_name)
    self.assertEqual(results[1].example, "Hi")
    self.assertEqual(
        results[1].inference, " World!")  # Same mock response for both

    self.assertEqual(mock_openai_client.completions.create.call_count, 2)
    mock_openai_client.completions.create.assert_any_call(
        model=self.model_name, prompt="Hello")
    mock_openai_client.completions.create.assert_any_call(
        model=self.model_name, prompt="Hi")

  @patch('openai.OpenAI')
  def test_request_chat_model_success(self, mock_openai_client_constructor):
    mock_openai_client = MagicMock()
    # Simulate chat model by checking a mock attribute on the client's chat completions path
    # This is a bit of a hack for testing the path in generate_completion
    mock_openai_client.chat.completions.with_raw_response.create.binary_relative_path = "chat.completions"
    mock_openai_client_constructor.return_value = mock_openai_client

    # Mock the response from client.chat.completions.create
    mock_chat_response = ChatCompletion(
        id="chatcmpl-test",
        object="chat.completion",
        created=12345,
        model=self.chat_model_name,
        choices=[
            ChatChoice(
                index=0,
                message=ChatCompletionMessage(
                    role="assistant", content="There!"),
                finish_reason="stop")
        ])
    mock_openai_client.chat.completions.create.return_value = mock_chat_response

    handler = OpenAIModelHandler(
        api_key=self.api_key, model=self.chat_model_name)
    client = handler.load_model()  # This calls create_client
    prompts = [
        "User prompt 1", [{
            "role": "user", "content": "User prompt 2"
        }]
    ]  # Test both string and message list
    results_generator = handler.request(prompts, client, {"temperature": 0.5})
    results = list(results_generator)

    self.assertEqual(len(results), 2)
    self.assertIsInstance(results[0], PredictionResult)
    self.assertEqual(results[0].example, "User prompt 1")
    self.assertEqual(results[0].inference, "There!")
    self.assertEqual(results[0].model_id, self.chat_model_name)

    self.assertEqual(
        results[1].example, [{
            "role": "user", "content": "User prompt 2"
        }])
    self.assertEqual(results[1].inference, "There!")

    self.assertEqual(mock_openai_client.chat.completions.create.call_count, 2)
    mock_openai_client.chat.completions.create.assert_any_call(
        model=self.chat_model_name,
        messages=[{
            "role": "user", "content": "User prompt 1"
        }],
        temperature=0.5)
    mock_openai_client.chat.completions.create.assert_any_call(
        model=self.chat_model_name,
        messages=[{
            "role": "user", "content": "User prompt 2"
        }],
        temperature=0.5)

  @patch('openai.OpenAI')
  def test_request_failure_propagates(self, mock_openai_client_constructor):
    mock_openai_client = MagicMock()
    mock_openai_client_constructor.return_value = mock_openai_client

    # Simulate an API error during the first call
    mock_openai_client.completions.create.side_effect = RateLimitError(
        "rate limited", response=MagicMock(), body=None)

    handler = OpenAIModelHandler(api_key=self.api_key, model=self.model_name)
    client = handler.load_model()
    prompts = ["Prompt that will fail"]

    with self.assertRaises(RateLimitError):
      list(handler.request(prompts, client, {}))

    mock_openai_client.completions.create.assert_called_once_with(
        model=self.model_name, prompt="Prompt that will fail")

  def test_batch_elements_kwargs(self):
    handler = OpenAIModelHandler(
        api_key=self.api_key,
        model=self.model_name,
        min_batch_size=1,
        max_batch_size=10,
        max_batch_duration_secs=30)
    kwargs = handler.batch_elements_kwargs()
    self.assertEqual(kwargs['min_batch_size'], 1)
    self.assertEqual(kwargs['max_batch_size'], 10)
    self.assertEqual(kwargs['max_batch_duration_secs'], 30)

  def test_validate_inference_args(self):
    handler = OpenAIModelHandler(api_key=self.api_key, model=self.model_name)
    # This method is a pass-through for now, so just ensure it doesn't raise.
    try:
      handler.validate_inference_args({"temperature": 0.7})
      handler.validate_inference_args(None)
    except Exception as e:
      self.fail(f"validate_inference_args raised an exception: {e}")


if __name__ == '__main__':
  unittest.main()
