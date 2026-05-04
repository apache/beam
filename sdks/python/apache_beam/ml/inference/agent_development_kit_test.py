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

import asyncio
import unittest
from unittest import mock

try:
  from google.adk.agents import Agent

  from apache_beam.ml.inference.agent_development_kit import ADKAgentModelHandler
  from apache_beam.ml.inference.base import PredictionResult
except ImportError:
  raise unittest.SkipTest('google-adk dependencies are not installed')


def _make_mock_agent(name: str = "test_agent") -> mock.MagicMock:
  """Returns a mock that quacks like a google.adk.agents.Agent."""
  agent = mock.MagicMock()
  agent.name = name
  return agent


def _make_mock_runner(
    agent: mock.MagicMock,
    final_text: str = "Hello from agent",
) -> mock.MagicMock:
  """Returns a mock Runner whose run_async yields one final-response event."""
  # Build a mock event that looks like a final response
  content = mock.MagicMock()
  content.text = final_text

  event = mock.MagicMock()
  event.is_final_response.return_value = True
  event.content = content

  async def _async_gen(*args, **kwargs):
    yield event

  runner = mock.MagicMock()
  runner.agent = agent
  runner.run_async = mock.MagicMock(side_effect=_async_gen)
  runner.session_service = mock.MagicMock()
  return runner


# ---------------------------------------------------------------------------
# Helper: patch ADK imports inside the module under test so tests work even
# when google-adk is installed (avoids constructing real ADK objects).
# ---------------------------------------------------------------------------
_MODULE = "apache_beam.ml.inference.agent_development_kit"


class TestADKAgentModelHandlerInit(unittest.TestCase):
  """Tests for __init__ argument validation."""
  def test_raises_if_agent_is_none(self):
    with self.assertRaises((ValueError, TypeError)):
      ADKAgentModelHandler(agent=None)

  def test_accepts_agent_object(self):
    agent = _make_mock_agent()
    handler = ADKAgentModelHandler(agent=agent)
    self.assertEqual(handler._agent_or_factory, agent)

  def test_accepts_agent_factory_callable(self):
    agent = _make_mock_agent()
    factory = lambda: agent
    handler = ADKAgentModelHandler(agent=factory)
    self.assertTrue(callable(handler._agent_or_factory))

  def test_default_app_name(self):
    agent = _make_mock_agent()
    handler = ADKAgentModelHandler(agent=agent)
    self.assertEqual(handler._app_name, "beam_inference")

  def test_custom_app_name(self):
    agent = _make_mock_agent()
    handler = ADKAgentModelHandler(agent=agent, app_name="my_app")
    self.assertEqual(handler._app_name, "my_app")

  def test_metrics_namespace(self):
    agent = _make_mock_agent()
    handler = ADKAgentModelHandler(agent=agent)
    self.assertEqual(handler.get_metrics_namespace(), "ADKAgentModelHandler")


class TestLoadModel(unittest.TestCase):
  """Tests for load_model / Runner construction."""
  def test_load_model_with_agent_object(self):
    def get_current_time(city: str) -> dict:
      """Returns the current time in a specified city."""
      return {"status": "success", "city": city, "time": "10:30 AM"}

    agent = Agent(
        model='gemini-3-flash-preview',
        name='root_agent',
        description="Tells the current time in a specified city.",
        instruction=
        "You are a helpful assistant that tells the current time in cities. "
        "Use the 'get_current_time' tool for this purpose.",
        tools=[get_current_time],
    )
    handler = ADKAgentModelHandler(agent=agent, app_name="test_app")
    runner = handler.load_model()

    self.assertEqual(agent, runner.agent)

  @mock.patch(f"{_MODULE}.Runner")
  @mock.patch(f"{_MODULE}.InMemorySessionService")
  def test_load_model_calls_factory(self, mock_session_cls, mock_runner_cls):
    agent = _make_mock_agent()
    factory = mock.MagicMock(return_value=agent)

    handler = ADKAgentModelHandler(agent=factory)
    handler.load_model()

    factory.assert_called_once()
    mock_runner_cls.assert_called_once_with(
        agent=agent,
        app_name="beam_inference",
        session_service=mock_session_cls.return_value,
    )


class TestRunInference(unittest.TestCase):
  """Tests for run_inference output and batching."""
  def test_string_input_returns_prediction_result(self):
    agent = _make_mock_agent()
    runner = _make_mock_runner(agent, final_text="Paris")

    handler = ADKAgentModelHandler(agent=agent)
    results = list(
        handler.run_inference(
            batch=["What is the capital of France?"], model=runner))

    self.assertEqual(len(results), 1)
    pr = results[0]
    self.assertIsInstance(pr, PredictionResult)
    self.assertEqual(pr.example, "What is the capital of France?")
    self.assertEqual(pr.inference, "Paris")
    self.assertEqual(pr.model_id, "test_agent")

  def test_batch_of_strings(self):
    agent = _make_mock_agent()
    runner = _make_mock_runner(agent, final_text="answer")

    handler = ADKAgentModelHandler(agent=agent)
    results = list(
        handler.run_inference(batch=["q1", "q2", "q3"], model=runner))

    self.assertEqual(len(results), 3)
    self.assertEqual([r.example for r in results], ["q1", "q2", "q3"])

  def test_content_object_input(self):
    """Non-string inputs (types.Content) are passed through unchanged."""
    agent = _make_mock_agent()
    runner = _make_mock_runner(agent, final_text="Berlin")

    content_input = mock.MagicMock()  # simulates types.Content

    handler = ADKAgentModelHandler(agent=agent)
    results = list(handler.run_inference(batch=[content_input], model=runner))

    self.assertEqual(len(results), 1)
    self.assertEqual(results[0].example, content_input)
    self.assertEqual(results[0].inference, "Berlin")

  def test_none_inference_args_uses_defaults(self):
    agent = _make_mock_agent()
    runner = _make_mock_runner(agent)

    handler = ADKAgentModelHandler(agent=agent)
    results = list(
        handler.run_inference(
            batch=["hello"], model=runner, inference_args=None))
    self.assertEqual(len(results), 1)

  def test_custom_user_id_passed_to_runner(self):
    agent = _make_mock_agent()
    runner = _make_mock_runner(agent)

    handler = ADKAgentModelHandler(agent=agent)
    handler.run_inference(
        batch=["hi"],
        model=runner,
        inference_args={"user_id": "custom_user"},
    )

    call_kwargs = runner.run_async.call_args[1]
    self.assertEqual(call_kwargs["user_id"], "custom_user")


class TestSessionManagement(unittest.TestCase):
  """Tests for session creation and session_id handling."""
  def test_each_element_gets_unique_session_by_default(self):
    agent = _make_mock_agent()
    runner = _make_mock_runner(agent)

    handler = ADKAgentModelHandler(agent=agent)
    handler.run_inference(batch=["a", "b", "c"], model=runner)

    # create_session should have been called 3 times with distinct session IDs
    calls = runner.session_service.create_session.call_args_list
    self.assertEqual(len(calls), 3)
    session_ids = [c[1]["session_id"] for c in calls]
    self.assertEqual(len(set(session_ids)), 3, "Expected unique session IDs")

  def test_shared_session_id_from_inference_args(self):
    agent = _make_mock_agent()
    runner = _make_mock_runner(agent)

    handler = ADKAgentModelHandler(agent=agent)
    handler.run_inference(
        batch=["turn1", "turn2"],
        model=runner,
        inference_args={"session_id": "my-session"},
    )

    calls = runner.session_service.create_session.call_args_list
    session_ids = [c[1]["session_id"] for c in calls]
    self.assertTrue(
        all(sid == "my-session" for sid in session_ids),
        "All elements should share the provided session_id",
    )

  def test_session_created_with_correct_app_name(self):
    agent = _make_mock_agent()
    runner = _make_mock_runner(agent)

    handler = ADKAgentModelHandler(agent=agent, app_name="my_app")
    handler.run_inference(batch=["hello"], model=runner)

    call_kwargs = runner.session_service.create_session.call_args[1]
    self.assertEqual(call_kwargs["app_name"], "my_app")


class TestResponseExtraction(unittest.TestCase):
  """Tests for extraction of the final response from the event stream."""
  def test_returns_none_when_no_final_response(self):
    """Agent emits only non-final events; inference should be None."""
    agent = _make_mock_agent()

    # Build a runner that yields only non-final events
    non_final_event = mock.MagicMock()
    non_final_event.is_final_response.return_value = False

    async def _async_gen(*args, **kwargs):
      yield non_final_event

    runner = mock.MagicMock()
    runner.agent = agent
    runner.run_async = mock.MagicMock(side_effect=_async_gen)
    runner.session_service = mock.MagicMock()

    handler = ADKAgentModelHandler(agent=agent)
    results = list(handler.run_inference(batch=["hello"], model=runner))

    self.assertEqual(len(results), 1)
    self.assertIsNone(results[0].inference)

  def test_returns_none_when_final_event_has_no_content(self):
    agent = _make_mock_agent()

    event = mock.MagicMock()
    event.is_final_response.return_value = True
    event.content = None

    async def _async_gen(*args, **kwargs):
      yield event

    runner = mock.MagicMock()
    runner.agent = agent
    runner.run_async = mock.MagicMock(side_effect=_async_gen)
    runner.session_service = mock.MagicMock()

    handler = ADKAgentModelHandler(agent=agent)
    results = list(handler.run_inference(batch=["hello"], model=runner))

    self.assertIsNone(results[0].inference)

  def test_stops_after_first_final_response(self):
    """Multiple final events: only the first one's text should be used."""
    agent = _make_mock_agent()

    def _make_event(text: str):
      content = mock.MagicMock()
      content.text = text
      event = mock.MagicMock()
      event.is_final_response.return_value = True
      event.content = content
      return event

    async def _async_gen(*args, **kwargs):
      yield _make_event("first")
      yield _make_event("second")

    runner = mock.MagicMock()
    runner.agent = agent
    runner.run_async = mock.MagicMock(side_effect=_async_gen)
    runner.session_service = mock.MagicMock()

    handler = ADKAgentModelHandler(agent=agent)
    results = list(handler.run_inference(batch=["hi"], model=runner))

    self.assertEqual(results[0].inference, "first")

  def test_invoke_agent_static_method_directly(self):
    """Unit test the async _invoke_agent helper directly."""
    agent = _make_mock_agent()
    runner = _make_mock_runner(agent, final_text="direct result")

    result = asyncio.run(
        ADKAgentModelHandler._invoke_agent(
            runner, "user", "session-1", mock.MagicMock()))
    self.assertEqual(result, "direct result")


if __name__ == '__main__':
  unittest.main()
