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

"""ModelHandler for running agents built with the Google Agent Development Kit.

This module provides :class:`ADKAgentModelHandler`, a Beam
:class:`~apache_beam.ml.inference.base.ModelHandler` that wraps an ADK
:class:`google.adk.agents.llm_agent.LlmAgent` so it can be used with the
:class:`~apache_beam.ml.inference.base.RunInference` transform.

Typical usage::

    import apache_beam as beam
    from apache_beam.ml.inference.base import RunInference
    from apache_beam.ml.inference.agent_development_kit import ADKAgentModelHandler
    from google.adk.agents import LlmAgent

    agent = LlmAgent(
        name="my_agent",
        model="gemini-2.0-flash",
        instruction="You are a helpful assistant.",
    )

    with beam.Pipeline() as p:
        results = (
            p
            | beam.Create(["What is the capital of France?"])
            | RunInference(ADKAgentModelHandler(agent=agent))
        )

If your agent contains state that is not picklable (e.g. tool closures that
capture unpicklable objects), pass a zero-arg factory callable instead::

    handler = ADKAgentModelHandler(agent=lambda: LlmAgent(...))

"""

import asyncio
import logging
import uuid
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any
from typing import Optional
from typing import Union

from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult

try:
  from google.adk import sessions
  from google.adk.agents import Agent
  from google.adk.runners import Runner
  from google.adk.sessions import BaseSessionService
  from google.adk.sessions import InMemorySessionService
  from google.genai.types import Content as genai_Content
  from google.genai.types import Part as genai_Part
  ADK_AVAILABLE = True
except ImportError:
  ADK_AVAILABLE = False
  genai_Content = Any  # type: ignore[assignment, misc]
  genai_Part = Any  # type: ignore[assignment, misc]

LOGGER = logging.getLogger("ADKAgentModelHandler")

# Type alias for an agent or factory that produces one
_AgentOrFactory = Union["Agent", Callable[[], "Agent"]]


class ADKAgentModelHandler(ModelHandler[str | genai_Content,
                                        PredictionResult,
                                        "Runner"]):
  """ModelHandler for running ADK agents with the Beam RunInference transform.

  Accepts either a fully constructed :class:`google.adk.agents.Agent` or a
  zero-arg factory callable that produces one. The factory form is useful when
  the agent contains state that is not picklable and therefore cannot be
  serialized alongside the pipeline graph.

  Each call to :meth:`run_inference` invokes the agent once per element in the
  batch. By default every invocation uses a fresh, isolated session (stateless).
  Stateful multi-turn conversations can be achieved by passing a ``session_id``
  key inside ``inference_args``; elements sharing the same ``session_id`` will
  continue the same conversation history.

  Args:
    agent: A pre-constructed :class:`~google.adk.agents.Agent` instance, or a
      zero-arg callable that returns one. The callable form defers agent
      construction to worker ``load_model`` time, which is useful when the
      agent cannot be serialized.
    app_name: The ADK application name used to namespace sessions. Defaults to
      ``"beam_inference"``.
    session_service_factory: Optional zero-arg callable returning a
      :class:`~google.adk.sessions.BaseSessionService`. When ``None``, an
      :class:`~google.adk.sessions.InMemorySessionService` is created
      automatically.
    min_batch_size: Optional minimum batch size.
    max_batch_size: Optional maximum batch size.
    max_batch_duration_secs: Optional maximum time to buffer a batch before
      emitting; used in streaming contexts.
    max_batch_weight: Optional maximum total weight of a batch.
    element_size_fn: Optional function that returns the size (weight) of an
      element.
  """
  def __init__(
      self,
      agent: _AgentOrFactory,
      app_name: str = "beam_inference",
      session_service_factory: Optional[Callable[[],
                                                 "BaseSessionService"]] = None,
      *,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      max_batch_weight: Optional[int] = None,
      element_size_fn: Optional[Callable[[Any], int]] = None,
      **kwargs):
    if not ADK_AVAILABLE:
      raise ImportError(
          "google-adk is required to use ADKAgentModelHandler. "
          "Install it with: pip install google-adk")

    if agent is None:
      raise ValueError("'agent' must be an Agent instance or a callable.")

    self._agent_or_factory = agent
    self._app_name = app_name
    self._session_service_factory = session_service_factory

    super().__init__(
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
        max_batch_weight=max_batch_weight,
        element_size_fn=element_size_fn,
        **kwargs)

  def load_model(self) -> "Runner":
    """Instantiates the ADK Runner on the worker.

    Resolves the agent (calling the factory if a callable was provided), then
    creates a :class:`~google.adk.runners.Runner` backed by the configured
    session service.

    Returns:
      A fully initialised :class:`~google.adk.runners.Runner`.
    """
    if callable(self._agent_or_factory) and not isinstance(
        self._agent_or_factory, Agent):
      agent = self._agent_or_factory()
    else:
      agent = self._agent_or_factory

    if self._session_service_factory is not None:
      session_service = self._session_service_factory()
    else:
      session_service = InMemorySessionService()

    runner = Runner(
        agent=agent,
        app_name=self._app_name,
        session_service=session_service,
    )
    LOGGER.info(
        "Loaded ADK Runner for agent '%s' (app_name='%s')",
        agent.name,
        self._app_name,
    )
    return runner

  def run_inference(
      self,
      batch: Sequence[str | genai_Content],
      model: "Runner",
      inference_args: Optional[dict[str, Any]] = None,
  ) -> Iterable[PredictionResult]:
    """Runs the ADK agent on each element in the batch.

    Each element is sent to the agent as a new user turn. The final response
    text from the agent is returned as the ``inference`` field of a
    :class:`~apache_beam.ml.inference.base.PredictionResult`.

    Args:
      batch: A sequence of inputs, each of which is either a ``str`` (the user
        message text) or a :class:`google.genai.types.Content` object (for
        richer multi-part messages).
      model: The :class:`~google.adk.runners.Runner` returned by
        :meth:`load_model`.
      inference_args: Optional dict of extra arguments. Supported keys:

        - ``"session_id"`` (:class:`str`): If supplied, all elements in this
          batch share this session ID, enabling stateful multi-turn
          conversations. If omitted, each element receives a unique auto-
          generated session ID.
        - ``"user_id"`` (:class:`str`): The user identifier to pass to the
          runner. Defaults to ``"beam_user"``.

    Returns:
      An iterable of :class:`~apache_beam.ml.inference.base.PredictionResult`,
      one per input element.
    """
    if inference_args is None:
      inference_args = {}

    user_id: str = inference_args.get("user_id", "beam_user")
    agent_invocations = []
    elements_with_sessions = []

    for element in batch:
      session_id: str = inference_args.get("session_id", str(uuid.uuid4()))

      # Ensure a session exists for this invocation
      try:
        model.session_service.create_session(
            app_name=self._app_name,
            user_id=user_id,
            session_id=session_id,
        )
      except sessions.SessionExistsError:
        # It's okay if the session already exists for shared session IDs.
        pass

      # Wrap plain strings in a Content object
      if isinstance(element, str):
        # pyrefly: ignore[bad-instantiation]
        message = genai_Content(role="user", parts=[genai_Part(text=element)])
      else:
        # Assume the caller has already constructed a types.Content object
        message = element

      agent_invocations.append(
          self._invoke_agent(model, user_id, session_id, message))
      elements_with_sessions.append(element)

    # Run all agent invocations concurrently
    async def _run_concurrently():
      return await asyncio.gather(*agent_invocations)

    response_texts = asyncio.run(_run_concurrently())

    results = []
    for i, element in enumerate(elements_with_sessions):
      results.append(
          PredictionResult(
              example=element,
              inference=response_texts[i],
              model_id=model.agent.name,
          ))

    return results

  @staticmethod
  async def _invoke_agent(
      runner: "Runner",
      user_id: str,
      session_id: str,
      message: genai_Content,
  ) -> Optional[str]:
    """Drives the ADK event loop and returns the final response text.

    Args:
      runner: The ADK Runner to invoke.
      user_id: The user ID for this invocation.
      session_id: The session ID for this invocation.
      message: The :class:`google.genai.types.Content` to send.

    Returns:
      The text of the agent's final response, or ``None`` if the agent
      produced no final text response.
    """
    async for event in runner.run_async(
        user_id=user_id,
        session_id=session_id,
        new_message=message,
    ):
      if event.is_final_response():
        if event.content:
          return event.content.text
    return None

  def get_metrics_namespace(self) -> str:
    return "ADKAgentModelHandler"
