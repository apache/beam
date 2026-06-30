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
from apache_beam.ml.inference.base import SubprocessModelHandler

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

if ADK_AVAILABLE:
  try:
    from google.adk.models.base_llm import BaseLlm
  except ImportError:
    try:
      from google.adk.models import BaseLlm
    except ImportError:
      BaseLlm = object
      
  class BeamPlaceholderModel(BaseLlm):
    """Placeholder model to be used when the model will be injected by ADKAgentModelHandler."""
    def __init__(self):
      pass
    async def generate_content_async(self, *args, **kwargs):
      raise NotImplementedError("Placeholder model cannot be used for inference.")
else:
  class BeamPlaceholderModel(str):
    """Placeholder model to be used when the model will be injected by ADKAgentModelHandler.
    
    Fallback when ADK is not available.
    """
    def __new__(cls):
      return super().__new__(cls, "beam-placeholder-model")
  class Agent:
    pass
  class Runner:
    pass
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
  continue the same conversation history. When using stateful conversations,
  it is recommended to use a custom session_service_factory to provide a session
  service implementation which can be managed across multiple workers (e.g.
  :class:`~google.adk.sessions.DatabaseSessionService`). The default
  :class:`~google.adk.sessions.InMemorySessionService` will not correctly track
  the same session across multiple workers.

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
      underlying_model_handler: Optional[SubprocessModelHandler] = None,
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
    self._underlying_model_handler = underlying_model_handler
    self._current_port = None

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
    local_model = None
    underlying_model = None
    
    if self._underlying_model_handler is not None:
      underlying_model = self._underlying_model_handler.load_model()
      self._current_port = self._underlying_model_handler.get_port(underlying_model)
      model_name = self._underlying_model_handler.get_model_name()

      from google.adk.models.lite_llm import LiteLlm
      local_model = LiteLlm(
          model=model_name,
          api_base=f"http://localhost:{self._current_port}/v1"
      )

    # Resolve agent and inject model
    if callable(self._agent_or_factory) and not isinstance(
        self._agent_or_factory, Agent):
      import inspect
      sig = inspect.signature(self._agent_or_factory)
      params = list(sig.parameters.values())
      required_params = [
          p for p in params
          if p.default is inspect.Parameter.empty and p.kind not in (
              inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
      ]
      
      if len(required_params) == 1:
        if local_model is None:
          raise ValueError("Agent factory expects 1 argument but no local model was configured.")
        agent = self._agent_or_factory(local_model)
      elif len(required_params) == 0:
        if local_model is not None and len(params) > 0:
          agent = self._agent_or_factory(local_model)
        else:
          agent = self._agent_or_factory()
          if local_model is not None:
            if not isinstance(agent.model, BeamPlaceholderModel) and agent.model is not None:
              raise ValueError(
                  f"Agent model must be BeamPlaceholderModel or None when using local model. "
                  f"Found: {agent.model}")
            self._set_agent_model(agent, local_model, is_root=True)
      else:
        raise ValueError("Agent factory must take 0 or 1 required argument.")
    else:
      agent = self._agent_or_factory
      if local_model is not None:
        if not isinstance(agent.model, BeamPlaceholderModel) and agent.model is not None:
          raise ValueError(
              f"Agent model must be BeamPlaceholderModel or None when using local model. "
              f"Found: {agent.model}")
        self._set_agent_model(agent, local_model, is_root=True)

    # Validation when local model is NOT used
    if local_model is None:
      if isinstance(agent.model, BeamPlaceholderModel):
        raise ValueError("Agent model cannot be BeamPlaceholderModel when no local model is configured.")

    if self._session_service_factory is not None:
      session_service = self._session_service_factory()
    else:
      session_service = InMemorySessionService()

    runner = Runner(
        agent=agent,
        app_name=self._app_name,
        session_service=session_service,
    )
    
    if underlying_model is not None:
      runner._underlying_model = underlying_model

    LOGGER.info(
        "Loaded ADK Runner for agent '%s' (app_name='%s')",
        agent.name,
        self._app_name,
    )
    return runner

  def _set_agent_model(self, agent: "Agent", model: Any, is_root: bool = False):
    if is_root:
      if isinstance(agent.model, BeamPlaceholderModel) or agent.model is None:
        agent.model = model
    else:
      if isinstance(agent.model, BeamPlaceholderModel):
        agent.model = model

    # Speculative propagation to subagents/tools
    if getattr(agent, 'tools', None) is not None:
      for tool in agent.tools:
        if hasattr(tool, 'agent'):
          self._set_agent_model(tool.agent, model, is_root=False)
        elif isinstance(tool, Agent):
          self._set_agent_model(tool, model, is_root=False)

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
    underlying_model = None
    if self._underlying_model_handler is not None:
      underlying_model = getattr(model, '_underlying_model', None)
      if underlying_model is not None:
        port = self._underlying_model_handler.get_port(underlying_model)
        if port != self._current_port:
          LOGGER.info("Local model server port changed to %d, updating agent.", port)
          self._update_agent_port(model.agent, port)
          self._current_port = port

    try:
      return self._run_inference_internal(batch, model, inference_args)
    except Exception as e:
      if self._underlying_model_handler is not None and underlying_model is not None:
        LOGGER.warning("Inference failed, triggering local server connectivity check.")
        try:
          self._underlying_model_handler.check_connectivity(underlying_model)
        except Exception as recovery_err:
          LOGGER.error("Failed during connectivity check: %s", recovery_err)
      raise e

  def _run_inference_internal(
      self,
      batch: Sequence[str | genai_Content],
      model: "Runner",
      inference_args: Optional[dict[str, Any]] = None,
  ) -> Iterable[PredictionResult]:
    if inference_args is None:
      inference_args = {}

    user_id: str = inference_args.get("user_id", "beam_user")
    agent_invocations = []
    elements_with_sessions = []

    for element in batch:
      session_id: str = inference_args.get("session_id", str(uuid.uuid4()))

      # Wrap plain strings in a Content object
      if isinstance(element, str):
        # pyrefly: ignore[bad-instantiation]
        message = genai_Content(role="user", parts=[genai_Part(text=element)])
      else:
        # Assume the caller has already constructed a types.Content object
        message = element

      agent_invocations.append(
          self._invoke_agent(
              model, user_id, session_id, self._app_name, message))
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

  def _update_agent_port(self, agent: "Agent", port: int):
    if ADK_AVAILABLE:
      from google.adk.models.lite_llm import LiteLlm
      if hasattr(agent, 'model') and isinstance(agent.model, LiteLlm):
        agent.model = LiteLlm(
            model=agent.model.model,
            api_base=f"http://localhost:{port}/v1"
        )
    if getattr(agent, 'tools', None) is not None:
      for tool in agent.tools:
        if hasattr(tool, 'agent'):
          self._update_agent_port(tool.agent, port)
        elif isinstance(tool, Agent):
          self._update_agent_port(tool, port)

  def share_model_across_processes(self) -> bool:
    if self._underlying_model_handler is not None:
      return self._underlying_model_handler.share_model_across_processes()
    return super().share_model_across_processes()

  @staticmethod
  async def _invoke_agent(
      runner: "Runner",
      user_id: str,
      session_id: str,
      app_name: str,
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
    # Check for your specific session ID
    try:
      # Attempt to get the specific session
      await runner.session_service.get_session(session_id)
    except Exception as e:
      await runner.session_service.create_session(
          app_name=app_name,
          user_id=user_id,
          session_id=session_id,
      )

    async for event in runner.run_async(
        user_id=user_id,
        session_id=session_id,
        new_message=message,
    ):
      if event.is_final_response():
        if event.content and event.content.parts:
          return "".join([p.text for p in event.content.parts])
        raise ValueError(
            f"Agent {runner.agent.name} did not return a response, "
            f"final event: {event}")

    raise ValueError(f"Agent {runner.agent.name} did not return a response")

  def get_metrics_namespace(self) -> str:
    return "ADKAgentModelHandler"
