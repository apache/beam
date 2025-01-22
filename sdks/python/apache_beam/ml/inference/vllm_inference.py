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
import logging
import os
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence
from typing import Tuple

from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.utils import subprocess_server
from openai import AsyncOpenAI
from openai import OpenAI

try:
  import vllm  # pylint: disable=unused-import
  logging.info('vllm module successfully imported.')
except ModuleNotFoundError:
  msg = 'vllm module was not found. This is ok as long as the specified ' \
    'runner has vllm dependencies installed.'
  logging.warning(msg)

__all__ = [
    'OpenAIChatMessage',
    'VLLMCompletionsModelHandler',
    'VLLMChatModelHandler',
]


@dataclass(frozen=True)
class OpenAIChatMessage():
  """"
  Dataclass containing previous chat messages in conversation.
  Role is the entity that sent the message (either 'user' or 'system').
  Content is the contents of the message.
  """
  role: str
  content: str


def start_process(cmd) -> Tuple[subprocess.Popen, int]:
  port, = subprocess_server.pick_port(None)
  cmd = [arg.replace('{{PORT}}', str(port)) for arg in cmd]  # pylint: disable=not-an-iterable
  logging.info("Starting service with %s", str(cmd).replace("',", "'"))
  process = subprocess.Popen(
      cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

  # Emit the output of this command as info level logging.
  def log_stdout():
    line = process.stdout.readline()
    while line:
      # The log obtained from stdout is bytes, decode it into string.
      # Remove newline via rstrip() to not print an empty line.
      logging.info(line.decode(errors='backslashreplace').rstrip())
      line = process.stdout.readline()

  t = threading.Thread(target=log_stdout)
  t.daemon = True
  t.start()
  return process, port


def getVLLMClient(port) -> OpenAI:
  openai_api_key = "EMPTY"
  openai_api_base = f"http://localhost:{port}/v1"
  return OpenAI(
      api_key=openai_api_key,
      base_url=openai_api_base,
  )


def getAsyncVLLMClient(port) -> AsyncOpenAI:
  openai_api_key = "EMPTY"
  openai_api_base = f"http://localhost:{port}/v1"
  return AsyncOpenAI(
      api_key=openai_api_key,
      base_url=openai_api_base,
  )


class _VLLMModelServer():
  def __init__(self, model_name: str, vllm_server_kwargs: Dict[str, str]):
    self._model_name = model_name
    self._vllm_server_kwargs = vllm_server_kwargs
    self._server_started = False
    self._server_process = None
    self._server_port: int = -1

    self.start_server()

  def start_server(self, retries=3):
    if not self._server_started:
      server_cmd = [
          sys.executable,
          '-m',
          'vllm.entrypoints.openai.api_server',
          '--model',
          self._model_name,
          '--port',
          '{{PORT}}',
      ]
      for k, v in self._vllm_server_kwargs.items():
        server_cmd.append(f'--{k}')
        server_cmd.append(v)
      self._server_process, self._server_port = start_process(server_cmd)

    self.check_connectivity(retries)

  def get_server_port(self) -> int:
    if not self._server_started:
      self.start_server()
    return self._server_port

  def check_connectivity(self, retries=3):
    client = getVLLMClient(self._server_port)
    while self._server_process.poll() is None:
      try:
        models = client.models.list().data
        logging.info('models: %s' % models)
        if len(models) > 0:
          self._server_started = True
          return
      except:  # pylint: disable=bare-except
        pass
      # Sleep while bringing up the process
      time.sleep(5)

    if retries == 0:
      self._server_started = False
      raise Exception(
          "Failed to start vLLM server, polling process exited with code " +
          "%s.  Next time a request is tried, the server will be restarted" %
          self._server_process.poll())
    else:
      self.start_server(retries - 1)


class VLLMCompletionsModelHandler(ModelHandler[str,
                                               PredictionResult,
                                               _VLLMModelServer]):
  def __init__(
      self,
      model_name: str,
      vllm_server_kwargs: Optional[Dict[str, str]] = None):
    """Implementation of the ModelHandler interface for vLLM using text as
    input.

    Example Usage::

      pcoll | RunInference(VLLMModelHandler(model_name='facebook/opt-125m'))

    Args:
      model_name: The vLLM model. See
        https://docs.vllm.ai/en/latest/models/supported_models.html for
        supported models.
      vllm_server_kwargs: Any additional kwargs to be passed into your vllm
        server when it is being created. Will be invoked using
        `python -m vllm.entrypoints.openai.api_serverv <beam provided args>
        <vllm_server_kwargs>`. For example, you could pass
        `{'echo': 'true'}` to prepend new messages with the previous message.
        For a list of possible kwargs, see
        https://docs.vllm.ai/en/latest/serving/openai_compatible_server.html#extra-parameters-for-completions-api
    """
    self._model_name = model_name
    self._vllm_server_kwargs: Dict[str, str] = vllm_server_kwargs or {}
    self._env_vars = {}

  def load_model(self) -> _VLLMModelServer:
    return _VLLMModelServer(self._model_name, self._vllm_server_kwargs)

  async def _async_run_inference(
      self,
      batch: Sequence[str],
      model: _VLLMModelServer,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    client = getAsyncVLLMClient(model.get_server_port())
    inference_args = inference_args or {}
    async_predictions = []
    for prompt in batch:
      try:
        completion = client.completions.create(
            model=self._model_name, prompt=prompt, **inference_args)
        async_predictions.append(completion)
      except Exception as e:
        model.check_connectivity()
        raise e

    predictions = []
    for p in async_predictions:
      try:
        predictions.append(await p)
      except Exception as e:
        model.check_connectivity()
        raise e

    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

  def run_inference(
      self,
      batch: Sequence[str],
      model: _VLLMModelServer,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Runs inferences on a batch of text strings.

    Args:
      batch: A sequence of examples as text strings.
      model: A _VLLMModelServer containing info for connecting to the server.
      inference_args: Any additional arguments for an inference.

    Returns:
      An Iterable of type PredictionResult.
    """
    return asyncio.run(self._async_run_inference(batch, model, inference_args))

  def share_model_across_processes(self) -> bool:
    return True


class VLLMChatModelHandler(ModelHandler[Sequence[OpenAIChatMessage],
                                        PredictionResult,
                                        _VLLMModelServer]):
  def __init__(
      self,
      model_name: str,
      chat_template_path: Optional[str] = None,
      vllm_server_kwargs: Optional[Dict[str, str]] = None):
    """ Implementation of the ModelHandler interface for vLLM using previous
    messages as input.

    Example Usage::

      pcoll | RunInference(VLLMModelHandler(model_name='facebook/opt-125m'))

    Args:
      model_name: The vLLM model. See
        https://docs.vllm.ai/en/latest/models/supported_models.html for
        supported models.
      chat_template_path: Path to a chat template. This file must be accessible
        from your runner's execution environment, so it is recommended to use
        a cloud based file storage system (e.g. Google Cloud Storage).
        For info on chat templates, see:
        https://docs.vllm.ai/en/latest/serving/openai_compatible_server.html#chat-template
      vllm_server_kwargs: Any additional kwargs to be passed into your vllm
        server when it is being created. Will be invoked using
        `python -m vllm.entrypoints.openai.api_serverv <beam provided args>
        <vllm_server_kwargs>`. For example, you could pass
        `{'echo': 'true'}` to prepend new messages with the previous message.
        For a list of possible kwargs, see
        https://docs.vllm.ai/en/latest/serving/openai_compatible_server.html#extra-parameters-for-chat-api
    """
    self._model_name = model_name
    self._vllm_server_kwargs: Dict[str, str] = vllm_server_kwargs or {}
    self._env_vars = {}
    self._chat_template_path = chat_template_path
    self._chat_file = f'template-{uuid.uuid4().hex}.jinja'

  def load_model(self) -> _VLLMModelServer:
    chat_template_contents = ''
    if self._chat_template_path is not None:
      local_chat_template_path = os.path.join(os.getcwd(), self._chat_file)
      if not os.path.exists(local_chat_template_path):
        with FileSystems.open(self._chat_template_path) as fin:
          chat_template_contents = fin.read().decode()
        with open(local_chat_template_path, 'a') as f:
          f.write(chat_template_contents)
      self._vllm_server_kwargs['chat_template'] = local_chat_template_path

    return _VLLMModelServer(self._model_name, self._vllm_server_kwargs)

  async def _async_run_inference(
      self,
      batch: Sequence[Sequence[OpenAIChatMessage]],
      model: _VLLMModelServer,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    client = getAsyncVLLMClient(model.get_server_port())
    inference_args = inference_args or {}
    async_predictions = []
    for messages in batch:
      formatted = []
      for message in messages:
        formatted.append({"role": message.role, "content": message.content})
      try:
        completion = client.chat.completions.create(
            model=self._model_name, messages=formatted, **inference_args)
        async_predictions.append(completion)
      except Exception as e:
        model.check_connectivity()
        raise e

    predictions = []
    for p in async_predictions:
      try:
        predictions.append(await p)
      except Exception as e:
        model.check_connectivity()
        raise e

    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

  def run_inference(
      self,
      batch: Sequence[Sequence[OpenAIChatMessage]],
      model: _VLLMModelServer,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Runs inferences on a batch of text strings.

    Args:
      batch: A sequence of examples as OpenAI messages.
      model: A _VLLMModelServer for connecting to the spun up server.
      inference_args: Any additional arguments for an inference.

    Returns:
      An Iterable of type PredictionResult.
    """
    return asyncio.run(self._async_run_inference(batch, model, inference_args))

  def share_model_across_processes(self) -> bool:
    return True
