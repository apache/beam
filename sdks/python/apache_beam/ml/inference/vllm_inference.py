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

from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.utils import subprocess_server
from dataclasses import dataclass
from openai import OpenAI
import logging
import threading
import time
import subprocess
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence
from typing import Tuple

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


class _VLLMModelServer():
  def __init__(self, model_name):
    self._model_name = model_name
    self._server_started = False
    self._server_process = None
    self._server_port = None

    self.start_server()

  def start_server(self, retries=3):
    if not self._server_started:
      self._server_process, self._server_port = start_process([
          'python',
          '-m',
          'vllm.entrypoints.openai.api_server',
          '--model',
          self._model_name,
          '--port',
          '{{PORT}}',
      ])

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
      raise Exception(
          "Failed to start vLLM server, process exited with code %s" %
          self._server_process.poll())
    else:
      self.start_server(retries - 1)

  def get_server_port(self) -> int:
    if not self._server_started:
      self.start_server()
    return self._server_port


class VLLMCompletionsModelHandler(ModelHandler[str,
                                               PredictionResult,
                                               _VLLMModelServer]):
  def __init__(
      self,
      model_name: str,
  ):
    """Implementation of the ModelHandler interface for vLLM using text as
    input.

    Example Usage::

      pcoll | RunInference(VLLMModelHandler(model_name='facebook/opt-125m'))

    Args:
      model_name: The vLLM model. See
        https://docs.vllm.ai/en/latest/models/supported_models.html for
        supported models.
    """
    self._model_name = model_name
    self._env_vars = {}

  def load_model(self) -> _VLLMModelServer:
    return _VLLMModelServer(self._model_name)

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
    client = getVLLMClient(model.get_server_port())
    inference_args = inference_args or {}
    predictions = []
    for prompt in batch:
      completion = client.completions.create(
          model=self._model_name, prompt=prompt, **inference_args)
      predictions.append(completion)
    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

  def share_model_across_processes(self) -> bool:
    return True

  def should_skip_batching(self) -> bool:
    # Batching does not help since vllm is already doing dynamic batching and
    # each request is sent one by one anyways
    # TODO(https://github.com/apache/beam/issues/32528): We should add support
    # for taking in batches and doing a bunch of async calls. That will end up
    # being more efficient when we can do in bundle batching.
    return True


class VLLMChatModelHandler(ModelHandler[Sequence[OpenAIChatMessage],
                                        PredictionResult,
                                        _VLLMModelServer]):
  def __init__(
      self,
      model_name: str,
  ):
    """ Implementation of the ModelHandler interface for vLLM using previous
    messages as input.

    Example Usage::

      pcoll | RunInference(VLLMModelHandler(model_name='facebook/opt-125m'))

    Args:
      model_name: The vLLM model. See
        https://docs.vllm.ai/en/latest/models/supported_models.html for
        supported models.
    """
    self._model_name = model_name
    self._env_vars = {}

  def load_model(self) -> _VLLMModelServer:
    return _VLLMModelServer(self._model_name)

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
    client = getVLLMClient(model.get_server_port())
    inference_args = inference_args or {}
    predictions = []
    for messages in batch:
      completion = client.chat.completions.create(
          model=self._model_name, messages=messages, **inference_args)
      predictions.append(completion)
    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

  def share_model_across_processes(self) -> bool:
    return True

  def should_skip_batching(self) -> bool:
    # Batching does not help since vllm is already doing dynamic batching and
    # each request is sent one by one anyways
    # TODO(https://github.com/apache/beam/issues/32528): We should add support
    # for taking in batches and doing a bunch of async calls. That will end up
    # being more efficient when we can do in bundle batching.
    return True
