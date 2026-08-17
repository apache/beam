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
import os
import sys
import types
import unittest
from unittest import mock

# Protect against environments where the OpenAI python library is not
# available. The command-construction tests below do not actually need a
# real OpenAI client; stubbing the module is enough for vllm_inference to
# import cleanly.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  import openai  # pylint: disable=unused-import
except ImportError:
  openai = types.ModuleType('openai')

  class _FakeOpenAI:
    pass

  openai.AsyncOpenAI = _FakeOpenAI
  openai.OpenAI = _FakeOpenAI
  sys.modules['openai'] = openai

from apache_beam.ml.inference import vllm_inference


class _FakeProcess:
  def __init__(self):
    self.returncode = None

  def poll(self):
    return self.returncode

  def terminate(self):
    self.returncode = 0

  def wait(self, timeout=None):
    return self.returncode

  def kill(self):
    self.returncode = -9


class _FakeModels:
  def list(self):
    return types.SimpleNamespace(data=[object()])


class _FakeClient:
  def __init__(self):
    self.models = _FakeModels()

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    return False


def _record_start_process(commands):
  def start_process(cmd):
    commands.append(list(cmd))
    return _FakeProcess(), 10000 + len(commands)

  return start_process


class VLLMInferenceTest(unittest.TestCase):
  def test_native_vllm_starts_single_server_process(self):
    commands = []
    with mock.patch.object(vllm_inference,
                           'start_process',
                           _record_start_process(commands)):
      with mock.patch.object(vllm_inference, 'getVLLMClient'):
        vllm_inference.getVLLMClient.return_value = _FakeClient()
        vllm_inference.VLLMCompletionsModelHandler(
            model_name='test-model',
            vllm_server_kwargs={
                'gpu-memory-utilization': '0.9'
            }).load_model()
    self.assertEqual(1, len(commands))
    self.assertIn('vllm.entrypoints.openai.api_server', commands[0])
    self.assertIn('--model', commands[0])
    self.assertIn('test-model', commands[0])
    self.assertIn('--gpu-memory-utilization', commands[0])
    self.assertIn('0.9', commands[0])
    self.assertNotIn('dynamo.frontend', commands[0])
    self.assertNotIn('dynamo.vllm', commands[0])

  def test_dynamo_starts_frontend_and_engine_with_separate_kwargs(self):
    commands = []
    with mock.patch.dict(os.environ,
                         {'ETCD_ENDPOINTS': 'http://127.0.0.1:2379'}):
      with mock.patch.object(vllm_inference,
                             'start_process',
                             _record_start_process(commands)):
        with mock.patch.object(vllm_inference, 'getVLLMClient'):
          vllm_inference.getVLLMClient.return_value = _FakeClient()
          vllm_inference.VLLMCompletionsModelHandler(
              model_name='test-model',
              vllm_server_kwargs={
                  'tensor-parallel-size': '1'
              },
              use_dynamo=True,
              dynamo_frontend_kwargs={
                  'router-mode': 'round-robin'
              }).load_model()
    self.assertEqual(2, len(commands))
    frontend_cmd = commands[0]
    engine_cmd = commands[1]
    self.assertIn('dynamo.frontend', frontend_cmd)
    self.assertIn('--http-port', frontend_cmd)
    self.assertIn('--discovery-backend', frontend_cmd)
    self.assertIn('--request-plane', frontend_cmd)
    self.assertIn('--event-plane', frontend_cmd)
    self.assertIn('--router-mode', frontend_cmd)
    self.assertIn('--no-router-kv-events', frontend_cmd)
    self.assertNotIn('--model', frontend_cmd)
    self.assertNotIn('--tensor-parallel-size', frontend_cmd)
    self.assertNotIn('--kv-events-config', frontend_cmd)
    self.assertIn('dynamo.vllm', engine_cmd)
    self.assertIn('--model', engine_cmd)
    self.assertIn('test-model', engine_cmd)
    self.assertIn('--discovery-backend', engine_cmd)
    self.assertIn('--request-plane', engine_cmd)
    self.assertIn('--event-plane', engine_cmd)
    self.assertIn('--kv-events-config', engine_cmd)
    self.assertIn('--tensor-parallel-size', engine_cmd)
    self.assertNotIn('--http-port', engine_cmd)
    self.assertNotIn('--router-mode', engine_cmd)
    self.assertNotIn('--no-router-kv-events', engine_cmd)

  def test_validate_inference_args_accepts_openai_request_kwargs(self):
    vllm_inference.VLLMCompletionsModelHandler(
        'test-model').validate_inference_args({'max_tokens': 8})
    vllm_inference.VLLMChatModelHandler('test-model').validate_inference_args(
        {'max_tokens': 8})


if __name__ == '__main__':
  unittest.main()
