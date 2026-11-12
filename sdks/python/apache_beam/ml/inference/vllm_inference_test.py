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
import gc
import os
import sys
import types
import unittest
from unittest import mock

import apache_beam as beam
from apache_beam.ml.inference import autoscaling_metrics
from apache_beam.ml.inference import prefix_batching
from apache_beam.ml.inference.autoscaling_metrics import AutoscalingMetricConfig
from apache_beam.ml.inference.autoscaling_metrics import PrometheusNumLoadedModelsPublisher
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.prefix_batching import PrefixBatchingConfig
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Protect against environments where the OpenAI python library is not
# available. The command-construction tests below do not actually need a
# real OpenAI client; stubbing the module is enough for vllm_inference to
# import cleanly.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
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


def _record_start_process(commands, procs=None):
  def start_process(cmd):
    commands.append(list(cmd))
    proc = _FakeProcess()
    if procs is not None:
      procs.append(proc)
    return proc, 10000 + len(commands)

  return start_process


class _RecordingVLLMHandler(vllm_inference.VLLMCompletionsModelHandler):
  """Stub VLLMCompletionsModelHandler that records batches without a server."""
  def load_model(self):
    return None

  def run_inference(self, batch, model, inference_args=None):
    batch_list = list(batch)
    return [
        PredictionResult(example=prompt, inference={'batch': batch_list})
        for prompt in batch_list
    ]


class _RecordingChatHandler(vllm_inference.VLLMChatModelHandler):
  """Stub VLLMChatModelHandler that records batches without a server."""
  def load_model(self):
    return None

  def run_inference(self, batch, model, inference_args=None):
    batch_list = list(batch)
    tags = [
        '/'.join(f'{m.role}:{m.content}' for m in msgs) for msgs in batch_list
    ]
    return [
        PredictionResult(example=msgs, inference={'batch': tags})
        for msgs in batch_list
    ]


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
            },
            enable_autoscaling_metric=False).load_model()
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
          server = vllm_inference.VLLMCompletionsModelHandler(
              model_name='test-model',
              vllm_server_kwargs={
                  'tensor-parallel-size': '1'
              },
              use_dynamo=True,
              dynamo_frontend_kwargs={
                  'router-mode': 'round-robin'
              }).load_model()
    self.assertIsNone(server._num_loaded_models_publisher)
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

  def test_autoscaling_publishes_capacity_from_vllm_metrics(self):
    """The server publishes the capacity estimated from vLLM's metrics."""
    handler = vllm_inference.VLLMCompletionsModelHandler(
        model_name='test-model',
        autoscaling_config=AutoscalingMetricConfig(
            multiplier=3.0, poll_interval_secs=7.0))
    with (
        mock.patch.object(vllm_inference,
                          'start_process',
                          _record_start_process([])),
        mock.patch.object(vllm_inference,
                          'getVLLMClient',
                          return_value=_FakeClient()),
        mock.patch.object(PrometheusNumLoadedModelsPublisher, 'start'),
    ):
      server = handler.load_model()
    publisher = server._num_loaded_models_publisher
    self.assertEqual(publisher._poll_interval_secs, 7.0)
    signal = publisher._read_capacity

    scrapes = [
        'vllm:request_success_total{finished_reason="stop"} 10\n'
        'vllm:num_requests_running 4\n',
        'vllm:request_success_total{finished_reason="stop"} 25\n'
        'vllm:request_success_total{finished_reason="length"} 5\n'
        'vllm:num_requests_running 4\n',
    ]
    ports = []
    with (
        mock.patch.object(autoscaling_metrics,
                          '_scrape_prometheus_metrics',
                          side_effect=lambda port, **_:
                          (ports.append(port), scrapes.pop(0))[1]),
        mock.patch.object(autoscaling_metrics.time,
                          'monotonic',
                          side_effect=[100.0, 105.0]),
    ):
      self.assertIsNone(signal())  # Baseline sample.
      # 20 completions over 5s with 4 in flight -> mu=1, p90=4, x3 -> 12.
      self.assertAlmostEqual(signal(), 12.0)
    self.assertEqual(ports, [server._server_port, server._server_port])

  def test_autoscaling_publisher_follows_server_lifecycle(self):
    """The publisher starts once the server is up and stops with it."""
    commands = []

    def load(**kwargs):
      with (
          mock.patch.object(vllm_inference,
                            'start_process',
                            _record_start_process(commands)),
          mock.patch.object(vllm_inference,
                            'getVLLMClient',
                            return_value=_FakeClient()),
      ):
        return vllm_inference.VLLMCompletionsModelHandler(
            model_name='test-model', **kwargs).load_model()

    self.assertIsNone(
        load(enable_autoscaling_metric=False)._num_loaded_models_publisher)

    with mock.patch.object(autoscaling_metrics,
                           '_scrape_prometheus_metrics',
                           return_value=None):
      server = load(
          autoscaling_config=AutoscalingMetricConfig(poll_interval_secs=60))
      publisher = server._num_loaded_models_publisher
      self.addCleanup(publisher.stop)
      self.assertEqual(publisher._metrics_port_fn(), 10002)
      self.assertTrue(publisher._thread.is_alive())

      server._stop_processes()
      self.assertIsNone(server._num_loaded_models_publisher)
      self.assertIsNone(publisher._thread)
      self.assertNotIn(
          publisher._metric_name, PrometheusNumLoadedModelsPublisher._active)

      # Restarting the server creates a new publisher bound to the new port.
      with (
          mock.patch.object(vllm_inference,
                            'start_process',
                            _record_start_process(commands)),
          mock.patch.object(vllm_inference,
                            'getVLLMClient',
                            return_value=_FakeClient()),
      ):
        server.start_server()
      restarted_publisher = server._num_loaded_models_publisher
      self.addCleanup(restarted_publisher.stop)
      self.assertEqual(restarted_publisher._metrics_port_fn(), 10003)
      self.assertTrue(restarted_publisher._thread.is_alive())
      server.__del__()
      self.assertIsNone(restarted_publisher._thread)

  def test_autoscaling_publisher_does_not_keep_server_alive(self):
    """A dropped server is garbage-collected, stopping publisher and process."""
    procs = []
    with (
        mock.patch.object(vllm_inference,
                          'start_process',
                          _record_start_process([], procs=procs)),
        mock.patch.object(vllm_inference,
                          'getVLLMClient',
                          return_value=_FakeClient()),
        mock.patch.object(autoscaling_metrics,
                          '_scrape_prometheus_metrics',
                          return_value=None),
    ):
      server = vllm_inference.VLLMCompletionsModelHandler(
          model_name='test-model',
          autoscaling_config=AutoscalingMetricConfig(
              poll_interval_secs=60)).load_model()
      publisher = server._num_loaded_models_publisher
      self.addCleanup(publisher.stop)
      thread = publisher._thread
      self.assertTrue(thread.is_alive())
      del server
      gc.collect()
      self.assertEqual(procs[0].returncode, 0)
      thread.join(timeout=5)
      self.assertFalse(thread.is_alive())
      self.assertTrue(publisher._stop_event.is_set())

  def test_autoscaling_tag_prefers_run_inference_identifier(self):
    """The metric tag is the identifier RunInference reports (uuid or given).

    That is what the autoscaler looks up and it is stable across model
    updates; the sanitized model name is only a fallback for handlers used
    outside RunInference, and an explicit config tag always wins.
    """
    def tag_for(handler):
      with (
          mock.patch.object(vllm_inference,
                            'start_process',
                            _record_start_process([])),
          mock.patch.object(vllm_inference,
                            'getVLLMClient',
                            return_value=_FakeClient()),
          mock.patch.object(PrometheusNumLoadedModelsPublisher, 'start'),
      ):
        return handler.load_model()._num_loaded_models_publisher._metric_name

    # Outside RunInference: sanitized model name.
    handler = vllm_inference.VLLMCompletionsModelHandler(
        model_name='org/model:v1')
    self.assertEqual(tag_for(handler), 'num_loaded_models_org_model_v1')

    # Inside RunInference: the reported identifier, verbatim (uuid-like or
    # user-provided with characters sanitize would have changed).
    handler.set_model_identifier('3f2a9c0e1b4d4f6e8a7b6c5d4e3f2a1b')
    self.assertEqual(
        tag_for(handler), 'num_loaded_models_3f2a9c0e1b4d4f6e8a7b6c5d4e3f2a1b')
    handler.set_model_identifier('my-step/model')
    self.assertEqual(tag_for(handler), 'num_loaded_models_my-step/model')

    # Explicit config tag overrides both.
    pinned = vllm_inference.VLLMCompletionsModelHandler(
        model_name='org/model:v1',
        autoscaling_config=AutoscalingMetricConfig(tag='pinned'))
    pinned.set_model_identifier('ignored')
    self.assertEqual(tag_for(pinned), 'num_loaded_models_pinned')

    # Chat handler follows the same rule.
    chat = vllm_inference.VLLMChatModelHandler(model_name='org/model:v1')
    chat.set_model_identifier('chat-id')
    self.assertEqual(tag_for(chat), 'num_loaded_models_chat-id')

  def test_prefix_batching_configuration(self):
    comp_handler = vllm_inference.VLLMCompletionsModelHandler(
        model_name='test-model',
        prefix_batching_config=PrefixBatchingConfig(
            prefix_char_len=32, num_shards=4))
    comp_kwargs = comp_handler.batch_elements_kwargs()
    self.assertIsInstance(
        comp_kwargs['key_fn'], prefix_batching.PromptPrefixKeyFn)
    self.assertEqual(comp_kwargs['key_fn']._prefix_char_len, 32)
    self.assertEqual(comp_kwargs['num_shards'], 4)
    self.assertEqual(comp_kwargs['max_batch_duration_secs'], 1.0)

    chat_handler = vllm_inference.VLLMChatModelHandler(
        model_name='test-model',
        prefix_batching_config=PrefixBatchingConfig(prefix_char_len=64),
        max_batch_duration_secs=2.5)
    chat_kwargs = chat_handler.batch_elements_kwargs()
    self.assertIsInstance(
        chat_kwargs['key_fn'], prefix_batching.ChatPrefixKeyFn)
    self.assertEqual(chat_kwargs['key_fn']._prefix_char_len, 64)
    self.assertEqual(chat_kwargs['max_batch_duration_secs'], 2.5)
    # num_shards unset in the config -> the prefix-batching default reaches
    # BatchElements.
    self.assertEqual(
        chat_kwargs['num_shards'], prefix_batching.DEFAULT_NUM_SHARDS)

    # A custom key_fn is used verbatim; unset config leaves batching unkeyed.
    custom_handler = vllm_inference.VLLMCompletionsModelHandler(
        model_name='test-model',
        prefix_batching_config=PrefixBatchingConfig(key_fn=len))
    self.assertIs(custom_handler.batch_elements_kwargs()['key_fn'], len)
    plain_handler = vllm_inference.VLLMCompletionsModelHandler(
        model_name='test-model')
    self.assertNotIn('key_fn', plain_handler.batch_elements_kwargs())

  def test_run_inference_with_prefix_batching_and_preprocess_fn(self):
    records = [
        {
            'id': 1, 'prompt': 'PREFIX_ALPHA: question 1'
        },
        {
            'id': 2, 'prompt': 'PREFIX_BETA_: question 2'
        },
        {
            'id': 3, 'prompt': 'PREFIX_ALPHA: question 3'
        },
        {
            'id': 4, 'prompt': 'PREFIX_BETA_: question 4'
        },
    ]
    handler = _RecordingVLLMHandler(
        model_name='test-model',
        # One shard per prefix so both prompts of a prefix deterministically
        # land in the same batch (the default spreads a prefix over 8 shards).
        prefix_batching_config=PrefixBatchingConfig(
            prefix_char_len=12, num_shards=1),
        min_batch_size=2,
        max_batch_size=2,
        max_batch_duration_secs=1.0).with_preprocess_fn(lambda r: r['prompt'])

    with TestPipeline('FnApiRunner') as p:
      preds = (
          p
          | beam.Create(records, reshuffle=False)
          | RunInference(handler))
      grouped_batches = preds | beam.Map(
          lambda r: (r.example, sorted(r.inference['batch'])))
      expected = [
          (
              'PREFIX_ALPHA: question 1',
              ['PREFIX_ALPHA: question 1', 'PREFIX_ALPHA: question 3']),
          (
              'PREFIX_ALPHA: question 3',
              ['PREFIX_ALPHA: question 1', 'PREFIX_ALPHA: question 3']),
          (
              'PREFIX_BETA_: question 2',
              ['PREFIX_BETA_: question 2', 'PREFIX_BETA_: question 4']),
          (
              'PREFIX_BETA_: question 4',
              ['PREFIX_BETA_: question 2', 'PREFIX_BETA_: question 4']),
      ]
      assert_that(grouped_batches, equal_to(expected))

  def test_chat_run_inference_with_prefix_batching(self):
    msg = vllm_inference.OpenAIChatMessage
    convos = [
        [msg('system', 'SYS_ALPHA'), msg('user', 'q1')],
        [msg('system', 'SYS_BETA_'), msg('user', 'q2')],
        [msg('system', 'SYS_ALPHA'), msg('user', 'q3')],
        [msg('system', 'SYS_BETA_'), msg('user', 'q4')],
    ]
    handler = _RecordingChatHandler(
        model_name='test-model',
        prefix_batching_config=PrefixBatchingConfig(
            prefix_char_len=9, num_shards=1),
        min_batch_size=2,
        max_batch_size=2)

    with TestPipeline('FnApiRunner') as p:
      preds = (p | beam.Create(convos, reshuffle=False) | RunInference(handler))
      grouped_batches = preds | beam.Map(lambda r: sorted(r.inference['batch']))
      alpha = ['system:SYS_ALPHA/user:q1', 'system:SYS_ALPHA/user:q3']
      beta = ['system:SYS_BETA_/user:q2', 'system:SYS_BETA_/user:q4']
      assert_that(grouped_batches, equal_to([alpha, alpha, beta, beta]))


if __name__ == '__main__':
  unittest.main()
