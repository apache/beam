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
import io
import unittest
from unittest import mock

from apache_beam.ml.inference import autoscaling_metrics


def _publisher(model_identifier='step-id', port=None, **kwargs):
  """A publisher for a fake server exposing ``done_total`` and ``running``."""
  return autoscaling_metrics.NumLoadedModelsPublisher(
      metrics_port_fn=lambda: port,
      completed_metric='done_total',
      running_metric='running',
      model_identifier=model_identifier,
      **kwargs)


class AutoscalingMetricsTest(unittest.TestCase):
  def test_sanitize_metric_tag(self):
    self.assertEqual(
        autoscaling_metrics.sanitize_metric_tag('Qwen/Qwen2.5-7B:v1'),
        'Qwen_Qwen2_5_7B_v1')
    self.assertEqual(
        autoscaling_metrics.sanitize_metric_tag('normal_tag_1'), 'normal_tag_1')

  def test_parse_prometheus_metric(self):
    prom_text = (
        '# HELP vllm:num_requests_running Running requests\n'
        '# TYPE vllm:num_requests_running gauge\n'
        'vllm:num_requests_running{model_name="qwen"} 4.0\n'
        'vllm:num_requests_running{model_name="other"} 6.0\n')
    self.assertEqual(
        autoscaling_metrics.parse_prometheus_metric(
            prom_text,
            'vllm:num_requests_running',
            label_filter={'model_name': 'qwen'}),
        4.0)
    self.assertEqual(
        autoscaling_metrics.parse_prometheus_metric(
            prom_text, 'vllm:num_requests_running'),
        10.0)
    self.assertIsNone(
        autoscaling_metrics.parse_prometheus_metric(prom_text, 'vllm:missing'))

  def test_parse_prometheus_metric_exposition_details(self):
    prom_text = (
        # Series of one counter that differ only by label are summed.
        'vllm:request_success_total{finished_reason="stop"} 10.0\n'
        'vllm:request_success_total{finished_reason="length"} 5.0\n'
        # A metric whose name merely starts with the requested one is ignored.
        'vllm:request_success_total_created 1.7e9\n'
        # Optional trailing timestamps are ignored.
        'nv_inference_request_success{model="a"} 3 1700000000000\n'
        # Quoted label values may contain commas, braces and escapes.
        'labeled{path="a,b}c",quote="say \\"hi\\"",n="x\\ny"} 7\n'
        '  \n'
        'not a sample line\n')
    parse = autoscaling_metrics.parse_prometheus_metric
    self.assertEqual(parse(prom_text, 'vllm:request_success_total'), 15.0)
    self.assertEqual(parse(prom_text, 'nv_inference_request_success'), 3.0)
    self.assertEqual(
        parse(prom_text, 'labeled', label_filter={'path': 'a,b}c'}), 7.0)
    self.assertEqual(
        parse(prom_text, 'labeled', label_filter={'quote': 'say "hi"'}), 7.0)
    self.assertEqual(
        parse(prom_text, 'labeled', label_filter={'n': 'x\ny'}), 7.0)
    self.assertIsNone(
        parse(prom_text, 'labeled', label_filter={'path': 'other'}))

  def test_capacity_estimator(self):
    estimator = autoscaling_metrics.CompletionRateCapacityEstimator(
        multiplier=2.0, min_value=1, window_secs=60.0, running_percentile=90.0)
    # First sample establishes baseline -> None.
    self.assertIsNone(estimator.update(10.0, 4.0, now=100.0))
    # delta=20 over dt=5s with running=4 -> mu = 20 / (4 * 5) = 1.0,
    # running_ref = 4.0, multiplier = 2.0 -> 8.0.
    self.assertAlmostEqual(estimator.update(30.0, 4.0, now=105.0), 8.0)

  def test_capacity_estimator_skips_counter_reset_and_clock_skew(self):
    estimator = autoscaling_metrics.CompletionRateCapacityEstimator(
        window_secs=60.0, min_requests=0)
    self.assertIsNone(estimator.update(50.0, 2.0, now=100.0))
    # Counter went down (server restart) -> skipped, new baseline.
    self.assertIsNone(estimator.update(5.0, 2.0, now=105.0))
    # Non-increasing timestamp -> skipped.
    self.assertIsNone(estimator.update(9.0, 2.0, now=105.0))
    # delta=4 over 5s with running=2 -> mu=0.4, p90=2 -> 0.8 -> min_value 1.
    self.assertAlmostEqual(estimator.update(13.0, 2.0, now=110.0), 1.0)

  def test_capacity_estimator_warmup_min_requests_and_timeout(self):
    # Requires >= 3 completed requests before the 10s warmup timeout.
    estimator = autoscaling_metrics.CompletionRateCapacityEstimator(
        min_requests=3, warmup_timeout_secs=10.0)
    self.assertIsNone(estimator.update(0.0, 2.0, now=100.0))  # Baseline.
    # 1 completion (< 3) at elapsed=5s (< 10s) -> None.
    self.assertIsNone(estimator.update(1.0, 2.0, now=105.0))
    # 4 completions (>= 3) at elapsed=8s -> publishes
    # (4 / (2*5 + 2*3)) * 2 = 0.5 -> clamped to min_value=1.0.
    self.assertAlmostEqual(estimator.update(4.0, 2.0, now=108.0), 1.0)

    slow = autoscaling_metrics.CompletionRateCapacityEstimator(
        min_requests=3, warmup_timeout_secs=10.0)
    self.assertIsNone(slow.update(0.0, 1.0, now=100.0))  # Baseline.
    # 1 completion (< 3) at elapsed=5s -> None.
    self.assertIsNone(slow.update(1.0, 1.0, now=105.0))
    # 2 completions (< 3) but elapsed=10s (>= timeout) -> publishes.
    self.assertAlmostEqual(slow.update(2.0, 1.0, now=110.0), 1.0)

  def test_capacity_estimator_skips_idle_ticks_without_diluting_p90(self):
    estimator = autoscaling_metrics.CompletionRateCapacityEstimator(
        multiplier=1.0,
        min_value=1,
        window_secs=60.0,
        running_percentile=90.0,
        min_requests=1)
    self.assertIsNone(estimator.update(0.0, 5.0, now=100.0))  # Baseline.
    # 25 completions over 5s with running=5 -> mu=1.0, p90=5 -> 5.0.
    self.assertAlmostEqual(estimator.update(25.0, 5.0, now=105.0), 5.0)
    # Three completely idle ticks (delta=0, running=0) -> None, without adding
    # r=0 samples that would dilute p90 concurrency.
    for t in (110.0, 115.0, 120.0):
      self.assertIsNone(estimator.update(25.0, 0.0, now=t))
    # 5 completions that already finished before the scrape (delta=5,
    # running=0): total_completed=30, total_slot_secs=25 -> mu=1.2, p90=5 -> 6.
    self.assertAlmostEqual(estimator.update(30.0, 0.0, now=125.0), 6.0)

  def test_capacity_estimator_defaults_to_wall_clock(self):
    estimator = autoscaling_metrics.CompletionRateCapacityEstimator(
        min_requests=0)
    with mock.patch.object(autoscaling_metrics.time,
                           'time',
                           side_effect=[100.0, 105.0]):
      self.assertIsNone(estimator.update(0.0, 1.0))
      self.assertAlmostEqual(estimator.update(10.0, 1.0), 2.0)

  def test_publisher_publishes_signal_values_as_is(self):
    """Each capacity is published rounded; None skips the tick."""
    cell = (
        autoscaling_metrics.MetricsEnvironment.process_wide_container().
        get_distribution(
            autoscaling_metrics.MetricName(
                autoscaling_metrics.MODEL_MANAGER_METRICS_NAMESPACE,
                'num_loaded_models_sink_test')))
    cell.reset()
    self.addCleanup(cell.reset)
    published = []
    signal_values = [3.4, None, 7.6]

    def load_signal_fn(port):
      del port
      if published:
        # Record what the previous tick left in the cell.
        published[-1] = cell.get_cumulative().sum
      if not signal_values:
        pub._stop_event.set()  # pylint: disable=protected-access
        return None
      published.append(None)
      return signal_values.pop(0)

    pub = _publisher(
        'sink_test',
        port=8000,
        config=autoscaling_metrics.AutoscalingMetricConfig(
            poll_interval_secs=0, load_signal_fn=load_signal_fn))
    # Run the polling loop on this thread so the test is deterministic.
    pub._run()  # pylint: disable=protected-access
    # 3.4 -> 3; None leaves 3 in place; 7.6 -> 8.
    self.assertEqual(published, [3, 3, 8])
    self.assertEqual(cell.get_cumulative().count, 1)

  def test_publish_resets_process_wide_distribution_cell(self):
    pub = _publisher('reset_test')
    cell = (
        autoscaling_metrics.MetricsEnvironment.process_wide_container().
        get_distribution(
            autoscaling_metrics.MetricName(
                autoscaling_metrics.MODEL_MANAGER_METRICS_NAMESPACE,
                'num_loaded_models_reset_test')))
    cell.reset()
    self.addCleanup(cell.reset)
    for _ in range(10):
      pub._publish(2.0)
    self.assertEqual(cell.get_cumulative().count, 1)
    self.assertEqual(cell.get_cumulative().sum, 2)
    pub._publish(5.0)
    self.assertEqual(cell.get_cumulative().count, 1)
    self.assertEqual(cell.get_cumulative().sum, 5)

  def test_publisher_for_same_tag_supersedes_previous(self):
    """A reloaded model's publisher takes over its tag from the stale one."""
    active = autoscaling_metrics.NumLoadedModelsPublisher._active
    fast = autoscaling_metrics.AutoscalingMetricConfig(poll_interval_secs=0.01)
    first = _publisher('reload', config=fast)
    second = _publisher('reload', config=fast)
    other = _publisher('other', config=fast)
    try:
      first.start()
      other.start()
      self.assertTrue(first._thread.is_alive())
      second.start()
      # first is stopped and unregistered; second owns the tag; other is
      # untouched.
      self.assertTrue(first._stop_event.is_set())
      self.assertIsNone(first._thread)
      self.assertTrue(second._thread.is_alive())
      self.assertTrue(other._thread.is_alive())
      self.assertIs(active['num_loaded_models_reload'], second)
      # Stopping the stale one again must not unregister the new owner.
      first.stop()
      self.assertIs(active['num_loaded_models_reload'], second)
    finally:
      first.stop()
      second.stop()
      other.stop()
    self.assertNotIn('num_loaded_models_reload', active)

  def test_publisher_metric_tag(self):
    def tag(model_identifier, model_name='', **config):
      publisher = _publisher(
          model_identifier,
          model_name=model_name,
          config=autoscaling_metrics.AutoscalingMetricConfig(**config))
      return publisher._metric_name

    # The RunInference identifier is used verbatim (it has to match the step
    # annotation), even if it contains characters sanitize would change.
    self.assertEqual(
        tag('my-step/id', 'org/model:v1'), 'num_loaded_models_my-step/id')
    # Outside RunInference: the sanitized model name.
    self.assertEqual(
        tag(None, 'org/model:v1'), 'num_loaded_models_org_model_v1')
    # An explicit tag always wins.
    self.assertEqual(
        tag('step-id', 'model', tag='custom'), 'num_loaded_models_custom')
    with self.assertRaises(ValueError):
      tag(None, '')

  def test_publisher_config_and_defaults(self):
    publisher = _publisher(
        config=autoscaling_metrics.AutoscalingMetricConfig(
            poll_interval_secs=7.0))
    self.assertEqual(publisher._poll_interval_secs, 7.0)
    # No config -> defaults.
    self.assertEqual(
        _publisher()._poll_interval_secs,
        autoscaling_metrics.DEFAULT_POLL_INTERVAL_SECS)

  def test_publisher_reads_server_capacity(self):
    """The handler supplies metric names and a port; the rest is built in."""
    port = [None]
    publisher = autoscaling_metrics.NumLoadedModelsPublisher(
        metrics_port_fn=lambda: port[0],
        completed_metric='done_total',
        running_metric='running',
        model_identifier='step-id',
        config=autoscaling_metrics.AutoscalingMetricConfig(multiplier=2.0))
    signal = publisher._read_capacity  # What the publisher's thread polls.

    # Server not up yet -> skip the tick without scraping.
    with mock.patch('urllib.request.urlopen') as urlopen:
      self.assertIsNone(signal())
      urlopen.assert_not_called()

    port[0] = 8000
    responses = [
        'done_total 10\nrunning 4\n',
        'done_total 30\nrunning 4\n',
    ]

    def fake_urlopen(url, timeout=5.0):
      self.assertEqual(url, 'http://localhost:8000/metrics')
      del timeout
      return io.BytesIO(responses.pop(0).encode('utf-8'))

    with mock.patch('urllib.request.urlopen', side_effect=fake_urlopen):
      with mock.patch.object(autoscaling_metrics.time,
                             'time',
                             side_effect=[100.0, 105.0]):
        self.assertIsNone(signal())  # Baseline sample.
        # delta=20 over 5s with running=4 -> mu=1, p90=4, multiplier 2 -> 8.
        self.assertAlmostEqual(signal(), 8.0)

  def test_publisher_skips_unreadable_scrapes(self):
    publisher = _publisher(
        port=8000,
        config=autoscaling_metrics.AutoscalingMetricConfig(min_requests=0),
        label_filter={'model': 'a'})
    signal = publisher._read_capacity
    scrapes = [
        None,  # Endpoint unreachable.
        'done_total{model="a"} 1\n',  # Gauge missing.
        # Only model "a" counts.
        'done_total{model="a"} 10\ndone_total{model="b"} 99\n'
        'running{model="a"} 2\nrunning{model="b"} 50\n',
        'done_total{model="a"} 20\ndone_total{model="b"} 999\n'
        'running{model="a"} 2\nrunning{model="b"} 50\n',
    ]
    with mock.patch.object(autoscaling_metrics,
                           'scrape_prometheus_metrics',
                           side_effect=lambda port: scrapes.pop(0)):
      with mock.patch.object(autoscaling_metrics.time,
                             'time',
                             side_effect=[100.0, 110.0]):
        self.assertIsNone(signal())
        self.assertIsNone(signal())
        self.assertIsNone(signal())  # Baseline sample.
        # delta=10 over 10s with running=2 -> mu=0.5, p90=2 -> 1.0.
        self.assertAlmostEqual(signal(), 1.0)

  def test_publisher_load_signal_fn_override(self):
    publisher = _publisher(
        port=8000,
        config=autoscaling_metrics.AutoscalingMetricConfig(
            load_signal_fn=lambda port: port / 1000.0))
    with mock.patch('urllib.request.urlopen') as urlopen:
      self.assertEqual(publisher._read_capacity(), 8.0)
      urlopen.assert_not_called()


if __name__ == '__main__':
  unittest.main()
