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

  def test_num_loaded_models_publisher_aggregation(self):
    samples = [(100.0 + i * 10, float(i + 1)) for i in range(10)]
    pub = autoscaling_metrics.NumLoadedModelsPublisher(
        tag='test',
        signal_fn=lambda: 1.0,
        aggregation='p90',
        aggregation_window_secs=1800.0)
    pub._samples = list(samples)
    self.assertEqual(pub._aggregate(now=200.0), 9.0)

    pub._samples = [(100.0, 2.0), (1900.0, 8.0), (1950.0, 10.0)]
    self.assertEqual(pub._aggregate(now=2000.0), 10.0)

    pub._samples = [(100.0, 2.0), (102.0, 5.0), (104.0, 3.0)]
    pub._aggregation_window_secs = 60.0
    pub._aggregation = 'max'
    self.assertEqual(pub._aggregate(now=105.0), 5.0)
    pub._aggregation = 'last'
    self.assertEqual(pub._aggregate(now=105.0), 3.0)
    pub._aggregation = 'mean'
    self.assertAlmostEqual(pub._aggregate(now=105.0), 3.3333333333333335)

  def test_publish_resets_process_wide_distribution_cell(self):
    pub = autoscaling_metrics.NumLoadedModelsPublisher(
        tag='reset_test', signal_fn=lambda: 1.0, multiplier=1.0)
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
    first = autoscaling_metrics.NumLoadedModelsPublisher(
        tag='reload', signal_fn=lambda: None, poll_interval_secs=0.01)
    second = autoscaling_metrics.NumLoadedModelsPublisher(
        tag='reload', signal_fn=lambda: None, poll_interval_secs=0.01)
    other = autoscaling_metrics.NumLoadedModelsPublisher(
        tag='other', signal_fn=lambda: None, poll_interval_secs=0.01)
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

  def test_prometheus_counter_gauge_sampler(self):
    sampler = autoscaling_metrics.PrometheusCounterGaugeSampler(
        completed_metric='vllm:request_success_total',
        running_metric='vllm:num_requests_running')
    responses = [
        'vllm:request_success_total 10.0\nvllm:num_requests_running 4.0\n',
        # Missing the gauge -> None.
        'vllm:request_success_total 11.0\n',
    ]

    def fake_urlopen(url, timeout=5.0):
      self.assertEqual(url, 'http://localhost:8000/metrics')
      del timeout
      return io.BytesIO(responses.pop(0).encode('utf-8'))

    with mock.patch('urllib.request.urlopen', side_effect=fake_urlopen):
      self.assertEqual(sampler(8000), (10.0, 4.0))
      self.assertIsNone(sampler(8000))
    # Unreachable endpoint -> None.
    with mock.patch('urllib.request.urlopen', side_effect=OSError('refused')):
      self.assertIsNone(sampler(8000))

  def test_prometheus_counter_gauge_sampler_label_filter(self):
    sampler = autoscaling_metrics.PrometheusCounterGaugeSampler(
        completed_metric='nv_inference_request_success',
        running_metric='nv_inference_pending_request_count',
        label_filter={'model': 'a'})
    body = (
        'nv_inference_request_success{model="a"} 10\n'
        'nv_inference_request_success{model="b"} 99\n'
        'nv_inference_pending_request_count{model="a"} 2\n'
        'nv_inference_pending_request_count{model="b"} 50\n')
    with mock.patch('urllib.request.urlopen',
                    return_value=io.BytesIO(body.encode('utf-8'))):
      # Only model "a" counts.
      self.assertEqual(sampler(8002), (10.0, 2.0))

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

  def test_autoscaling_metric_config_resolve_tag(self):
    cfg = autoscaling_metrics.AutoscalingMetricConfig(
        poll_interval_secs=7.0,
        multiplier=2.0,
        signal_window_secs=30.0,
        min_requests=5,
        warmup_timeout_secs=15.0,
        aggregation='lifetime_max',
        aggregation_window_secs=40.0)
    # The RunInference identifier is used verbatim (it has to match the step
    # annotation), even if it contains characters sanitize would change.
    self.assertEqual(
        cfg.resolve_tag('my-step/id', 'org/model:v1').tag, 'my-step/id')
    # Outside RunInference: the sanitized fallback name.
    self.assertEqual(cfg.resolve_tag(None, 'org/model:v1').tag, 'org_model_v1')
    # An explicit tag always wins.
    self.assertEqual(
        autoscaling_metrics.AutoscalingMetricConfig(tag='custom').resolve_tag(
            'step-id', 'model').tag,
        'custom')
    with self.assertRaises(ValueError):
      autoscaling_metrics.AutoscalingMetricConfig().resolve_tag(None, '')
    with self.assertRaises(ValueError):
      autoscaling_metrics.AutoscalingMetricConfig().make_publisher(lambda: None)

    estimator = cfg.make_capacity_estimator()
    self.assertEqual(estimator._multiplier, 2.0)
    self.assertEqual(estimator._window_secs, 30.0)
    self.assertEqual(estimator._min_requests, 5)
    self.assertEqual(estimator._warmup_timeout_secs, 15.0)
    publisher = cfg.resolve_tag('step-id').make_publisher(lambda: None)
    self.assertEqual(publisher._metric_name, 'num_loaded_models_step-id')
    self.assertEqual(publisher._aggregation, 'lifetime_max')
    self.assertEqual(publisher._poll_interval_secs, 7.0)
    # The multiplier is applied by the estimator, never a second time here.
    self.assertEqual(publisher._multiplier, 1.0)


if __name__ == '__main__':
  unittest.main()
