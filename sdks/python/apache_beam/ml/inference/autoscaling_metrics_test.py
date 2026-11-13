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
import threading
import unittest
from unittest import mock

from apache_beam.metrics.cells import DistributionData
from apache_beam.ml.inference import autoscaling_metrics


def _publisher(model_identifier='step-id', port=None, **kwargs):
  """A publisher for a fake server exposing ``done_total`` and ``running``."""
  return autoscaling_metrics.PrometheusNumLoadedModelsPublisher(
      metrics_port_fn=lambda: port,
      completed_metric='done_total',
      running_metric='running',
      model_identifier=model_identifier,
      **kwargs)


def _cell(model_identifier):
  """The process-wide ``num_loaded_models_<id>`` cell."""
  return autoscaling_metrics.MetricsEnvironment.process_wide_container(
  ).get_distribution(
      autoscaling_metrics.MetricName(
          autoscaling_metrics.MODEL_MANAGER_METRICS_NAMESPACE,
          f'num_loaded_models_{model_identifier}'))


def _published(model_identifier):
  return _cell(model_identifier).get_cumulative()


class AutoscalingMetricsTest(unittest.TestCase):
  def test_sanitize_metric_tag(self):
    self.assertEqual(
        autoscaling_metrics._sanitize_metric_tag('Qwen/Qwen2.5-7B:v1'),
        'Qwen_Qwen2_5_7B_v1')
    self.assertEqual(
        autoscaling_metrics._sanitize_metric_tag('normal_tag_1'),
        'normal_tag_1')

  def test_parse_prometheus_metric(self):
    prom_text = (
        '# HELP vllm:num_requests_running Running requests\n'
        '# TYPE vllm:num_requests_running gauge\n'
        'vllm:num_requests_running{model_name="qwen"} 4.0\n'
        'vllm:num_requests_running{model_name="other"} 6.0\n')
    self.assertEqual(
        autoscaling_metrics._parse_prometheus_metric(
            prom_text,
            'vllm:num_requests_running',
            label_filter={'model_name': 'qwen'}),
        4.0)
    self.assertEqual(
        autoscaling_metrics._parse_prometheus_metric(
            prom_text, 'vllm:num_requests_running'),
        10.0)
    self.assertIsNone(
        autoscaling_metrics._parse_prometheus_metric(prom_text, 'vllm:missing'))

  def test_parse_prometheus_metric_exposition_details(self):
    prom_text = (
        # Series of one counter that differ only by label are summed.
        'vllm:request_success_total{finished_reason="stop"} 10.0\n'
        'vllm:request_success_total{finished_reason="length"} 5.0\n'
        # A metric whose name merely starts with the requested one is ignored.
        'vllm:request_success_total_created 1.7e9\n'
        # Values that aren't numbers are skipped.
        'vllm:request_success_total{finished_reason="abort"} n/a\n'
        # Optional trailing timestamps are ignored.
        'nv_inference_request_success{model="a"} 3 1700000000000\n'
        # Quoted label values may contain commas, braces and escapes.
        'labeled{path="a,b}c",quote="say \\"hi\\"",n="x\\ny"} 7\n'
        '  \n'
        'not a sample line\n')
    parse = autoscaling_metrics._parse_prometheus_metric
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

  def test_scrape_prometheus_metrics(self):
    def fake_urlopen(url, timeout):
      self.assertEqual(url, 'http://localhost:8000/metrics')
      self.assertEqual(timeout, 5.0)
      return io.BytesIO(b'running 4\n')

    with mock.patch('urllib.request.urlopen', side_effect=fake_urlopen):
      self.assertEqual(
          autoscaling_metrics._scrape_prometheus_metrics(8000), 'running 4\n')
    # Any failure (e.g. the server is restarting) -> None.
    with mock.patch('urllib.request.urlopen', side_effect=OSError('refused')):
      self.assertIsNone(autoscaling_metrics._scrape_prometheus_metrics(8000))

  def test_capacity_estimator(self):
    estimator = autoscaling_metrics._CompletionRateCapacityEstimator(
        multiplier=2.0, min_value=1, window_secs=60.0, running_percentile=90.0)
    # First sample establishes baseline -> None.
    self.assertIsNone(estimator.update(10.0, 4.0, now=100.0))
    # delta=20 over dt=5s with running=4 -> mu = 20 / (4 * 5) = 1.0,
    # running_ref = 4.0, multiplier = 2.0 -> 8.0.
    self.assertAlmostEqual(estimator.update(30.0, 4.0, now=105.0), 8.0)

  def test_capacity_estimator_skips_counter_reset_and_clock_skew(self):
    estimator = autoscaling_metrics._CompletionRateCapacityEstimator(
        window_secs=60.0, min_requests=0)
    self.assertIsNone(estimator.update(50.0, 2.0, now=100.0))
    # Counter went down (server restart) -> skipped, new baseline.
    self.assertIsNone(estimator.update(5.0, 2.0, now=105.0))
    # Non-increasing timestamp -> skipped.
    self.assertIsNone(estimator.update(9.0, 2.0, now=105.0))
    # Negative gauge reading -> skipped.
    self.assertIsNone(estimator.update(9.0, -1.0, now=107.0))
    # delta=4 over 3s with running=2 -> mu=0.67, p90=2 -> 1.33.
    self.assertAlmostEqual(estimator.update(13.0, 2.0, now=110.0), 4 / 3)

  def test_capacity_estimator_running_percentile(self):
    def run(running_percentile):
      estimator = autoscaling_metrics._CompletionRateCapacityEstimator(
          min_value=0, min_requests=0, running_percentile=running_percentile)
      estimator.update(0.0, 1.0, now=0.0)
      # Ten 1s intervals with 1..10 in flight, each completing as many
      # requests: sum(completed) == sum(running * secs), so mu = 1.
      for i in range(1, 11):
        value = estimator.update(i * (i + 1) / 2, float(i), now=float(i))
      return value

    self.assertEqual(run(90.0), 9.0)  # Nearest rank: the 9th of 10.
    self.assertEqual(run(50.0), 5.0)
    self.assertEqual(run(100.0), 10.0)

  def test_capacity_estimator_warmup_min_requests_and_timeout(self):
    # Requires >= 3 completed requests before the 10s warmup timeout.
    estimator = autoscaling_metrics._CompletionRateCapacityEstimator(
        min_requests=3, warmup_timeout_secs=10.0)
    self.assertIsNone(estimator.update(0.0, 2.0, now=100.0))  # Baseline.
    # 1 completion (< 3) at elapsed=5s (< 10s) -> None.
    self.assertIsNone(estimator.update(1.0, 2.0, now=105.0))
    # 4 completions (>= 3) at elapsed=8s -> publishes
    # (4 / (2*5 + 2*3)) * 2 = 0.5 -> clamped to min_value=1.0.
    self.assertAlmostEqual(estimator.update(4.0, 2.0, now=108.0), 1.0)

    slow = autoscaling_metrics._CompletionRateCapacityEstimator(
        min_requests=3, warmup_timeout_secs=10.0)
    self.assertIsNone(slow.update(0.0, 1.0, now=100.0))  # Baseline.
    # 1 completion (< 3) at elapsed=5s -> None.
    self.assertIsNone(slow.update(1.0, 1.0, now=105.0))
    # 2 completions (< 3) but elapsed=10s (>= timeout) -> publishes.
    self.assertAlmostEqual(slow.update(2.0, 1.0, now=110.0), 1.0)

  def test_capacity_estimator_skips_idle_ticks_without_diluting_p90(self):
    estimator = autoscaling_metrics._CompletionRateCapacityEstimator(
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

  def test_capacity_estimator_no_estimate_until_a_request_completes(self):
    """In-flight requests with no completions never report the floor."""
    # Slow first requests: still nothing completed after the warmup timeout.
    slow = autoscaling_metrics._CompletionRateCapacityEstimator(
        min_requests=3, warmup_timeout_secs=10.0)
    self.assertIsNone(slow.update(0.0, 4.0, now=100.0))  # Baseline.
    for t in (105.0, 110.0, 115.0):
      self.assertIsNone(slow.update(0.0, 4.0, now=t))
    # The first completions produce an estimate.
    self.assertIsNotNone(slow.update(8.0, 4.0, now=120.0))

    # First poll after an idle period longer than the window: the earlier
    # samples have left the window and the new requests haven't finished yet.
    for aggregation in ('concurrency', 'window_mean', 'window_max'):
      resumed = autoscaling_metrics._CompletionRateCapacityEstimator(
          min_value=1,
          window_secs=10.0,
          min_requests=0,
          aggregation=aggregation)
      resumed.update(0.0, 2.0, now=0.0)
      self.assertGreater(resumed.update(20.0, 2.0, now=5.0), 1.0)
      for t in (10.0, 15.0, 20.0):
        self.assertIsNone(resumed.update(20.0, 0.0, now=t))  # Idle.
      self.assertIsNone(resumed.update(20.0, 3.0, now=25.0), aggregation)

    # lifetime_max keeps reporting its peak.
    peak = autoscaling_metrics._CompletionRateCapacityEstimator(
        min_value=1,
        window_secs=10.0,
        min_requests=0,
        aggregation='lifetime_max')
    peak.update(0.0, 2.0, now=0.0)
    self.assertEqual(peak.update(20.0, 2.0, now=5.0), 4.0)
    self.assertIsNone(peak.update(20.0, 0.0, now=20.0))  # Idle.
    self.assertEqual(peak.update(20.0, 3.0, now=25.0), 4.0)

  def test_capacity_estimator_defaults_to_monotonic_clock(self):
    estimator = autoscaling_metrics._CompletionRateCapacityEstimator(
        min_requests=0)
    with mock.patch.object(autoscaling_metrics.time,
                           'monotonic',
                           side_effect=[100.0, 105.0]):
      self.assertIsNone(estimator.update(0.0, 1.0))
      self.assertAlmostEqual(estimator.update(10.0, 1.0), 2.0)

  def test_capacity_estimator_completion_rate_aggregations(self):
    """The completion-rate modes ignore concurrency."""
    def run(aggregation, **kwargs):
      estimator = autoscaling_metrics._CompletionRateCapacityEstimator(
          min_value=0,
          min_requests=0,
          window_secs=60.0,
          aggregation=aggregation,
          **kwargs)
      # (completed_total, running, now): rates of 2, 6, idle, 1 req/s.
      samples = [(0, 3, 0.0), (10, 3, 5.0), (40, 8, 10.0), (40, 0, 15.0),
                 (45, 1, 20.0)]
      return [estimator.update(c, r, now=t) for c, r, t in samples]

    # 10 + 30 + 5 completions over the three non-idle 5s intervals.
    self.assertEqual(run('window_mean'), [None, 2.0, 4.0, None, 3.0])
    self.assertEqual(run('window_max'), [None, 2.0, 6.0, None, 6.0])
    self.assertEqual(run('lifetime_max'), [None, 2.0, 6.0, None, 6.0])
    self.assertEqual(
        run('lifetime_max', multiplier=0.5), [None, 1.0, 3.0, None, 3.0])

  def test_capacity_estimator_max_modes_differ_once_peak_leaves_window(self):
    def run(aggregation, window_secs=10.0):
      estimator = autoscaling_metrics._CompletionRateCapacityEstimator(
          min_value=0,
          min_requests=0,
          window_secs=window_secs,
          aggregation=aggregation)
      # Rates: 6 req/s, then 1 req/s for three intervals.
      samples = [(0, 1, 0.0), (30, 1, 5.0), (35, 1, 10.0), (40, 1, 15.0),
                 (45, 1, 20.0)]
      return [estimator.update(c, r, now=t) for c, r, t in samples][-1]

    # The 6 req/s interval ended at t=5, outside the 10s window at t=20.
    self.assertEqual(run('window_max'), 1.0)
    self.assertEqual(run('lifetime_max'), 6.0)
    # window_secs=0 keeps every interval.
    self.assertEqual(run('window_max', window_secs=0), 6.0)

  def test_capacity_estimator_completion_rate_works_without_in_flight(self):
    """A server whose gauge always reads 0 still gets a completion rate."""
    concurrency = autoscaling_metrics._CompletionRateCapacityEstimator(
        min_requests=0)
    rate = autoscaling_metrics._CompletionRateCapacityEstimator(
        min_value=0, min_requests=0, aggregation='window_mean')
    for estimator in (concurrency, rate):
      estimator.update(0, 0, now=0.0)
    self.assertIsNone(concurrency.update(500, 0, now=5.0))
    self.assertEqual(rate.update(500, 0, now=5.0), 100.0)

  def test_capacity_estimator_rejects_unknown_aggregation(self):
    with self.assertRaises(ValueError):
      autoscaling_metrics._CompletionRateCapacityEstimator(aggregation='p90')

  def test_publisher_publishes_signal_values_as_is(self):
    """Each capacity is published rounded; None or an error skips the tick."""
    _cell('sink_test').reset()
    self.addCleanup(lambda: _cell('sink_test').reset())
    published = []
    signal_values = [3.4, None, RuntimeError('scrape broke'), 7.6]

    def load_signal_fn(port):
      del port
      if published:
        # Record what the previous tick left in the cell.
        published[-1] = _published('sink_test').sum
      if not signal_values:
        pub._stop_event.set()  # pylint: disable=protected-access
        return None
      published.append(None)
      value = signal_values.pop(0)
      if isinstance(value, Exception):
        raise value
      return value

    pub = _publisher(
        'sink_test',
        port=8000,
        config=autoscaling_metrics.AutoscalingMetricConfig(
            poll_interval_secs=0, load_signal_fn=load_signal_fn))
    # Run the polling loop on this thread so the test is deterministic.
    with self.assertLogs(level='ERROR') as logs:
      pub._run()  # pylint: disable=protected-access
    self.assertIn('num_loaded_models_sink_test', logs.output[0])
    # 3.4 -> 3; None and the error leave 3 in place; 7.6 -> 8.
    self.assertEqual(published, [3, 3, 3, 8])
    self.assertEqual(_published('sink_test').count, 1)

  def test_publish_resets_process_wide_distribution_cell(self):
    pub = _publisher('reset_test')
    _cell('reset_test').reset()
    self.addCleanup(lambda: _cell('reset_test').reset())
    # A stale value, e.g. from before a model reload.
    _cell('reset_test').update(7)
    for _ in range(10):
      pub._publish(2.0)
    self.assertEqual(_published('reset_test'), DistributionData(2, 1, 2, 2))
    pub._publish(5.0)
    self.assertEqual(_published('reset_test'), DistributionData(5, 1, 5, 5))

  def test_publish_never_exposes_an_empty_cell(self):
    """A concurrent reader must never see count=0 between updates."""
    pub = _publisher('atomic_test')
    self.addCleanup(lambda: _cell('atomic_test').reset())
    pub._publish(1.0)
    stop = threading.Event()
    counts = set()

    def read():
      while not stop.is_set():
        counts.add(_published('atomic_test').count)

    reader = threading.Thread(target=read)
    reader.start()
    try:
      for i in range(20000):
        pub._publish(float(i % 7 + 1))
    finally:
      stop.set()
      reader.join()
    self.assertEqual(counts, {1})

  def test_publisher_for_same_tag_supersedes_previous(self):
    """A reloaded model's publisher takes over its tag from the stale one."""
    active = autoscaling_metrics.PrometheusNumLoadedModelsPublisher._active
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
      # Starting again is a no-op.
      thread = second._thread
      second.start()
      self.assertIs(second._thread, thread)
      # The stale one can neither take the tag back nor unregister the owner.
      first.start()
      self.assertIsNone(first._thread)
      first.stop()
      self.assertIs(active['num_loaded_models_reload'], second)
      self.assertTrue(second._thread.is_alive())
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
    self.assertEqual(
        _publisher(
            config=autoscaling_metrics.AutoscalingMetricConfig(
                aggregation='lifetime_max'))._estimator._aggregation,
        'lifetime_max')
    # No config -> defaults.
    self.assertEqual(
        _publisher()._poll_interval_secs,
        autoscaling_metrics.DEFAULT_POLL_INTERVAL_SECS)

  def test_publisher_reads_server_capacity(self):
    """The handler supplies metric names and a port; the rest is built in."""
    port = [None]
    publisher = autoscaling_metrics.PrometheusNumLoadedModelsPublisher(
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
                             'monotonic',
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
                           '_scrape_prometheus_metrics',
                           side_effect=lambda port: scrapes.pop(0)):
      with mock.patch.object(autoscaling_metrics.time,
                             'monotonic',
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
