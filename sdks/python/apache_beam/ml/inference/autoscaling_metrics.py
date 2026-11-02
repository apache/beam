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

"""Capacity-based autoscaling signal for model handlers that run a model server.

Dataflow's GPU autoscaling reads the process-wide distribution
``num_loaded_models_<tag>`` in the ``BeamML_ModelManager`` namespace as the
number of messages each worker can hold, and sizes the pool roughly as
``ceil(backlog / value)``. ``ModelManager`` publishes the number of loaded
model copies there. A handler that serves inference through an out-of-process
model server (vLLM, Triton, ...) always has exactly one copy per worker, so it
publishes an estimate of the worker's capacity under that metric instead.

The pieces, none of which depend on a particular model server:

* :class:`PrometheusCounterGaugeSampler` reads a server's completed-requests
  counter and in-flight gauge from its Prometheus ``/metrics`` endpoint.
* :class:`CompletionRateCapacityEstimator` turns those samples into a
  per-worker capacity estimate. It does no I/O.
* :class:`NumLoadedModelsPublisher` publishes a capacity signal under
  ``num_loaded_models_<tag>`` from a background thread.
* :class:`AutoscalingMetricConfig` holds the user-facing knobs, resolves the
  metric tag, and builds the estimator and publisher.

A handler typically wires them as::

  config = config.resolve_tag(model_identifier, model_name)
  sampler = PrometheusCounterGaugeSampler(completed_metric, running_metric)
  estimator = config.make_capacity_estimator()

  def signal():
    sample = sampler(metrics_port)
    return None if sample is None else estimator.update(*sample)

  publisher = config.make_publisher(signal)
  publisher.start()  # Once the server is reachable; publisher.stop() at exit.
"""

# pytype: skip-file

import dataclasses
import logging
import re
import threading
import time
import urllib.request
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from typing import NamedTuple
from typing import Optional

from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.metricbase import MetricName

__all__ = [
    'AutoscalingMetricConfig',
    'CompletionRateCapacityEstimator',
    'NumLoadedModelsPublisher',
    'PrometheusCounterGaugeSampler',
    'parse_prometheus_metric',
    'sanitize_metric_tag',
    'scrape_prometheus_metrics',
]

# The metric the runner reads (shared with model_manager.py).
MODEL_MANAGER_METRICS_NAMESPACE = 'BeamML_ModelManager'
NUM_LOADED_MODELS_METRIC_PREFIX = 'num_loaded_models_'

# Publisher defaults. The capacity estimate is already smoothed, so by default
# the publisher forwards the latest value without a second smoothing layer.
DEFAULT_POLL_INTERVAL_SECS = 5.0
DEFAULT_AGGREGATION = 'last'
DEFAULT_AGGREGATION_WINDOW_SECS = 1800.0  # 30 minutes.

# Estimator defaults.
# A larger multiplier lets each worker hold more backlog (fewer workers); a
# smaller one scales out more aggressively.
DEFAULT_MULTIPLIER = 1.0
DEFAULT_SIGNAL_WINDOW_SECS = 1800.0  # 30 minutes.
DEFAULT_RUNNING_PERCENTILE = 90.0
DEFAULT_MIN_REQUESTS = 3
DEFAULT_WARMUP_TIMEOUT_SECS = 10.0

# One sample line of the Prometheus text format:
#   metric_name{label="value",...} value [timestamp]
_PROMETHEUS_SAMPLE_RE = re.compile(
    r'(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)'
    r'(?:\{(?P<labels>(?:[^"}]|"(?:[^"\\]|\\.)*")*)\})?'
    r'\s+(?P<value>\S+)')
# One label pair; values are quoted and may contain \\, \" and \n escapes.
_PROMETHEUS_LABEL_RE = re.compile(
    r'(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"(?P<value>(?:[^"\\]|\\.)*)"')
_PROMETHEUS_ESCAPE_RE = re.compile(r'\\(.)')

_WINDOWED_AGGREGATIONS = ('max', 'mean')
_PERCENTILE_AGGREGATION_RE = re.compile(r'p(\d+)')


def sanitize_metric_tag(name: str) -> str:
  """Turns a model name into a metric-tag-safe token.

  Only used for the fallback tag of handlers running outside RunInference; the
  RunInference model identifier is always used verbatim.

  Args:
    name: The model name (may contain ``/``, ``:`` etc.).

  Returns:
    ``name`` with characters outside ``[A-Za-z0-9_]`` replaced by ``_``.
  """
  return ''.join(c if (c.isalnum() or c == '_') else '_' for c in name)


def scrape_prometheus_metrics(
    port: int,
    timeout_secs: float = 5.0,
    path: str = '/metrics') -> Optional[str]:
  """Fetches a local Prometheus endpoint.

  Args:
    port: Port of the server on ``localhost``.
    timeout_secs: Request timeout.
    path: Endpoint path.

  Returns:
    The response body, or ``None`` on any failure (e.g. the server is still
    starting or restarting), so callers can simply skip that sample.
  """
  url = f'http://localhost:{port}{path}'
  try:
    with urllib.request.urlopen(url, timeout=timeout_secs) as response:
      return response.read().decode('utf-8', errors='replace')
  except Exception:  # pylint: disable=broad-except
    return None


def _unescape_label_value(value: str) -> str:
  """Undoes the backslash escapes in a Prometheus label value."""
  return _PROMETHEUS_ESCAPE_RE.sub(
      lambda m: '\n' if m.group(1) == 'n' else m.group(1), value)


def _parse_prometheus_labels(label_str: str) -> dict[str, str]:
  """Parses the inside of the ``{k="v",...}`` section of a sample line."""
  return {
      match['name']: _unescape_label_value(match['value'])
      for match in _PROMETHEUS_LABEL_RE.finditer(label_str)
  }


def parse_prometheus_metric(
    metrics_text: str,
    metric_name: str,
    label_filter: Optional[Mapping[str, str]] = None,
) -> Optional[float]:
  """Sums the values of one metric from Prometheus text-format exposition.

  Args:
    metrics_text: The full body returned by the server's ``/metrics`` endpoint.
    metric_name: The exact sample name to match (e.g.
      ``vllm:num_requests_running``).
    label_filter: If provided, only samples whose labels contain all of these
      key/value pairs are summed.

  Returns:
    The sum of the matching sample values, or ``None`` if there are none.
  """
  total: Optional[float] = None
  for line in metrics_text.splitlines():
    match = _PROMETHEUS_SAMPLE_RE.match(line.strip())
    # Comment, blank and other-metric lines don't match.
    if match is None or match['name'] != metric_name:
      continue
    if label_filter:
      labels = _parse_prometheus_labels(match['labels'] or '')
      if any(labels.get(k) != v for k, v in label_filter.items()):
        continue
    try:
      value = float(match['value'])
    except ValueError:
      continue
    total = value if total is None else total + value
  return total


def _nearest_rank_percentile(values: list[float], q: float) -> float:
  """Nearest-rank percentile (``q`` in [0, 100]) of a non-empty list."""
  ordered = sorted(values)
  q = min(max(q, 0.0), 100.0)
  # Nearest rank ceil(q / 100 * n), in arithmetic that avoids float rounding.
  rank = int((q * len(ordered) + 99) // 100)
  return ordered[min(max(rank - 1, 0), len(ordered) - 1)]


def _parse_aggregation(aggregation: str) -> tuple[str, Optional[float]]:
  """Validates an aggregation policy; returns ``(mode, percentile)``."""
  mode = (aggregation or DEFAULT_AGGREGATION).lower()
  if mode in ('last', 'lifetime_max') + _WINDOWED_AGGREGATIONS:
    return mode, None
  percentile = _PERCENTILE_AGGREGATION_RE.fullmatch(mode)
  if percentile and 0 <= int(percentile.group(1)) <= 100:
    return mode, float(percentile.group(1))
  raise ValueError(
      f'Unknown aggregation {aggregation!r}: expected one of last, max, mean, '
      'lifetime_max, or pNN with NN in [0, 100].')


class NumLoadedModelsPublisher:
  """Publishes a capacity signal as the ``num_loaded_models_<tag>`` metric.

  A daemon thread calls ``signal_fn`` every ``poll_interval_secs``, applies the
  ``aggregation`` policy, scales the result by ``multiplier`` and publishes it
  to the process-wide distribution the runner reads. A ``signal_fn`` result of
  ``None`` (e.g. the server is restarting) skips the tick and leaves the last
  published value in place.

  Aggregation policies:

  * ``'last'`` (default): the latest sample. Use it when ``signal_fn`` already
    smooths its output, as :class:`CompletionRateCapacityEstimator` does.
  * ``'max'``: the peak over the trailing ``aggregation_window_secs``.
  * ``'pNN'`` (e.g. ``'p90'``): a percentile over that window. Smoother than
    ``'max'``: it ignores brief spikes.
  * ``'mean'``: the average over that window.
  * ``'lifetime_max'``: the peak since the publisher started. It never decays,
    so the pool stays sized for the worst load seen.

  The publisher never reports 0, whether on start, on stop or on a skipped
  tick: a 0 tells the runner the worker has no capacity and can trigger a
  spurious downscale. Only one publisher per metric name is active in a
  process; starting a new one (e.g. after a model reload) stops the previous
  one.

  Args:
    tag: Metric tag. It must equal the RunInference model identifier the runner
      looks up (see :meth:`AutoscalingMetricConfig.resolve_tag`).
    signal_fn: Returns the current capacity sample, or ``None`` to skip.
    poll_interval_secs: Seconds between samples.
    aggregation: One of the policies above.
    aggregation_window_secs: Trailing window for ``'max'``, ``'pNN'`` and
      ``'mean'``; 0 keeps every sample.
    multiplier: Factor applied to every published value. Leave it at 1.0 when
      ``signal_fn`` already applies one, as the estimator does.

  Raises:
    ValueError: if ``tag`` is empty or ``aggregation`` is not a known policy.
  """
  # The publisher owning each metric name in this process, so a reloaded
  # model's publisher can supersede the stale one.
  _active: dict[str, 'NumLoadedModelsPublisher'] = {}
  _active_lock = threading.Lock()

  def __init__(
      self,
      tag: str,
      signal_fn: Callable[[], Optional[float]],
      poll_interval_secs: float = DEFAULT_POLL_INTERVAL_SECS,
      aggregation: str = DEFAULT_AGGREGATION,
      aggregation_window_secs: float = DEFAULT_AGGREGATION_WINDOW_SECS,
      multiplier: float = DEFAULT_MULTIPLIER,
  ):
    if not tag:
      raise ValueError('num_loaded_models tag must be a non-empty string.')
    self._metric_name = NUM_LOADED_MODELS_METRIC_PREFIX + tag
    self._signal_fn = signal_fn
    self._poll_interval_secs = poll_interval_secs
    self._aggregation, self._percentile = _parse_aggregation(aggregation)
    self._aggregation_window_secs = max(float(aggregation_window_secs), 0.0)
    self._multiplier = float(multiplier)
    # (timestamp, value) samples kept for the windowed policies.
    self._samples: list[tuple[float, float]] = []
    self._lifetime_max: Optional[float] = None
    self._stop_event = threading.Event()
    self._thread: Optional[threading.Thread] = None

  def start(self) -> None:
    """Starts the background polling thread.

    The metric stays absent until the first sample is published.
    """
    if self._thread is not None:
      return
    with NumLoadedModelsPublisher._active_lock:
      previous = NumLoadedModelsPublisher._active.get(self._metric_name)
      NumLoadedModelsPublisher._active[self._metric_name] = self
    if previous is not None and previous is not self:
      logging.info(
          'Replacing the active publisher for %s (model reload or server '
          'restart).',
          self._metric_name)
      previous.stop()
    self._thread = threading.Thread(
        target=self._run, name='num-loaded-models-publisher', daemon=True)
    self._thread.start()

  def stop(self) -> None:
    """Stops polling.

    Publishes no final 0: when a worker is drained during a downscale, a 0
    would shrink the aggregate capacity and could drive a feedback loop of
    further downscales. The drained worker's value ages out instead.
    """
    self._stop_event.set()
    with NumLoadedModelsPublisher._active_lock:
      if NumLoadedModelsPublisher._active.get(self._metric_name) is self:
        del NumLoadedModelsPublisher._active[self._metric_name]
    thread = self._thread
    if thread is not None:
      thread.join(timeout=self._poll_interval_secs + 5)
      self._thread = None

  def _run(self) -> None:
    while not self._stop_event.is_set():
      try:
        sample = self._signal_fn()
        if sample is not None:
          self._publish(self._record(float(sample), time.time()))
      except Exception:  # pylint: disable=broad-except
        logging.exception(
            'Failed to publish autoscaling metric %s; will retry.',
            self._metric_name)
      self._stop_event.wait(self._poll_interval_secs)

  def _record(self, sample: float, now: float) -> float:
    """Adds a sample and returns the value to publish under the policy."""
    if self._aggregation == 'last':
      return sample
    if self._aggregation == 'lifetime_max':
      if self._lifetime_max is None or sample > self._lifetime_max:
        self._lifetime_max = sample
      return self._lifetime_max
    self._samples.append((now, sample))
    if self._aggregation_window_secs > 0:
      cutoff = now - self._aggregation_window_secs
      self._samples = [(t, v) for (t, v) in self._samples if t >= cutoff]
    values = [v for (_, v) in self._samples]
    if self._aggregation == 'max':
      return max(values)
    if self._aggregation == 'mean':
      return sum(values) / len(values)
    return _nearest_rank_percentile(values, self._percentile)

  def _publish(self, value: float) -> None:
    published = int(round(value * self._multiplier))
    # The SDK harness reports process-wide distributions without resetting
    # them, and Dataflow reads their cumulative mean (sum / count). Resetting
    # before each update keeps the cell at (sum=published, count=1), so the
    # runner sees the latest value rather than an average since worker start.
    cell = MetricsEnvironment.process_wide_container().get_distribution(
        MetricName(MODEL_MANAGER_METRICS_NAMESPACE, self._metric_name))
    current = cell.get_cumulative()
    if current.count != 1 or current.sum != published:
      cell.reset()
      cell.update(published)


class PrometheusCounterGaugeSampler:
  """Reads a completed-requests counter and an in-flight gauge from a server.

  Calling the sampler with the server's metrics port scrapes
  ``http://localhost:<port>/metrics`` and returns ``(completed_total,
  running)``, each summed over the samples that match ``label_filter``. The two
  metric names are the only server-specific inputs, e.g.
  ``vllm:request_success_total`` and ``vllm:num_requests_running`` for vLLM.

  Args:
    completed_metric: Name of the monotonically increasing counter of completed
      requests.
    running_metric: Name of the gauge of requests currently in flight.
    label_filter: Optional labels a sample must have to be counted, e.g. to
      select one model on a multi-model server.
    timeout_secs: Scrape timeout.
  """
  def __init__(
      self,
      completed_metric: str,
      running_metric: str,
      label_filter: Optional[Mapping[str, str]] = None,
      timeout_secs: float = 5.0,
  ):
    self._completed_metric = completed_metric
    self._running_metric = running_metric
    self._label_filter = dict(label_filter) if label_filter else None
    self._timeout_secs = timeout_secs

  def __call__(self, port: int) -> Optional[tuple[float, float]]:
    """Returns ``(completed_total, running)``, or ``None`` if unavailable."""
    text = scrape_prometheus_metrics(port, timeout_secs=self._timeout_secs)
    if text is None:
      return None
    completed = parse_prometheus_metric(
        text, self._completed_metric, label_filter=self._label_filter)
    running = parse_prometheus_metric(
        text, self._running_metric, label_filter=self._label_filter)
    if completed is None or running is None:
      return None
    return completed, running


class _Interval(NamedTuple):
  """One sampling interval in the estimator's trailing window."""
  end_ts: float
  completed: float  # Requests completed during the interval.
  running: float  # Requests in flight at the end of the interval.
  secs: float  # Length of the interval.


class CompletionRateCapacityEstimator:
  """Estimates how many messages a worker can absorb.

  Feed it periodic samples of a model server's completed-requests counter and
  in-flight gauge (e.g. from :class:`PrometheusCounterGaugeSampler`) through
  :meth:`update`. Over a trailing ``window_secs`` window it computes::

      mu          = sum(completed) / sum(running * secs)
      running_ref = percentile(running, running_percentile)
      capacity    = max(min_value, mu * running_ref * multiplier)

  ``mu`` is the throughput of one in-flight slot (roughly ``1 / latency``),
  computed as a ratio of windowed sums so discrete completions don't make it
  jumpy. ``running_ref`` is the concurrency the worker typically sustains; the
  p90 rather than the max avoids latching onto rare backlog spikes. Their
  product is the worker's sustained throughput.

  Normalizing by concurrency keeps the estimate steady when load changes: right
  after a scaling event, when the runner rebalances work and a worker is
  briefly starved, its raw completion rate collapses, but ``mu`` and
  ``running_ref`` hold, so the estimate does not crater and trigger a downscale
  spiral.

  The estimator suits servers whose requests take long enough to be seen in
  flight by periodic sampling, such as LLM servers with latencies of a second
  or more. For servers with millisecond latencies the in-flight gauge reads
  almost always 0; pass the publisher a different signal instead.

  The estimate is already smoothed, so publish it with ``aggregation='last'``.
  This class does no I/O.

  Args:
    multiplier: Dimensionless factor on the estimate; larger means fewer
      workers.
    min_value: Floor of the estimate.
    window_secs: Trailing window; 0 keeps every interval.
    running_percentile: Percentile of in-flight concurrency used as
      ``running_ref``.
    min_requests: Completions a newly started worker needs before it reports,
      unless ``warmup_timeout_secs`` passes first.
    warmup_timeout_secs: Longest time after a worker first sees work that it
      waits for ``min_requests``.
  """
  def __init__(
      self,
      multiplier: float = DEFAULT_MULTIPLIER,
      min_value: int = 1,
      window_secs: float = DEFAULT_SIGNAL_WINDOW_SECS,
      running_percentile: float = DEFAULT_RUNNING_PERCENTILE,
      min_requests: int = DEFAULT_MIN_REQUESTS,
      warmup_timeout_secs: float = DEFAULT_WARMUP_TIMEOUT_SECS,
  ):
    self._multiplier = max(float(multiplier), 0.0)
    self._min_value = int(min_value)
    self._window_secs = max(float(window_secs), 0.0)
    self._running_percentile = min(max(float(running_percentile), 0.0), 100.0)
    self._min_requests = max(int(min_requests), 0)
    self._warmup_timeout_secs = max(float(warmup_timeout_secs), 0.0)
    self._last_count: Optional[float] = None
    self._last_ts: Optional[float] = None
    self._first_active_ts: Optional[float] = None
    self._samples: list[_Interval] = []

  def update(
      self,
      completed_total: float,
      running: float,
      now: Optional[float] = None) -> Optional[float]:
    """Records one sample and returns the current estimate.

    Args:
      completed_total: Current value of the completed-requests counter.
      running: Current number of in-flight requests.
      now: Sample timestamp in seconds; defaults to ``time.time()``.

    Returns:
      The capacity estimate, or ``None`` when there is not enough data (first
      sample, counter reset, idle window or warmup); the caller should then
      keep its previous value.
    """
    if now is None:
      now = time.time()
    prev_count, prev_ts = self._last_count, self._last_ts
    self._last_count, self._last_ts = completed_total, now
    if prev_count is None or prev_ts is None:
      return None  # The first sample only sets the baseline.
    secs = now - prev_ts
    completed = completed_total - prev_count
    if secs <= 0 or completed < 0 or running < 0:
      return None  # Clock skew, counter reset (server restart) or bad gauge.

    if self._window_secs > 0:
      cutoff = now - self._window_secs
      self._samples = [s for s in self._samples if s.end_ts >= cutoff]
    if completed == 0 and running == 0:
      # Skip fully idle intervals so troughs don't drag running_ref to 0.
      return None
    self._samples.append(
        _Interval(now, float(completed), float(running), float(secs)))

    total_completed = sum(s.completed for s in self._samples)
    slot_secs = sum(s.running * s.secs for s in self._samples)
    if slot_secs <= 0:
      return None  # Nothing was in flight during the window.
    if self._first_active_ts is None:
      self._first_active_ts = prev_ts
    if (total_completed < self._min_requests and
        now - self._first_active_ts < self._warmup_timeout_secs):
      # Wait until a new worker is calibrated, so it doesn't briefly report
      # the floor value for the whole job.
      return None

    mu = total_completed / slot_secs
    running_ref = _nearest_rank_percentile(
        [s.running for s in self._samples if s.running > 0],
        self._running_percentile)
    return max(float(self._min_value), mu * running_ref * self._multiplier)


@dataclass(frozen=True)
class AutoscalingMetricConfig:
  """User-facing knobs for the capacity-based autoscaling signal.

  Model handlers that publish the signal take an instance, typically as an
  ``autoscaling_config`` argument; leave it unset for the defaults. A handler
  calls :meth:`resolve_tag`, builds its signal from a server-specific sampler
  and :meth:`make_capacity_estimator` (or from ``load_signal_fn``), and passes
  the signal to :meth:`make_publisher`.

  Attributes:
    tag: Metric tag to publish under. Leave unset so the handler uses the
      RunInference model identifier (see :meth:`resolve_tag`), which is what
      the runner looks up and which stays stable across model updates. Only
      set it if you pass the same value as ``RunInference(model_identifier=)``.
    poll_interval_secs: How often to sample and publish.
    multiplier: Factor on the capacity estimate; larger means fewer workers,
      smaller scales out more aggressively.
    signal_window_secs: Trailing window of the capacity estimator.
    min_requests: Completions a newly started worker needs before it
      publishes, unless ``warmup_timeout_secs`` passes first.
    warmup_timeout_secs: Longest time a new worker waits for ``min_requests``.
    aggregation: Publisher policy on top of the already-smoothed estimate (see
      :class:`NumLoadedModelsPublisher`). Keep ``'last'`` unless you want a
      second smoothing layer, e.g. ``'lifetime_max'`` to never scale down.
    aggregation_window_secs: Window of the ``'max'``, ``'pNN'`` and ``'mean'``
      policies.
    load_signal_fn: Optional replacement for the estimator: given the server's
      metrics port, returns the capacity directly. Handlers that support it
      call it instead of the estimator.
  """
  tag: Optional[str] = None
  poll_interval_secs: float = DEFAULT_POLL_INTERVAL_SECS
  multiplier: float = DEFAULT_MULTIPLIER
  signal_window_secs: float = DEFAULT_SIGNAL_WINDOW_SECS
  min_requests: int = DEFAULT_MIN_REQUESTS
  warmup_timeout_secs: float = DEFAULT_WARMUP_TIMEOUT_SECS
  aggregation: str = DEFAULT_AGGREGATION
  aggregation_window_secs: float = DEFAULT_AGGREGATION_WINDOW_SECS
  load_signal_fn: Optional[Callable[[int], Optional[float]]] = None

  def resolve_tag(
      self,
      model_identifier: Optional[str],
      fallback_name: str = '') -> 'AutoscalingMetricConfig':
    """Returns a copy whose ``tag`` is set.

    Precedence: an explicit ``tag``; then ``model_identifier``, used verbatim
    because it must equal the identifier RunInference annotates the step with
    (see ``ModelHandler.set_model_identifier``); then ``fallback_name`` passed
    through :func:`sanitize_metric_tag`, for handlers used outside RunInference.

    Raises:
      ValueError: if no tag can be derived.
    """
    if self.tag:
      return self
    tag = model_identifier or sanitize_metric_tag(fallback_name or '')
    if not tag:
      raise ValueError(
          'Cannot derive a num_loaded_models tag: pass a model_identifier, a '
          'fallback_name, or set AutoscalingMetricConfig.tag.')
    return dataclasses.replace(self, tag=tag)

  def make_capacity_estimator(self) -> CompletionRateCapacityEstimator:
    """Builds a :class:`CompletionRateCapacityEstimator` from these knobs."""
    return CompletionRateCapacityEstimator(
        multiplier=self.multiplier,
        window_secs=self.signal_window_secs,
        min_requests=self.min_requests,
        warmup_timeout_secs=self.warmup_timeout_secs)

  def make_publisher(
      self, signal_fn: Callable[[],
                                Optional[float]]) -> NumLoadedModelsPublisher:
    """Builds a :class:`NumLoadedModelsPublisher` for ``signal_fn``.

    The multiplier is not applied again here; the estimator already applies it.

    Raises:
      ValueError: if ``tag`` is unset; call :meth:`resolve_tag` first.
    """
    if not self.tag:
      raise ValueError(
          'AutoscalingMetricConfig.tag is unset; call resolve_tag() first.')
    return NumLoadedModelsPublisher(
        tag=self.tag,
        signal_fn=signal_fn,
        poll_interval_secs=self.poll_interval_secs,
        aggregation=self.aggregation,
        aggregation_window_secs=self.aggregation_window_secs,
    )
