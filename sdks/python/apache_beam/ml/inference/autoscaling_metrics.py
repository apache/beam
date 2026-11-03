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

Nothing here depends on a particular model server. There are three classes,
each with one job:

* :class:`CompletionRateCapacityEstimator` *computes* a smoothed per-worker
  capacity from a server's completed-requests counter and in-flight gauge. It
  does no I/O.
* :class:`NumLoadedModelsPublisher` *publishes*: a background thread polls a
  capacity signal and writes each value to ``num_loaded_models_<tag>`` as is.
* :class:`AutoscalingMetricConfig` *configures*: it holds the user-facing
  knobs, resolves the metric tag, and builds the publisher.

plus :func:`scrape_prometheus_metrics` and :func:`parse_prometheus_metric` to
read a server's Prometheus ``/metrics`` endpoint.

A handler for a model server that exposes a completed-requests counter and an
in-flight gauge only supplies the server-specific parts, the two metric names
and how to find the server's port, and gets a ready publisher::

  config = config.resolve_tag(model_identifier, model_name)
  publisher = config.make_model_server_publisher(
      metrics_port_fn=lambda: server.port if server.is_up else None,
      completed_metric='vllm:request_success_total',
      running_metric='vllm:num_requests_running')
  publisher.start()  # Once the server is reachable; publisher.stop() at exit.

Handlers whose server needs a different capacity signal build it themselves
and pass it to :meth:`AutoscalingMetricConfig.make_publisher`.
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
    'parse_prometheus_metric',
    'sanitize_metric_tag',
    'scrape_prometheus_metrics',
]

# The metric the runner reads (shared with model_manager.py).
MODEL_MANAGER_METRICS_NAMESPACE = 'BeamML_ModelManager'
NUM_LOADED_MODELS_METRIC_PREFIX = 'num_loaded_models_'

DEFAULT_POLL_INTERVAL_SECS = 5.0

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


def _parse_prometheus_labels(label_str: str) -> dict[str, str]:
  """Parses the inside of ``{k="v",...}``, undoing backslash escapes."""
  def unescape(match: re.Match) -> str:
    return '\n' if match.group(1) == 'n' else match.group(1)

  return {
      match['name']: _PROMETHEUS_ESCAPE_RE.sub(unescape, match['value'])
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


class NumLoadedModelsPublisher:
  """Publishes a capacity signal as the ``num_loaded_models_<tag>`` metric.

  A daemon thread calls ``signal_fn`` every ``poll_interval_secs`` and writes
  each value, rounded to an int, to the process-wide distribution the runner
  reads. The publisher does not transform values: any smoothing belongs in
  ``signal_fn`` (e.g. :class:`CompletionRateCapacityEstimator`, which already
  smooths and never returns less than its ``min_value``).

  A ``signal_fn`` result of ``None`` (e.g. the server is restarting) skips the
  tick and leaves the last published value in place. The publisher never
  injects a 0 of its own, on start, on stop or on a skipped tick: a 0 tells the
  runner the worker has no capacity and can trigger a spurious downscale. Only
  one publisher per metric name is active in a process; starting a new one
  (e.g. after a model reload) stops the previous one.

  Args:
    tag: Metric tag. It must equal the RunInference model identifier the runner
      looks up (see :meth:`AutoscalingMetricConfig.resolve_tag`).
    signal_fn: Returns the current capacity, or ``None`` to skip the tick.
    poll_interval_secs: Seconds between polls.

  Raises:
    ValueError: if ``tag`` is empty.
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
  ):
    if not tag:
      raise ValueError('num_loaded_models tag must be a non-empty string.')
    self._metric_name = NUM_LOADED_MODELS_METRIC_PREFIX + tag
    self._signal_fn = signal_fn
    self._poll_interval_secs = poll_interval_secs
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
    would shrink the pool's total capacity and could drive a feedback loop of
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
        value = self._signal_fn()
        if value is not None:
          self._publish(value)
      except Exception:  # pylint: disable=broad-except
        logging.exception(
            'Failed to publish autoscaling metric %s; will retry.',
            self._metric_name)
      self._stop_event.wait(self._poll_interval_secs)

  def _publish(self, value: float) -> None:
    published = int(round(value))
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


class _Interval(NamedTuple):
  """One sampling interval in the estimator's trailing window."""
  end_ts: float
  completed: float  # Requests completed during the interval.
  running: float  # Requests in flight at the end of the interval.
  secs: float  # Length of the interval.


class CompletionRateCapacityEstimator:
  """Estimates how many messages a worker can absorb.

  Feed it periodic samples of a model server's completed-requests counter and
  in-flight gauge through :meth:`update`. Over a trailing ``window_secs`` window it computes::

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

  The estimate is already smoothed; publish it as is. This class does no I/O.

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
  calls :meth:`resolve_tag` and then :meth:`make_model_server_publisher` with
  its server's metric names, or builds its own signal and passes it to
  :meth:`make_publisher`.

  Attributes:
    tag: Metric tag to publish under. Leave unset so the handler uses the
      RunInference model identifier (see :meth:`resolve_tag`), which is what
      the runner looks up and which stays stable across model updates. Only
      set it if you pass the same value as ``RunInference(model_identifier=)``.
    poll_interval_secs: How often to sample and publish.
    multiplier: Factor on the capacity estimate; larger means fewer workers,
      smaller scales out more aggressively. Applied once, by the estimator.
    signal_window_secs: Trailing window of the capacity estimator.
    min_requests: Completions a newly started worker needs before it
      publishes, unless ``warmup_timeout_secs`` passes first.
    warmup_timeout_secs: Longest time a new worker waits for ``min_requests``.
    load_signal_fn: Optional replacement for the estimator: given the server's
      metrics port, returns the capacity directly. Used by
      :meth:`make_model_server_publisher`.
  """
  tag: Optional[str] = None
  poll_interval_secs: float = DEFAULT_POLL_INTERVAL_SECS
  multiplier: float = DEFAULT_MULTIPLIER
  signal_window_secs: float = DEFAULT_SIGNAL_WINDOW_SECS
  min_requests: int = DEFAULT_MIN_REQUESTS
  warmup_timeout_secs: float = DEFAULT_WARMUP_TIMEOUT_SECS
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

  def make_model_server_publisher(
      self,
      metrics_port_fn: Callable[[], Optional[int]],
      completed_metric: str,
      running_metric: str,
      label_filter: Optional[Mapping[str, str]] = None,
  ) -> NumLoadedModelsPublisher:
    """Builds a publisher of a model server's capacity.

    Every poll scrapes the server's Prometheus ``/metrics`` endpoint, reads the
    two metrics, feeds them to a :class:`CompletionRateCapacityEstimator` built
    from these knobs, and publishes the estimate. If ``load_signal_fn`` is set,
    it replaces the scrape and the estimate.

    Args:
      metrics_port_fn: Returns the port serving ``/metrics`` while the server
        is up, or ``None`` while it is not (the publisher then skips the tick).
      completed_metric: Name of the server's counter of completed requests,
        e.g. ``vllm:request_success_total``.
      running_metric: Name of the server's gauge of in-flight requests, e.g.
        ``vllm:num_requests_running``.
      label_filter: Optional labels a sample must have to be counted, e.g. to
        select one model on a multi-model server.

    Raises:
      ValueError: if ``tag`` is unset; call :meth:`resolve_tag` first.
    """
    estimator = CompletionRateCapacityEstimator(
        multiplier=self.multiplier,
        window_secs=self.signal_window_secs,
        min_requests=self.min_requests,
        warmup_timeout_secs=self.warmup_timeout_secs)

    def capacity() -> Optional[float]:
      """One poll: returns the current capacity, or None to skip the tick."""
      port = metrics_port_fn()
      if port is None:
        return None  # The server is not up.
      if self.load_signal_fn is not None:
        return self.load_signal_fn(port)
      text = scrape_prometheus_metrics(port)
      if text is None:
        return None
      completed = parse_prometheus_metric(text, completed_metric, label_filter)
      running = parse_prometheus_metric(text, running_metric, label_filter)
      if completed is None or running is None:
        return None
      return estimator.update(completed, running)

    return self.make_publisher(capacity)

  def make_publisher(
      self, signal_fn: Callable[[],
                                Optional[float]]) -> NumLoadedModelsPublisher:
    """Builds a :class:`NumLoadedModelsPublisher` for ``signal_fn``.

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
    )
