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
``num_loaded_models_<tag>`` in the ``BeamML_ModelManager`` namespace as how
many requests each worker can handle, and sizes the pool roughly as
``ceil(backlog / value)``. ``ModelManager`` publishes the number of loaded
model copies there. A handler that serves inference through an out-of-process
model server (vLLM, Triton, ...) always has exactly one copy per worker, so it
publishes an estimate of the worker's capacity under that metric instead.

:class:`PrometheusNumLoadedModelsPublisher` reads the server's metrics over
HTTP, so the server must:

* run on the same worker as the handler and serve the Prometheus text format
  at ``http://localhost:<port>/metrics``;
* export a counter of completed requests (monotonically increasing, reset only
  when the server restarts), e.g. vLLM's ``vllm:request_success_total``; and
* export a gauge of the requests currently in flight, e.g. vLLM's
  ``vllm:num_requests_running``.

A handler for such a server (vLLM, for example) supplies only the two metric
names, a way to find the port, and the RunInference model identifier::

  publisher = PrometheusNumLoadedModelsPublisher(
      metrics_port_fn=lambda: server.port if server.is_up else None,
      completed_metric='vllm:request_success_total',
      running_metric='vllm:num_requests_running',
      model_identifier=model_identifier,
      model_name=model_name,
      config=autoscaling_config)  # An AutoscalingMetricConfig, or None.
  publisher.start()  # Once the server is reachable; publisher.stop() at exit.

If the server exports different metrics,
``AutoscalingMetricConfig.load_signal_fn`` can replace the scrape and the
estimate: it is given the server's port and returns the capacity directly.

The public API is :class:`PrometheusNumLoadedModelsPublisher` and
:class:`AutoscalingMetricConfig`; everything else is an implementation detail.
"""

# pytype: skip-file

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
    'PrometheusNumLoadedModelsPublisher',
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

# How _CompletionRateCapacityEstimator turns its samples into an estimate.
CONCURRENCY = 'concurrency'
WINDOW_MEAN = 'window_mean'
WINDOW_MAX = 'window_max'
LIFETIME_MAX = 'lifetime_max'
_AGGREGATIONS = (CONCURRENCY, WINDOW_MEAN, WINDOW_MAX, LIFETIME_MAX)

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


def _sanitize_metric_tag(name: str) -> str:
  """Turns a model name into a metric-tag-safe token.

  Only used for the fallback tag of handlers running outside RunInference; the
  RunInference model identifier is always used verbatim.

  Args:
    name: The model name (may contain ``/``, ``:`` etc.).

  Returns:
    ``name`` with characters outside ``[A-Za-z0-9_]`` replaced by ``_``.
  """
  return ''.join(c if (c.isalnum() or c == '_') else '_' for c in name)


def _scrape_prometheus_metrics(
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


def _parse_prometheus_metric(
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


class _Interval(NamedTuple):
  """One sampling interval in the estimator's trailing window."""
  end_ts: float
  completed: float  # Requests completed during the interval.
  running: float  # Requests in flight at the end of the interval.
  secs: float  # Length of the interval.


class _CompletionRateCapacityEstimator:
  """Estimates how many requests a worker can handle.

  Feed it periodic samples of a model server's completed-requests counter and
  in-flight gauge through :meth:`update`. Each sample closes an interval, and
  ``aggregation`` picks how the intervals become an estimate. Every mode
  returns ``max(min_value, value * multiplier)``. This class does no I/O.

  ``'concurrency'`` (the default) computes, over a trailing ``window_secs``
  window::

      mu          = sum(completed) / sum(running * secs)
      running_ref = percentile(running, running_percentile)
      value       = mu * running_ref

  ``mu`` is the throughput of one in-flight slot (roughly ``1 / latency``),
  computed as a ratio of windowed sums so discrete completions don't make it
  jumpy. ``running_ref`` is the concurrency the worker typically sustains; the
  p90 rather than the max avoids latching onto rare backlog spikes. Their
  product is the worker's sustained throughput.

  Normalizing by concurrency keeps the estimate steady when load changes: right
  after a scaling event, when the runner rebalances work and a worker is
  briefly starved, its raw completion rate collapses, but ``mu`` and
  ``running_ref`` hold, so the estimate does not crater and trigger a downscale
  spiral. This mode suits servers whose requests take long enough to be seen
  in flight by periodic sampling, such as LLM servers with latencies of a
  second or more; for millisecond latencies the gauge almost always reads 0.

  The other modes use the raw completion rate and ignore concurrency, so
  they also work for millisecond-latency servers:

  * ``'window_mean'``: completions per second over the trailing window. Drops
    when a worker is starved.
  * ``'window_max'``: the highest single-interval completion rate in the
    trailing window.
  * ``'lifetime_max'``: the highest single-interval completion rate seen since
    the estimator was created. Never decays.

  Single-interval rates are noisy when requests take seconds, since a few
  completions can land in one interval; the max modes latch onto such bursts.

  In every mode, intervals with no completions and nothing in flight are
  skipped, so idle periods don't drag the estimate down. While requests are in
  flight but none has completed within the window, there is no estimate either
  (except in ``'lifetime_max'`` mode once a peak has been seen).

  Units: every mode is in requests per second. The runner reads the published
  value as how many requests one worker can handle, so each worker is sized
  for about one second of its sustained throughput; ``multiplier`` scales
  that, e.g. 2.0 sizes each worker for two seconds' worth of requests.

  Args:
    multiplier: Dimensionless factor on the estimate; larger means fewer
      workers.
    min_value: Floor of the estimate.
    window_secs: Trailing window; 0 keeps every interval. ``'lifetime_max'``
      uses it only to count completions for ``min_requests``.
    running_percentile: Percentile of in-flight concurrency used as
      ``running_ref``; ``'concurrency'`` mode only.
    min_requests: Completions a newly started worker needs before it reports,
      unless ``warmup_timeout_secs`` passes first.
    warmup_timeout_secs: Longest time after a worker first sees work that it
      waits for ``min_requests``.
    aggregation: ``'concurrency'``, ``'window_mean'``, ``'window_max'`` or
      ``'lifetime_max'``.

  Raises:
    ValueError: if ``aggregation`` is not one of the above.
  """
  def __init__(
      self,
      multiplier: float = DEFAULT_MULTIPLIER,
      min_value: int = 1,
      window_secs: float = DEFAULT_SIGNAL_WINDOW_SECS,
      running_percentile: float = DEFAULT_RUNNING_PERCENTILE,
      min_requests: int = DEFAULT_MIN_REQUESTS,
      warmup_timeout_secs: float = DEFAULT_WARMUP_TIMEOUT_SECS,
      aggregation: str = CONCURRENCY,
  ):
    if aggregation not in _AGGREGATIONS:
      raise ValueError(
          f'Unknown aggregation {aggregation!r}; expected one of '
          f'{", ".join(_AGGREGATIONS)}.')
    self._aggregation = aggregation
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
    self._peak_rate = 0.0  # Highest single-interval rate, for lifetime_max.

  def update(
      self,
      completed_total: float,
      running: float,
      now: Optional[float] = None) -> Optional[float]:
    """Records one sample and returns the current estimate.

    Args:
      completed_total: Current value of the completed-requests counter.
      running: Current number of in-flight requests.
      now: Sample timestamp in seconds; defaults to ``time.monotonic()``.

    Returns:
      The capacity estimate, or ``None`` when there is not enough data (first
      sample, counter reset, idle window or warmup); the caller should then
      keep its previous value.
    """
    if now is None:
      now = time.monotonic()
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
      # Skip fully idle intervals so idle periods don't drag the estimate down.
      return None
    self._samples.append(
        _Interval(now, float(completed), float(running), float(secs)))
    self._peak_rate = max(self._peak_rate, completed / secs)

    total_completed = sum(s.completed for s in self._samples)
    slot_secs = sum(s.running * s.secs for s in self._samples)
    if self._aggregation == CONCURRENCY and slot_secs <= 0:
      return None  # Nothing was in flight during the window.
    if self._first_active_ts is None:
      self._first_active_ts = prev_ts
    if total_completed <= 0 and not (self._aggregation == LIFETIME_MAX and
                                     self._peak_rate > 0):
      # Requests are in flight but none has completed in the window yet, e.g.
      # slow first requests or the first poll after an idle period. That says
      # nothing about capacity, so keep the previous value instead of
      # reporting the floor.
      return None
    if (total_completed < self._min_requests and
        now - self._first_active_ts < self._warmup_timeout_secs):
      # Wait until a new worker is calibrated, so it doesn't briefly report
      # the floor value for the whole job.
      return None

    if self._aggregation == WINDOW_MEAN:
      value = total_completed / sum(s.secs for s in self._samples)
    elif self._aggregation == WINDOW_MAX:
      value = max(s.completed / s.secs for s in self._samples)
    elif self._aggregation == LIFETIME_MAX:
      value = self._peak_rate
    else:
      mu = total_completed / slot_secs
      running_ref = _nearest_rank_percentile(
          [s.running for s in self._samples if s.running > 0],
          self._running_percentile)
      value = mu * running_ref
    return max(float(self._min_value), value * self._multiplier)


@dataclass(frozen=True)
class AutoscalingMetricConfig:
  """User-facing knobs for the capacity-based autoscaling signal.

  Model handlers that publish the signal take an instance, typically as an
  ``autoscaling_config`` argument; leave it unset for the defaults.

  Attributes:
    poll_interval_secs: How often to read the server and publish.
    multiplier: Factor on the capacity estimate; larger means fewer workers,
      smaller scales out more aggressively.
    signal_window_secs: Trailing window of the capacity estimate.
    min_requests: Completions a newly started worker needs before it
      publishes, unless ``warmup_timeout_secs`` passes first.
    warmup_timeout_secs: Longest time a new worker waits for ``min_requests``.
    aggregation: How the estimate, in requests per second, is computed:

      * ``'concurrency'`` (default): per-slot throughput over the window times
        the p90 in-flight concurrency. Holds steady when a worker is briefly
        starved; suits servers with request latencies of a second or more.
      * ``'window_mean'``: completions per second over the window.
      * ``'window_max'``: the highest single-interval completion rate in the
        window.
      * ``'lifetime_max'``: the highest single-interval completion rate seen
        so far; never decays.
    load_signal_fn: Optional replacement for the built-in estimate: given the
      server's metrics port, returns the capacity directly.
    tag: Optional metric tag override. Leave unset so the RunInference model
      identifier is used, which is what the runner looks up and which stays
      stable across model updates. Only set it if you pass the same value as
      ``RunInference(model_identifier=...)``.
  """
  poll_interval_secs: float = DEFAULT_POLL_INTERVAL_SECS
  multiplier: float = DEFAULT_MULTIPLIER
  signal_window_secs: float = DEFAULT_SIGNAL_WINDOW_SECS
  min_requests: int = DEFAULT_MIN_REQUESTS
  warmup_timeout_secs: float = DEFAULT_WARMUP_TIMEOUT_SECS
  aggregation: str = CONCURRENCY
  load_signal_fn: Optional[Callable[[int], Optional[float]]] = None
  tag: Optional[str] = None


class PrometheusNumLoadedModelsPublisher:
  """Publishes a Prometheus model server's capacity for autoscaling.

  The capacity is published as ``num_loaded_models_<tag>``.

  The server must meet the requirements in the module docstring: a Prometheus
  text-format ``/metrics`` endpoint on ``localhost`` exporting a
  completed-requests counter and an in-flight gauge.

  Every ``poll_interval_secs`` a daemon thread reads that endpoint, feeds the
  two metrics to a
  capacity estimator, and writes the estimate, rounded
  to an int, to the process-wide distribution the runner reads. If
  ``config.load_signal_fn`` is set, it replaces the scrape and the estimate.

  A poll is skipped, leaving the last published value in place, while the
  server is down, when the scrape fails, or while the estimator has no
  estimate yet. The publisher never injects a 0 of its own, on start, on stop
  or on a skipped poll: a 0 tells the runner the worker has no capacity and can
  trigger a spurious downscale. Only one publisher per metric name is active in
  a process; starting a new one (e.g. after a model reload) stops the previous
  one.

  The metric tag is ``config.tag`` if set; else ``model_identifier``, used
  verbatim because it must equal the identifier RunInference annotates the
  step with (see ``ModelHandler.set_model_identifier``); else ``model_name``
  with characters outside ``[A-Za-z0-9_]`` replaced by ``_``, for handlers
  used outside RunInference.

  Args:
    metrics_port_fn: Returns the port serving ``/metrics`` while the server is
      up, or ``None`` while it is not.
    completed_metric: Name of the server's counter of completed requests, e.g.
      ``vllm:request_success_total``.
    running_metric: Name of the server's gauge of in-flight requests, e.g.
      ``vllm:num_requests_running``.
    model_identifier: The identifier RunInference reported through
      ``ModelHandler.set_model_identifier``, or ``None`` outside RunInference.
    model_name: Fallback for the metric tag outside RunInference.
    config: Knobs; ``None`` uses the defaults.
    label_filter: Optional labels a sample must have to be counted, e.g. to
      select one model on a multi-model server.

  Raises:
    ValueError: if no metric tag can be derived.
  """
  # The publisher owning each metric name in this process, so a reloaded
  # model's publisher can supersede the stale one.
  _active: dict[str, 'PrometheusNumLoadedModelsPublisher'] = {}
  _active_lock = threading.Lock()

  def __init__(
      self,
      *,
      metrics_port_fn: Callable[[], Optional[int]],
      completed_metric: str,
      running_metric: str,
      model_identifier: Optional[str],
      model_name: str = '',
      config: Optional[AutoscalingMetricConfig] = None,
      label_filter: Optional[Mapping[str, str]] = None,
  ):
    config = config or AutoscalingMetricConfig()
    tag = config.tag or model_identifier or _sanitize_metric_tag(model_name)
    if not tag:
      raise ValueError(
          'Cannot derive a num_loaded_models tag: pass a model_identifier, a '
          'model_name, or set AutoscalingMetricConfig.tag.')
    self._metric_name = NUM_LOADED_MODELS_METRIC_PREFIX + tag
    self._metrics_port_fn = metrics_port_fn
    self._completed_metric = completed_metric
    self._running_metric = running_metric
    self._label_filter = dict(label_filter) if label_filter else None
    self._load_signal_fn = config.load_signal_fn
    self._poll_interval_secs = config.poll_interval_secs
    self._estimator = _CompletionRateCapacityEstimator(
        multiplier=config.multiplier,
        window_secs=config.signal_window_secs,
        min_requests=config.min_requests,
        warmup_timeout_secs=config.warmup_timeout_secs,
        aggregation=config.aggregation)
    self._stop_event = threading.Event()
    self._thread: Optional[threading.Thread] = None

  def start(self) -> None:
    """Starts the background polling thread.

    The metric stays absent until the first sample is published. Does nothing
    if already started or once stopped, so a superseded publisher can't take
    its metric back from the newer one; create a new publisher instead.
    """
    if self._thread is not None or self._stop_event.is_set():
      return
    with PrometheusNumLoadedModelsPublisher._active_lock:
      previous = PrometheusNumLoadedModelsPublisher._active.get(
          self._metric_name)
      PrometheusNumLoadedModelsPublisher._active[self._metric_name] = self
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
    with PrometheusNumLoadedModelsPublisher._active_lock:
      if PrometheusNumLoadedModelsPublisher._active.get(
          self._metric_name) is self:
        del PrometheusNumLoadedModelsPublisher._active[self._metric_name]
    thread = self._thread
    if thread is not None:
      thread.join(timeout=self._poll_interval_secs + 5)
      self._thread = None

  def _run(self) -> None:
    while not self._stop_event.is_set():
      try:
        capacity = self._read_capacity()
        if capacity is not None:
          self._publish(capacity)
      except Exception:  # pylint: disable=broad-except
        logging.exception(
            'Failed to publish autoscaling metric %s; will retry.',
            self._metric_name)
      self._stop_event.wait(self._poll_interval_secs)

  def _read_capacity(self) -> Optional[float]:
    """Reads the server once; returns its capacity, or None to skip."""
    port = self._metrics_port_fn()
    if port is None:
      return None  # The server is not up.
    if self._load_signal_fn is not None:
      return self._load_signal_fn(port)
    text = _scrape_prometheus_metrics(port)
    if text is None:
      return None
    completed = _parse_prometheus_metric(
        text, self._completed_metric, self._label_filter)
    running = _parse_prometheus_metric(
        text, self._running_metric, self._label_filter)
    if completed is None or running is None:
      return None
    return self._estimator.update(completed, running)

  def _publish(self, value: float) -> None:
    published = int(round(value))
    # The SDK harness reports process-wide distributions without resetting
    # them, and Dataflow reads their cumulative mean (sum / count). Keeping the
    # cell at (sum=published, count=1) makes the runner see the latest value
    # rather than an average since worker start. The harness reads the cell
    # from another thread, so replace the data in one step with reset_to():
    # reset() then update() would briefly expose an empty cell (count=0).
    cell = MetricsEnvironment.process_wide_container().get_distribution(
        MetricName(MODEL_MANAGER_METRICS_NAMESPACE, self._metric_name))
    current = cell.get_cumulative()
    if current.count != 1 or current.sum != published:
      cell.reset_to(published)
