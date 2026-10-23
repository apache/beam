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

"""Publishes the ``num_loaded_models_<tag>`` autoscaling signal for a model
server running alongside a RunInference worker.

Dataflow's horizontal autoscaler scales GPU/model pools on the process-wide
metric ``num_loaded_models_<tag>`` in the ``BeamML_ModelManager`` namespace
(see ``model_manager.py`` and
``//dist_proc/windmill/autoscaling/horizontal_scaling_policy.cc``), reading it
as *the number of messages each worker can hold*. A model server such as vLLM
or Triton always has exactly one loaded copy per worker, so instead the handler
publishes that metric with a per-worker capacity estimate derived from the
server's own Prometheus ``/metrics`` endpoint.

Building blocks, from the bottom up:

* :func:`scrape_prometheus_metrics` / :func:`parse_prometheus_metric` -- fetch
  and read Prometheus text exposition.
* :class:`CompletionRateSignal` -- estimates per-worker capacity as completion
  rate x in-flight concurrency over a trailing window (already smoothed).
* :class:`NumLoadedModelsPublisher` -- polls a signal on a background thread and
  publishes it, optionally applying a policy such as ``'lifetime_max'`` on top.
* :class:`AutoscalingMetricConfig` -- the user-facing knobs; a handler takes
  one as ``autoscaling_config`` and hands it to whatever owns the server
  process.

Nothing here is specific to a particular model server; the server-specific
piece is just the pair of metric names passed to :class:`CompletionRateSignal`.
"""

# pytype: skip-file

import dataclasses
import logging
import threading
import time
import urllib.request
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Optional

from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.metrics.metricbase import MetricName

__all__ = [
    'AutoscalingMetricConfig',
    'CompletionRateSignal',
    'NumLoadedModelsPublisher',
    'parse_prometheus_metric',
    'sanitize_metric_tag',
    'scrape_prometheus_metrics',
]

MODEL_MANAGER_METRICS_NAMESPACE = 'BeamML_ModelManager'
NUM_LOADED_MODELS_METRIC_PREFIX = 'num_loaded_models_'

DEFAULT_POLL_INTERVAL_SECS = 5.0
# Dimensionless multiplier applied to the capacity estimate before it is
# published. Windmill sizes the pool as ``ceil(backlog / reported)``, so a
# larger value lets each worker hold more backlog (fewer workers) and a smaller
# one scales out more aggressively.
DEFAULT_MULTIPLIER = 1.0
# Trailing window and concurrency percentile used by CompletionRateSignal.
DEFAULT_SIGNAL_WINDOW_SECS = 1800.0  # 30 minutes.
DEFAULT_RUNNING_PERCENTILE = 90.0
DEFAULT_MIN_REQUESTS = 3
DEFAULT_WARMUP_TIMEOUT_SECS = 10.0
# CompletionRateSignal is already smoothed, so the publisher forwards its latest
# value unchanged by default (no second smoothing layer). 'lifetime_max' etc.
# remain available as an explicit policy on top.
DEFAULT_AGGREGATION = 'last'
DEFAULT_AGGREGATION_WINDOW_SECS = 1800.0  # 30 minutes.


def sanitize_metric_tag(name: str) -> str:
  """Turns a model name into a metric-tag-safe token for the autoscaler.

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
  """Fetches ``http://localhost:<port><path>``; None on any failure."""
  url = f'http://localhost:{port}{path}'
  try:
    with urllib.request.urlopen(url, timeout=timeout_secs) as response:
      return response.read().decode('utf-8', errors='replace')
  except Exception:  # pylint: disable=broad-except
    return None


def _parse_prometheus_labels(label_str: str) -> dict[str, str]:
  """Parses the ``{k="v",...}`` label section of a Prometheus sample line."""
  labels: dict[str, str] = {}
  i = 0
  n = len(label_str)
  while i < n:
    eq = label_str.find('=', i)
    if eq == -1:
      break
    name = label_str[i:eq].strip()
    j = eq + 1
    if j < n and label_str[j] == '"':
      j += 1
      value_chars = []
      while j < n and label_str[j] != '"':
        if label_str[j] == '\\' and j + 1 < n:
          j += 1
        value_chars.append(label_str[j])
        j += 1
      value = ''.join(value_chars)
      j += 1  # skip closing quote
    else:
      comma = label_str.find(',', j)
      comma = n if comma == -1 else comma
      value = label_str[j:comma].strip()
      j = comma
    labels[name] = value
    comma = label_str.find(',', j)
    if comma == -1:
      break
    i = comma + 1
  return labels


def parse_prometheus_metric(
    metrics_text: str,
    metric_name: str,
    label_filter: Optional[Mapping[str, str]] = None,
) -> Optional[float]:
  """Sums the values of a metric from Prometheus text-format exposition.

  Args:
    metrics_text: The full body returned by the server's ``/metrics`` endpoint.
    metric_name: The metric family name to match (e.g.
      ``vllm:num_requests_running``).
    label_filter: If provided, only samples whose labels contain all of these
      key/value pairs are summed.

  Returns:
    The sum of the matching sample values, or ``None`` if the metric is absent.
  """
  total: Optional[float] = None
  for raw_line in metrics_text.splitlines():
    line = raw_line.strip()
    if not line or line.startswith('#'):
      continue
    if not line.startswith(metric_name):
      continue
    remainder = line[len(metric_name):]
    if remainder and remainder[0] not in (' ', '\t', '{'):
      continue
    labels: Mapping[str, str] = {}
    if remainder.startswith('{'):
      close = remainder.find('}')
      if close == -1:
        continue
      labels = _parse_prometheus_labels(remainder[1:close])
      remainder = remainder[close + 1:]
    parts = remainder.split()
    if not parts:
      continue
    try:
      value = float(parts[0])
    except ValueError:
      continue
    if label_filter and any(labels.get(k) != v
                            for k, v in label_filter.items()):
      continue
    total = value if total is None else total + value
  return total


def _nearest_rank_percentile(values: list[float], q: float) -> float:
  """Nearest-rank percentile (``q`` in [0, 100]) of a non-empty list."""
  ordered = sorted(values)
  q = min(max(q, 0.0), 100.0)
  rank = int((q * len(ordered) + 99) // 100)  # ceil(q / 100 * n).
  idx = min(max(rank - 1, 0), len(ordered) - 1)
  return ordered[idx]


class NumLoadedModelsPublisher:
  """Publishes the ``num_loaded_models_<tag>`` autoscaling metric on a timer.

  ``signal_fn`` produces the raw load sample (e.g. requests running in
  parallel, or an estimated per-worker capacity); it is polled on a background
  daemon thread every ``poll_interval_secs``. Returning ``None`` (e.g. a
  transient scrape failure) skips that tick, leaving the running state and last
  published value unchanged.

  The published value is an aggregate of the observed samples, chosen so the
  pool does not scale *down* the instant an input burst clears (which would
  thrash and then be unable to absorb the next burst):

    * ``'lifetime_max'`` -- the maximum sample observed over the whole
                     pipeline lifetime (monotonic; never decays). Sizes the
                     fleet to the worst load ever seen and holds it.
    * ``'max'``   -- peak sample in the trailing ``aggregation_window_secs``
                     window; decays that long after load drops.
    * ``'p<NN>'`` -- a windowed percentile, e.g. ``'p90'``. Smoother than max:
                     ignores brief spikes and decays as low samples fill it.
    * ``'mean'``  -- the average over the window.
    * ``'last'``  -- the raw last sample (no smoothing). Use this when
                     ``signal_fn`` already smooths its output.
  """
  # The publisher currently owning each metric name in this process. A model
  # update or server restart creates a new server (and publisher) for the same
  # identifier; starting it supersedes the stale one so exactly one thread
  # publishes per metric name.
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
    self._aggregation = (aggregation or DEFAULT_AGGREGATION).lower()
    self._aggregation_window_secs = max(float(aggregation_window_secs), 0.0)
    self._multiplier = float(multiplier)
    # Trailing (timestamp, value) samples for the windowed aggregation modes.
    self._samples: list[tuple[float, float]] = []
    # Monotonic peak for the 'lifetime_max' mode (no sample retention).
    self._lifetime_max: Optional[float] = None
    self._stop_event = threading.Event()
    self._thread: Optional[threading.Thread] = None

  def _publish(self, value: float) -> None:
    ivalue = int(round(value * self._multiplier))
    # Process-wide DistributionCells are read without resetting by the Python
    # SDK harness (_request_harness_monitoring_infos), and Dataflow's worker
    # harness reports their cumulative mean (sum / count). Resetting before
    # updating keeps (sum=ivalue, count=1) so the latest capacity is reported
    # instead of an unweighted lifetime average since worker boot.
    cell = MetricsEnvironment.process_wide_container().get_distribution(
        MetricName(MODEL_MANAGER_METRICS_NAMESPACE, self._metric_name))
    cumulative = cell.get_cumulative()
    if cumulative.count != 1 or cumulative.sum != ivalue:
      cell.reset()
      cell.update(ivalue)

  def _aggregate(self, now: float) -> Optional[float]:
    """Aggregates in-window samples per ``self._aggregation``.

    Evicts samples older than ``aggregation_window_secs`` first, then reduces
    the remainder.

    Args:
      now: Current timestamp used for window eviction.

    Returns:
      The aggregated value, or ``None`` if the window is empty.
    """
    if self._aggregation_window_secs > 0:
      cutoff = now - self._aggregation_window_secs
      self._samples = [(t, v) for (t, v) in self._samples if t >= cutoff]
    values = [v for (_, v) in self._samples]
    if not values:
      return None
    mode = self._aggregation
    if mode == 'last':
      return values[-1]
    if mode == 'mean':
      return sum(values) / len(values)
    if mode.startswith('p'):
      try:
        q = float(int(mode[1:]))
      except ValueError:
        return max(values)
      return _nearest_rank_percentile(values, q)
    return max(values)  # 'max' and unknown modes.

  def _run(self) -> None:
    while not self._stop_event.is_set():
      try:
        value = self._signal_fn()
        if value is not None:
          if self._aggregation == 'lifetime_max':
            # Monotonic running max over the whole pipeline lifetime; no
            # per-sample retention and never decays.
            self._lifetime_max = (
                float(value) if self._lifetime_max is None else max(
                    self._lifetime_max, float(value)))
            aggregate = self._lifetime_max
          else:
            now = time.time()
            self._samples.append((now, float(value)))
            aggregate = self._aggregate(now)
          if aggregate is not None:
            self._publish(aggregate)
      except Exception:  # pylint: disable=broad-except
        logging.exception(
            'Failed to publish autoscaling metric %s; will retry.',
            self._metric_name,
        )
      self._stop_event.wait(self._poll_interval_secs)

  def start(self) -> None:
    """Starts the background polling thread.

    Deliberately publishes no initial 0: reporting 0 would tell the autoscaler
    this replica has no capacity and could trigger a spurious downscale before
    the first real sample lands. The metric stays absent until the first
    aggregate is published.
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

    Deliberately publishes no final 0. When a worker is drained during a
    downscale, reporting 0 shrinks the aggregate capacity and can drive a
    feedback loop of further downscales that tears down healthy replicas. The
    drained worker's contribution ages out via metric staleness instead.
    """
    self._stop_event.set()
    with NumLoadedModelsPublisher._active_lock:
      if NumLoadedModelsPublisher._active.get(self._metric_name) is self:
        del NumLoadedModelsPublisher._active[self._metric_name]
    thread = self._thread
    if thread is not None:
      thread.join(timeout=self._poll_interval_secs + 5)
      self._thread = None


class CompletionRateSignal:
  """Stateful ``num_loaded_models`` signal: per-slot throughput x concurrency.

  Works for any model server that exposes a Prometheus ``/metrics`` endpoint
  with a monotonically increasing *completed requests* counter
  (``completed_metric``) and an *in-flight requests* gauge
  (``running_metric``), e.g. vLLM's ``vllm:request_success_total`` /
  ``vllm:num_requests_running``. Each call scrapes both from
  ``http://localhost:<port>/metrics``. Over a trailing ``window_secs`` window it
  derives:

    * ``mu`` -- per-slot throughput,
      ``sum(delta_completed) / sum(running * dt)``
      (a ratio of windowed sums, so discrete completions don't make it bursty).
      This approximates ``1 / avg_latency`` and is roughly independent of the
      offered load.
    * ``running_ref`` -- the ``running_percentile`` (default p90) of observed
      in-flight concurrency. p90 rather than max avoids latching onto rare,
      heavily backlogged spikes that overstate normal-load capacity.

  and reports a per-worker message capacity::

      reported = mu * running_ref * multiplier

  i.e. the worker's sustained throughput scaled by a dimensionless tuning
  ``multiplier``. Windmill's GPU-intensive autoscaler sizes the pool as
  ``desired = ceil(backlog / reported)`` (see
  //dist_proc/windmill/autoscaling/horizontal_scaling_policy.cc). Normalizing by
  ``running`` makes the estimate load-independent: right after an autoscaling
  event, when Windmill rebalances keys and the worker is briefly starved, the
  raw completion rate collapses but ``mu`` holds and ``running_ref`` still
  reflects the concurrency the worker normally serves -- so ``reported`` stays
  steady instead of cratering to the floor and triggering a downscale spiral.

  The output is already windowed; feed it to ``NumLoadedModelsPublisher`` with
  ``aggregation='last'`` unless you deliberately want a second policy layer
  (e.g. ``'lifetime_max'`` to hold the peak capacity estimate).
  """
  def __init__(
      self,
      completed_metric: str,
      running_metric: str,
      label_filter: Optional[Mapping[str, str]] = None,
      multiplier: float = DEFAULT_MULTIPLIER,
      min_value: int = 1,
      window_secs: float = DEFAULT_SIGNAL_WINDOW_SECS,
      running_percentile: float = DEFAULT_RUNNING_PERCENTILE,
      min_requests: int = DEFAULT_MIN_REQUESTS,
      warmup_timeout_secs: float = DEFAULT_WARMUP_TIMEOUT_SECS,
  ):
    self._completed_metric = completed_metric
    self._running_metric = running_metric
    self._label_filter = dict(label_filter) if label_filter else None
    self._multiplier = max(float(multiplier), 0.0)
    self._min_value = int(min_value)
    self._window_secs = max(float(window_secs), 0.0)
    self._running_percentile = min(max(float(running_percentile), 0.0), 100.0)
    self._min_requests = max(int(min_requests), 0)
    self._warmup_timeout_secs = max(float(warmup_timeout_secs), 0.0)
    self._last_count: Optional[float] = None
    self._last_ts: Optional[float] = None
    self._first_active_ts: Optional[float] = None
    # Trailing (timestamp, delta_completed, running, dt) samples.
    self._samples: list[tuple[float, float, float, float]] = []

  def __call__(self, port: int, timeout_secs: float = 5.0) -> Optional[float]:
    text = scrape_prometheus_metrics(port, timeout_secs=timeout_secs)
    if text is None:
      return None
    completed = parse_prometheus_metric(
        text, self._completed_metric, label_filter=self._label_filter)
    running = parse_prometheus_metric(
        text, self._running_metric, label_filter=self._label_filter)
    if completed is None or running is None:
      return None
    now = time.time()
    prev_count, prev_ts = self._last_count, self._last_ts
    self._last_count, self._last_ts = completed, now
    if prev_count is None or prev_ts is None:
      # First sample only establishes a baseline; a rate needs two samples.
      return None
    dt = now - prev_ts
    delta = completed - prev_count
    if dt <= 0 or delta < 0:
      # Clock skew or counter reset (server restart); skip this tick.
      return None
    if running < 0:
      return None
    # Evict samples older than the window, and skip completely idle intervals so
    # trough/idle periods do not dilute running_ref (p90 concurrency) to 0.
    if self._window_secs > 0:
      cutoff = now - self._window_secs
      self._samples = [s for s in self._samples if s[0] >= cutoff]
    if delta == 0 and running == 0:
      return None
    self._samples.append((now, float(delta), float(running), float(dt)))
    # Per-slot throughput as a ratio of windowed sums (stable vs. per-tick):
    # total completions divided by total slot-seconds of in-flight work.
    total_completed = sum(d for (_, d, _, _) in self._samples)
    total_slot_secs = sum(r * dts for (_, _, r, dts) in self._samples)
    if total_slot_secs <= 0:
      # Window is entirely idle (no in-flight requests); nothing to report.
      return None
    if self._first_active_ts is None:
      self._first_active_ts = prev_ts
    if (total_completed < self._min_requests and
        (now - self._first_active_ts) < self._warmup_timeout_secs):
      # Hold off publishing until a newly started worker completes at least
      # min_requests or warmup_timeout_secs elapses, so an uncalibrated cold
      # worker does not briefly overwrite the job-wide metric with 1.
      return None
    mu = total_completed / total_slot_secs
    running_ref = _nearest_rank_percentile(
        [r for (_, _, r, _) in self._samples if r > 0],
        self._running_percentile)
    reported = mu * running_ref * self._multiplier
    return max(float(self._min_value), reported)


@dataclass(frozen=True)
class AutoscalingMetricConfig:
  """User-facing knobs for the ``num_loaded_models_<tag>`` autoscaling signal.

  Pass an instance to a model handler (e.g.
  ``VLLMCompletionsModelHandler(..., autoscaling_config=...)``) to override the
  defaults; leave it unset for the defaults. Each field:

  * ``tag`` -- metric tag to publish under. Leave unset (the default) so the
    handler fills it in from the identifier RunInference reports for the step
    (``RunInference(model_identifier=...)``, or the random id it generates),
    which is what Dataflow's autoscaler looks up and which stays stable across
    model updates. Only set it explicitly if you also pass the same value as
    ``model_identifier``.
  * ``poll_interval_secs`` -- how often to sample and publish (default 5).
  * ``multiplier`` -- dimensionless factor applied to the capacity estimate
    before publishing; larger values mean fewer workers, smaller values scale
    out more aggressively (default 1.0).
  * ``signal_window_secs`` -- trailing window over which
    :class:`CompletionRateSignal` estimates throughput and p90 concurrency
    (default 1800). Ignored when ``load_signal_fn`` is set.
  * ``min_requests`` -- minimum completed requests before a newly started
    worker begins publishing, unless ``warmup_timeout_secs`` elapses first
    (default 3).
  * ``warmup_timeout_secs`` -- maximum time (in seconds) since a worker first
    sees active requests to wait for ``min_requests`` before publishing anyway
    (default 10).
  * ``aggregation`` -- optional policy the publisher applies on top of the
    (already smoothed) signal: ``'last'`` (default; forward the latest
    estimate), ``'lifetime_max'`` (hold the peak estimate for the whole run,
    never decays), or ``'max'``/``'p90'``/``'p95'``/``'mean'`` over
    ``aggregation_window_secs``. Only change this from ``'last'`` when you
    deliberately want a second smoothing layer, e.g. to stop the pool scaling
    down the instant a burst clears.
  * ``aggregation_window_secs`` -- trailing window for the windowed
    ``aggregation`` modes; unused by ``'last'`` and ``'lifetime_max'``
    (default 1800).
  * ``load_signal_fn`` -- optional override that, given the server's metrics
    port, returns the per-worker capacity value directly, bypassing the
    built-in completion-rate signal.

  Whatever owns the server process calls :meth:`make_completion_rate_signal` /
  :meth:`make_publisher` on the resolved config.
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

  def resolve(self, default_tag: str) -> 'AutoscalingMetricConfig':
    """Returns a copy whose ``tag`` is set, falling back to ``default_tag``.

    ``default_tag`` is used verbatim: when it is the identifier RunInference
    reports for the step it must match that annotation exactly.
    """
    if self.tag:
      return self
    if not default_tag:
      raise ValueError('default_tag must be a non-empty string.')
    return dataclasses.replace(self, tag=default_tag)

  def make_completion_rate_signal(
      self,
      completed_metric: str,
      running_metric: str,
      label_filter: Optional[Mapping[str, str]] = None,
  ) -> CompletionRateSignal:
    return CompletionRateSignal(
        completed_metric=completed_metric,
        running_metric=running_metric,
        label_filter=label_filter,
        multiplier=self.multiplier,
        window_secs=self.signal_window_secs,
        min_requests=self.min_requests,
        warmup_timeout_secs=self.warmup_timeout_secs)

  def make_publisher(
      self, signal_fn: Callable[[],
                                Optional[float]]) -> NumLoadedModelsPublisher:
    if not self.tag:
      raise ValueError(
          'AutoscalingMetricConfig.tag is unset; call resolve(default_tag) '
          'first.')
    return NumLoadedModelsPublisher(
        tag=self.tag,
        signal_fn=signal_fn,
        poll_interval_secs=self.poll_interval_secs,
        aggregation=self.aggregation,
        aggregation_window_secs=self.aggregation_window_secs,
    )
