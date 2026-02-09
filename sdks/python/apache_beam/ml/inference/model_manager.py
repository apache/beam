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

"""Module for managing ML models in Apache Beam pipelines.

This module provides classes and functions to efficiently manage multiple
machine learning models within Apache Beam pipelines. It includes functionality
for loading, caching, and updating models using multi-process shared memory,
ensuring that models are reused across different workers to optimize resource
usage and performance.
"""

import gc
import heapq
import itertools
import logging
import subprocess
import threading
import time
from collections import Counter
from collections import OrderedDict
from collections import defaultdict
from collections import deque
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Tuple

import numpy as np
import torch
from scipy.optimize import nnls

from apache_beam.utils import multi_process_shared

logger = logging.getLogger(__name__)


class GPUMonitor:
  """Monitors GPU memory usage in a separate thread using nvidia-smi.

  This class continuously polls GPU memory statistics to track current usage
  and peak usage over a sliding time window. It serves as the source of truth
  for the ModelManager's resource decisions.

  Attributes:
    fallback_memory_mb: Default total memory if hardware detection fails.
    poll_interval: Seconds between memory checks.
    peak_window_seconds: Duration to track peak memory usage.
  """
  def __init__(
      self,
      fallback_memory_mb: float = 16000.0,
      poll_interval: float = 0.5,
      peak_window_seconds: float = 30.0):
    self._current_usage = 0.0
    self._peak_usage = 0.0
    self._total_memory = fallback_memory_mb
    self._poll_interval = poll_interval
    self._peak_window_seconds = peak_window_seconds
    self._memory_history = deque()
    self._running = False
    self._thread = None
    self._lock = threading.Lock()

  def _detect_hardware(self):
    try:
      cmd = [
          "nvidia-smi",
          "--query-gpu=memory.total",
          "--format=csv,noheader,nounits"
      ]
      output = subprocess.check_output(cmd, text=True).strip()
      self._total_memory = float(output)
      return True
    except (FileNotFoundError, subprocess.CalledProcessError):
      logger.warning(
          "nvidia-smi not found or failed. Defaulting total memory to %s MB",
          self._total_memory)
      return False
    except Exception as e:
      logger.warning(
          "Error parsing nvidia-smi output: %s. "
          "Defaulting total memory to %s MB",
          e,
          self._total_memory)
      return False

  def start(self):
    self._gpu_available = self._detect_hardware()
    if self._running or not self._gpu_available:
      return
    self._running = True
    self._thread = threading.Thread(target=self._poll_loop, daemon=True)
    self._thread.start()

  def stop(self):
    self._running = False
    if self._thread:
      self._thread.join()

  def reset_peak(self):
    with self._lock:
      now = time.time()
      self._memory_history.clear()
      self._memory_history.append((now, self._current_usage))
      self._peak_usage = self._current_usage

  def get_stats(self) -> Tuple[float, float, float]:
    with self._lock:
      return self._current_usage, self._peak_usage, self._total_memory

  def refresh(self):
    """Forces an immediate poll of the GPU."""
    usage = self._get_nvidia_smi_used()
    now = time.time()
    with self._lock:
      self._current_usage = usage
      self._memory_history.append((now, usage))
      # Recalculate peak immediately
      while self._memory_history and (now - self._memory_history[0][0]
                                      > self._peak_window_seconds):
        self._memory_history.popleft()
      self._peak_usage = (
          max(m for _, m in self._memory_history)
          if self._memory_history else usage)

  def _get_nvidia_smi_used(self) -> float:
    try:
      cmd = [
          "nvidia-smi",
          "--query-gpu=memory.free",
          "--format=csv,noheader,nounits"
      ]
      output = subprocess.check_output(cmd, text=True).strip()
      free_memory = float(output)
      return self._total_memory - free_memory
    except Exception as e:
      logger.warning('Failed to get GPU memory usage: %s', e)
      return 0.0

  def _poll_loop(self):
    while self._running:
      usage = self._get_nvidia_smi_used()
      now = time.time()
      with self._lock:
        self._current_usage = usage
        self._memory_history.append((now, usage))
        while self._memory_history and (now - self._memory_history[0][0]
                                        > self._peak_window_seconds):
          self._memory_history.popleft()
        self._peak_usage = (
            max(m for _, m in self._memory_history)
            if self._memory_history else usage)
      time.sleep(self._poll_interval)


class ResourceEstimator:
  """Estimates individual model memory usage using statistical observation.

  Uses Non-Negative Least Squares (NNLS) to deduce the memory footprint of
  individual models based on aggregate system memory readings and the
  configuration of active models at that time.
  """
  def __init__(self, smoothing_factor: float = 0.2, min_data_points: int = 5):
    self.smoothing_factor = smoothing_factor
    self.min_data_points = min_data_points
    self.estimates: Dict[str, float] = {}
    self.history = defaultdict(lambda: deque(maxlen=20))
    self.known_models = set()
    self._lock = threading.Lock()

  def is_unknown(self, model_tag: str) -> bool:
    with self._lock:
      return model_tag not in self.estimates

  def get_estimate(self, model_tag: str, default_mb: float = 4000.0) -> float:
    with self._lock:
      return self.estimates.get(model_tag, default_mb)

  def set_initial_estimate(self, model_tag: str, cost: float):
    with self._lock:
      self.estimates[model_tag] = cost
      self.known_models.add(model_tag)
      logger.info("Initial Profile for %s: %s MB", model_tag, cost)

  def add_observation(
      self, active_snapshot: Dict[str, int], peak_memory: float):
    if active_snapshot:
      model_list = "\n".join(
          f"\t- {model}: {count}"
          for model, count in sorted(active_snapshot.items()))
    else:
      model_list = "\t- None"

    logger.info(
        "Adding Observation:\n PeakMemory: %.1f MB\n  Instances:\n%s",
        peak_memory,
        model_list)
    if not active_snapshot:
      return
    with self._lock:
      config_key = tuple(sorted(active_snapshot.items()))
      self.history[config_key].append(peak_memory)
      for tag in active_snapshot:
        self.known_models.add(tag)
      self._solve()

  def _solve(self):
    """
    Solves Ax=b using raw readings (no pre-averaging) and NNLS.
    This creates a 'tall' matrix A where every memory reading is
    a separate equation.
    """
    unique = sorted(list(self.known_models))

    # We need to build the matrix first to know if we have enough data points
    A, b = [], []

    for config_key, mem_values in self.history.items():
      if not mem_values:
        continue

      # 1. Create the feature row for this configuration ONCE
      # (It represents the model counts + bias)
      counts = dict(config_key)
      feature_row = [counts.get(model, 0) for model in unique]
      feature_row.append(1)  # Bias column

      # 2. Add a separate row to the matrix for EVERY individual reading
      # Instead of averaging, we flatten the history into the matrix
      for reading in mem_values:
        A.append(feature_row)  # The inputs (models) stay the same
        b.append(reading)  # The output (memory) varies due to noise

    # Convert to numpy for SciPy
    A = np.array(A)
    b = np.array(b)

    if len(
        self.history.keys()) < len(unique) + 1 or len(A) < self.min_data_points:
      # Not enough data to solve yet
      return

    logger.info(
        "Solving with %s total observations for %s models.",
        len(A),
        len(unique))

    try:
      # Solve using Non-Negative Least Squares
      # x will be >= 0
      x, _ = nnls(A, b)

      weights = x[:-1]
      bias = x[-1]

      for i, model in enumerate(unique):
        calculated_cost = weights[i]

        if model in self.estimates:
          old = self.estimates[model]
          new = (old * (1 - self.smoothing_factor)) + (
              calculated_cost * self.smoothing_factor)
          self.estimates[model] = new
        else:
          self.estimates[model] = calculated_cost

        logger.info(
            "Updated Estimate for %s: %.1f MB", model, self.estimates[model])
      logger.info("System Bias: %s MB", bias)

    except Exception as e:
      logger.error("Solver failed: %s", e)


class QueueTicket:
  def __init__(self, priority, ticket_num, tag):
    self.priority = priority
    self.ticket_num = ticket_num
    self.tag = tag
    self.wake_event = threading.Event()

  def __lt__(self, other):
    return (self.priority, self.ticket_num) < (other.priority, other.ticket_num)


class ModelManager:
  """Manages model lifecycles, caching, and resource arbitration.

  This class acts as the central controller for acquiring model instances.

  1. LRU Caching of idle models.
  2. Resource estimation and admission control (preventing OOM).
  3. Dynamic eviction of low-priority models, determined by count of 
     pending requests, when space is needed.
  4. 'Isolation Mode' for safely profiling unknown models.
  """
  def __init__(
      self,
      monitor: Optional['GPUMonitor'] = None,
      slack_percentage: float = 0.10,
      poll_interval: float = 0.5,
      peak_window_seconds: float = 30.0,
      min_data_points: int = 5,
      smoothing_factor: float = 0.2,
      eviction_cooldown_seconds: float = 10.0,
      min_model_copies: int = 1,
      wait_timeout_seconds: float = 300.0,
      lock_timeout_seconds: float = 60.0):

    self._estimator = ResourceEstimator(
        min_data_points=min_data_points, smoothing_factor=smoothing_factor)
    self._monitor = monitor if monitor else GPUMonitor(
        poll_interval=poll_interval, peak_window_seconds=peak_window_seconds)
    self._slack_percentage = slack_percentage

    self._eviction_cooldown = eviction_cooldown_seconds
    self._min_model_copies = min_model_copies
    self._wait_timeout_seconds = wait_timeout_seconds
    self._lock_timeout_seconds = lock_timeout_seconds

    # Resource State
    self._models = defaultdict(list)
    # Idle LRU used to track released models that
    # can be freed or reused upon request.
    self._idle_lru = OrderedDict()
    self._active_counts = Counter()
    self._total_active_jobs = 0
    self._pending_reservations = 0.0

    # Isolation state used to profile unknown models,
    # ensuring they run alone to get accurate readings.
    # isolation_baseline represents the GPU usage before
    # loading the unknown model.
    self._isolation_mode = False
    self._isolation_baseline = 0.0

    # Waiting Queue and Ticketing to make sure we have fair ordering
    # and also priority for unknown models.
    self._wait_queue = []
    self._ticket_counter = itertools.count()
    self._cancelled_tickets = set()
    # TODO: Consider making the wait to be smarter, i.e.
    # splitting read/write etc. to avoid potential contention.
    self._cv = threading.Condition()

    self._monitor.start()

  def all_models(self, tag) -> list[Any]:
    return self._models[tag]

  # Should hold _cv lock when calling
  def try_enter_isolation_mode(self, tag: str, ticket_num: int) -> bool:
    if self._total_active_jobs > 0:
      logger.info(
          "Waiting to enter isolation: tag=%s ticket num=%s", tag, ticket_num)
      self._cv.wait(timeout=self._lock_timeout_seconds)
      # return False since we have waited and need to re-evaluate
      # in caller to make sure our priority is still valid.
      return False

    logger.info("Unknown model %s detected. Flushing GPU.", tag)
    self._delete_all_models()

    self._isolation_mode = True
    self._total_active_jobs += 1
    self._isolation_baseline, _, _ = self._monitor.get_stats()
    self._monitor.reset_peak()
    return True

  # Should hold _cv lock when calling
  def should_spawn_model(self, tag: str, ticket_num: int) -> bool:
    curr, _, total = self._monitor.get_stats()
    est_cost = self._estimator.get_estimate(tag)
    limit = total * (1 - self._slack_percentage)

    # Use current usage for capacity check (ignore old spikes)
    if (curr + self._pending_reservations + est_cost) <= limit:
      self._pending_reservations += est_cost
      self._total_active_jobs += 1
      self._active_counts[tag] += 1
      return True

    # Evict to make space (passing tag to check demand/existence)
    if self._evict_to_make_space(limit, est_cost, requesting_tag=tag):
      return True

    # Manually log status for debugging if we are going to wait
    idle_count = 0
    other_idle_count = 0
    for item in self._idle_lru.items():
      if item[1][0] == tag:
        idle_count += 1
      else:
        other_idle_count += 1
    total_model_count = 0
    for _, instances in self._models.items():
      total_model_count += len(instances)
    curr, _, _ = self._monitor.get_stats()
    logger.info(
        "Waiting for resources to free up: "
        "tag=%s ticket num%s model count=%s "
        "idle count=%s resource usage=%.1f MB "
        "total models count=%s other idle=%s",
        tag,
        ticket_num,
        len(self._models[tag]),
        idle_count,
        curr,
        total_model_count,
        other_idle_count)
    # Wait since we couldn't make space and
    # added timeout to avoid missed notify call.
    self._cv.wait(timeout=self._lock_timeout_seconds)
    return False

  def _wake_next_in_queue(self):
    if self._wait_queue:
      # Clean up cancelled tickets at head of queue
      while self._wait_queue and self._wait_queue[
          0].ticket_num in self._cancelled_tickets:
        self._cancelled_tickets.remove(self._wait_queue[0].ticket_num)
        heapq.heappop(self._wait_queue)
      next_inline = self._wait_queue[0]
      next_inline.wake_event.set()

  def _wait_in_queue(self, ticket: QueueTicket):
    self._cv.release()
    try:
      ticket.wake_event.wait(timeout=self._lock_timeout_seconds)
      ticket.wake_event.clear()
    finally:
      self._cv.acquire()

  def acquire_model(self, tag: str, loader_func: Callable[[], Any]) -> Any:
    current_priority = 0 if self._estimator.is_unknown(tag) else 1
    ticket_num = next(self._ticket_counter)
    my_ticket = QueueTicket(current_priority, ticket_num, tag)

    with self._cv:
      # FAST PATH: Grab from idle LRU if available
      if not self._isolation_mode:
        cached_instance = self._try_grab_from_lru(tag)
        if cached_instance:
          return cached_instance

      # SLOW PATH: Enqueue and wait for turn to acquire model,
      # with unknown models having priority and order enforced
      # by ticket number as FIFO.
      logger.info(
          "Acquire Queued: tag=%s, priority=%d "
          "total models count=%s ticket num=%s",
          tag,
          current_priority,
          len(self._models[tag]),
          ticket_num)
      heapq.heappush(self._wait_queue, my_ticket)

      est_cost = 0.0
      is_unknown = False
      wait_time_start = time.time()

      try:
        while True:
          wait_time_elapsed = time.time() - wait_time_start
          if wait_time_elapsed > self._wait_timeout_seconds:
            raise RuntimeError(
                f"Timeout waiting to acquire model: {tag} "
                f"after {wait_time_elapsed:.1f} seconds.")
          if not self._wait_queue or self._wait_queue[
              0].ticket_num != ticket_num:
            logger.info(
                "Waiting for its turn: tag=%s ticket num=%s", tag, ticket_num)
            self._wait_in_queue(my_ticket)
            continue

          # Re-evaluate priority in case model became known during wait
          is_unknown = self._estimator.is_unknown(tag)
          real_priority = 0 if is_unknown else 1

          # If priority changed, reinsert into queue and wait
          if current_priority != real_priority:
            heapq.heappop(self._wait_queue)
            current_priority = real_priority
            my_ticket = QueueTicket(current_priority, ticket_num, tag)
            heapq.heappush(self._wait_queue, my_ticket)
            self._wake_next_in_queue()
            continue

          # Try grab from LRU again in case model was released during wait
          cached_instance = self._try_grab_from_lru(tag)
          if cached_instance:
            return cached_instance

          # Path A: Isolation
          if is_unknown:
            if self.try_enter_isolation_mode(tag, ticket_num):
              # We got isolation, can proceed to spawn
              break
            else:
              # We waited, need to re-evaluate our turn
              # because priority may have changed during the wait
              continue

          # Path B: Concurrent
          else:
            if self._isolation_mode:
              logger.info(
                  "Waiting due to isolation in progress: tag=%s ticket num%s",
                  tag,
                  ticket_num)
              self._wait_in_queue(my_ticket)
              continue

            if self.should_spawn_model(tag, ticket_num):
              est_cost = self._estimator.get_estimate(tag)
              # We can proceed to spawn since we have resources
              break
            else:
              # We waited, need to re-evaluate our turn
              # because priority may have changed during the wait
              continue

      finally:
        # Remove self from wait queue once done
        if self._wait_queue and self._wait_queue[0].ticket_num == ticket_num:
          heapq.heappop(self._wait_queue)
        else:
          # Marked as cancelled so that we skip when we reach head later
          self._cancelled_tickets.add(ticket_num)
        self._wake_next_in_queue()

      return self._spawn_new_model(tag, loader_func, is_unknown, est_cost)

  def release_model(self, tag: str, instance: Any):
    with self._cv:
      try:
        self._total_active_jobs -= 1
        if self._active_counts[tag] > 0:
          self._active_counts[tag] -= 1

        self._idle_lru[id(instance)] = (tag, instance, time.time())

        # Update estimator with latest stats
        _, peak_during_job, _ = self._monitor.get_stats()

        if self._isolation_mode and self._active_counts[tag] == 0:
          # For isolation mode, we directly set the initial estimate
          # so that we can quickly learn the model cost.
          cost = max(0, peak_during_job - self._isolation_baseline)
          self._estimator.set_initial_estimate(tag, cost)
          self._isolation_mode = False
          self._isolation_baseline = 0.0
        else:
          # Regular update for known models
          snapshot = {
              t: len(instances)
              for t, instances in self._models.items() if len(instances) > 0
          }
          if snapshot:
            self._estimator.add_observation(snapshot, peak_during_job)

      finally:
        self._wake_next_in_queue()
        self._cv.notify_all()

  def _try_grab_from_lru(self, tag: str) -> Any:
    target_key = None
    target_instance = None

    for key, (t, instance, _) in reversed(self._idle_lru.items()):
      if t == tag:
        target_key = key
        target_instance = instance
        break

    if target_instance:
      # Found an idle model, remove from LRU and return
      del self._idle_lru[target_key]
      self._active_counts[tag] += 1
      self._total_active_jobs += 1
      return target_instance

    logger.info("No idle model found for tag: %s", tag)
    return None

  def _evict_to_make_space(
      self, limit: float, est_cost: float, requesting_tag: str) -> bool:
    """
    Evicts models based on Demand Magnitude + Tiers.
    Crucially: If we have 0 active copies of 'requesting_tag', we FORCE eviction
    of the lowest-demand candidate to avoid starvation.
    Returns True if space was made, False otherwise.
    """
    curr, _, _ = self._monitor.get_stats()
    projected_usage = curr + self._pending_reservations + est_cost

    if projected_usage <= limit:
      # Memory usage changed and we are already under limit
      return True

    now = time.time()

    # Calculate the demand from the wait queue
    # TODO: Also factor in the active counts to avoid thrashing
    demand_map = Counter()
    for item in self._wait_queue:
      demand_map[item.tag] += 1

    my_demand = demand_map[requesting_tag]
    am_i_starving = len(self._models[requesting_tag]) == 0

    candidates = []
    for key, (tag, instance, release_time) in self._idle_lru.items():
      candidate_demand = demand_map[tag]

      # TODO: Try to avoid churn if demand is similar
      if not am_i_starving and candidate_demand >= my_demand:
        continue

      # Attempts to score candidates based on hotness and manually
      # specified minimum copies. Demand is weighted heavily to
      # ensure we evict low-demand models first.
      age = now - release_time
      is_cold = age >= self._eviction_cooldown

      total_copies = len(self._models[tag])
      is_surplus = total_copies > self._min_model_copies

      if is_cold and is_surplus: tier = 0
      elif not is_cold and is_surplus: tier = 1
      elif is_cold and not is_surplus: tier = 2
      else: tier = 3

      score = (candidate_demand * 10) + tier

      candidates.append((score, release_time, key, tag, instance))

    candidates.sort(key=lambda x: (x[0], x[1]))

    # Evict candidates until we are under limit
    for score, _, key, tag, instance in candidates:
      if projected_usage <= limit:
        break

      if key not in self._idle_lru: continue

      self._perform_eviction(key, tag, instance, score)

      curr, _, _ = self._monitor.get_stats()
      projected_usage = curr + self._pending_reservations + est_cost

    return projected_usage <= limit

  def _delete_instance(self, instance: Any):
    if isinstance(instance, str):
      # If the instance is a string, it's a uuid used
      # to retrieve the model from MultiProcessShared
      multi_process_shared.MultiProcessShared(
          lambda: "N/A", tag=instance).unsafe_hard_delete()
    if hasattr(instance, 'mock_model_unsafe_hard_delete'):
      # Call the mock unsafe hard delete method for testing
      instance.mock_model_unsafe_hard_delete()
    del instance

  def _perform_eviction(self, key: str, tag: str, instance: Any, score: int):
    logger.info("Evicting Model: %s (Score %d)", tag, score)
    curr, _, _ = self._monitor.get_stats()
    logger.info("Resource Usage Before Eviction: %.1f MB", curr)

    if key in self._idle_lru:
      del self._idle_lru[key]

    for i, inst in enumerate(self._models[tag]):
      if instance == inst:
        del self._models[tag][i]
        break

    self._delete_instance(instance)
    gc.collect()
    torch.cuda.empty_cache()
    self._monitor.refresh()
    self._monitor.reset_peak()
    curr, _, _ = self._monitor.get_stats()
    logger.info("Resource Usage After Eviction: %.1f MB", curr)

  def _spawn_new_model(
      self,
      tag: str,
      loader_func: Callable[[], Any],
      is_unknown: bool,
      est_cost: float) -> Any:
    try:
      with self._cv:
        logger.info("Loading Model: %s (Unknown: %s)", tag, is_unknown)
        baseline_snap, _, _ = self._monitor.get_stats()
        instance = loader_func()
        _, peak_during_load, _ = self._monitor.get_stats()

        snapshot = {tag: 1}
        self._estimator.add_observation(
            snapshot, peak_during_load - baseline_snap)

        if not is_unknown:
          self._pending_reservations = max(
              0.0, self._pending_reservations - est_cost)
        self._models[tag].append(instance)
      return instance

    except Exception as e:
      logger.error("Load Failed: %s. Error: %s", tag, e)
      with self._cv:
        self._total_active_jobs -= 1
        if is_unknown:
          self._isolation_mode = False
          self._isolation_baseline = 0.0
        else:
          self._pending_reservations = max(
              0.0, self._pending_reservations - est_cost)
          self._active_counts[tag] -= 1
        self._cv.notify_all()
      raise e

  def _delete_all_models(self):
    self._idle_lru.clear()
    for _, instances in self._models.items():
      for instance in instances:
        self._delete_instance(instance)
    self._models.clear()
    self._active_counts.clear()
    gc.collect()
    torch.cuda.empty_cache()
    self._monitor.refresh()
    self._monitor.reset_peak()

  def _force_reset(self):
    logger.warning("Force Reset Triggered")
    self._delete_all_models()
    self._models = defaultdict(list)
    self._idle_lru = OrderedDict()
    self._active_counts = Counter()
    self._wait_queue = []
    self._total_active_jobs = 0
    self._pending_reservations = 0.0
    self._isolation_mode = False
    self._isolation_baseline = 0.0

  def shutdown(self):
    self._delete_all_models()
    self._monitor.stop()

  def __del__(self):
    self.shutdown()

  def __exit__(self, exc_type, exc_value, traceback):
    self.shutdown()
