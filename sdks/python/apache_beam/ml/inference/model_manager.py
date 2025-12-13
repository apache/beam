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

import time
import threading
import subprocess
import logging
import gc
import numpy as np
from scipy.optimize import nnls
import torch
import heapq
import itertools
from collections import defaultdict, deque, Counter, OrderedDict
from typing import Dict, Any, Tuple, Optional, Callable

logger = logging.getLogger(__name__)


class GPUMonitor:
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
    self._gpu_available = self._detect_hardware()

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
      cmd = "nvidia-smi --query-gpu=memory.free --format=csv,noheader,nounits"
      output = subprocess.check_output(cmd, shell=True).decode("utf-8").strip()
      free_memory = float(output)
      return self._total_memory - free_memory
    except Exception:
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
    logger.info(
        "Adding Observation: Snapshot=%s, PeakMemory=%.1f MB",
        active_snapshot,
        peak_memory)
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


class ModelManager:
  _lock = threading.Lock()

  def __init__(
      self,
      monitor: Optional['GPUMonitor'] = None,
      slack_percentage: float = 0.10,
      poll_interval: float = 0.5,
      peak_window_seconds: float = 30.0,
      min_data_points: int = 5,
      smoothing_factor: float = 0.2,
      eviction_cooldown_seconds: float = 10.0,
      min_model_copies: int = 1):

    self._estimator = ResourceEstimator(
        min_data_points=min_data_points, smoothing_factor=smoothing_factor)
    self._monitor = monitor if monitor else GPUMonitor(
        poll_interval=poll_interval, peak_window_seconds=peak_window_seconds)
    self._slack_percentage = slack_percentage

    self._eviction_cooldown = eviction_cooldown_seconds
    self._min_model_copies = min_model_copies

    # Resource State
    self._models = defaultdict(list)
    self._idle_lru = OrderedDict()
    self._active_counts = Counter()
    self._total_active_jobs = 0
    self._pending_reservations = 0.0

    self._isolation_mode = False
    self._pending_isolation_count = 0
    self._isolation_baseline = 0.0

    self._wait_queue = []
    self._ticket_counter = itertools.count()
    self._cv = threading.Condition()
    self._load_lock = threading.Lock()

    self._monitor.start()

  def all_models(self, tag) -> list[Any]:
    return self._models[tag]

  def acquire_model(self, tag: str, loader_func: Callable[[], Any]) -> Any:
    current_priority = 0 if self._estimator.is_unknown(tag) else 1
    ticket_num = next(self._ticket_counter)
    my_id = object()

    with self._cv:
      # FAST PATH
      if self._pending_isolation_count == 0 and not self._isolation_mode:
        cached_instance = self._try_grab_from_lru(tag)
        if cached_instance:
          return cached_instance

      # SLOW PATH
      logger.info("Acquire Queued: tag=%s, priority=%d", tag, current_priority)
      heapq.heappush(
          self._wait_queue, (current_priority, ticket_num, my_id, tag))

      should_spawn = False
      est_cost = 0.0
      is_unknown = False

      try:
        while True:
          if not self._wait_queue or self._wait_queue[0][2] is not my_id:
            self._cv.wait()
            continue

          real_is_unknown = self._estimator.is_unknown(tag)
          real_priority = 0 if real_is_unknown else 1

          if current_priority != real_priority:
            heapq.heappop(self._wait_queue)
            current_priority = real_priority
            heapq.heappush(
                self._wait_queue, (current_priority, ticket_num, my_id, tag))
            self._cv.notify_all()
            continue

          cached_instance = self._try_grab_from_lru(tag)
          if cached_instance:
            return cached_instance

          is_unknown = real_is_unknown

          # Path A: Isolation
          if is_unknown:
            if self._total_active_jobs > 0:
              self._cv.wait()
              continue

            logger.info("Unknown model %s detected. Flushing GPU.", tag)
            self._delete_all_models()

            self._isolation_mode = True
            self._total_active_jobs += 1
            self._isolation_baseline, _, _ = self._monitor.get_stats()
            self._monitor.reset_peak()
            should_spawn = True
            break

          # Path B: Concurrent
          else:
            if self._pending_isolation_count > 0 or self._isolation_mode:
              self._cv.wait()
              continue

            curr, _, total = self._monitor.get_stats()
            est_cost = self._estimator.get_estimate(tag)
            limit = total * (1 - self._slack_percentage)

            # Use current usage for capacity check (ignore old spikes)
            if (curr + self._pending_reservations + est_cost) <= limit:
              self._pending_reservations += est_cost
              self._total_active_jobs += 1
              self._active_counts[tag] += 1
              should_spawn = True
              break

            # Evict to make space (passing tag to check demand/existence)
            if self._evict_to_make_space(limit, est_cost, requesting_tag=tag):
              continue

            self._cv.wait(timeout=10.0)

      finally:
        if self._wait_queue and self._wait_queue[0][2] is my_id:
          heapq.heappop(self._wait_queue)
        else:
          for i, item in enumerate(self._wait_queue):
            if item[2] is my_id:
              self._wait_queue.pop(i)
              heapq.heapify(self._wait_queue)
        self._cv.notify_all()

    if should_spawn:
      return self._spawn_new_model(tag, loader_func, is_unknown, est_cost)

  def release_model(self, tag: str, instance: Any):
    with self._cv:
      try:
        self._total_active_jobs -= 1
        if self._active_counts[tag] > 0:
          self._active_counts[tag] -= 1

        self._idle_lru[id(instance)] = (tag, instance, time.time())

        _, peak_during_job, _ = self._monitor.get_stats()

        if self._isolation_mode and self._active_counts[tag] == 0:
          cost = max(0, peak_during_job - self._isolation_baseline)
          self._estimator.set_initial_estimate(tag, cost)
          self._isolation_mode = False
          self._isolation_baseline = 0.0
        else:
          snapshot = {
              t: len(instances)
              for t, instances in self._models.items() if len(instances) > 0
          }
          if snapshot:
            self._estimator.add_observation(snapshot, peak_during_job)

      finally:
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
      del self._idle_lru[target_key]
      self._active_counts[tag] += 1
      self._total_active_jobs += 1
      return target_instance
    return None

  def _evict_to_make_space(
      self, limit: float, est_cost: float, requesting_tag: str) -> bool:
    """
    Evicts models based on Demand Magnitude + Tiers.
    Crucially: If we have 0 active copies of 'requesting_tag', we FORCE eviction
    of the lowest-demand candidate to avoid starvation.
    """
    evicted_something = False
    curr, _, _ = self._monitor.get_stats()
    projected_usage = curr + self._pending_reservations + est_cost

    if projected_usage <= limit:
      return False

    now = time.time()

    demand_map = Counter()
    for item in self._wait_queue:
      if len(item) >= 4:
        demand_map[item[3]] += 1

    my_demand = demand_map[requesting_tag]
    am_i_starving = len(self._models[requesting_tag]) == 0

    candidates = []
    for key, (tag, instance, release_time) in self._idle_lru.items():
      candidate_demand = demand_map[tag]

      if not am_i_starving:
        if candidate_demand >= my_demand:
          continue

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

    for score, _, key, tag, instance in candidates:
      if projected_usage <= limit:
        break

      if key not in self._idle_lru: continue

      self._perform_eviction(key, tag, instance, score)
      evicted_something = True

      curr, _, _ = self._monitor.get_stats()
      projected_usage = curr + self._pending_reservations + est_cost

    return evicted_something

  def _perform_eviction(self, key, tag, instance, score):
    logger.info("Evicting Model: %s (Score %d)", tag, score)

    if key in self._idle_lru:
      del self._idle_lru[key]

    if hasattr(instance, "unsafe_hard_delete"):
      instance.unsafe_hard_delete()

    if instance in self._models[tag]:
      self._models[tag].remove(instance)

    del instance
    gc.collect()
    torch.cuda.empty_cache()
    self._monitor.refresh()
    self._monitor.reset_peak()

  def _spawn_new_model(self, tag, loader_func, is_unknown, est_cost):
    try:
      with self._load_lock:
        logger.info("Loading Model: %s (Unknown: %s)", tag, is_unknown)
        isolation_baseline_snap, _, _ = self._monitor.get_stats()
        instance = loader_func()
        _, peak_during_load, _ = self._monitor.get_stats()

      with self._cv:
        snapshot = {tag: 1}
        self._estimator.add_observation(
            snapshot, peak_during_load - isolation_baseline_snap)

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
        if hasattr(instance, "unsafe_hard_delete"):
          instance.unsafe_hard_delete()
        del instance
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
    self._pending_isolation_count = 0
    self._isolation_baseline = 0.0

  def shutdown(self):
    self._delete_all_models()
    gc.collect()
    torch.cuda.empty_cache()
    self._monitor.stop()

  def __del__(self):
    self.shutdown()

  def __exit__(self, exc_type, exc_value, traceback):
    self.shutdown()
