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
# TODO: https://github.com/apache/beam/issues/21822
# mypy: ignore-errors

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
from collections import defaultdict, deque, Counter
from contextlib import contextmanager
from typing import Dict, Any, Tuple, Optional, Callable

# Configure Logging
logger = logging.getLogger(__name__)

# Constants
SLACK_PERCENTAGE = 0.15
POLL_INTERVAL = 0.5
PEAK_WINDOW_SECONDS = 30.0
SMOOTHING_FACTOR = 0.2


@contextmanager
def cuda_oom_guard(description: str):
  """Safely catches OOM, clears cache, and re-raises."""
  try:
    yield
  except torch.cuda.OutOfMemoryError as e:
    logger.error("CUDA OOM DETECTED during: %s", description)
    gc.collect()
    torch.cuda.empty_cache()
    raise e


class GPUMonitor:
  def __init__(self, fallback_memory_mb: float = 16000.0):
    self._current_usage = 0.0
    self._peak_usage = 0.0
    self._total_memory = fallback_memory_mb
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

  def _get_nvidia_smi_used(self) -> float:
    try:
      cmd = "nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits"
      output = subprocess.check_output(cmd, shell=True).decode("utf-8").strip()
      return float(output)
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
                                        > PEAK_WINDOW_SECONDS):
          self._memory_history.popleft()
        self._peak_usage = (
            max(m for _, m in self._memory_history)
            if self._memory_history else usage)
      time.sleep(POLL_INTERVAL)


class ResourceEstimator:
  def __init__(self):
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

    if len(A) < len(unique) + 1:
      # Not enough data to solve yet
      return

    print(f"Solving with {len(A)} total observations for {len(unique)} models.")

    try:
      # Solve using Non-Negative Least Squares
      # x will be >= 0
      x, _ = nnls(A, b)

      weights = x[:-1]
      bias = x[-1]

      for i, model in enumerate(unique):
        calculated_cost = weights[i]
        print(f"Solved Cost for {model}: {calculated_cost:.1f} MB")

        if model in self.estimates:
          old = self.estimates[model]
          new = (old * (1 - SMOOTHING_FACTOR)) + (
              calculated_cost * SMOOTHING_FACTOR)
          self.estimates[model] = new
        else:
          self.estimates[model] = calculated_cost

      print(f"System Bias: {bias:.1f} MB")

    except Exception as e:
      logger.error("Solver failed: %s", e)


class ModelManager:
  _lock = threading.Lock()

  def __init__(self, monitor: Optional[GPUMonitor] = None):
    self.estimator = ResourceEstimator()
    self.monitor = monitor if monitor else GPUMonitor()

    self.models = defaultdict(list)
    self.idle_pool = defaultdict(list)
    self.active_counts = Counter()
    self.total_active_jobs = 0
    self.pending_reservations = 0.0

    # State Control
    self.isolation_mode = False
    self.pending_isolation_count = 0
    self.isolation_baseline = 0.0

    self._cv = threading.Condition()
    self.monitor.start()

  def all_models(self, tag) -> list[Any]:
    return self.models[tag]

  def acquire_model(self, tag: str, loader_func: Callable[[], Any]) -> Any:
    logger.info(
        "Acquiring model for tag: %s | "
        "idle_pool size: %d | "
        "active_count: %d | "
        "total_active_jobs: %d | "
        "pending_reservations: %.1f | "
        "isolation_mode: %s | "
        "pending_isolation_count: %d | "
        "estimator known: %s | "
        "estimator cost: %.1f MB",
        tag,
        len(self.idle_pool[tag]),
        self.active_counts[tag],
        self.total_active_jobs,
        self.pending_reservations,
        self.isolation_mode,
        self.pending_isolation_count,
        not self.estimator.is_unknown(tag),
        self.estimator.get_estimate(tag),
    )
    should_spawn = False
    est_cost = 0.0
    is_unknown = False

    with self._cv:
      while True:
        is_unknown = self.estimator.is_unknown(tag)

        # Path A: Isolation for Unknown Models
        if is_unknown:
          self.pending_isolation_count += 1
          try:
            while self.total_active_jobs > 0 or self.isolation_mode:
              self._cv.wait()
              if not self.estimator.is_unknown(tag):
                is_unknown = False
                break

            if not is_unknown:
              continue

            self.isolation_mode = True
            self.total_active_jobs += 1
            self.isolation_baseline, _, _ = self.monitor.get_stats()
            self.monitor.reset_peak()
            should_spawn = True
            break
          finally:
            self.pending_isolation_count -= 1
            if not should_spawn:
              self._cv.notify_all()

        # Path B: Concurrent Execution
        else:
          # Writer Priority (allow unknown models to drain system)
          if self.pending_isolation_count > 0 or self.isolation_mode:
            self._cv.wait()
            continue

          if self.idle_pool[tag]:
            instance = self.idle_pool[tag].pop()
            self.active_counts[tag] += 1
            self.total_active_jobs += 1
            return instance

          # Capacity Check
          curr, peak, total = self.monitor.get_stats()
          est_cost = self.estimator.get_estimate(tag)
          limit = total * (1 - SLACK_PERCENTAGE)
          base_usage = max(curr, peak)

          if (base_usage + self.pending_reservations + est_cost) <= limit:
            self.pending_reservations += est_cost
            self.total_active_jobs += 1
            self.active_counts[tag] += 1
            should_spawn = True
            break

          self._cv.wait()

      # Execution Logic (Spawn)
      if should_spawn:
        try:
          logger.info("Loading model for tag: %s", tag)
          isolation_baseline_snap, _, _ = self.monitor.get_stats()
          with cuda_oom_guard(f"Loading {tag}"):
            instance = loader_func()
          logger.info("Model loaded for tag: %s", tag)
          _, peak_during_load, _ = self.monitor.get_stats()
          snapshot = {tag: 1}
          self.estimator.add_observation(
              snapshot, peak_during_load - isolation_baseline_snap)

          if not is_unknown:
            self.pending_reservations = max(
                0.0, self.pending_reservations - est_cost)
          self.models[tag].append(instance)
          return instance

        except Exception as e:
          self.total_active_jobs -= 1
          if is_unknown:
            self.isolation_mode = False
            self.isolation_baseline = 0.0
          else:
            self.pending_reservations = max(
                0.0, self.pending_reservations - est_cost)
            self.active_counts[tag] -= 1
          self._cv.notify_all()
          raise e

  def release_model(self, tag: str, instance: Any):
    with self._cv:
      try:
        self.total_active_jobs -= 1
        if self.active_counts[tag] > 0:
          self.active_counts[tag] -= 1

        # Return to pool
        self.idle_pool[tag].append(instance)

        _, peak_during_job, _ = self.monitor.get_stats()

        if self.isolation_mode and self.active_counts[tag] == 0:
          cost = max(0, peak_during_job - self.isolation_baseline)
          self.estimator.set_initial_estimate(tag, cost)
          self.isolation_mode = False
          self.isolation_baseline = 0.0
        else:
          # Solver Snapshot
          snapshot = dict(self.active_counts)
          for pool_tag, models in self.idle_pool.items():
            snapshot[pool_tag] = snapshot.get(pool_tag, 0) + len(models)

          if snapshot:
            print(
                f"Release Snapshot: {snapshot}, Peak: {peak_during_job:.1f} MB")
            self.estimator.add_observation(snapshot, peak_during_job)

      finally:
        self._cv.notify_all()

  def force_reset(self):
    for _, instances in self.models.items():
      del instances[:]
    gc.collect()
    torch.cuda.empty_cache()

    self.models = defaultdict(list)
    self.idle_pool = defaultdict(list)
    self.active_counts = Counter()
    self.total_active_jobs = 0
    self.pending_reservations = 0.0
    self.isolation_mode = False
    self.pending_isolation_count = 0
    self.isolation_baseline = 0.0

  def shutdown(self):
    try:
      for _, instances in self.models.items():
        del instances[:]
      gc.collect()
      torch.cuda.empty_cache()
    except Exception as e:
      logger.error("Error during ModelManager shutdown: %s", e)
    self.monitor.stop()

  def __del__(self):
    self.shutdown()

  def __exit__(self, exc_type, exc_value, traceback):
    self.shutdown()
