import unittest
import time
import threading
import random
from concurrent.futures import ThreadPoolExecutor

# Import from the library file
from apache_beam.ml.inference.model_manager import ModelManager


class MockGPUMonitor:
  """
  Simulates GPU hardware with cumulative memory tracking.
  Allows simulating specific allocation spikes and baseline usage.
  """
  def __init__(self, total_memory=12000.0, peak_window: int = 5):
    self._current = 0.0
    self._peak = 0.0
    self._total = total_memory
    self._lock = threading.Lock()
    self.running = False
    self.history = []
    self.peak_window = peak_window

  def start(self):
    self.running = True

  def stop(self):
    self.running = False

  def get_stats(self):
    with self._lock:
      return self._current, self._peak, self._total

  def reset_peak(self):
    with self._lock:
      self._peak = self._current

  # --- Test Helper Methods ---
  def set_usage(self, current_mb):
    """Sets absolute usage (legacy helper)."""
    with self._lock:
      self._current = current_mb
      self._peak = max(self._peak, current_mb)

  def allocate(self, amount_mb):
    """Simulates memory allocation (e.g., tensors loaded to VRAM)."""
    with self._lock:
      self._current += amount_mb
      self.history.append(self._current)
      if len(self.history) > self.peak_window:
        self.history.pop(0)
      self._peak = max(self.history)

  def free(self, amount_mb):
    """Simulates memory freeing (not used often if pooling is active)."""
    with self._lock:
      self._current = max(0.0, self._current - amount_mb)
      self.history.append(self._current)
      if len(self.history) > self.peak_window:
        self.history.pop(0)
      self._peak = max(self.history)


class TestModelManager(unittest.TestCase):
  def setUp(self):
    """Force reset the Singleton ModelManager before every test."""
    # 1. Reset the Singleton instance
    ModelManager._instance = None

    # 2. Instantiate Mock Monitor directly
    self.mock_monitor = MockGPUMonitor()

    # 3. Inject Mock Monitor into Manager
    self.manager = ModelManager(monitor=self.mock_monitor)

  def tearDown(self):
    self.manager.shutdown()

  def test_model_manager_capacity_check(self):
    """
    Test that the manager blocks when spawning models exceeds the limit,
    and unblocks when resources become available (via reuse).
    """
    model_name = "known_model"
    model_cost = 3000.0
    # Total Memory: 12000. Limit (15% slack) ~ 10200.
    # 3 * 3000 = 9000 (OK).
    # 4 * 3000 = 12000 (Over Limit).

    self.manager.estimator.set_initial_estimate(model_name, model_cost)

    acquired_refs = []

    def loader():
      self.mock_monitor.allocate(model_cost)
      return model_name

    # 1. Saturate GPU with 3 models (9000 MB usage)
    for _ in range(3):
      inst = self.manager.acquire_model(model_name, loader)
      acquired_refs.append(inst)

    # 2. Spawn one more (Should Block because 9000 + 3000 > Limit)
    def run_inference():
      return self.manager.acquire_model(model_name, loader)

    with ThreadPoolExecutor(max_workers=1) as executor:
      future = executor.submit(run_inference)

      # Verify it blocks
      try:
        future.result(timeout=0.5)
        self.fail("Should have blocked due to capacity")
      except TimeoutError:
        pass

      # 3. Release resources to unblock
      # Releasing one puts it in the idle pool.
      # The blocked thread should wake up, see the idle one in the pool,
      # and reuse it.
      item_to_release = acquired_refs.pop()
      self.manager.release_model(model_name, item_to_release)

      # 4. Verify Success
      # The previous logic required a manual notify loop because set_usage
      # didn't notify. release_model calls notify_all(), so standard futures
      # waiting works here.
      result = future.result(timeout=2.0)
      self.assertIsNotNone(result)

      # Verify we reused the released instance (optimization check)
      self.assertEqual(result, item_to_release)

  def test_model_manager_unknown_model_runs_isolated(self):
    """Test that a model with no history runs in isolation."""
    model_name = "unknown_model_v1"
    self.assertTrue(self.manager.estimator.is_unknown(model_name))

    def dummy_loader():
      time.sleep(0.05)
      return "model_instance"

    instance = self.manager.acquire_model(model_name, dummy_loader)

    self.assertTrue(self.manager.isolation_mode)
    self.assertEqual(self.manager.total_active_jobs, 1)

    self.manager.release_model(model_name, instance)
    self.assertFalse(self.manager.isolation_mode)
    self.assertFalse(self.manager.estimator.is_unknown(model_name))

  def test_model_manager_concurrent_execution(self):
    """Test that multiple small known models can run together."""
    model_a = "small_model_a"
    model_b = "small_model_b"

    self.manager.estimator.set_initial_estimate(model_a, 1000.0)
    self.manager.estimator.set_initial_estimate(model_b, 1000.0)
    self.mock_monitor.set_usage(1000.0)

    inst_a = self.manager.acquire_model(model_a, lambda: "A")
    inst_b = self.manager.acquire_model(model_b, lambda: "B")

    self.assertEqual(self.manager.total_active_jobs, 2)

    self.manager.release_model(model_a, inst_a)
    self.manager.release_model(model_b, inst_b)
    self.assertEqual(self.manager.total_active_jobs, 0)

  def test_model_manager_concurrent_mixed_workload_convergence(self):
    """
    Simulates a production environment with multiple model types running
    concurrently. Verifies that the estimator converges.
    """
    # --- Configuration ---
    TRUE_COSTS = {"model_small": 1500.0, "model_medium": 3000.0}

    def run_job(model_name):
      cost = TRUE_COSTS[model_name]

      # Loader: Simulates the initial memory spike when loading to VRAM
      def loader():
        self.mock_monitor.allocate(cost)
        time.sleep(0.01)
        return f"instance_{model_name}"

      # 1. Acquire
      # Note: If reused, loader isn't called, so memory stays stable.
      # If new, loader runs and bumps monitor memory.
      instance = self.manager.acquire_model(model_name, loader)

      # 2. Simulate Inference Work
      # In a real GPU, inference might spike memory further (activations).
      # For this test, we assume the 'cost' captures the peak usage.
      time.sleep(random.uniform(0.01, 0.05))

      # 3. Release
      self.manager.release_model(model_name, instance)

    # Create a workload stream
    # 15 Small jobs, 15 Medium jobs, mixed order
    workload = ["model_small"] * 15 + ["model_medium"] * 15
    random.shuffle(workload)

    # We use a thread pool slightly larger than the theoretical capacity
    # to force queuing and reuse logic.
    # Capacity ~12000. Small=1500, Med=3000.
    # Max concurrent approx: 4 Med (12000) or 8 Small (12000).
    with ThreadPoolExecutor(max_workers=8) as executor:
      futures = [executor.submit(run_job, name) for name in workload]
      for f in futures:
        f.result()

    # --- Assertions ---
    est_small = self.manager.estimator.get_estimate("model_small")
    est_med = self.manager.estimator.get_estimate("model_medium")

    # Check convergence (allow some margin for solver approximation)
    self.assertAlmostEqual(est_small, TRUE_COSTS["model_small"], delta=100.0)
    self.assertAlmostEqual(est_med, TRUE_COSTS["model_medium"], delta=100.0)

  def test_model_manager_oom_recovery(self):
    """Test that the manager recovers state if a loader crashes."""
    model_name = "crasher_model"
    self.manager.estimator.set_initial_estimate(model_name, 1000.0)

    def crashing_loader():
      raise RuntimeError("CUDA OOM or similar")

    with self.assertRaises(RuntimeError):
      self.manager.acquire_model(model_name, crashing_loader)

    self.assertEqual(self.manager.total_active_jobs, 0)
    self.assertEqual(self.manager.pending_reservations, 0.0)
    self.assertFalse(self.manager._cv._is_owned())

  def test_single_model_convergence_with_fluctuations(self):
    """
        Tests that the estimator converges to the true usage with:
        1. A single model type.
        2. Initial 'Load' cost that is lower than 'Inference' cost.
        3. High variance/fluctuation during inference.
        """
    model_name = "fluctuating_model"
    model_cost = 3000.0
    load_cost = 2000.0  # Initial load cost underestimates true cost

    def loader():
      self.mock_monitor.allocate(load_cost)
      return model_name

    # Check that initial estimate is only the load cost
    model = self.manager.acquire_model(model_name, loader)
    self.manager.release_model(model_name, model)
    initial_est = self.manager.estimator.get_estimate(model_name)
    self.assertEqual(initial_est, load_cost)

    def run_inference():
      model = self.manager.acquire_model(model_name, loader)
      noise = model_cost - load_cost + random.uniform(-300.0, 300.0)
      self.mock_monitor.allocate(noise)
      time.sleep(0.1)
      self.mock_monitor.free(noise)
      self.manager.release_model(model_name, model)
      return

    with ThreadPoolExecutor(max_workers=8) as executor:
      futures = [executor.submit(run_inference) for _ in range(100)]

    for f in futures:
      f.result()

    est_cost = self.manager.estimator.get_estimate(model_name)
    self.assertAlmostEqual(est_cost, model_cost, delta=100.0)


if __name__ == "__main__":
  unittest.main()
