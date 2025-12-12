import unittest
import time
import threading
import random
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from apache_beam.ml.inference.model_manager import ModelManager, GPUMonitor, ResourceEstimator


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
      self.history = [self._current]

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


class MockModel:
  def __init__(self, name, size, monitor):
    self.name = name
    self.size = size
    self.monitor = monitor
    self.deleted = False
    self.monitor.allocate(size)

  def unsafe_hard_delete(self):
    if not self.deleted:
      self.monitor.free(self.size)
      self.deleted = True


class TestModelManager(unittest.TestCase):
  def setUp(self):
    """Force reset the Singleton ModelManager before every test."""
    ModelManager._instance = None
    self.mock_monitor = MockGPUMonitor()
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
    self.manager._estimator.set_initial_estimate(model_name, model_cost)
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
      try:
        future.result(timeout=0.5)
        self.fail("Should have blocked due to capacity")
      except TimeoutError:
        pass

      # 3. Release resources to unblock
      item_to_release = acquired_refs.pop()
      self.manager.release_model(model_name, item_to_release)

      result = future.result(timeout=2.0)
      self.assertIsNotNone(result)
      self.assertEqual(result, item_to_release)

  def test_model_manager_unknown_model_runs_isolated(self):
    """Test that a model with no history runs in isolation."""
    model_name = "unknown_model_v1"
    self.assertTrue(self.manager._estimator.is_unknown(model_name))

    def dummy_loader():
      time.sleep(0.05)
      return "model_instance"

    instance = self.manager.acquire_model(model_name, dummy_loader)

    self.assertTrue(self.manager._isolation_mode)
    self.assertEqual(self.manager._total_active_jobs, 1)

    self.manager.release_model(model_name, instance)
    self.assertFalse(self.manager._isolation_mode)
    self.assertFalse(self.manager._estimator.is_unknown(model_name))

  def test_model_manager_concurrent_execution(self):
    """Test that multiple small known models can run together."""
    model_a = "small_model_a"
    model_b = "small_model_b"

    self.manager._estimator.set_initial_estimate(model_a, 1000.0)
    self.manager._estimator.set_initial_estimate(model_b, 1000.0)
    self.mock_monitor.set_usage(1000.0)

    inst_a = self.manager.acquire_model(model_a, lambda: "A")
    inst_b = self.manager.acquire_model(model_b, lambda: "B")

    self.assertEqual(self.manager._total_active_jobs, 2)

    self.manager.release_model(model_a, inst_a)
    self.manager.release_model(model_b, inst_b)
    self.assertEqual(self.manager._total_active_jobs, 0)

  def test_model_manager_concurrent_mixed_workload_convergence(self):
    """
    Simulates a production environment with multiple model types running
    concurrently. Verifies that the estimator converges.
    """
    TRUE_COSTS = {"model_small": 1500.0, "model_medium": 3000.0}

    def run_job(model_name):
      cost = TRUE_COSTS[model_name]

      def loader():
        model = MockModel(model_name, cost, self.mock_monitor)
        return model

      instance = self.manager.acquire_model(model_name, loader)
      time.sleep(random.uniform(0.01, 0.05))
      self.manager.release_model(model_name, instance)

    # Create a workload stream
    workload = ["model_small"] * 15 + ["model_medium"] * 15
    random.shuffle(workload)

    with ThreadPoolExecutor(max_workers=8) as executor:
      futures = [executor.submit(run_job, name) for name in workload]
      for f in futures:
        f.result()

    est_small = self.manager._estimator.get_estimate("model_small")
    est_med = self.manager._estimator.get_estimate("model_medium")

    self.assertAlmostEqual(est_small, TRUE_COSTS["model_small"], delta=100.0)
    self.assertAlmostEqual(est_med, TRUE_COSTS["model_medium"], delta=100.0)

  def test_model_manager_oom_recovery(self):
    """Test that the manager recovers state if a loader crashes."""
    model_name = "crasher_model"
    self.manager._estimator.set_initial_estimate(model_name, 1000.0)

    def crashing_loader():
      raise RuntimeError("CUDA OOM or similar")

    with self.assertRaises(RuntimeError):
      self.manager.acquire_model(model_name, crashing_loader)

    self.assertEqual(self.manager._total_active_jobs, 0)
    self.assertEqual(self.manager._pending_reservations, 0.0)
    self.assertFalse(self.manager._cv._is_owned())

  def test_model_managaer_force_reset_on_exception(self):
    """Test that force_reset clears all models from the manager."""
    model_name = "test_model"

    def dummy_loader():
      self.mock_monitor.allocate(1000.0)
      raise RuntimeError("Simulated loader exception")

    try:
      instance = self.manager.acquire_model(
          model_name, lambda: "model_instance")
      self.manager.release_model(model_name, instance)
      instance = self.manager.acquire_model(model_name, dummy_loader)
    except RuntimeError:
      self.manager._force_reset()
      self.assertTrue(len(self.manager._models[model_name]) == 0)
      self.assertEqual(self.manager._total_active_jobs, 0)
      self.assertEqual(self.manager._pending_reservations, 0.0)
      self.assertFalse(self.manager._isolation_mode)
      pass

    instance = self.manager.acquire_model(model_name, lambda: "model_instance")
    self.manager.release_model(model_name, instance)

  def test_single_model_convergence_with_fluctuations(self):
    """
    Tests that the estimator converges to the true usage with fluctuations.
    """
    model_name = "fluctuating_model"
    model_cost = 3000.0
    load_cost = 2000.0

    def loader():
      self.mock_monitor.allocate(load_cost)
      return model_name

    model = self.manager.acquire_model(model_name, loader)
    self.manager.release_model(model_name, model)
    initial_est = self.manager._estimator.get_estimate(model_name)
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

    est_cost = self.manager._estimator.get_estimate(model_name)
    self.assertAlmostEqual(est_cost, model_cost, delta=100.0)


class TestModelManagerEviction(unittest.TestCase):
  def setUp(self):
    self.mock_monitor = MockGPUMonitor(total_memory=12000.0)
    ModelManager._instance = None
    self.manager = ModelManager(
        monitor=self.mock_monitor,
        slack_percentage=0.0,
        min_data_points=1,
        eviction_cooldown_seconds=10.0,
        min_model_copies=1)

  def tearDown(self):
    self.manager.shutdown()

  def create_loader(self, name, size):
    return lambda: MockModel(name, size, self.mock_monitor)

  def test_basic_lru_eviction(self):
    self.manager._estimator.set_initial_estimate("A", 4000)
    self.manager._estimator.set_initial_estimate("B", 4000)
    self.manager._estimator.set_initial_estimate("C", 5000)

    model_a = self.manager.acquire_model("A", self.create_loader("A", 4000))
    self.manager.release_model("A", model_a)

    model_b = self.manager.acquire_model("B", self.create_loader("B", 4000))
    self.manager.release_model("B", model_b)

    key_a = list(self.manager._idle_lru.keys())[0]
    self.manager._idle_lru[key_a] = ("A", model_a, time.time() - 20.0)

    key_b = list(self.manager._idle_lru.keys())[1]
    self.manager._idle_lru[key_b] = ("B", model_b, time.time() - 20.0)

    model_a_again = self.manager.acquire_model(
        "A", self.create_loader("A", 4000))
    self.manager.release_model("A", model_a_again)

    self.manager.acquire_model("C", self.create_loader("C", 5000))

    self.assertEqual(len(self.manager.all_models("B")), 0)
    self.assertEqual(len(self.manager.all_models("A")), 1)

  def test_chained_eviction(self):
    self.manager._estimator.set_initial_estimate("big_guy", 8000)
    models = []
    for i in range(4):
      name = f"small_{i}"
      m = self.manager.acquire_model(name, self.create_loader(name, 3000))
      self.manager.release_model(name, m)
      models.append(m)

    self.manager.acquire_model("big_guy", self.create_loader("big_guy", 8000))

    self.assertTrue(models[0].deleted)
    self.assertTrue(models[1].deleted)
    self.assertTrue(models[2].deleted)
    self.assertFalse(models[3].deleted)

  def test_active_models_are_protected(self):
    self.manager._estimator.set_initial_estimate("A", 6000)
    self.manager._estimator.set_initial_estimate("B", 4000)
    self.manager._estimator.set_initial_estimate("C", 4000)

    model_a = self.manager.acquire_model("A", self.create_loader("A", 6000))
    model_b = self.manager.acquire_model("B", self.create_loader("B", 4000))
    self.manager.release_model("B", model_b)

    key_b = list(self.manager._idle_lru.keys())[0]
    self.manager._idle_lru[key_b] = ("B", model_b, time.time() - 20.0)

    def acquire_c():
      return self.manager.acquire_model("C", self.create_loader("C", 4000))

    with ThreadPoolExecutor(max_workers=1) as executor:
      future = executor.submit(acquire_c)
      model_c = future.result(timeout=2.0)

    self.assertTrue(model_b.deleted)
    self.assertFalse(model_a.deleted)

    self.manager.release_model("A", model_a)
    self.manager.release_model("C", model_c)

  def test_unknown_model_clears_memory(self):
    self.manager._estimator.set_initial_estimate("A", 2000)
    model_a = self.manager.acquire_model("A", self.create_loader("A", 2000))
    self.manager.release_model("A", model_a)
    self.assertFalse(model_a.deleted)

    self.assertTrue(self.manager._estimator.is_unknown("X"))
    model_x = self.manager.acquire_model("X", self.create_loader("X", 10000))

    self.assertTrue(model_a.deleted, "Model A should be deleted for isolation")
    self.assertEqual(len(self.manager.all_models("A")), 0)
    self.assertTrue(self.manager._isolation_mode)
    self.manager.release_model("X", model_x)

  def test_concurrent_eviction_pressure(self):
    def worker(idx):
      name = f"model_{idx % 5}"
      try:
        m = self.manager.acquire_model(name, self.create_loader(name, 4000))
        time.sleep(0.001)
        self.manager.release_model(name, m)
      except Exception:
        pass

    with ThreadPoolExecutor(max_workers=8) as executor:
      futures = [executor.submit(worker, i) for i in range(50)]
      for f in futures:
        f.result()

    curr, _, _ = self.mock_monitor.get_stats()
    expected_usage = 0
    for _, instances in self.manager._models.items():
      expected_usage += len(instances) * 4000

    self.assertAlmostEqual(curr, expected_usage)

  def test_starvation_prevention_overrides_demand(self):
    self.manager._estimator.set_initial_estimate("A", 12000)
    m_a = self.manager.acquire_model("A", self.create_loader("A", 12000))
    self.manager.release_model("A", m_a)

    def cycle_a():
      try:
        m = self.manager.acquire_model("A", self.create_loader("A", 12000))
        time.sleep(0.3)
        self.manager.release_model("A", m)
      except Exception:
        pass

    executor = ThreadPoolExecutor(max_workers=5)
    for _ in range(5):
      executor.submit(cycle_a)

    def acquire_b():
      return self.manager.acquire_model("B", self.create_loader("B", 4000))

    b_future = executor.submit(acquire_b)
    model_b = b_future.result()

    self.assertTrue(m_a.deleted)
    self.manager.release_model("B", model_b)
    executor.shutdown(wait=True)


class TestGPUMonitor(unittest.TestCase):
  def setUp(self):
    self.subprocess_patcher = patch('subprocess.check_output')
    self.mock_subprocess = self.subprocess_patcher.start()

  def tearDown(self):
    self.subprocess_patcher.stop()

  def test_init_hardware_detected(self):
    """Test that init correctly reads total memory when nvidia-smi exists."""
    self.mock_subprocess.return_value = "24576"
    monitor = GPUMonitor()
    self.assertTrue(monitor._gpu_available)
    self.assertEqual(monitor._total_memory, 24576.0)

  def test_init_hardware_missing(self):
    """Test fallback behavior when nvidia-smi is missing."""
    self.mock_subprocess.side_effect = FileNotFoundError()
    monitor = GPUMonitor(fallback_memory_mb=12000.0)
    self.assertFalse(monitor._gpu_available)
    self.assertEqual(monitor._total_memory, 12000.0)

  @patch('time.sleep')
  def test_polling_updates_stats(self, mock_sleep):
    """Test that the polling loop updates current and peak usage."""
    def subprocess_side_effect(*args, **kwargs):
      if isinstance(args[0], list) and "memory.total" in args[0][1]:
        return "16000"

      if isinstance(args[0], str) and "memory.free" in args[0]:
        return b"12000"

      raise Exception("Unexpected command")

    self.mock_subprocess.side_effect = subprocess_side_effect
    self.mock_subprocess.return_value = None

    monitor = GPUMonitor()
    monitor.start()
    time.sleep(0.1)
    curr, peak, total = monitor.get_stats()
    monitor.stop()

    self.assertEqual(curr, 4000.0)
    self.assertEqual(peak, 4000.0)
    self.assertEqual(total, 16000.0)

  def test_reset_peak(self):
    """Test that resetting peak usage works."""
    monitor = GPUMonitor()
    monitor._gpu_available = True

    with monitor._lock:
      monitor._current_usage = 2000.0
      monitor._peak_usage = 8000.0
      monitor._memory_history.append((time.time(), 8000.0))
      monitor._memory_history.append((time.time(), 2000.0))

    monitor.reset_peak()

    _, peak, _ = monitor.get_stats()
    self.assertEqual(peak, 2000.0)


class TestResourceEstimatorSolver(unittest.TestCase):
  def setUp(self):
    self.estimator = ResourceEstimator()

  @patch('apache_beam.ml.inference.model_manager.nnls')
  def test_solver_respects_min_data_points(self, mock_nnls):
    mock_nnls.return_value = ([100.0, 50.0], 0.0)

    self.estimator.add_observation({'model_A': 1}, 500)
    self.estimator.add_observation({'model_B': 1}, 500)
    self.assertFalse(mock_nnls.called)

    self.estimator.add_observation({'model_A': 1, 'model_B': 1}, 1000)
    self.assertFalse(mock_nnls.called)

    self.estimator.add_observation({'model_A': 1}, 500)
    self.assertFalse(mock_nnls.called)

    self.estimator.add_observation({'model_B': 1}, 500)
    self.assertTrue(mock_nnls.called)

  @patch('apache_beam.ml.inference.model_manager.nnls')
  def test_solver_respects_unique_model_constraint(self, mock_nnls):
    mock_nnls.return_value = ([100.0, 100.0, 50.0], 0.0)

    for _ in range(5):
      self.estimator.add_observation({'model_A': 1, 'model_B': 1}, 800)

    for _ in range(5):
      self.estimator.add_observation({'model_C': 1}, 400)

    self.assertFalse(mock_nnls.called)

    self.estimator.add_observation({'model_A': 1}, 300)
    self.estimator.add_observation({'model_B': 1}, 300)

    self.assertTrue(mock_nnls.called)


if __name__ == "__main__":
  unittest.main()
