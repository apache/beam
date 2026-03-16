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

"""Tests for pipeline_options_context module.

These tests verify that the contextvar-based approach properly isolates
pipeline options across threads and async tasks, preventing race conditions.
"""

import asyncio
import threading
import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options_context import get_pipeline_options
from apache_beam.options.pipeline_options_context import scoped_pipeline_options


class PipelineConstructionOptionsTest(unittest.TestCase):
  def test_nested_scoping(self):
    """Test that nested scopes properly restore outer options."""
    outer_options = PipelineOptions(['--runner=DirectRunner'])
    inner_options = PipelineOptions(['--runner=DataflowRunner'])

    with scoped_pipeline_options(outer_options):
      self.assertIs(get_pipeline_options(), outer_options)

      with scoped_pipeline_options(inner_options):
        self.assertIs(get_pipeline_options(), inner_options)

      self.assertIs(get_pipeline_options(), outer_options)

    self.assertIsNone(get_pipeline_options())

  def test_exception_in_scope_restores_options(self):
    """Test that options are restored even when an exception is raised."""
    outer_options = PipelineOptions(['--runner=DirectRunner'])
    inner_options = PipelineOptions(['--runner=DataflowRunner'])

    with scoped_pipeline_options(outer_options):
      try:
        with scoped_pipeline_options(inner_options):
          self.assertIs(get_pipeline_options(), inner_options)
          raise ValueError("Test exception")
      except ValueError:
        pass

      self.assertIs(get_pipeline_options(), outer_options)

  def test_different_threads_see_their_own_isolated_options(self):
    """Test that different threads see their own isolated options."""
    results = {}
    errors = []
    barrier = threading.Barrier(2)

    def thread_worker(thread_id, runner_name):
      try:
        options = PipelineOptions([f'--runner={runner_name}'])
        with scoped_pipeline_options(options):
          barrier.wait(timeout=5)

          current = get_pipeline_options()
          results[thread_id] = current.get_all_options()['runner']
          import time
          time.sleep(0.01)

          current_after = get_pipeline_options()
          if current_after is not current:
            errors.append(
                f"Thread {thread_id}: options changed during execution")
      except Exception as e:
        errors.append(f"Thread {thread_id}: {e}")

    thread1 = threading.Thread(target=thread_worker, args=(1, 'DirectRunner'))
    thread2 = threading.Thread(target=thread_worker, args=(2, 'DataflowRunner'))

    thread1.start()
    thread2.start()

    thread1.join(timeout=5)
    thread2.join(timeout=5)

    self.assertEqual(errors, [])
    self.assertEqual(results[1], 'DirectRunner')
    self.assertEqual(results[2], 'DataflowRunner')

  def test_asyncio_task_isolation(self):
    """Test that different asyncio tasks see their own isolated options."""
    async def async_worker(
        task_id, runner_name, results, ready_event, go_event):
      options = PipelineOptions([f'--runner={runner_name}'])
      with scoped_pipeline_options(options):
        ready_event.set()
        await go_event.wait()
        current = get_pipeline_options()
        results[task_id] = current.get_all_options()['runner']
        await asyncio.sleep(0.01)
        current_after = get_pipeline_options()
        assert current_after is current, \
            f"Task {task_id}: options changed during execution"

    async def run_test():
      results = {}
      ready_events = [asyncio.Event() for _ in range(2)]
      go_event = asyncio.Event()

      task1 = asyncio.create_task(
          async_worker(1, 'DirectRunner', results, ready_events[0], go_event))
      task2 = asyncio.create_task(
          async_worker(2, 'DataflowRunner', results, ready_events[1], go_event))

      # Wait for both tasks to be ready
      await asyncio.gather(*[e.wait() for e in ready_events])
      # Signal all tasks to proceed
      go_event.set()

      await asyncio.gather(task1, task2)
      return results

    results = asyncio.run(run_test())
    self.assertEqual(results[1], 'DirectRunner')
    self.assertEqual(results[2], 'DataflowRunner')

  def test_transform_sees_pipeline_options(self):
    """Test that a transform can access pipeline options during expand()."""
    class OptionsCapturingTransform(beam.PTransform):
      """Transform that captures pipeline options during expand()."""
      def __init__(self, expected_job_name):
        self.expected_job_name = expected_job_name
        self.captured_options = None

      def expand(self, pcoll):
        # This runs during pipeline construction
        self.captured_options = get_pipeline_options()
        return pcoll | beam.Map(lambda x: x)

    options = PipelineOptions(['--job_name=test_job_123'])
    transform = OptionsCapturingTransform('test_job_123')

    with beam.Pipeline(options=options) as p:
      _ = p | beam.Create([1, 2, 3]) | transform

    # Verify the transform saw the correct options
    self.assertIsNotNone(transform.captured_options)
    self.assertEqual(
        transform.captured_options.get_all_options()['job_name'],
        'test_job_123')

  def test_coder_sees_correct_options_during_run(self):
    """Test that coders see correct pipeline options during proto conversion.

    This tests the run path where as_deterministic_coder() is called during
    to_runner_api() proto conversion.
    """
    from apache_beam.coders import coders
    from apache_beam.utils import shared

    errors = []

    class WeakRefDict(dict):
      pass

    class TestKey:
      def __init__(self, value):
        self.value = value

      def __eq__(self, other):
        return isinstance(other, TestKey) and self.value == other.value

      def __hash__(self):
        return hash(self.value)

    class OptionsCapturingKeyCoder(coders.Coder):
      """Coder that captures pipeline options in as_deterministic_coder."""
      shared_handle = shared.Shared()

      def encode(self, value):
        return str(value.value).encode('utf-8')

      def decode(self, encoded):
        return TestKey(encoded.decode('utf-8'))

      def is_deterministic(self):
        return False

      def as_deterministic_coder(self, step_label, error_message=None):
        opts = get_pipeline_options()
        if opts is not None:
          results = OptionsCapturingKeyCoder.shared_handle.acquire(WeakRefDict)
          job_name = opts.get_all_options().get('job_name')
          results['Worker1'] = job_name
        return self

    beam.coders.registry.register_coder(TestKey, OptionsCapturingKeyCoder)

    results = OptionsCapturingKeyCoder.shared_handle.acquire(WeakRefDict)

    job_name = 'gbk_job'
    options = PipelineOptions([f'--job_name={job_name}'])

    with beam.Pipeline(options=options) as p:
      _ = (
          p
          | beam.Create([(TestKey(1), 'a'), (TestKey(2), 'b')])
          | beam.GroupByKey())

    self.assertEqual(errors, [], f"Errors occurred: {errors}")
    self.assertEqual(
        results.get('Worker1'),
        job_name,
        f"Worker1 saw wrong options: {results}")
    self.assertFalse(get_pipeline_options() == options)

  def test_barrier_inside_default_type_hints(self):
    """Test race condition detection with barrier inside default_type_hints.

    This test reliably detects race conditions because:
    1. Both threads start pipeline construction simultaneously
    2. Inside default_type_hints (which is called during Pipeline.apply()),
       both threads hit a barrier and wait for each other
    3. At this point, BOTH threads are inside scoped_pipeline_options
    4. When they continue, they read options - with a global var, they'd see
       the wrong values because the last thread to set options would win
    """

    results = {}
    errors = []
    inner_barrier = threading.Barrier(2)

    class BarrierTransform(beam.PTransform):
      """Transform that synchronizes threads INSIDE default_type_hints."""
      def __init__(self, worker_id, results_dict, barrier):
        self.worker_id = worker_id
        self.results_dict = results_dict
        self.barrier = barrier

      def expand(self, pcoll):
        return pcoll | beam.Map(lambda x: x)

      def default_type_hints(self):
        self.barrier.wait(timeout=5)
        opts = get_pipeline_options()
        if opts is not None:
          job_name = opts.get_all_options().get('job_name')
          if self.worker_id not in self.results_dict:
            self.results_dict[self.worker_id] = job_name

        return super().default_type_hints()

    def construct_pipeline(worker_id):
      try:
        job_name = f'barrier_job_{worker_id}'
        options = PipelineOptions([f'--job_name={job_name}'])
        transform = BarrierTransform(worker_id, results, inner_barrier)

        with beam.Pipeline(options=options) as p:
          _ = p | beam.Create([1, 2, 3]) | transform

      except Exception as e:
        import traceback
        errors.append(f"Worker {worker_id}: {e}\n{traceback.format_exc()}")

    thread1 = threading.Thread(
        target=construct_pipeline, args=(1, ))
    thread2 = threading.Thread(
        target=construct_pipeline, args=(2, ))

    thread1.start()
    thread2.start()

    thread1.join(timeout=10)
    thread2.join(timeout=10)

    self.assertEqual(errors, [], f"Errors occurred: {errors}")
    self.assertEqual(
        results.get(1),
        'barrier_job_1',
        f"Worker 1 saw wrong options: {results}")
    self.assertEqual(
        results.get(2),
        'barrier_job_2',
        f"Worker 2 saw wrong options: {results}")


class PipelineSubclassApplyTest(unittest.TestCase):
  def test_subclass_apply_called_on_recursive_paths(self):
    """Test that Pipeline subclass overrides of apply() are respected.

    _apply_internal's recursive calls must go through self.apply(), not
    self._apply_internal(), so that subclass interceptions are not skipped.
    """
    apply_calls = []

    class TrackingPipeline(beam.Pipeline):
      def apply(self, transform, pvalueish=None, label=None):
        apply_calls.append(label or transform.label)
        return super().apply(transform, pvalueish, label)

    options = PipelineOptions(['--job_name=subclass_test'])
    with TrackingPipeline(options=options) as p:
      # "my_label" >> transform creates a _NamedPTransform, which triggers
      # two recursive apply() calls: one to unwrap _NamedPTransform, and
      # one to handle the label argument.
      _ = p | beam.Create([1, 2, 3]) | "my_label" >> beam.Map(lambda x: x)

    # beam.Create goes through apply() once (no recursion).
    # "my_label" >> Map triggers: apply(_NamedPTransform) -> apply(Map,
    # label="my_label") -> apply(Map). That's 3 calls through apply().
    # Total: 1 (Create) + 3 (Map) = 4 calls minimum.
    map_calls = [c for c in apply_calls if c == 'my_label' or c == 'Map']
    self.assertGreaterEqual(
        len(map_calls),
        3,
        f"Expected at least 3 apply() calls for the Map transform "
        f"(NamedPTransform unwrap + label handling + final), "
        f"got {len(map_calls)}. All calls: {apply_calls}")


if __name__ == '__main__':
  unittest.main()
