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

# pytype: skip-file

import time
from typing import Any
from typing import List
import unittest

from apache_beam.coders import FastPrimitivesCoder
from apache_beam.coders import WindowedValueCoder
from apache_beam.coders.coders import Coder
from apache_beam.runners.worker.data_sampler import DataSampler
from apache_beam.runners.worker.data_sampler import ElementSampler
from apache_beam.runners.worker.data_sampler import OutputSampler
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils import thread_pool_executor
from apache_beam.utils.windowed_value import WindowedValue

import apache_beam as beam
class SlowGenerator(beam.DoFn):
  def process(self, e):
    import time
    for i in range(100):
      yield i
      time.sleep(1)

class DataSamplerTest(unittest.TestCase):
  # def test_pipeline(self):
  #   from apache_beam.options.pipeline_options import PipelineOptions
  #   options = PipelineOptions(experiments=['enable_data_sampling'])
  #   p = beam.Pipeline(options=options)
  #   p | beam.Impulse() | beam.ParDo(SlowGenerator()) | beam.Map(print)
  #   p.run()

  def test_single_output(self):
    """Simple test for a single sample."""
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    output_sampler = data_sampler.sample_output('1', coder)
    output_sampler.sample('a')

    self.assertEqual(data_sampler.samples(), {'1': [coder.encode_nested('a')]})

  def test_multiple_outputs(self):
    """Tests that multiple PCollections have their own sampler."""
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    data_sampler.sample_output('1', coder).sample('a')
    data_sampler.sample_output('2', coder).sample('a')

    self.assertEqual(
        data_sampler.samples(), {
            '1': [coder.encode_nested('a')], '2': [coder.encode_nested('a')]
        })

  def gen_samples(self, data_sampler: DataSampler, coder: Coder):
    data_sampler.sample_output('a', coder).sample('1')
    data_sampler.sample_output('a', coder).sample('2')
    data_sampler.sample_output('b', coder).sample('3')
    data_sampler.sample_output('b', coder).sample('4')
    data_sampler.sample_output('c', coder).sample('5')
    data_sampler.sample_output('c', coder).sample('6')

  def test_sample_filters_single_pcollection_ids(self):
    """Tests the samples can be filtered based on a single pcollection id."""
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    self.gen_samples(data_sampler, coder)
    self.assertEqual(
        data_sampler.samples(pcollection_ids=['a']),
        {'a': [coder.encode_nested('1'), coder.encode_nested('2')]})

    self.assertEqual(
        data_sampler.samples(pcollection_ids=['b']),
        {'b': [coder.encode_nested('3'), coder.encode_nested('4')]})

  def test_sample_filters_multiple_pcollection_ids(self):
    """Tests the samples can be filtered based on a multiple pcollection ids."""
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    self.gen_samples(data_sampler, coder)
    self.assertEqual(
        data_sampler.samples(pcollection_ids=['a', 'c']),
        {
            'a': [coder.encode_nested('1'), coder.encode_nested('2')],
            'c': [coder.encode_nested('5'), coder.encode_nested('6')]
        })


class FakeClock:
  def __init__(self):
    self.clock = 0

  def time(self):
    return self.clock


class OutputSamplerTest(unittest.TestCase):
  def setUp(self):
    self.fake_clock = FakeClock()

  def control_time(self, new_time):
    self.fake_clock.clock = new_time

  def test_samples_first_n(self):
    """Tests that the first elements are always sampled."""
    coder = FastPrimitivesCoder()
    sampler = OutputSampler(coder)

    for i in range(15):
      sampler.sample(i)

    self.assertEqual(
        sampler.flush(), [coder.encode_nested(i) for i in range(10)])

  def test_acts_like_circular_buffer(self):
    """Tests that the buffer overwrites old samples."""
    coder = FastPrimitivesCoder()
    sampler = OutputSampler(coder, max_samples=2)

    for i in range(10):
      sampler.sample(i)

    self.assertEqual(sampler.flush(), [coder.encode_nested(i) for i in (8, 9)])

  def test_samples_every_n_secs(self):
    """Tests that the buffer overwrites old samples."""
    coder = FastPrimitivesCoder()
    sampler = OutputSampler(
        coder, max_samples=1, sample_every_sec=10, clock=self.fake_clock)

    # Always samples the first ten.
    for i in range(10):
      sampler.sample(i)
    self.assertEqual(sampler.flush(), [coder.encode_nested(9)])

    # Start at t=0
    sampler.sample(10)
    self.assertEqual(len(sampler.flush()), 0)

    # Still not over threshold yet.
    self.control_time(9)
    for i in range(100):
      sampler.sample(i)
    self.assertEqual(len(sampler.flush()), 0)

    # First sample after 10s.
    self.control_time(10)
    sampler.sample(10)
    self.assertEqual(sampler.flush(), [coder.encode_nested(10)])

    # No samples between tresholds.
    self.control_time(15)
    for i in range(100):
      sampler.sample(i)
    self.assertEqual(len(sampler.flush()), 0)

    # Second sample after 20s.
    self.control_time(20)
    sampler.sample(11)
    self.assertEqual(sampler.flush(), [coder.encode_nested(11)])

  def test_can_sample_windowed_value(self):
    """Tests that values with WindowedValueCoders are sampled wholesale."""
    data_sampler = DataSampler()
    coder = WindowedValueCoder(FastPrimitivesCoder())
    value = WindowedValue('Hello, World!', 0, [GlobalWindow()])
    data_sampler.sample_output('1', coder).sample(value)

    self.assertEqual(
        data_sampler.samples(), {'1': [coder.encode_nested(value)]})

  def test_can_sample_non_windowed_value(self):
    """Tests that windowed values with WindowedValueCoders sample only the
    value.

    This is important because the Python SDK wraps all values in a WindowedValue
    even if the coder is not a WindowedValueCoder. In this case, the value must
    be retrieved from the WindowedValue to match the correct coder.
    """
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()
    data_sampler.sample_output('1', coder).sample(
        WindowedValue('Hello, World!', 0, [GlobalWindow()]))

    self.assertEqual(
        data_sampler.samples(), {'1': [coder.encode_nested('Hello, World!')]})

  def test_serial_performance(self):
    print('test_serial_performance')
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    sampler = data_sampler.sample_output('1', coder)
    sampler._sample_timer.stop()

    warmup = 1000000
    for i in range(1000000):
      pass

    num_iterations = 10000000
    start = time.time()
    el_sampler = sampler.element_sampler()
    for i in range(num_iterations):
      el_sampler.el = i
      # sampler.sample()
    end = time.time()
    duration = end - start

    print('total time: %s secs' % duration)
    print('time per iteration: %s usecs' % (duration / num_iterations * 1e6))

  def test_serial_performance_with_generation(self):
    print('test_serial_performance_with_generation')
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    sampler = data_sampler.sample_output('1', coder)
    sampler._sample_timer.stop()

    warmup = 1000000
    for i in range(1000000):
      pass

    num_iterations = 10000000
    start = time.time()
    el_sampler = sampler.element_sampler()
    el_sampler.generation = 0
    for i in range(num_iterations):
      try:
        el_sampler.el = i#(el_sampler.generation, i)
        # el_sampler.generation += 1
        # sampler.sample()
      except:
        pass
    end = time.time()
    duration = end - start

    print('total time: %s secs' % duration)
    print('time per iteration: %s usecs' % (duration / num_iterations * 1e6))

  def test_serial_performance_with_try(self):
    print('test_serial_performance_with_try')
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()

    sampler = data_sampler.sample_output('1', coder)
    sampler._sample_timer.stop()

    warmup = 1000000
    for i in range(1000000):
      pass

    num_iterations = 10000000
    start = time.time()
    el_sampler = sampler.element_sampler()
    for i in range(num_iterations):
      try:
        sampler.el = i
        # sampler.sample()
      except:
        pass
    end = time.time()
    duration = end - start

    print('total time: %s secs' % duration)
    print('time per iteration: %s usecs' % (duration / num_iterations * 1e6))

  def test_loop_performance(self):
    print('test_loop_performance')
    num_trials = 100
    num_iterations = 10000000
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()
    sampler = data_sampler.sample_output('1', coder)
    sampler._sample_timer.stop()

    def do_trial():
      warmup = 1000000
      for i in range(1000000):
        pass

      el_sampler = sampler.element_sampler()
      start = time.time()
      a = 0
      for i in range(num_iterations):
        pass
      end = time.time()
      duration = end - start
      return duration / num_iterations * 1e9

    import sys
    toolbar_width = num_trials
    sys.stdout.write("[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['

    durations = []
    for i in range(num_trials):
      durations.append(do_trial())
      sys.stdout.write("-")
      sys.stdout.flush()

    import numpy as np
    import scipy.stats as st

    print('')
    print('mean: ', np.mean(durations), 'ns')
    print('95%: ',
          st.t.interval(
              0.95,
              df=len(durations)-1,
              loc=np.mean(durations),
              scale=st.sem(durations)))

  def test_el_sampler_performance(self):
    print('test_el_sampler_performance')
    num_trials = 100
    num_iterations = 10000000
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()
    sampler = data_sampler.sample_output('1', coder)
    sampler._sample_timer.stop()

    def do_trial():
      warmup = 1000000
      for i in range(1000000):
        pass

      el_sampler = sampler.element_sampler()
      start = time.time()
      for i in range(num_iterations):
        el_sampler.el = i
      end = time.time()
      duration = end - start
      return duration / num_iterations * 1e9

    import sys
    toolbar_width = num_trials
    sys.stdout.write("[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['

    durations = []
    for i in range(num_trials):
      durations.append(do_trial())
      sys.stdout.write("-")
      sys.stdout.flush()

    import numpy as np
    import scipy.stats as st

    print('')
    print('mean: ', np.mean(durations), 'ns')
    print('95%: ',
          st.t.interval(
              0.95,
              df=len(durations)-1,
              loc=np.mean(durations),
              scale=st.sem(durations)))

  def test_try_performance(self):
    print('test_try_performance')
    num_trials = 100
    num_iterations = 10000000

    def do_trial_no_try():
      warmup = 1000000
      for i in range(1000000):
        pass

      start = time.time()
      a = 0
      for i in range(num_iterations):
        a += 1
      end = time.time()
      duration = end - start
      return duration / num_iterations * 1e9

    def do_trial_with_try():
      warmup = 1000000
      for i in range(1000000):
        pass

      start = time.time()
      a = 0
      for i in range(num_iterations):
        try:
          a += 1
        except:
          pass
      end = time.time()
      duration = end - start
      ret = duration / num_iterations * 1e9
      if ret > 60 or ret < 30:
        print(ret)
      return ret

    import sys
    toolbar_width = num_trials
    sys.stdout.write("[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['

    no_try_durations = []
    with_try_durations = []
    for i in range(num_trials):
      no_try_durations.append(do_trial_no_try())
      with_try_durations.append(do_trial_with_try())
      sys.stdout.write("-")
      sys.stdout.flush()

    import numpy as np
    import scipy.stats as st


    print('')
    print('No try mean: ', np.mean(no_try_durations), 'ns')
    print('Yes try mean: ', np.mean(with_try_durations), 'ns')
    print('No try 95%: ',
          st.t.interval(
              0.95,
              df=len(no_try_durations)-1,
              loc=np.mean(no_try_durations),
              scale=st.sem(no_try_durations)))
    print('Yes try 95%: ',
          st.t.interval(
              0.95,
              df=len(with_try_durations)-1,
              loc=np.mean(with_try_durations),
              scale=st.sem(with_try_durations)))

    # print('total time: %s secs' % duration)
    # print('time per iteration: %s usecs' % (duration / num_iterations * 1e6))


  def test_multithreaded_performance(self):
    print('test_multithreaded_performance')
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()
    worker_thread_pool = thread_pool_executor.shared_unbounded_instance()

    num_iterations = 1000000
    num_tasks = 10
    timings = [0] * num_tasks

    num_tasks_ready = 0

    class TaskState:
      num_iterations: int = 0
      num_tasks: int = 0
      num_tasks_ready: int = 0
      ready: bool = False

    def task(task_id, task_state):
      try:
        output_sampler = data_sampler.sample_output(str(task_id), coder)
        element_sampler = output_sampler.element_sampler()
        task_state.num_tasks_ready += 1
        while task_state.num_tasks_ready < task_state.num_tasks:
          pass

        while not task_state.ready:
          pass

        for i in range(task_state.num_iterations):
          element_sampler.el = i
        task_state.num_tasks -= 1
      except Exception as e:
        print(e)

    task_state = TaskState()
    task_state.num_iterations = num_iterations
    task_state.num_tasks = num_tasks
    task_state.num_tasks_ready = 0

    for i in range(num_tasks):
      worker_thread_pool.submit(task, i, task_state)

    while task_state.num_tasks_ready != num_tasks:
      pass

    start = time.time()
    task_state.ready = True
    while task_state.num_tasks > 0:
      pass
    end = time.time()
    worker_thread_pool.shutdown()

    duration = end - start
    print('total time: %s secs' % duration)
    print('time per iteration: %s usecs' % (duration / (num_tasks * num_iterations) * 1e6))

    # for i in range(num_tasks):
    #   duration = timings[i]
    #   print('thread %s ==============' % i)
    #   print('total time: %s secs' % duration)
    #   print('time per iteration: %s usecs' % (duration / num_iterations * 1e6))
    #   print('')

  def test_contention_performance(self):
    print('test_contention_performance')
    data_sampler = DataSampler()
    coder = FastPrimitivesCoder()
    worker_thread_pool = thread_pool_executor.shared_unbounded_instance()

    num_iterations = 1000000
    num_tasks = 10
    timings = [0] * num_tasks

    num_tasks_ready = 0

    class TaskState:
      num_iterations: int = 0
      num_tasks: int = 0
      num_tasks_ready: int = 0
      ready: bool = False

    def task(task_id, task_state):
      try:
        output_sampler = data_sampler.sample_output('1', coder)
        element_sampler = output_sampler.element_sampler()
        task_state.num_tasks_ready += 1
        while task_state.num_tasks_ready < task_state.num_tasks:
          pass

        while not task_state.ready:
          pass

        for i in range(task_state.num_iterations):
          element_sampler.el = i
        task_state.num_tasks -= 1
      except Exception as e:
        print(e)

    task_state = TaskState()
    task_state.num_iterations = num_iterations
    task_state.num_tasks = num_tasks
    task_state.num_tasks_ready = 0

    for i in range(num_tasks):
      worker_thread_pool.submit(task, i, task_state)

    while task_state.num_tasks_ready != num_tasks:
      pass

    start = time.time()
    task_state.ready = True
    while task_state.num_tasks > 0:
      pass
    end = time.time()
    worker_thread_pool.shutdown()

    duration = end - start
    print('total time: %s secs' % duration)
    print('time per iteration: %s usecs' % (duration / (num_tasks * num_iterations) * 1e6))

  def test_watchdog_performance(self):
    print('test_watchdog_performance')
    data_sampler = DataSampler(sample_every_sec=0.5)
    coder = FastPrimitivesCoder()

    sampler = data_sampler.sample_output('1', coder)
    el_sampler = sampler.element_sampler()

    num_iterations = 100000000
    start = time.time()

    for i in range(num_iterations):
      el_sampler.el = i

    end = time.time()
    sampler._sample_timer.stop()
    duration = end - start

    print('total time: %s secs' % duration)
    print('time per iteration: %s usecs' % (duration / num_iterations * 1e6))

if __name__ == '__main__':
  unittest.main()
