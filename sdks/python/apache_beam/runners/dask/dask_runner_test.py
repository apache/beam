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
import datetime
import inspect
import logging
import multiprocessing
import typing as t
import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing import test_pipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import window
from apache_beam.utils.windowed_value import WindowedValue

try:
  import dask
  import dask.bag as db
  import dask.distributed as ddist

  from apache_beam.runners.dask.dask_runner import DaskOptions  # pylint: disable=ungrouped-imports
  from apache_beam.runners.dask.dask_runner import DaskRunner  # pylint: disable=ungrouped-imports
  from apache_beam.runners.dask.transform_evaluator import DaskBagWindowedIterator  # pylint: disable=ungrouped-imports
except (ImportError, ModuleNotFoundError):
  raise unittest.SkipTest('Dask must be installed to run tests.')


def _consume_distributed_side_inputs(value, iterable, values, one):
  ddist.get_worker()  # Assert that side inputs are consumed in a worker task.
  return value, list(iterable), values, one


def _run_distributed_side_input_pipeline(lazy, n_workers, threads_per_worker):
  with ddist.LocalCluster(n_workers=n_workers,
                          threads_per_worker=threads_per_worker,
                          processes=False,
                          dashboard_address=None,
                          silence_logs=logging.ERROR) as cluster:
    with ddist.Client(cluster):
      args = [
          '--dask_client_address',
          cluster.scheduler_address,
          '--dask_partition_size',
          '1'
      ]
      if lazy:
        args.append('--dask_lazy_side_inputs')
      options = PipelineOptions(args)
      with test_pipeline.TestPipeline(runner=DaskRunner(),
                                      options=options) as p:
        main = p | 'main' >> beam.Create([10])
        side = p | 'side' >> beam.Create([2, 3])
        singleton = p | 'singleton' >> beam.Create([5])
        result = main | beam.Map(
            _consume_distributed_side_inputs,
            beam.pvalue.AsIter(side),
            beam.pvalue.AsList(side),
            beam.pvalue.AsSingleton(singleton))
        assert_that(result, equal_to([(10, [2, 3], [2, 3], 5)]))
      p.result.client.close()


def _run_distributed_partition_error():
  @dask.delayed
  def fail():
    raise ValueError('later partition failed')

  def consume():
    bag = db.from_delayed([dask.delayed(lambda: [1])(), fail()])
    values = iter(
        DaskBagWindowedIterator(
            bag, window.GlobalWindows(), lazy_side_inputs=True))
    assert next(values).value == 1
    try:
      next(values)
    except ValueError as exc:
      assert str(exc) == 'later partition failed'
    else:
      raise AssertionError('later partition error was swallowed')

  with ddist.LocalCluster(n_workers=1,
                          threads_per_worker=1,
                          processes=False,
                          dashboard_address=None,
                          silence_logs=logging.ERROR) as cluster:
    with ddist.Client(cluster) as client:
      client.submit(consume).result(timeout=20)


class DaskBagWindowedIteratorTest(unittest.TestCase):
  def test_default_materializes_bag_before_first_value(self):
    computed = []

    @dask.delayed
    def partition(number):
      computed.append(number)
      return [number]

    bag = db.from_delayed([partition(1), partition(2)])
    with dask.config.set(scheduler='synchronous'):
      values = iter(DaskBagWindowedIterator(bag, window.GlobalWindows()))
      self.assertEqual(next(values).value, 1)
      self.assertCountEqual(computed, [1, 2])

  def test_computes_partitions_as_values_are_consumed(self):
    computed = []

    @dask.delayed
    def partition(number):
      computed.append(number)
      return [number]

    bag = db.from_delayed([partition(1), partition(2)])
    with dask.config.set(scheduler='synchronous'):
      values = iter(
          DaskBagWindowedIterator(
              bag, window.GlobalWindows(), lazy_side_inputs=True))
      self.assertEqual(next(values).value, 1)
      self.assertEqual(computed, [1])
      self.assertEqual(next(values).value, 2)
      self.assertEqual(computed, [1, 2])
      self.assertRaises(StopIteration, next, values)

  def test_empty_partitions_and_repeated_consumption(self):
    bag = db.from_delayed([
        dask.delayed(lambda: [])(),
        dask.delayed(lambda: [1, 2])(),
    ])
    side_input = DaskBagWindowedIterator(
        bag, window.GlobalWindows(), lazy_side_inputs=True)
    with dask.config.set(scheduler='synchronous'):
      self.assertEqual([value.value for value in side_input], [1, 2])
      self.assertEqual([value.value for value in side_input], [1, 2])

  def test_partition_error_is_raised_when_reached(self):
    @dask.delayed
    def failing_partition():
      raise ValueError('partition failed')

    bag = db.from_delayed([
        dask.delayed(lambda: [1])(),
        failing_partition(),
    ])
    with dask.config.set(scheduler='synchronous'):
      values = iter(
          DaskBagWindowedIterator(
              bag, window.GlobalWindows(), lazy_side_inputs=True))
      self.assertEqual(next(values).value, 1)
      with self.assertRaisesRegex(ValueError, 'partition failed'):
        next(values)

  def test_preserves_order_and_window_conversion(self):
    existing = WindowedValue('existing', 1, (window.IntervalWindow(0, 5), ))
    bag = db.from_sequence(
        [window.TimestampedValue('timestamped', 7), existing, 'plain'],
        partition_size=1)
    with dask.config.set(scheduler='synchronous'):
      values = list(
          DaskBagWindowedIterator(
              bag, window.FixedWindows(5), lazy_side_inputs=True))
    self.assertEqual([value.value for value in values],
                     ['timestamped', 'existing', 'plain'])
    self.assertEqual(values[0].windows, (window.IntervalWindow(5, 10), ))
    self.assertIs(values[1], existing)
    self.assertEqual(values[2].windows, (window.GlobalWindow(), ))


class DaskDistributedSideInputTest(unittest.TestCase):
  def _run_with_watchdog(self, target, *args):
    process = multiprocessing.get_context('spawn').Process(
        target=target, args=args)
    process.start()
    process.join(timeout=45)
    if process.is_alive():
      process.terminate()
      process.join(timeout=5)
      if process.is_alive():
        process.kill()
        process.join()
      self.fail('distributed side-input computation timed out')
    self.assertEqual(process.exitcode, 0)

  def test_lazy_pipeline_with_one_worker_one_thread(self):
    self._run_with_watchdog(_run_distributed_side_input_pipeline, True, 1, 1)

  def test_lazy_pipeline_with_two_workers(self):
    self._run_with_watchdog(_run_distributed_side_input_pipeline, True, 2, 2)

  def test_default_pipeline_with_one_worker_one_thread(self):
    self._run_with_watchdog(_run_distributed_side_input_pipeline, False, 1, 1)

  def test_later_partition_error_in_worker(self):
    self._run_with_watchdog(_run_distributed_partition_error)


class DaskOptionsTest(unittest.TestCase):
  def test_parses_connection_timeout__defaults_to_none(self):
    default_options = PipelineOptions([])
    default_dask_options = default_options.view_as(DaskOptions)
    self.assertEqual(None, default_dask_options.timeout)

  def test_parses_connection_timeout__parses_int(self):
    conn_options = PipelineOptions('--dask_connection_timeout 12'.split())
    dask_conn_options = conn_options.view_as(DaskOptions)
    self.assertEqual(12, dask_conn_options.timeout)

  def test_parses_connection_timeout__handles_bad_input(self):
    err_options = PipelineOptions('--dask_connection_timeout foo'.split())
    dask_err_options = err_options.view_as(DaskOptions)
    self.assertEqual(dask.config.no_default, dask_err_options.timeout)

  def test_parser_destinations__agree_with_dask_client(self):
    options = PipelineOptions(
        '--dask_client_address localhost:8080 --dask_connection_timeout 600 '
        '--dask_scheduler_file foobar.cfg --dask_client_name charlie '
        '--dask_connection_limit 1024'.split())
    dask_options = options.view_as(DaskOptions)

    # Get the argument names for the constructor.
    client_args = list(inspect.signature(ddist.Client).parameters)

    for opt_name in dask_options.get_all_options(drop_default=True).keys():
      with self.subTest(f'{opt_name} in dask.distributed.Client constructor'):
        self.assertIn(opt_name, client_args)

  def test_parser_extract_bag_kwargs__deletes_dask_kwargs(self):
    options = PipelineOptions('--dask_npartitions 8'.split())
    dask_options = options.view_as(DaskOptions).get_all_options()

    self.assertIn('npartitions', dask_options)
    bag_kwargs = DaskOptions._extract_bag_kwargs(dask_options)
    self.assertNotIn('npartitions', dask_options)
    self.assertEqual(bag_kwargs, {'npartitions': 8})

  def test_parser_extract_bag_kwargs__unconfigured(self):
    options = PipelineOptions()
    dask_options = options.view_as(DaskOptions).get_all_options()

    # It's present as a default option.
    self.assertIn('npartitions', dask_options)
    bag_kwargs = DaskOptions._extract_bag_kwargs(dask_options)
    self.assertNotIn('npartitions', dask_options)
    self.assertEqual(bag_kwargs, {})


class DaskRunnerRunPipelineTest(unittest.TestCase):
  """Test class used to introspect the dask runner via a debugger."""
  def setUp(self) -> None:
    self.pipeline = test_pipeline.TestPipeline(runner=DaskRunner())

  def test_create(self):
    with self.pipeline as p:
      pcoll = p | beam.Create([1])
      assert_that(pcoll, equal_to([1]))

  def test_create_multiple(self):
    with self.pipeline as p:
      pcoll = p | beam.Create([1, 2, 3, 4])
      assert_that(pcoll, equal_to([1, 2, 3, 4]))

  def test_create_and_map(self):
    def double(x):
      return x * 2

    with self.pipeline as p:
      pcoll = p | beam.Create([1]) | beam.Map(double)
      assert_that(pcoll, equal_to([2]))

  def test_create_and_map_multiple(self):
    def double(x):
      return x * 2

    with self.pipeline as p:
      pcoll = p | beam.Create([1, 2]) | beam.Map(double)
      assert_that(pcoll, equal_to([2, 4]))

  def test_create_and_map_many(self):
    def double(x):
      return x * 2

    with self.pipeline as p:
      pcoll = p | beam.Create(list(range(1, 11))) | beam.Map(double)
      assert_that(pcoll, equal_to(list(range(2, 21, 2))))

  def test_create_map_and_groupby(self):
    def double(x):
      return x * 2, x

    with self.pipeline as p:
      pcoll = p | beam.Create([1]) | beam.Map(double) | beam.GroupByKey()
      assert_that(pcoll, equal_to([(2, [1])]))

  def test_create_map_and_groupby_multiple(self):
    def double(x):
      return x * 2, x

    with self.pipeline as p:
      pcoll = (
          p
          | beam.Create([1, 2, 1, 2, 3])
          | beam.Map(double)
          | beam.GroupByKey())
      assert_that(pcoll, equal_to([(2, [1, 1]), (4, [2, 2]), (6, [3])]))

  def test_map_with_positional_side_input(self):
    def mult_by(x, y):
      return x * y

    with self.pipeline as p:
      side = p | "side" >> beam.Create([3])
      pcoll = (
          p
          | "main" >> beam.Create([1])
          | beam.Map(mult_by, beam.pvalue.AsSingleton(side)))
      assert_that(pcoll, equal_to([3]))

  def test_map_with_keyword_side_input(self):
    def mult_by(x, y):
      return x * y

    with self.pipeline as p:
      side = p | "side" >> beam.Create([3])
      pcoll = (
          p
          | "main" >> beam.Create([1])
          | beam.Map(mult_by, y=beam.pvalue.AsSingleton(side)))
      assert_that(pcoll, equal_to([3]))

  def test_pardo_side_inputs(self):
    def cross_product(elem, sides):
      for side in sides:
        yield elem, side

    with self.pipeline as p:
      main = p | "main" >> beam.Create(["a", "b", "c"])
      side = p | "side" >> beam.Create(["x", "y"])
      assert_that(
          main | beam.FlatMap(cross_product, beam.pvalue.AsList(side)),
          equal_to([
              ("a", "x"),
              ("b", "x"),
              ("c", "x"),
              ("a", "y"),
              ("b", "y"),
              ("c", "y"),
          ]),
      )

  def test_pardo_side_input_dependencies(self):
    with self.pipeline as p:
      inputs = [p | beam.Create([None])]
      for k in range(1, 10):
        inputs.append(
            inputs[0]
            | beam.ParDo(
                ExpectingSideInputsFn(f"Do{k}"),
                *[beam.pvalue.AsList(inputs[s]) for s in range(1, k)],
            ))

  def test_pardo_side_input_sparse_dependencies(self):
    with self.pipeline as p:
      inputs = []

      def choose_input(s):
        return inputs[(389 + s * 5077) % len(inputs)]

      for k in range(20):
        num_inputs = int((k * k % 16)**0.5)
        if num_inputs == 0:
          inputs.append(p | f"Create{k}" >> beam.Create([f"Create{k}"]))
        else:
          inputs.append(
              choose_input(0)
              | beam.ParDo(
                  ExpectingSideInputsFn(f"Do{k}"),
                  *[
                      beam.pvalue.AsList(choose_input(s))
                      for s in range(1, num_inputs)
                  ],
              ))

  @unittest.expectedFailure
  def test_pardo_windowed_side_inputs(self):
    with self.pipeline as p:
      # Now with some windowing.
      pcoll = (
          p
          | beam.Create(list(range(10)))
          | beam.Map(lambda t: window.TimestampedValue(t, t)))
      # Intentionally choosing non-aligned windows to highlight the transition.
      main = pcoll | "WindowMain" >> beam.WindowInto(window.FixedWindows(5))
      side = pcoll | "WindowSide" >> beam.WindowInto(window.FixedWindows(7))
      res = main | beam.Map(
          lambda x, s: (x, sorted(s)), beam.pvalue.AsList(side))
      assert_that(
          res,
          equal_to([
              # The window [0, 5) maps to the window [0, 7).
              (0, list(range(7))),
              (1, list(range(7))),
              (2, list(range(7))),
              (3, list(range(7))),
              (4, list(range(7))),
              # The window [5, 10) maps to the window [7, 14).
              (5, list(range(7, 10))),
              (6, list(range(7, 10))),
              (7, list(range(7, 10))),
              (8, list(range(7, 10))),
              (9, list(range(7, 10))),
          ]),
          label="windowed",
      )

  def test_flattened_side_input(self, with_transcoding=True):
    with self.pipeline as p:
      main = p | "main" >> beam.Create([None])
      side1 = p | "side1" >> beam.Create([("a", 1)])
      side2 = p | "side2" >> beam.Create([("b", 2)])
      if with_transcoding:
        # Also test non-matching coder types (transcoding required)
        third_element = [("another_type")]
      else:
        third_element = [("b", 3)]
      side3 = p | "side3" >> beam.Create(third_element)
      side = (side1, side2) | beam.Flatten()
      assert_that(
          main | beam.Map(lambda a, b: (a, b), beam.pvalue.AsDict(side)),
          equal_to([(None, {
              "a": 1, "b": 2
          })]),
          label="CheckFlattenAsSideInput",
      )
      assert_that(
          (side, side3) | "FlattenAfter" >> beam.Flatten(),
          equal_to([("a", 1), ("b", 2)] + third_element),
          label="CheckFlattenOfSideInput",
      )

  def test_gbk_side_input(self):
    with self.pipeline as p:
      main = p | "main" >> beam.Create([None])
      side = p | "side" >> beam.Create([("a", 1)]) | beam.GroupByKey()
      assert_that(
          main | beam.Map(lambda a, b: (a, b), beam.pvalue.AsDict(side)),
          equal_to([(None, {
              "a": [1]
          })]),
      )

  def test_multimap_side_input(self):
    with self.pipeline as p:
      main = p | "main" >> beam.Create(["a", "b"])
      side = p | "side" >> beam.Create([("a", 1), ("b", 2), ("a", 3)])
      assert_that(
          main
          | beam.Map(
              lambda k, d: (k, sorted(d[k])), beam.pvalue.AsMultiMap(side)),
          equal_to([("a", [1, 3]), ("b", [2])]),
      )

  def test_multimap_multiside_input(self):
    # A test where two transforms in the same stage consume the same PCollection
    # twice as side input.
    with self.pipeline as p:
      main = p | "main" >> beam.Create(["a", "b"])
      side = p | "side" >> beam.Create([("a", 1), ("b", 2), ("a", 3)])
      assert_that(
          main
          | "first map" >> beam.Map(
              lambda k, d, l: (k, sorted(d[k]), sorted([e[1] for e in l])),
              beam.pvalue.AsMultiMap(side),
              beam.pvalue.AsList(side),
          )
          | "second map" >> beam.Map(
              lambda k, d, l:
              (k[0], sorted(d[k[0]]), sorted([e[1] for e in l])),
              beam.pvalue.AsMultiMap(side),
              beam.pvalue.AsList(side),
          ),
          equal_to([("a", [1, 3], [1, 2, 3]), ("b", [2], [1, 2, 3])]),
      )

  def test_multimap_side_input_type_coercion(self):
    with self.pipeline as p:
      main = p | "main" >> beam.Create(["a", "b"])
      # The type of this side-input is forced to Any (overriding type
      # inference). Without type coercion to Tuple[Any, Any], the usage of this
      # side-input in AsMultiMap() below should fail.
      side = p | "side" >> beam.Create([("a", 1), ("b", 2),
                                        ("a", 3)]).with_output_types(t.Any)
      assert_that(
          main
          | beam.Map(
              lambda k, d: (k, sorted(d[k])), beam.pvalue.AsMultiMap(side)),
          equal_to([("a", [1, 3]), ("b", [2])]),
      )

  def test_pardo_unfusable_side_inputs__one(self):
    def cross_product(elem, sides):
      for side in sides:
        yield elem, side

    with self.pipeline as p:
      pcoll = p | "Create1" >> beam.Create(["a", "b"])
      assert_that(
          pcoll |
          "FlatMap1" >> beam.FlatMap(cross_product, beam.pvalue.AsList(pcoll)),
          equal_to([("a", "a"), ("a", "b"), ("b", "a"), ("b", "b")]),
          label="assert_that1",
      )

  def test_pardo_unfusable_side_inputs__two(self):
    def cross_product(elem, sides):
      for side in sides:
        yield elem, side

    with self.pipeline as p:
      pcoll = p | "Create2" >> beam.Create(["a", "b"])

      derived = ((pcoll, )
                 | beam.Flatten()
                 | beam.Map(lambda x: (x, x))
                 | beam.GroupByKey()
                 | "Unkey" >> beam.Map(lambda kv: kv[0]))
      assert_that(
          pcoll | "FlatMap2" >> beam.FlatMap(
              cross_product, beam.pvalue.AsList(derived)),
          equal_to([("a", "a"), ("a", "b"), ("b", "a"), ("b", "b")]),
          label="assert_that2",
      )

  def test_groupby_with_fixed_windows(self):
    def double(x):
      return x * 2, x

    def add_timestamp(pair):
      delta = datetime.timedelta(seconds=pair[1] * 60)
      now = (datetime.datetime.now() + delta).timestamp()
      return window.TimestampedValue(pair, now)

    with self.pipeline as p:
      pcoll = (
          p
          | beam.Create([1, 2, 1, 2, 3])
          | beam.Map(double)
          | beam.WindowInto(window.FixedWindows(60))
          | beam.Map(add_timestamp)
          | beam.GroupByKey())
      assert_that(pcoll, equal_to([(2, [1, 1]), (4, [2, 2]), (6, [3])]))

  def test_groupby_string_keys(self):
    with self.pipeline as p:
      pcoll = (
          p
          | beam.Create([('a', 1), ('a', 2), ('b', 3), ('b', 4)])
          | beam.GroupByKey())
      assert_that(pcoll, equal_to([('a', [1, 2]), ('b', [3, 4])]))


class ExpectingSideInputsFn(beam.DoFn):
  def __init__(self, name):
    self._name = name

  def default_label(self):
    return self._name

  def process(self, element, *side_inputs):
    if not all(list(s) for s in side_inputs):
      raise ValueError(f"Missing data in side input {side_inputs}")
    yield self._name


if __name__ == '__main__':
  unittest.main()
