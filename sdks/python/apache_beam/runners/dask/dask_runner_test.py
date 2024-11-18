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
import typing as t
import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing import test_pipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import window

try:
  import dask
  import dask.distributed as ddist

  from apache_beam.runners.dask.dask_runner import DaskOptions  # pylint: disable=ungrouped-imports
  from apache_beam.runners.dask.dask_runner import DaskRunner  # pylint: disable=ungrouped-imports
except (ImportError, ModuleNotFoundError):
  raise unittest.SkipTest('Dask must be installed to run tests.')


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
              lambda k,
              d,
              l: (k, sorted(d[k]), sorted([e[1] for e in l])),
              beam.pvalue.AsMultiMap(side),
              beam.pvalue.AsList(side),
          )
          | "second map" >> beam.Map(
              lambda k,
              d,
              l: (k[0], sorted(d[k[0]]), sorted([e[1] for e in l])),
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
