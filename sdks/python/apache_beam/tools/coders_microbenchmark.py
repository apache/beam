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
"""A microbenchmark for measuring performance of coders.

This runs a sequence of encode-decode operations on random inputs
to collect performance of various coders.

To evaluate coders performance we approximate the behavior
how the coders are used in PCollections: we encode and decode
a list of elements. An element can be a string, a list of integers,
a windowed value, or any other object we want a coder to process.

Run as:
  python -m apache_beam.tools.coders_microbenchmark

"""

from __future__ import absolute_import
from __future__ import print_function

import argparse
import random
import re
import string
import sys

from past.builtins import unicode

from apache_beam.coders import coders
from apache_beam.tools import utils
from apache_beam.transforms import window
from apache_beam.utils import windowed_value


def coder_benchmark_factory(coder, generate_fn):
  """Creates a benchmark that encodes and decodes a list of elements.

  Args:
    coder: coder to use to encode an element.
    generate_fn: a callable that generates an element.
  """

  class CoderBenchmark(object):
    def __init__(self, num_elements_per_benchmark):
      self._coder = coders.IterableCoder(coder)
      self._list = [generate_fn()
                    for _ in range(num_elements_per_benchmark)]

    def __call__(self):
      # Calling coder operations on a single element at a time may incur
      # unrelevant overhead. To compensate, we use a list elements.
      _ = self._coder.decode(self._coder.encode(self._list))

  CoderBenchmark.__name__ = "%s, %s" % (
      generate_fn.__name__, str(coder))

  return CoderBenchmark


def small_int():
  return random.randint(0, 127)


def large_int():
  return random.randint(sys.maxsize >> 2, sys.maxsize)


def random_string(length):
  return unicode(''.join(random.choice(
      string.ascii_letters + string.digits) for _ in range(length)))


def small_string():
  return random_string(4)


def large_string():
  return random_string(100)


def list_int(size):
  return [small_int() for _ in range(size)]


def dict_int_int(size):
  return {i: i for i in list_int(size)}


def small_list():
  return list_int(10)


def large_list():
  # Bool is the last item in FastPrimitiveCoders before pickle.
  return [bool(k) for k in list_int(1000)]


def small_tuple():
  # Benchmark a common case of 2-element tuples.
  return tuple(list_int(2))


def large_tuple():
  return tuple(large_list())


def small_dict():
  return {i: i for i in small_list()}


def large_dict():
  return {i: i for i in large_list()}


def large_iterable():
  yield 'a' * coders.coder_impl.SequenceCoderImpl._DEFAULT_BUFFER_SIZE
  for k in range(1000):
    yield k


def globally_windowed_value():
  return windowed_value.WindowedValue(
      value=small_int(),
      timestamp=12345678,
      windows=(window.GlobalWindow(),))


def random_windowed_value(num_windows):
  return windowed_value.WindowedValue(
      value=small_int(),
      timestamp=12345678,
      windows=tuple(
          window.IntervalWindow(i * 10, i * 10 + small_int())
          for i in range(num_windows)
      ))


def wv_with_one_window():
  return random_windowed_value(num_windows=1)


def wv_with_multiple_windows():
  return random_windowed_value(num_windows=32)


def run_coder_benchmarks(
    num_runs, input_size, seed, verbose, filter_regex='.*'):
  random.seed(seed)

  # TODO(BEAM-4441): Pick coders using type hints, for example:
  # tuple_coder = typecoders.registry.get_coder(typehints.Tuple[int, ...])
  benchmarks = [
      coder_benchmark_factory(
          coders.FastPrimitivesCoder(), small_int),
      coder_benchmark_factory(
          coders.FastPrimitivesCoder(), large_int),
      coder_benchmark_factory(
          coders.FastPrimitivesCoder(), small_string),
      coder_benchmark_factory(
          coders.FastPrimitivesCoder(), large_string),
      coder_benchmark_factory(
          coders.FastPrimitivesCoder(),
          small_list),
      coder_benchmark_factory(
          coders.IterableCoder(coders.FastPrimitivesCoder()),
          small_list),
      coder_benchmark_factory(
          coders.FastPrimitivesCoder(),
          large_list),
      coder_benchmark_factory(
          coders.IterableCoder(coders.FastPrimitivesCoder()),
          large_list),
      coder_benchmark_factory(
          coders.IterableCoder(coders.FastPrimitivesCoder()),
          large_iterable),
      coder_benchmark_factory(
          coders.FastPrimitivesCoder(),
          small_tuple),
      coder_benchmark_factory(
          coders.FastPrimitivesCoder(),
          large_tuple),
      coder_benchmark_factory(
          coders.FastPrimitivesCoder(),
          small_dict),
      coder_benchmark_factory(
          coders.FastPrimitivesCoder(),
          large_dict),
      coder_benchmark_factory(
          coders.WindowedValueCoder(coders.FastPrimitivesCoder()),
          wv_with_one_window),
      coder_benchmark_factory(
          coders.WindowedValueCoder(coders.FastPrimitivesCoder(),
                                    coders.IntervalWindowCoder()),
          wv_with_multiple_windows),
      coder_benchmark_factory(
          coders.WindowedValueCoder(coders.FastPrimitivesCoder(),
                                    coders.GlobalWindowCoder()),
          globally_windowed_value),
      coder_benchmark_factory(
          coders.LengthPrefixCoder(coders.FastPrimitivesCoder()),
          small_int)
  ]

  suite = [utils.BenchmarkConfig(b, input_size, num_runs) for b in benchmarks
           if re.search(filter_regex, b.__name__, flags=re.I)]
  utils.run_benchmarks(suite, verbose=verbose)


if __name__ == "__main__":

  parser = argparse.ArgumentParser()
  parser.add_argument('--filter', default='.*')
  parser.add_argument('--num_runs', default=20, type=int)
  parser.add_argument('--num_elements_per_benchmark', default=1000, type=int)
  parser.add_argument('--seed', default=42, type=int)
  options = parser.parse_args()

  utils.check_compiled("apache_beam.coders.coder_impl")

  num_runs = 20
  num_elements_per_benchmark = 1000
  seed = 42  # Fix the seed for better consistency

  run_coder_benchmarks(
      options.num_runs, options.num_elements_per_benchmark, options.seed,
      verbose=True, filter_regex=options.filter)
