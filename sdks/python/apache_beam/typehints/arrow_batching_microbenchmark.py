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

"""A microbenchmark for pyarrow batch creation.

This microbenchmark exercises the PyarrowBatchConverter.produce_batch method
for different batch sizes.
"""

import argparse
import logging

import pyarrow as pa

from apache_beam.portability.api import schema_pb2
from apache_beam.tools import utils
from apache_beam.typehints.arrow_type_compatibility import PyarrowBatchConverter
from apache_beam.typehints.arrow_type_compatibility import beam_schema_from_arrow_schema
from apache_beam.typehints.schemas import typing_from_runner_api


def benchmark_produce_batch(size):
  batch = pa.Table.from_pydict({
      'foo': pa.array(range(size), type=pa.int64()),
      'bar': pa.array([i / size for i in range(size)], type=pa.float64()),
      'baz': pa.array([str(i) for i in range(size)], type=pa.string()),
  })
  beam_schema = beam_schema_from_arrow_schema(batch.schema)
  element_type = typing_from_runner_api(
      schema_pb2.FieldType(row_type=schema_pb2.RowType(schema=beam_schema)))

  batch_converter = PyarrowBatchConverter.from_typehints(element_type, pa.Table)
  elements = list(batch_converter.explode_batch(batch))

  def _do_benchmark():
    _ = batch_converter.produce_batch(elements)

  return _do_benchmark


def run_benchmark(
    starting_point=1, num_runs=10, num_elements_step=300, verbose=True):
  suite = [
      utils.LinearRegressionBenchmarkConfig(
          benchmark_produce_batch, starting_point, num_elements_step, num_runs)
  ]
  return utils.run_benchmarks(suite, verbose=verbose)


if __name__ == '__main__':
  logging.basicConfig()

  parser = argparse.ArgumentParser()
  parser.add_argument('--num_runs', default=10, type=int)
  parser.add_argument('--starting_point', default=50, type=int)
  parser.add_argument('--increment', default=1000, type=int)
  parser.add_argument('--verbose', default=True, type=bool)
  options = parser.parse_args()

  run_benchmark(
      options.starting_point,
      options.num_runs,
      options.increment,
      options.verbose)
