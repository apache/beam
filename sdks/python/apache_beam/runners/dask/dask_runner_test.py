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
import inspect
import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing import test_pipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

try:
  from apache_beam.runners.dask.dask_runner import DaskOptions
  from apache_beam.runners.dask.dask_runner import DaskRunner
  import dask
  import dask.distributed as ddist
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

  def test_create_and_map(self):
    def double(x):
      return x * 2

    with self.pipeline as p:
      pcoll = p | beam.Create([1]) | beam.Map(double)
      assert_that(pcoll, equal_to([2]))

  def test_create_map_and_groupby(self):
    def double(x):
      return x * 2, x

    with self.pipeline as p:
      pcoll = p | beam.Create([1]) | beam.Map(double) | beam.GroupByKey()
      assert_that(pcoll, equal_to([(2, [1])]))


if __name__ == '__main__':
  unittest.main()
