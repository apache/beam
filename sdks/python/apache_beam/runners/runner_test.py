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

"""Unit tests for the PipelineRunner and DirectPipelineRunner classes.

Note that PipelineRunner and DirectPipelineRunner functionality is tested in all
the other unit tests. In this file we choose to test only aspects related to
caching and clearing values that are not tested elsewhere.
"""

import unittest

import apache_beam as beam

from apache_beam.internal import apiclient
from apache_beam.pipeline import Pipeline
from apache_beam.runners import create_runner
from apache_beam.runners import DataflowPipelineRunner
from apache_beam.runners import DirectPipelineRunner
import apache_beam.transforms as ptransform
from apache_beam.utils.options import PipelineOptions


class RunnerTest(unittest.TestCase):

  def test_create_runner(self):
    self.assertTrue(
        isinstance(create_runner('DirectPipelineRunner'), DirectPipelineRunner))
    self.assertTrue(
        isinstance(create_runner('DataflowPipelineRunner'),
                   DataflowPipelineRunner))
    self.assertTrue(
        isinstance(create_runner('BlockingDataflowPipelineRunner'),
                   DataflowPipelineRunner))
    self.assertRaises(ValueError, create_runner, 'xyz')

  def test_remote_runner_translation(self):
    remote_runner = DataflowPipelineRunner()
    p = Pipeline(remote_runner,
                 options=PipelineOptions([
                     '--dataflow_endpoint=ignored',
                     '--job_name=test-job',
                     '--project=test-project',
                     '--staging_location=ignored',
                     '--temp_location=/dev/null',
                     '--no_auth=True'
                 ]))

    (p | 'create' >> ptransform.Create([1, 2, 3])  # pylint: disable=expression-not-assigned
     | 'do' >> ptransform.FlatMap(lambda x: [(x, x)])
     | 'gbk' >> ptransform.GroupByKey())
    remote_runner.job = apiclient.Job(p.options)
    super(DataflowPipelineRunner, remote_runner).run(p)

  def test_no_group_by_key_directly_after_bigquery(self):
    remote_runner = DataflowPipelineRunner()
    p = Pipeline(remote_runner,
                 options=PipelineOptions([
                     '--dataflow_endpoint=ignored',
                     '--job_name=test-job',
                     '--project=test-project',
                     '--staging_location=ignored',
                     '--temp_location=/dev/null',
                     '--no_auth=True'
                 ]))
    rows = p | beam.io.Read('read',
                            beam.io.BigQuerySource('dataset.faketable'))
    with self.assertRaises(ValueError,
                           msg=('Coder for the GroupByKey operation'
                                '"GroupByKey" is not a key-value coder: '
                                'RowAsDictJsonCoder')):
      unused_invalid = rows | beam.GroupByKey()


if __name__ == '__main__':
  unittest.main()
