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

"""Unit tests for the PipelineRunner and DirectRunner classes.

Note that PipelineRunner and DirectRunner functionality is tested in all
the other unit tests. In this file we choose to test only aspects related to
caching and clearing values that are not tested elsewhere.
"""

from datetime import datetime
import json
import unittest

import hamcrest as hc

import apache_beam as beam

from apache_beam.internal import apiclient
from apache_beam.pipeline import Pipeline
from apache_beam.runners import create_runner
from apache_beam.runners import DataflowRunner
from apache_beam.runners import DirectRunner
from apache_beam.runners import TestDataflowRunner
import apache_beam.transforms as ptransform
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.utils.pipeline_options import PipelineOptions

from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.cells import DistributionResult
from apache_beam.metrics.execution import MetricResult
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.metricbase import MetricName


class RunnerTest(unittest.TestCase):
  default_properties = [
      '--dataflow_endpoint=ignored',
      '--job_name=test-job',
      '--project=test-project',
      '--staging_location=ignored',
      '--temp_location=/dev/null',
      '--no_auth=True']

  def test_create_runner(self):
    self.assertTrue(
        isinstance(create_runner('DirectRunner'), DirectRunner))
    self.assertTrue(
        isinstance(create_runner('DataflowRunner'),
                   DataflowRunner))
    self.assertTrue(
        isinstance(create_runner('BlockingDataflowRunner'),
                   DataflowRunner))
    self.assertTrue(
        isinstance(create_runner('TestDataflowRunner'),
                   TestDataflowRunner))
    self.assertRaises(ValueError, create_runner, 'xyz')
    # TODO(BEAM-1185): Remove when all references to PipelineRunners are gone.
    self.assertTrue(
        isinstance(create_runner('DirectPipelineRunner'), DirectRunner))
    self.assertTrue(
        isinstance(create_runner('DataflowPipelineRunner'),
                   DataflowRunner))
    self.assertTrue(
        isinstance(create_runner('BlockingDataflowPipelineRunner'),
                   DataflowRunner))

  def test_remote_runner_translation(self):
    remote_runner = DataflowRunner()
    p = Pipeline(remote_runner,
                 options=PipelineOptions(self.default_properties))

    (p | ptransform.Create([1, 2, 3])  # pylint: disable=expression-not-assigned
     | 'Do' >> ptransform.FlatMap(lambda x: [(x, x)])
     | ptransform.GroupByKey())
    remote_runner.job = apiclient.Job(p.options)
    super(DataflowRunner, remote_runner).run(p)

  def test_remote_runner_display_data(self):
    remote_runner = DataflowRunner()
    p = Pipeline(remote_runner,
                 options=PipelineOptions(self.default_properties))

    # TODO: Should not subclass ParDo. Switch to PTransform as soon as
    # composite transforms support display data.
    class SpecialParDo(beam.ParDo):
      def __init__(self, fn, now):
        super(SpecialParDo, self).__init__(fn)
        self.fn = fn
        self.now = now

      # Make this a list to be accessible within closure
      def display_data(self):
        return {'asubcomponent': self.fn,
                'a_class': SpecialParDo,
                'a_time': self.now}

    class SpecialDoFn(beam.DoFn):
      def display_data(self):
        return {'dofn_value': 42}

      def process(self):
        pass

    now = datetime.now()
    # pylint: disable=expression-not-assigned
    (p | ptransform.Create([1, 2, 3, 4, 5])
     | 'Do' >> SpecialParDo(SpecialDoFn(), now))

    remote_runner.job = apiclient.Job(p.options)
    super(DataflowRunner, remote_runner).run(p)
    job_dict = json.loads(str(remote_runner.job))
    steps = [step
             for step in job_dict['steps']
             if len(step['properties'].get('display_data', [])) > 0]
    step = steps[0]
    disp_data = step['properties']['display_data']
    disp_data = sorted(disp_data, key=lambda x: x['namespace']+x['key'])
    nspace = SpecialParDo.__module__+ '.'
    expected_data = [{'type': 'TIMESTAMP', 'namespace': nspace+'SpecialParDo',
                      'value': DisplayDataItem._format_value(now, 'TIMESTAMP'),
                      'key': 'a_time'},
                     {'type': 'STRING', 'namespace': nspace+'SpecialParDo',
                      'value': nspace+'SpecialParDo', 'key': 'a_class',
                      'shortValue': 'SpecialParDo'},
                     {'type': 'INTEGER', 'namespace': nspace+'SpecialDoFn',
                      'value': 42, 'key': 'dofn_value'}]
    expected_data = sorted(expected_data, key=lambda x: x['namespace']+x['key'])
    self.assertEqual(len(disp_data), 3)
    self.assertEqual(disp_data, expected_data)

  def test_direct_runner_metrics(self):
    from apache_beam.metrics.metric import Metrics

    class MyDoFn(beam.DoFn):
      def start_bundle(self):
        count = Metrics.counter(self.__class__, 'bundles')
        count.inc()

      def finish_bundle(self):
        count = Metrics.counter(self.__class__, 'finished_bundles')
        count.inc()

      def process(self, element):
        count = Metrics.counter(self.__class__, 'elements')
        count.inc()
        distro = Metrics.distribution(self.__class__, 'element_dist')
        distro.update(element)
        return [element]

    runner = DirectRunner()
    p = Pipeline(runner,
                 options=PipelineOptions(self.default_properties))
    # pylint: disable=expression-not-assigned
    (p | ptransform.Create([1, 2, 3, 4, 5])
     | 'Do' >> beam.ParDo(MyDoFn()))
    result = p.run()
    result.wait_until_finish()
    metrics = result.metrics().query()
    namespace = '{}.{}'.format(MyDoFn.__module__,
                               MyDoFn.__name__)

    hc.assert_that(
        metrics['counters'],
        hc.contains_inanyorder(
            MetricResult(
                MetricKey('Do', MetricName(namespace, 'elements')),
                5, 5),
            MetricResult(
                MetricKey('Do', MetricName(namespace, 'bundles')),
                1, 1),
            MetricResult(
                MetricKey('Do', MetricName(namespace, 'finished_bundles')),
                1, 1)))
    hc.assert_that(
        metrics['distributions'],
        hc.contains_inanyorder(
            MetricResult(
                MetricKey('Do', MetricName(namespace, 'element_dist')),
                DistributionResult(DistributionData(15, 5, 1, 5)),
                DistributionResult(DistributionData(15, 5, 1, 5)))))

  def test_no_group_by_key_directly_after_bigquery(self):
    remote_runner = DataflowRunner()
    p = Pipeline(remote_runner,
                 options=PipelineOptions([
                     '--dataflow_endpoint=ignored',
                     '--job_name=test-job',
                     '--project=test-project',
                     '--staging_location=ignored',
                     '--temp_location=/dev/null',
                     '--no_auth=True'
                 ]))
    rows = p | beam.io.Read(beam.io.BigQuerySource('dataset.faketable'))
    with self.assertRaises(ValueError,
                           msg=('Coder for the GroupByKey operation'
                                '"GroupByKey" is not a key-value coder: '
                                'RowAsDictJsonCoder')):
      unused_invalid = rows | beam.GroupByKey()


if __name__ == '__main__':
  unittest.main()
