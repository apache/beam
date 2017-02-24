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

"""
DataflowRunner implementation of MetricResults. It is in charge of
responding to queries of current metrics by going to the dataflow
service.
"""

from collections import defaultdict
from warnings import warn

from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricResult
from apache_beam.metrics.metric import MetricResults
from apache_beam.metrics.metricbase import MetricName


# TODO(pabloem)(JIRA-1381) Implement this once metrics are queriable from
# dataflow service
class DataflowMetrics(MetricResults):
  """Implementation of MetricResults class for the Dataflow runner."""

  def __init__(self, dataflow_client=None, job_result=None):
    """Initialize the Dataflow metrics object.

    Args:
      dataflow_client: apiclient.DataflowApplicationClient to interact with the
        dataflow service.
      job_result: DataflowPipelineResult with the state and id information of
        the job
    """
    super(DataflowMetrics, self).__init__()
    self._dataflow_client = dataflow_client
    self.job_result = job_result
    self._queried_after_termination = False
    self._cached_metrics = None

  def _populate_metric_results(self, response):
    """Take a list of metrics, and convert it to a list of MetricResult."""
    user_metrics = [metric
                    for metric in response.metrics
                    if metric.name.origin == 'user']

    # Get the tentative/committed versions of every metric together.
    metrics_by_name = defaultdict(lambda: {})
    for metric in user_metrics:
      tentative = [prop
                   for prop in metric.name.context.additionalProperties
                   if prop.key == 'tentative' and prop.value == 'true']
      key = 'tentative' if tentative else 'committed'
      metrics_by_name[metric.name.name][key] = metric

    # Now we create the MetricResult elements.
    result = []
    for name, metric in metrics_by_name.iteritems():
      if (name.endswith('(DIST)') or
          name.endswith('[MIN]') or
          name.endswith('[MAX]') or
          name.endswith('[MEAN]') or
          name.endswith('[COUNT]')):
        warn('Distribution metrics will be ignored in the MetricsResult.query'
             'method. You can see them in the Dataflow User Interface.')
        # Distributions are not yet fully supported in this runner
        continue
      [step, namespace, name] = name.split('/')
      key = MetricKey(step, MetricName(namespace, name))
      attempted = metric['tentative'].scalar.integer_value
      committed = metric['committed'].scalar.integer_value
      result.append(MetricResult(key, attempted=attempted, committed=committed))

    return result

  def _get_metrics_from_dataflow(self):
    """Return cached metrics or query the dataflow service."""
    try:
      job_id = self.job_result.job_id()
    except AttributeError:
      job_id = None
    if not job_id:
      raise ValueError('Can not query metrics. Job id is unknown.')

    if self._cached_metrics:
      return self._cached_metrics

    job_metrics = self._dataflow_client.get_job_metrics(job_id)
    # If the job has terminated, metrics will not change and we can cache them.
    if self.job_result._is_in_terminal_state():
      self._cached_metrics = job_metrics
    return job_metrics

  def query(self, filter=None):
    response = self._get_metrics_from_dataflow()
    counters = self._populate_metric_results(response)
    # TODO(pabloem): Populate distributions once they are available.
    return {'counters': [c for c in counters if self.matches(filter, c.key)],
            'distributions': []}
