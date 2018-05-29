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

import numbers
from collections import defaultdict

from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.cells import DistributionResult
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricResult
from apache_beam.metrics.metric import MetricResults
from apache_beam.metrics.metricbase import MetricName


def _get_match(proto, filter_fn):
  """Finds and returns the first element that matches a query.

  If no element matches the query, it throws ValueError.
  If more than one element matches the query, it returns only the first.
  """
  query = [elm for elm in proto if filter_fn(elm)]
  if len(query) == 0:
    raise ValueError('Could not find element')
  elif len(query) > 1:
    raise ValueError('Too many matches')

  return query[0]


class DataflowMetrics(MetricResults):
  """Implementation of MetricResults class for the Dataflow runner."""

  def __init__(self, dataflow_client=None, job_result=None, job_graph=None):
    """Initialize the Dataflow metrics object.

    Args:
      dataflow_client: apiclient.DataflowApplicationClient to interact with the
        dataflow service.
      job_result: DataflowPipelineResult with the state and id information of
        the job.
      job_graph: apiclient.Job instance to be able to translate between internal
        step names (e.g. "s2"), and user step names (e.g. "split").
    """
    super(DataflowMetrics, self).__init__()
    self._dataflow_client = dataflow_client
    self.job_result = job_result
    self._queried_after_termination = False
    self._cached_metrics = None
    self._job_graph = job_graph

  @staticmethod
  def _is_counter(metric_result):
    return isinstance(metric_result.attempted, numbers.Number)

  @staticmethod
  def _is_distribution(metric_result):
    return isinstance(metric_result.attempted, DistributionResult)

  def _translate_step_name(self, internal_name):
    """Translate between internal step names (e.g. "s1") and user step names."""
    if not self._job_graph:
      raise ValueError('Could not translate the internal step name.')

    try:
      step = _get_match(self._job_graph.proto.steps,
                        lambda x: x.name == internal_name)
      user_step_name = _get_match(
          step.properties.additionalProperties,
          lambda x: x.key == 'user_name').value.string_value
    except ValueError:
      raise ValueError('Could not translate the internal step name.')
    return user_step_name

  def _get_metric_key(self, metric):
    """Populate the MetricKey object for a queried metric result."""
    try:
      # If ValueError is thrown within this try-block, it is because of
      # one of the following:
      # 1. Unable to translate the step name. Only happening with improperly
      #   formatted job graph (unlikely), or step name not being the internal
      #   step name (only happens for unstructured-named metrics).
      # 2. Unable to unpack [step] or [namespace]; which should only happen
      #   for unstructured names.
      step = _get_match(metric.name.context.additionalProperties,
                        lambda x: x.key == 'step').value
      step = self._translate_step_name(step)
      namespace = _get_match(metric.name.context.additionalProperties,
                             lambda x: x.key == 'namespace').value
      name = metric.name.name
    except ValueError:
      return None

    return MetricKey(step, MetricName(namespace, name))

  def _populate_metric_results(self, response):
    """Take a list of metrics, and convert it to a list of MetricResult."""
    user_metrics = [metric
                    for metric in response.metrics
                    if metric.name.origin == 'user']

    # Get the tentative/committed versions of every metric together.
    metrics_by_name = defaultdict(lambda: {})
    for metric in user_metrics:
      if (metric.name.name.endswith('[MIN]') or
          metric.name.name.endswith('[MAX]') or
          metric.name.name.endswith('[MEAN]') or
          metric.name.name.endswith('[COUNT]')):
        # The Dataflow Service presents distribution metrics in two ways:
        # One way is as a single distribution object with all its fields, and
        # another way is as four different scalar metrics labeled as [MIN],
        # [MAX], [COUNT], [MEAN].
        # TODO(pabloem) remove these when distributions are not being broken up
        #  in the service.
        # The second way is only useful for the UI, and should be ignored.
        continue
      is_tentative = [prop
                      for prop in metric.name.context.additionalProperties
                      if prop.key == 'tentative' and prop.value == 'true']
      tentative_or_committed = 'tentative' if is_tentative else 'committed'

      metric_key = self._get_metric_key(metric)
      if metric_key is None:
        continue
      metrics_by_name[metric_key][tentative_or_committed] = metric

    # Now we create the MetricResult elements.
    result = []
    for metric_key, metric in metrics_by_name.iteritems():
      attempted = self._get_metric_value(metric['tentative'])
      committed = self._get_metric_value(metric['committed'])
      if attempted is None or committed is None:
        continue
      result.append(MetricResult(metric_key,
                                 attempted=attempted,
                                 committed=committed))

    return result

  def _get_metric_value(self, metric):
    """Get a metric result object from a MetricUpdate from Dataflow API."""
    if metric is None:
      return None

    if metric.scalar is not None:
      return metric.scalar.integer_value
    elif metric.distribution is not None:
      dist_count = _get_match(metric.distribution.object_value.properties,
                              lambda x: x.key == 'count').value.integer_value
      dist_min = _get_match(metric.distribution.object_value.properties,
                            lambda x: x.key == 'min').value.integer_value
      dist_max = _get_match(metric.distribution.object_value.properties,
                            lambda x: x.key == 'max').value.integer_value
      dist_sum = _get_match(metric.distribution.object_value.properties,
                            lambda x: x.key == 'sum').value.integer_value
      return DistributionResult(
          DistributionData(
              dist_sum, dist_count, dist_min, dist_max))
    else:
      return None

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
    if self.job_result.is_in_terminal_state():
      self._cached_metrics = job_metrics
    return job_metrics

  def query(self, filter=None):
    response = self._get_metrics_from_dataflow()
    metric_results = self._populate_metric_results(response)
    return {'counters': [elm for elm in metric_results
                         if self.matches(filter, elm.key)
                         and DataflowMetrics._is_counter(elm)],
            'distributions': [elm for elm in metric_results
                              if self.matches(filter, elm.key)
                              and DataflowMetrics._is_distribution(elm)],
            'gauges': []}  # TODO(pabloem): Add Gauge support for dataflow.
