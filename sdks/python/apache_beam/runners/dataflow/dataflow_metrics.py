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

# pytype: skip-file

import argparse
import logging
import numbers
import sys
from collections import defaultdict

from apache_beam.metrics.cells import DistributionData
from apache_beam.metrics.cells import DistributionResult
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.execution import MetricResult
from apache_beam.metrics.metric import MetricResults
from apache_beam.metrics.metricbase import MetricName
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions

_LOGGER = logging.getLogger(__name__)


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


# V1b3 MetricStructuredName keys to accept and copy to the MetricKey labels.
STEP_LABEL = 'step'
STRUCTURED_NAME_LABELS = set(
    ['execution_step', 'original_name', 'output_user_name'])


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
    super().__init__()
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
      raise ValueError(
          'Could not translate the internal step name %r since job graph is '
          'not available.' % internal_name)
    user_step_name = None
    if (self._job_graph and internal_name in
        self._job_graph.proto_pipeline.components.transforms.keys()):
      # Dataflow Runner v2 with portable job submission uses proto transform map
      # IDs for step names. Also PTransform.unique_name maps to user step names.
      # Hence we lookup user step names based on the proto.
      user_step_name = self._job_graph.proto_pipeline.components.transforms[
          internal_name].unique_name
    else:
      try:
        step = _get_match(
            self._job_graph.proto.steps, lambda x: x.name == internal_name)
        user_step_name = _get_match(
            step.properties.additionalProperties,
            lambda x: x.key == 'user_name').value.string_value
      except ValueError:
        pass  # Exception is handled below.
    if not user_step_name:
      raise ValueError(
          'Could not translate the internal step name %r.' % internal_name)
    return user_step_name

  def _get_metric_key(self, metric):
    """Populate the MetricKey object for a queried metric result."""
    step = ""
    name = metric.name.name  # Always extract a name
    labels = {}
    try:  # Try to extract the user step name.
      # If ValueError is thrown within this try-block, it is because of
      # one of the following:
      # 1. Unable to translate the step name. Only happening with improperly
      #   formatted job graph (unlikely), or step name not being the internal
      #   step name (only happens for unstructured-named metrics).
      # 2. Unable to unpack [step] or [namespace]; which should only happen
      #   for unstructured names.
      step = _get_match(
          metric.name.context.additionalProperties,
          lambda x: x.key == STEP_LABEL).value
      step = self._translate_step_name(step)
    except ValueError:
      pass

    namespace = "dataflow/v1b3"  # Try to extract namespace or add a default.
    try:
      namespace = _get_match(
          metric.name.context.additionalProperties,
          lambda x: x.key == 'namespace').value
    except ValueError:
      pass

    for kv in metric.name.context.additionalProperties:
      if kv.key in STRUCTURED_NAME_LABELS:
        labels[kv.key] = kv.value
    # Package everything besides namespace and name the labels as well,
    # including unmodified step names to assist in integration the exact
    # unmodified values which come from dataflow.
    return MetricKey(step, MetricName(namespace, name), labels=labels)

  def _populate_metrics(self, response, result, user_metrics=False):
    """Move metrics from response to results as MetricResults."""
    if user_metrics:
      metrics = [
          metric for metric in response.metrics if metric.name.origin == 'user'
      ]
    else:
      metrics = [
          metric for metric in response.metrics
          if metric.name.origin == 'dataflow/v1b3'
      ]

    # Get the tentative/committed versions of every metric together.
    metrics_by_name = defaultdict(lambda: {})
    for metric in metrics:
      if (metric.name.name.endswith('_MIN') or
          metric.name.name.endswith('_MAX') or
          metric.name.name.endswith('_MEAN') or
          metric.name.name.endswith('_COUNT')):
        # The Dataflow Service presents distribution metrics in two ways:
        # One way is as a single distribution object with all its fields, and
        # another way is as four different scalar metrics labeled as _MIN,
        # _MAX, _COUNT_, _MEAN.
        # TODO(pabloem) remove these when distributions are not being broken up
        #  in the service.
        # The second way is only useful for the UI, and should be ignored.
        continue
      is_tentative = [
          prop for prop in metric.name.context.additionalProperties
          if prop.key == 'tentative' and prop.value == 'true'
      ]
      tentative_or_committed = 'tentative' if is_tentative else 'committed'

      metric_key = self._get_metric_key(metric)
      if metric_key is None:
        continue
      metrics_by_name[metric_key][tentative_or_committed] = metric

    # Now we create the MetricResult elements.
    for metric_key, metric in metrics_by_name.items():
      attempted = self._get_metric_value(metric['tentative'])
      committed = self._get_metric_value(metric['committed'])
      result.append(
          MetricResult(metric_key, attempted=attempted, committed=committed))

  def _get_metric_value(self, metric):
    """Get a metric result object from a MetricUpdate from Dataflow API."""
    if metric is None:
      return None

    if metric.scalar is not None:
      return metric.scalar.integer_value
    elif metric.distribution is not None:
      dist_count = _get_match(
          metric.distribution.object_value.properties,
          lambda x: x.key == 'count').value.integer_value
      dist_min = _get_match(
          metric.distribution.object_value.properties,
          lambda x: x.key == 'min').value.integer_value
      dist_max = _get_match(
          metric.distribution.object_value.properties,
          lambda x: x.key == 'max').value.integer_value
      dist_sum = _get_match(
          metric.distribution.object_value.properties,
          lambda x: x.key == 'sum').value.integer_value
      if not dist_sum:
        # distribution metric is not meant to use on large values, but in case
        # it is, the value can overflow and become double_value, the correctness
        # of the value may not be guaranteed.
        _LOGGER.info(
            "Distribution metric sum value seems to have "
            "overflowed integer_value range, the correctness of sum or mean "
            "value may not be guaranteed: %s" % metric.distribution)
        dist_sum = int(
            _get_match(
                metric.distribution.object_value.properties,
                lambda x: x.key == 'sum').value.double_value)
      return DistributionResult(
          DistributionData(dist_sum, dist_count, dist_min, dist_max))
    else:
      return None

  def _get_metrics_from_dataflow(self, job_id=None):
    """Return cached metrics or query the dataflow service."""
    if not job_id:
      try:
        job_id = self.job_result.job_id()
      except AttributeError:
        job_id = None
    if not job_id:
      raise ValueError('Can not query metrics. Job id is unknown.')

    if self._cached_metrics:
      return self._cached_metrics

    job_metrics = self._dataflow_client.get_job_metrics(job_id)
    # If we cannot determine that the job has terminated,
    # then metrics will not change and we can cache them.
    if self.job_result and self.job_result.is_in_terminal_state():
      self._cached_metrics = job_metrics
    return job_metrics

  def all_metrics(self, job_id=None):
    """Return all user and system metrics from the dataflow service."""
    metric_results = []
    response = self._get_metrics_from_dataflow(job_id=job_id)
    self._populate_metrics(response, metric_results, user_metrics=True)
    self._populate_metrics(response, metric_results, user_metrics=False)
    return metric_results

  def query(self, filter=None):
    metric_results = []
    response = self._get_metrics_from_dataflow()
    self._populate_metrics(response, metric_results, user_metrics=True)
    return {
        self.COUNTERS: [
            elm for elm in metric_results if self.matches(filter, elm.key) and
            DataflowMetrics._is_counter(elm)
        ],
        self.DISTRIBUTIONS: [
            elm for elm in metric_results if self.matches(filter, elm.key) and
            DataflowMetrics._is_distribution(elm)
        ],
        self.GAUGES: []
    }  # TODO(pabloem): Add Gauge support for dataflow.


def main(argv):
  """Print the metric results for a the dataflow --job_id and --project.

  Instead of running an entire pipeline which takes several minutes, use this
  main method to display MetricResults for a specific --job_id and --project
  which takes only a few seconds.
  """
  # TODO(BEAM-6833): The MetricResults do not show translated step names as the
  # job_graph is not provided to DataflowMetrics.
  # Import here to avoid adding the dependency for local running scenarios.
  try:
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.runners.dataflow.internal import apiclient
  except ImportError:
    raise ImportError(
        'Google Cloud Dataflow runner not available, '
        'please install apache_beam[gcp]')
  if argv[0] == __file__:
    argv = argv[1:]
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '-j', '--job_id', type=str, help='The job id to query metrics for.')
  parser.add_argument(
      '-p',
      '--project',
      type=str,
      help='The project name to query metrics for.')
  flags = parser.parse_args(argv)

  # Get a Dataflow API client and set its project and job_id in the options.
  options = PipelineOptions()
  gcloud_options = options.view_as(GoogleCloudOptions)
  gcloud_options.project = flags.project
  dataflow_client = apiclient.DataflowApplicationClient(options)
  df_metrics = DataflowMetrics(dataflow_client)
  all_metrics = df_metrics.all_metrics(job_id=flags.job_id)
  _LOGGER.info(
      'Printing all MetricResults for %s in %s', flags.job_id, flags.project)
  for metric_result in all_metrics:
    _LOGGER.info(metric_result)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main(sys.argv)
