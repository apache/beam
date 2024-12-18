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

# pytype: skip-file

import logging

from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.execution import MetricKey
from apache_beam.metrics.metric import MetricName

_LOGGER = logging.getLogger(__name__)


def from_monitoring_infos(monitoring_info_list, user_metrics_only=False):
  """Groups MonitoringInfo objects into counters, distributions, gauges and
  string sets

  Args:
    monitoring_info_list: An iterable of MonitoringInfo objects.
    user_metrics_only: If true, includes user metrics only.
  Returns:
    A tuple containing three dictionaries: counters, distributions, gauges and
    string set, respectively. Each dictionary contains (MetricKey, metric
    result) pairs.
  """
  counters = {}
  distributions = {}
  gauges = {}
  string_sets = {}
  bounded_tries = {}

  for mi in monitoring_info_list:
    if (user_metrics_only and not monitoring_infos.is_user_monitoring_info(mi)):
      continue

    try:
      key = _create_metric_key(mi)
    except ValueError as e:
      _LOGGER.debug(str(e))
      continue
    metric_result = (monitoring_infos.extract_metric_result_map_value(mi))

    if monitoring_infos.is_counter(mi):
      counters[key] = metric_result
    elif monitoring_infos.is_distribution(mi):
      distributions[key] = metric_result
    elif monitoring_infos.is_gauge(mi):
      gauges[key] = metric_result
    elif monitoring_infos.is_string_set(mi):
      string_sets[key] = metric_result
    elif monitoring_infos.is_bounded_trie(mi):
      bounded_tries[key] = metric_result

  return counters, distributions, gauges, string_sets, bounded_tries


def _create_metric_key(monitoring_info):
  step_name = monitoring_infos.get_step_name(monitoring_info)
  namespace, name = monitoring_infos.parse_namespace_and_name(monitoring_info)
  return MetricKey(step_name, MetricName(namespace, name))
