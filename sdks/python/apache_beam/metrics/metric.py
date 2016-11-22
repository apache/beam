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
User-facing classes for Metrics API.

The classes in this file allow users to define and use metrics to be collected
and displayed as part of their pipeline execution.

Available classes:
- Metric - Base class of a metrics object. Provides very basic methods.
- Counter - Base class of a counter metric object. It provides methods to
    increment/decrement a count variable accross a pipeline execution.
- Distribution - Base class of a distribution metric object. It provides
    methods to keep track of statistics about the distribution of a variable.

- Metrics - This class lets pipeline and transform writers create and access
    metric objects such as counters, distributions, etc.
"""
import collections
import inspect

from apache_beam.metrics.internal import MetricsEnvironment
from apache_beam.metrics.internal import MetricName


class Metric(object):
  """ Base class of a metric object.
  """


class Counter(Metric):
  """ Base class of a Counter metric object
  """
  def inc(self, n=1):
    raise NotImplementedError

  def dec(self, n=1):
    self.inc(-n)


class Distribution(Metric):
  """ Base class of a Distribution metric object
  """
  def update(self, value):
    raise NotImplementedError


class Metrics(object):
  """ Lets users create/access metric objects during pipeline execution.
  """
  @staticmethod
  def get_namespace(namespace):
    if inspect.isclass(namespace):
      return '{}.{}'.format(namespace.__module__, namespace.__name__)
    elif isinstance(namespace, str):
      return namespace
    else:
      raise ValueError('Unknown namespace type')

  @staticmethod
  def counter(namespace, name):
    namespace = Metrics.get_namespace(namespace)
    return Metrics.DelegatingCounter(MetricName(namespace, name))

  @staticmethod
  def distribution(namespace, name):
    namespace = Metrics.get_namespace(namespace)
    return Metrics.DelegatingDistribution(MetricName(namespace, name))

  class DelegatingCounter(Counter):
    def __init__(self, metric_name):
      self.metric_name = metric_name

    def inc(self, n=1):
      container = MetricsEnvironment.current_container()
      if container is not None:
        container.get_Counter(self.metric_name).inc(n)

  class DelegatingDistribution(Distribution):
    def __init__(self, metric_name):
      self.metric_name = metric_name

    def update(self, value):
      container = MetricsEnvironment.current_container()
      if container is not None:
        container.get_Distribution(self.metric_name).update(value)


class MetricResults(object):
  @staticmethod
  def matches(metric, filter):
    #TODO
    return True

  def query(self):
    raise NotImplementedError


MetricResult = collections.namedtuple(
    'MetricResult', 'metric step committed attempted')


class MetricsFilter(object):
  # TODO implement
  pass
