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

"""DirectRunner, executing on the local machine.

The DirectRunner is a runner implementation that executes the entire
graph of transformations belonging to a pipeline on the local machine.
"""

from __future__ import absolute_import

import collections
import logging

from apache_beam.metrics.execution import MetricsEnvironment
from apache_beam.runners.direct.bundle_factory import BundleFactory
from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineRunner
from apache_beam.runners.runner import PipelineState
from apache_beam.runners.runner import PValueCache
from apache_beam.utils.pipeline_options import DirectOptions
from apache_beam.utils.value_provider import RuntimeValueProvider


class DirectRunner(PipelineRunner):
  """Executes a single pipeline on the local machine."""

  def __init__(self):
    self._cache = None

  def apply_CombinePerKey(self, transform, pcoll):
    # TODO: Move imports to top. Pipeline <-> Runner dependency cause problems
    # with resolving imports when they are at top.
    # pylint: disable=wrong-import-position
    from apache_beam.runners.direct.helper_transforms import LiftedCombinePerKey
    try:
      return pcoll | LiftedCombinePerKey(
          transform.fn, transform.args, transform.kwargs)
    except NotImplementedError:
      return transform.expand(pcoll)

  def run(self, pipeline):
    """Execute the entire pipeline and returns an DirectPipelineResult."""

    # TODO: Move imports to top. Pipeline <-> Runner dependency cause problems
    # with resolving imports when they are at top.
    # pylint: disable=wrong-import-position
    from apache_beam.runners.direct.consumer_tracking_pipeline_visitor import \
      ConsumerTrackingPipelineVisitor
    from apache_beam.runners.direct.evaluation_context import EvaluationContext
    from apache_beam.runners.direct.executor import Executor
    from apache_beam.runners.direct.transform_evaluator import \
      TransformEvaluatorRegistry

    MetricsEnvironment.set_metrics_supported(True)
    logging.info('Running pipeline with DirectRunner.')
    self.consumer_tracking_visitor = ConsumerTrackingPipelineVisitor()
    pipeline.visit(self.consumer_tracking_visitor)

    evaluation_context = EvaluationContext(
        pipeline.options,
        BundleFactory(stacked=pipeline.options.view_as(DirectOptions)
                      .direct_runner_use_stacked_bundle),
        self.consumer_tracking_visitor.root_transforms,
        self.consumer_tracking_visitor.value_to_consumers,
        self.consumer_tracking_visitor.step_names,
        self.consumer_tracking_visitor.views)

    evaluation_context.use_pvalue_cache(self._cache)

    executor = Executor(self.consumer_tracking_visitor.value_to_consumers,
                        TransformEvaluatorRegistry(evaluation_context),
                        evaluation_context)
    # Start the executor. This is a non-blocking call, it will start the
    # execution in background threads and return.

    if pipeline.options:
      # DirectRunner does not support RuntimeValueProviders.
      RuntimeValueProvider.set_runtime_options(None, {})

    executor.start(self.consumer_tracking_visitor.root_transforms)
    result = DirectPipelineResult(executor, evaluation_context)

    if self._cache:
      # We are running in eager mode, block until the pipeline execution
      # completes in order to have full results in the cache.
      result.wait_until_finish()
      self._cache.finalize()

    return result

  @property
  def cache(self):
    if not self._cache:
      self._cache = BufferingInMemoryCache()
    return self._cache.pvalue_cache


class BufferingInMemoryCache(object):
  """PValueCache wrapper for buffering bundles until a PValue is fully computed.

  BufferingInMemoryCache keeps an in memory cache of
  (applied_ptransform, tag) tuples. It accepts appending to existing cache
  entries until it is finalized. finalize() will make all the existing cached
  entries visible to the underyling PValueCache in their entirety, clean the in
  memory cache and stop accepting new cache entries.
  """

  def __init__(self):
    self._cache = collections.defaultdict(list)
    self._pvalue_cache = PValueCache()
    self._finalized = False

  @property
  def pvalue_cache(self):
    return self._pvalue_cache

  def append(self, applied_ptransform, tag, elements):
    assert not self._finalized
    assert elements is not None
    self._cache[(applied_ptransform, tag)].extend(elements)

  def finalize(self):
    """Make buffered cache elements visible to the underlying PValueCache."""
    assert not self._finalized
    for key, value in self._cache.iteritems():
      applied_ptransform, tag = key
      self._pvalue_cache.cache_output(applied_ptransform, tag, value)
    self._cache = None


class DirectPipelineResult(PipelineResult):
  """A DirectPipelineResult provides access to info about a pipeline."""

  def __init__(self, executor, evaluation_context):
    super(DirectPipelineResult, self).__init__(PipelineState.RUNNING)
    self._executor = executor
    self._evaluation_context = evaluation_context

  def _is_in_terminal_state(self):
    return self._state is not PipelineState.RUNNING

  def wait_until_finish(self, duration=None):
    if not self._is_in_terminal_state():
      if duration:
        raise NotImplementedError(
            'DirectRunner does not support duration argument.')
      try:
        self._executor.await_completion()
        self._state = PipelineState.DONE
      except:  # pylint: disable=broad-except
        self._state = PipelineState.FAILED
        raise
    return self._state

  def aggregated_values(self, aggregator_or_name):
    return self._evaluation_context.get_aggregator_values(aggregator_or_name)

  def metrics(self):
    return self._evaluation_context.metrics()


class EagerRunner(DirectRunner):

  is_eager = True
