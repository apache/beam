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

"""InProcessPipelineRunner, executing on the local machine."""

from __future__ import absolute_import

import collections
import logging

from apache_beam.runners.inprocess.bundle_factory import BundleFactory
from apache_beam.runners.inprocess.consumer_tracking_pipeline_visitor import ConsumerTrackingPipelineVisitor
from apache_beam.runners.inprocess.inprocess_evaluation_context import InProcessEvaluationContext
from apache_beam.runners.inprocess.inprocess_executor import InProcessExecutor
from apache_beam.runners.inprocess.transform_evaluator import TransformEvaluatorRegistry
from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineRunner
from apache_beam.runners.runner import PipelineState
from apache_beam.runners.runner import PValueCache


class InProcessPipelineRunner(PipelineRunner):
  """Executes a single pipeline on the local machine."""

  def __init__(self):
    self._cache = None

  def run(self, pipeline):
    """Execute the entire pipeline and returns an InProcessPipelineResult."""
    logging.info('Running pipeline with InProcessPipelineRunner.')
    self.visitor = ConsumerTrackingPipelineVisitor()
    pipeline.visit(self.visitor)

    evaluation_context = InProcessEvaluationContext(
        pipeline.options,
        BundleFactory(),
        self.visitor.root_transforms,
        self.visitor.value_to_consumers,
        self.visitor.step_names,
        self.visitor.views)

    evaluation_context.use_pvalue_cache(self._cache)

    executor = InProcessExecutor(self.visitor.value_to_consumers,
                                 TransformEvaluatorRegistry(evaluation_context),
                                 evaluation_context)
    # Start the executor. This is a non-blocking call, it will start the
    # execution in background threads and return.
    executor.start(self.visitor.root_transforms)
    result = InProcessPipelineResult(executor, evaluation_context)

    # TODO(altay): If blocking:
    # Block until the pipeline completes. This call will return after the
    # pipeline was fully terminated (successfully or with a failure).
    result.await_completion()

    if self._cache:
      self._cache.finalize()

    return result

  @property
  def cache(self):
    if not self._cache:
      self._cache = InProcessBufferingInMemoryCache()
    return self._cache.pvalue_cache

  def apply(self, transform, input):  # pylint: disable=redefined-builtin
    """Runner callback for a pipeline.apply call."""
    return transform.apply(input)


class InProcessBufferingInMemoryCache(object):
  """PValueCache wrapper for buffering bundles until a PValue is fully computed.

  InProcessBufferingInMemoryCache keeps an in memory cache of
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


class InProcessPipelineResult(PipelineResult):
  """A InProcessPipelineResult provides access to info about a pipeline."""

  def __init__(self, executor, evaluation_context):
    super(InProcessPipelineResult, self).__init__(PipelineState.RUNNING)
    self._executor = executor
    self._evaluation_context = evaluation_context

  def _is_in_terminal_state(self):
    return self._state is not PipelineState.RUNNING

  def await_completion(self):
    if not self._is_in_terminal_state():
      try:
        self._executor.await_completion()
        self._state = PipelineState.DONE
      except:  # pylint: disable=broad-except
        self._state = PipelineState.FAILED
        raise
    return self._state

  def aggregated_values(self, aggregator_or_name):
    return self._evaluation_context.get_aggregator_values(aggregator_or_name)
