# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""PipelineRunner, an abstract base runner object."""

from __future__ import absolute_import

import copy


def create_runner(runner_name):
  """Creates a runner instance from a runner class name.

  Args:
    runner_name: Name of the pipeline runner. Possible values are:
      DirectPipelineRunner, DataflowPipelineRunner and
      BlockingDataflowPipelineRunner.

  Returns:
    A runner object.

  Raises:
    RuntimeError: if an invalid runner name is used.
  """
  # pylint: disable=g-import-not-at-top
  if runner_name == 'DirectPipelineRunner':
    import google.cloud.dataflow.runners.direct_runner
    return google.cloud.dataflow.runners.direct_runner.DirectPipelineRunner()
  elif runner_name in ('DataflowPipelineRunner',
                       'BlockingDataflowPipelineRunner'):
    import google.cloud.dataflow.runners.dataflow_runner
    return google.cloud.dataflow.runners.dataflow_runner.DataflowPipelineRunner(
        blocking=runner_name == 'BlockingDataflowPipelineRunner')
  else:
    raise RuntimeError(
        'Unexpected pipeline runner: %s. Valid values are '
        'DirectPipelineRunner, DataflowPipelineRunner, or '
        'BlockingDataflowPipelineRunner.' % runner_name)


class PipelineRunner(object):
  """A runner of a pipeline object.

  The base runner provides a run() method for visiting every node in the
  pipeline's DAG and executing the transforms computing the PValue in the node.
  It also provides a clear() method for visiting every node and clearing out
  the values contained in PValue objects produced during a run.

  A custom runner will typically provide implementations for some of the
  transform methods (ParDo, GroupByKey, Create, etc.). It may also
  provide a new implementation for clear_pvalue(), which is used to wipe out
  materialized values in order to reduce footprint.
  """

  def run(self, pipeline, node=None):
    """Execute the entire pipeline or the sub-DAG reachable from a node."""

    # Imported here to avoid circular dependencies.
    # pylint: disable=g-import-not-at-top
    from google.cloud.dataflow.pipeline import PipelineVisitor

    class RunVisitor(PipelineVisitor):

      def __init__(self, runner):
        self.runner = runner

      def visit_transform(self, transform_node):
        self.runner.run_transform(transform_node)

    pipeline.visit(RunVisitor(self), node=node)
    return PipelineResult(state=PipelineState.DONE)

  def clear(self, pipeline, node=None):
    """Clear all nodes or nodes reachable from node of materialized values.

    Args:
      pipeline: Pipeline object containing PValues to be cleared.
      node: Optional node in the Pipeline processing DAG. If specified only
        nodes reachable from this node will be cleared (ancestors of the node).

    This method is not intended (for now) to be called by users of Runner
    objects. It is a hook for future layers on top of the current programming
    model to control how much of the previously computed values are kept
    around. Presumably an interactivity layer will use it. The simplest way
    to change the behavior would be to define a runner that overwrites the
    clear_pvalue() method since this method (runner.clear) will visit all
    relevant nodes and call clear_pvalue on them.

    """

    # Imported here to avoid circular dependencies.
    # pylint: disable=g-import-not-at-top
    from google.cloud.dataflow.pipeline import PipelineVisitor

    class ClearVisitor(PipelineVisitor):

      def __init__(self, runner):
        self.runner = runner

      def visit_value(self, value, _):
        self.runner.clear_pvalue(value)

    pipeline.visit(ClearVisitor(self), node=node)

  def apply(self, transform, input):
    """Runner callback for a pipeline.apply call.

    Args:
      transform: the transform to apply.
      input: transform's input (typically a PCollection).

    A concrete implementation of the Runner class may want to do custom
    pipeline construction for a given transform.  To override the behavior
    for a transform class Xyz, implement an apply_Xyz method with this same
    signature.
    """
    for cls in transform.__class__.mro():
      m = getattr(self, 'apply_%s' % cls.__name__, None)
      if m:
        return m(transform, input)
    raise NotImplementedError(
        'Execution of [%s] not implemented in runner %s.' % (transform, self))

  def apply_PTransform(self, transform, input):
    # The base case of apply is to call the transform's apply.
    return transform.apply(input)

  def run_transform(self, transform_node):
    """Runner callback for a pipeline.run call.

    Args:
      transform_node: transform node for the transform to run.

    A concrete implementation of the Runner class must implement run_Abc for
    some class Abc in the method resolution order for every non-composite
    transform Xyz in the pipeline.
    """
    for cls in transform_node.transform.__class__.mro():
      m = getattr(self, 'run_%s' % cls.__name__, None)
      if m:
        return m(transform_node)
    raise NotImplementedError(
        'Execution of [%s] not implemented in runner %s.' % (
            transform_node.transform, self))


class PValueCache(object):
  """Local cache for arbitrary information computed for PValue objects."""

  def __init__(self):
    # Cache of values computed while a runner executes a pipeline. This is a
    # dictionary of PValues and their computed values. Note that in principle
    # the runner could contain PValues from several pipelines without clashes
    # since a PValue is associated with one and only one pipeline. The keys of
    # the dictionary are PValue instance addresses obtained using id().
    self._cache = {}

  def __len__(self):
    return len(self._cache)

  def _get_pvalue_with_real_producer(self, pvalue):
    """Returns a pvalue with the real producer for the passed in pvalue.

    Args:
      pvalue: A PValue instance whose cached value is requested.

    Returns:
      A pvalue containing the real producer.

    During the runner's execution only the results of the primitive transforms
    are cached. Whenever we are looking for a PValue that is the output of a
    composite transform we need to find the output of its rightmost transform
    part.
    """
    real_producer = pvalue.producer
    while real_producer.parts:
      real_producer = real_producer.parts[-1]
    if real_producer != pvalue.producer:
      pvalue = copy.copy(pvalue)
      pvalue.producer = real_producer
    return pvalue

  def is_cached(self, pobj):
    # Import here to avoid circular dependencies.
    from google.cloud.dataflow.pipeline import AppliedPTransform
    if isinstance(pobj, AppliedPTransform):
      transform = pobj
    else:
      pobj = self._get_pvalue_with_real_producer(pobj)
      transform = pobj.producer
    return (id(transform), None) in self._cache

  def cache_output(self, transform, tag_or_value, value=None):
    if value is None:
      value = tag_or_value
      tag = None
    else:
      tag = tag_or_value
    self._cache[id(transform), tag] = value

  def get_pvalue(self, pvalue):
    """Gets the value associated with a PValue from the cache."""
    pvalue = self._get_pvalue_with_real_producer(pvalue)
    try:
      return self._cache[self.key(pvalue)]
    except KeyError:
      if pvalue.tag is not None and (id(pvalue.producer), None) in self._cache:
        # This is an undeclared, empty side output of a DoFn executed
        # in the local runner before this side output referenced.
        return []
      else:
        raise

  def clear_pvalue(self, pvalue):
    """Removes a PValue from the cache."""
    pvalue = self._get_pvalue_with_real_producer(pvalue)
    if self.is_cached(pvalue):
      del self._cache[self.key(pvalue)]

  def key(self, pobj):
    return id(pobj.producer), pobj.tag


class PipelineState(object):
  """State of the Pipeline, as returned by PipelineResult.current_state().

  This is meant to be the union of all the states any runner can put a
  pipeline in.  Currently, it represents the values of the dataflow
  API JobState enum.
  """
  UNKNOWN = 'UNKNOWN'  # not specified
  STOPPED = 'STOPPED'  # paused or not yet started
  RUNNING = 'RUNNING'  # currently running
  DONE = 'DONE'  # successfully completed (terminal state)
  FAILED = 'FAILED'  # failed (terminal state)
  CANCELLED = 'CANCELLED'  # explicitly cancelled (terminal state)
  UPDATED = 'UPDATED'  # replaced by another job (terminal state)
  DRAINING = 'DRAINING'  # still processing, no longer reading data
  DRAINED = 'DRAINED'  # draining completed (terminal state)


class PipelineResult(object):
  """A PipelineResult provides access to info about a pipeline."""

  def __init__(self, state):
    self._state = state

  def current_state(self):
    """Return the current state of running the pipeline."""
    return self._state
