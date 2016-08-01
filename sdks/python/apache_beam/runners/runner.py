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

"""PipelineRunner, an abstract base runner object."""

from __future__ import absolute_import

import logging
import os
import shelve
import shutil
import tempfile


_KNOWN_DIRECT_RUNNERS = ('DirectPipelineRunner', 'DiskCachedPipelineRunner',
                         'EagerPipelineRunner')
_KNOWN_DATAFLOW_RUNNERS = ('DataflowPipelineRunner',
                           'BlockingDataflowPipelineRunner')


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
  if runner_name in _KNOWN_DIRECT_RUNNERS:
    runner_name = 'apache_beam.runners.direct_runner.' + runner_name
  elif runner_name in _KNOWN_DATAFLOW_RUNNERS:
    runner_name = 'apache_beam.runners.dataflow_runner.' + runner_name

  if '.' in runner_name:
    module, runner = runner_name.rsplit('.', 1)
    return getattr(__import__(module, {}, {}, [runner], -1), runner)()
  else:
    raise ValueError(
        'Unexpected pipeline runner: %s. Valid values are %s '
        'or the fully qualified name of a PipelineRunner subclass.' % (
            runner_name,
            ', '.join(_KNOWN_DIRECT_RUNNERS + _KNOWN_DATAFLOW_RUNNERS)))


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

  def run(self, pipeline):
    """Execute the entire pipeline or the sub-DAG reachable from a node."""

    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.pipeline import PipelineVisitor

    class RunVisitor(PipelineVisitor):

      def __init__(self, runner):
        self.runner = runner

      def visit_transform(self, transform_node):
        try:
          self.runner.run_transform(transform_node)
        except:
          logging.error('Error while visiting %s', transform_node.full_label)
          raise

    pipeline.visit(RunVisitor(self))

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
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.pipeline import PipelineVisitor

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

  def __init__(self, use_disk_backed_cache=False):
    # Cache of values computed while a runner executes a pipeline. This is a
    # dictionary of PValues and their computed values. Note that in principle
    # the runner could contain PValues from several pipelines without clashes
    # since a PValue is associated with one and only one pipeline. The keys of
    # the dictionary are tuple of PValue instance addresses obtained using id()
    # and tag names converted to strings.

    self._use_disk_backed_cache = use_disk_backed_cache
    if use_disk_backed_cache:
      self._tempdir = tempfile.mkdtemp()
      self._cache = shelve.open(os.path.join(self._tempdir, 'shelve'))
    else:
      self._cache = {}

  def __del__(self):
    if self._use_disk_backed_cache:
      self._cache.close()
      shutil.rmtree(self._tempdir)

  def __len__(self):
    return len(self._cache)

  def to_cache_key(self, transform, tag):
    return str((id(transform), tag))

  def _ensure_pvalue_has_real_producer(self, pvalue):
    """Ensure the passed-in PValue has the real_producer attribute.

    Args:
      pvalue: A PValue instance whose cached value is requested.

    During the runner's execution only the results of the primitive transforms
    are cached. Whenever we are looking for a PValue that is the output of a
    composite transform we need to find the output of its rightmost transform
    part.
    """
    if not hasattr(pvalue, 'real_producer'):
      real_producer = pvalue.producer
      while real_producer.parts:
        real_producer = real_producer.parts[-1]
      pvalue.real_producer = real_producer

  def is_cached(self, pobj):
    from apache_beam.pipeline import AppliedPTransform
    if isinstance(pobj, AppliedPTransform):
      transform = pobj
      tag = None
    else:
      self._ensure_pvalue_has_real_producer(pobj)
      transform = pobj.real_producer
      tag = pobj.tag
    return self.to_cache_key(transform, tag) in self._cache

  def cache_output(self, transform, tag_or_value, value=None):
    if value is None:
      value = tag_or_value
      tag = None
    else:
      tag = tag_or_value
    self._cache[
        self.to_cache_key(transform, tag)] = [value, transform.refcounts[tag]]

  def get_pvalue(self, pvalue):
    """Gets the value associated with a PValue from the cache."""
    self._ensure_pvalue_has_real_producer(pvalue)
    try:
      value_with_refcount = self._cache[self.key(pvalue)]
      value_with_refcount[1] -= 1
      logging.debug('PValue computed by %s (tag %s): refcount: %d => %d',
                    pvalue.real_producer.full_label, self.key(pvalue)[1],
                    value_with_refcount[1] + 1, value_with_refcount[1])
      if value_with_refcount[1] <= 0:
        self.clear_pvalue(pvalue)
      return value_with_refcount[0]
    except KeyError:
      if (pvalue.tag is not None
          and self.to_cache_key(pvalue.real_producer, None) in self._cache):
        # This is an undeclared, empty side output of a DoFn executed
        # in the local runner before this side output referenced.
        return []
      else:
        raise

  def get_unwindowed_pvalue(self, pvalue):
    return [v.value for v in self.get_pvalue(pvalue)]

  def clear_pvalue(self, pvalue):
    """Removes a PValue from the cache."""
    if self.is_cached(pvalue):
      del self._cache[self.key(pvalue)]

  def key(self, pobj):
    self._ensure_pvalue_has_real_producer(pobj)
    return self.to_cache_key(pobj.real_producer, pobj.tag)


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

  # pylint: disable=unused-argument
  def aggregated_values(self, aggregator_or_name):
    """Return a dict of step names to values of the Aggregator."""
    logging.warn('%s does not implement aggregated_values',
                 self.__class__.__name__)
    return {}
