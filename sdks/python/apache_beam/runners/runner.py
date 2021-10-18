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

# pytype: skip-file

import importlib
import logging
import os
import shelve
import shutil
import tempfile
from typing import TYPE_CHECKING
from typing import Optional

from apache_beam.options.pipeline_options import StandardOptions

if TYPE_CHECKING:
  from apache_beam import pvalue
  from apache_beam import PTransform
  from apache_beam.options.pipeline_options import PipelineOptions
  from apache_beam.pipeline import AppliedPTransform
  from apache_beam.pipeline import Pipeline
  from apache_beam.pipeline import PipelineVisitor

__all__ = ['PipelineRunner', 'PipelineState', 'PipelineResult']

_RUNNER_MAP = {
    path.rsplit('.', maxsplit=1)[-1].lower(): path
    for path in StandardOptions.ALL_KNOWN_RUNNERS
}

# Allow this alias, but don't make public.
_RUNNER_MAP['pythonrpcdirectrunner'] = (
    'apache_beam.runners.experimental'
    '.python_rpc_direct.python_rpc_direct_runner.PythonRPCDirectRunner')

_LOGGER = logging.getLogger(__name__)


def create_runner(runner_name):
  # type: (str) -> PipelineRunner

  """For internal use only; no backwards-compatibility guarantees.

  Creates a runner instance from a runner class name.

  Args:
    runner_name: Name of the pipeline runner. Possible values are listed in
      _RUNNER_MAP above.

  Returns:
    A runner object.

  Raises:
    RuntimeError: if an invalid runner name is used.
  """

  # Get the qualified runner name by using the lower case runner name. If that
  # fails try appending the name with 'runner' and check if it matches.
  # If that also fails, use the given runner name as is.
  runner_name = _RUNNER_MAP.get(
      runner_name.lower(),
      _RUNNER_MAP.get(runner_name.lower() + 'runner', runner_name))

  if '.' in runner_name:
    module, runner = runner_name.rsplit('.', 1)
    try:
      return getattr(importlib.import_module(module), runner)()
    except ImportError:
      if 'dataflow' in runner_name.lower():
        raise ImportError(
            'Google Cloud Dataflow runner not available, '
            'please install apache_beam[gcp]')
      elif 'interactive' in runner_name.lower():
        raise ImportError(
            'Interactive runner not available, '
            'please install apache_beam[interactive]')
      else:
        raise
  else:
    raise ValueError(
        'Unexpected pipeline runner: %s. Valid values are %s '
        'or the fully qualified name of a PipelineRunner subclass.' %
        (runner_name, ', '.join(StandardOptions.KNOWN_RUNNER_NAMES)))


class PipelineRunner(object):
  """A runner of a pipeline object.

  The base runner provides a run() method for visiting every node in the
  pipeline's DAG and executing the transforms computing the PValue in the node.

  A custom runner will typically provide implementations for some of the
  transform methods (ParDo, GroupByKey, Create, etc.). It may also
  provide a new implementation for clear_pvalue(), which is used to wipe out
  materialized values in order to reduce footprint.
  """

  def run(self,
          transform,  # type: PTransform
          options=None  # type: Optional[PipelineOptions]
         ):
    # type: (...) -> PipelineResult

    """Run the given transform or callable with this runner.

    Blocks until the pipeline is complete.  See also `PipelineRunner.run_async`.
    """
    result = self.run_async(transform, options)
    result.wait_until_finish()
    return result

  def run_async(self,
                transform,  # type: PTransform
                options=None  # type: Optional[PipelineOptions]
               ):
    # type: (...) -> PipelineResult

    """Run the given transform or callable with this runner.

    May return immediately, executing the pipeline in the background.
    The returned result object can be queried for progress, and
    `wait_until_finish` may be called to block until completion.
    """
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam import PTransform
    from apache_beam.pvalue import PBegin
    from apache_beam.pipeline import Pipeline
    p = Pipeline(runner=self, options=options)
    if isinstance(transform, PTransform):
      p | transform
    else:
      transform(PBegin(p))
    return p.run()

  def run_pipeline(
      self,
      pipeline,  # type: Pipeline
      options  # type: PipelineOptions
  ):
    # type: (...) -> PipelineResult

    """Execute the entire pipeline or the sub-DAG reachable from a node.

    Runners should override this method.
    """
    raise NotImplementedError

  def apply(self,
            transform,  # type: PTransform
            input,  # type: Optional[pvalue.PValue]
            options  # type: PipelineOptions
           ):
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
        return m(transform, input, options)
    raise NotImplementedError(
        'Execution of [%s] not implemented in runner %s.' % (transform, self))

  def visit_transforms(
      self,
      pipeline,  # type: Pipeline
      options  # type: PipelineOptions
  ):
    # type: (...) -> None
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.pipeline import PipelineVisitor

    class RunVisitor(PipelineVisitor):
      def __init__(self, runner):
        # type: (PipelineRunner) -> None
        self.runner = runner

      def visit_transform(self, transform_node):
        try:
          self.runner.run_transform(transform_node, options)
        except:
          _LOGGER.error('Error while visiting %s', transform_node.full_label)
          raise

    pipeline.visit(RunVisitor(self))

  def apply_PTransform(self, transform, input, options):
    # The base case of apply is to call the transform's expand.
    return transform.expand(input)

  def run_transform(self,
                    transform_node,  # type: AppliedPTransform
                    options  # type: PipelineOptions
                   ):
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
        return m(transform_node, options)
    raise NotImplementedError(
        'Execution of [%s] not implemented in runner %s.' %
        (transform_node.transform, self))

  def is_fnapi_compatible(self):
    """Whether to enable the beam_fn_api experiment by default."""
    return True


class PValueCache(object):
  """For internal use only; no backwards-compatibility guarantees.

  Local cache for arbitrary information computed for PValue objects."""
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
    return transform.full_label, tag

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
    self._cache[self.to_cache_key(transform, tag)] = value

  def get_pvalue(self, pvalue):
    """Gets the value associated with a PValue from the cache."""
    self._ensure_pvalue_has_real_producer(pvalue)
    try:
      return self._cache[self.key(pvalue)]
    except KeyError:
      if (pvalue.tag is not None and
          self.to_cache_key(pvalue.real_producer, None) in self._cache):
        # This is an undeclared, empty output of a DoFn executed
        # in the local runner before this output was referenced.
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


# FIXME: replace with PipelineState(str, enum.Enum)
class PipelineState(object):
  """State of the Pipeline, as returned by :attr:`PipelineResult.state`.

  This is meant to be the union of all the states any runner can put a
  pipeline in. Currently, it represents the values of the dataflow
  API JobState enum.
  """
  UNKNOWN = 'UNKNOWN'  # not specified by a runner, or unknown to a runner.
  STARTING = 'STARTING'  # not yet started
  STOPPED = 'STOPPED'  # paused or not yet started
  RUNNING = 'RUNNING'  # currently running
  DONE = 'DONE'  # successfully completed (terminal state)
  FAILED = 'FAILED'  # failed (terminal state)
  CANCELLED = 'CANCELLED'  # explicitly cancelled (terminal state)
  UPDATED = 'UPDATED'  # replaced by another job (terminal state)
  DRAINING = 'DRAINING'  # still processing, no longer reading data
  DRAINED = 'DRAINED'  # draining completed (terminal state)
  PENDING = 'PENDING'  # the job has been created but is not yet running.
  CANCELLING = 'CANCELLING'  # job has been explicitly cancelled and is
  # in the process of stopping
  UNRECOGNIZED = 'UNRECOGNIZED'  # the job state reported by a runner cannot be
  # interpreted by the SDK.

  @classmethod
  def is_terminal(cls, state):
    return state in [
        cls.DONE, cls.FAILED, cls.CANCELLED, cls.UPDATED, cls.DRAINED
    ]


class PipelineResult(object):
  """A :class:`PipelineResult` provides access to info about a pipeline."""
  def __init__(self, state):
    self._state = state

  @property
  def state(self):
    """Return the current state of the pipeline execution."""
    return self._state

  def wait_until_finish(self, duration=None):
    """Waits until the pipeline finishes and returns the final status.

    Args:
      duration (int): The time to wait (in milliseconds) for job to finish.
        If it is set to :data:`None`, it will wait indefinitely until the job
        is finished.

    Raises:
      IOError: If there is a persistent problem getting job
        information.
      NotImplementedError: If the runner does not support this
        operation.

    Returns:
      The final state of the pipeline, or :data:`None` on timeout.
    """
    raise NotImplementedError

  def cancel(self):
    """Cancels the pipeline execution.

    Raises:
      IOError: If there is a persistent problem getting job
        information.
      NotImplementedError: If the runner does not support this
        operation.

    Returns:
      The final state of the pipeline.
    """
    raise NotImplementedError

  def metrics(self):
    """Returns :class:`~apache_beam.metrics.metric.MetricResults` object to
    query metrics from the runner.

    Raises:
      NotImplementedError: If the runner does not support this
        operation.
    """
    raise NotImplementedError

  # pylint: disable=unused-argument
  def aggregated_values(self, aggregator_or_name):
    """Return a dict of step names to values of the Aggregator."""
    _LOGGER.warning(
        '%s does not implement aggregated_values', self.__class__.__name__)
    return {}
