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
from typing import TYPE_CHECKING
from typing import Iterable
from typing import Optional

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import TypeOptions
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners.pipeline_utils import group_by_key_input_visitor
from apache_beam.transforms import environments

if TYPE_CHECKING:
  from apache_beam import pvalue
  from apache_beam import PTransform
  from apache_beam.pipeline import Pipeline

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


def create_runner(runner_name: str) -> 'PipelineRunner':
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
  def run(
      self,
      transform: 'PTransform',
      options: Optional[PipelineOptions] = None) -> 'PipelineResult':
    """Run the given transform or callable with this runner.

    Blocks until the pipeline is complete.  See also `PipelineRunner.run_async`.
    """
    result = self.run_async(transform, options)
    result.wait_until_finish()
    return result

  def run_async(
      self,
      transform: 'PTransform',
      options: Optional[PipelineOptions] = None) -> 'PipelineResult':
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

  def run_portable_pipeline(
      self, pipeline: beam_runner_api_pb2.Pipeline,
      options: PipelineOptions) -> 'PipelineResult':
    """Execute the entire pipeline.

    Runners should override this method.
    """
    raise NotImplementedError

  def default_environment(
      self, options: PipelineOptions) -> environments.Environment:
    """Returns the default environment that should be used for this runner.

    Runners may override this method to provide alternative environments.
    """
    return environments.Environment.from_options(
        options.view_as(PortableOptions))

  def run_pipeline(
      self, pipeline: 'Pipeline', options: PipelineOptions) -> 'PipelineResult':
    """Execute the entire pipeline or the sub-DAG reachable from a node.
    """
    pipeline.visit(
        group_by_key_input_visitor(
            not options.view_as(TypeOptions).allow_non_deterministic_key_coders)
    )

    # TODO: https://github.com/apache/beam/issues/19168
    # portable runner specific default
    if options.view_as(SetupOptions).sdk_location == 'default':
      options.view_as(SetupOptions).sdk_location = 'container'

    return self.run_portable_pipeline(
        pipeline.to_runner_api(
            default_environment=self.default_environment(options)),
        options)

  def apply(
      self,
      transform: 'PTransform',
      input: Optional['pvalue.PValue'],
      options: PipelineOptions):
    # TODO(robertwb): Remove indirection once internal references are fixed.
    return self.apply_PTransform(transform, input, options)

  def apply_PTransform(self, transform, input, options):
    # TODO(robertwb): Remove indirection once internal references are fixed.
    return transform.expand(input)

  def is_fnapi_compatible(self):
    """Whether to enable the beam_fn_api experiment by default."""
    return True

  def check_requirements(
      self,
      pipeline_proto: beam_runner_api_pb2.Pipeline,
      supported_requirements: Iterable[str]):
    """Check that this runner can satisfy all pipeline requirements."""

    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.runners.portability.fn_api_runner import translations
    supported_requirements = set(supported_requirements)
    for requirement in pipeline_proto.requirements:
      if requirement not in supported_requirements:
        raise ValueError(
            'Unable to run pipeline with requirement: %s' % requirement)
    for transform in pipeline_proto.components.transforms.values():
      if transform.spec.urn == common_urns.primitives.TEST_STREAM.urn:
        if common_urns.primitives.TEST_STREAM.urn not in supported_requirements:
          raise NotImplementedError(transform.spec.urn)
      elif transform.spec.urn in translations.PAR_DO_URNS:
        payload = beam_runner_api_pb2.ParDoPayload.FromString(
            transform.spec.payload)
        for timer in payload.timer_family_specs.values():
          if timer.time_domain not in (
              beam_runner_api_pb2.TimeDomain.EVENT_TIME,
              beam_runner_api_pb2.TimeDomain.PROCESSING_TIME):
            raise NotImplementedError(timer.time_domain)

  def default_pickle_library_override(self):
    """Default pickle library, can be overridden by runner implementation."""
    return None


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
  RESOURCE_CLEANING_UP = 'RESOURCE_CLEANING_UP'  # job's resources are being
  # cleaned up
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

  def wait_until_finish(self, duration=None):  # pylint: disable=unused-argument
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
    if not PipelineState.is_terminal(self._state):
      raise NotImplementedError()

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
    raise NotImplementedError()

  def metrics(self):
    """Returns :class:`~apache_beam.metrics.metric.MetricResults` object to
    query metrics from the runner.

    Raises:
      NotImplementedError: If the runner does not support this
        operation.
    """
    raise NotImplementedError()

  # pylint: disable=unused-argument
  def aggregated_values(self, aggregator_or_name):
    """Return a dict of step names to values of the Aggregator."""
    _LOGGER.warning(
        '%s does not implement aggregated_values', self.__class__.__name__)
    return {}
