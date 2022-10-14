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

"""DaskRunner, executing remote jobs on Dask.distributed.

The DaskRunner is a runner implementation that executes a graph of
transformations across processes and workers via Dask distributed's
scheduler.
"""
import argparse
import dataclasses
import typing as t

from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import AppliedPTransform
from apache_beam.pipeline import PipelineVisitor
from apache_beam.runners.dask.overrides import dask_overrides
from apache_beam.runners.dask.transform_evaluator import TRANSLATIONS
from apache_beam.runners.dask.transform_evaluator import NoOp
from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner
from apache_beam.runners.runner import PipelineResult
from apache_beam.runners.runner import PipelineState
from apache_beam.utils.interactive_utils import is_in_notebook


class DaskOptions(PipelineOptions):
  @staticmethod
  def _parse_timeout(candidate):
    try:
      return int(candidate)
    except (TypeError, ValueError):
      import dask
      return dask.config.no_default

  @classmethod
  def _add_argparse_args(cls, parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        '--dask_client_address',
        dest='address',
        type=str,
        default=None,
        help='Address of a dask Scheduler server. Will default to a '
        '`dask.LocalCluster()`.')
    parser.add_argument(
        '--dask_connection_timeout',
        dest='timeout',
        type=DaskOptions._parse_timeout,
        help='Timeout duration for initial connection to the scheduler.')
    parser.add_argument(
        '--dask_scheduler_file',
        dest='scheduler_file',
        type=str,
        default=None,
        help='Path to a file with scheduler information if available.')
    # TODO(alxr): Add options for security.
    parser.add_argument(
        '--dask_client_name',
        dest='name',
        type=str,
        default=None,
        help='Gives the client a name that will be included in logs generated '
        'on the scheduler for matters relating to this client.')
    parser.add_argument(
        '--dask_connection_limit',
        dest='connection_limit',
        type=int,
        default=512,
        help='The number of open comms to maintain at once in the connection '
        'pool.')


@dataclasses.dataclass
class DaskRunnerResult(PipelineResult):
  from dask import distributed

  client: distributed.Client
  futures: t.Sequence[distributed.Future]

  def __post_init__(self):
    super().__init__(PipelineState.RUNNING)

  def wait_until_finish(self, duration=None) -> str:
    try:
      if duration is not None:
        # Convert milliseconds to seconds
        duration /= 1000
      self.client.wait_for_workers(timeout=duration)
      self.client.gather(self.futures, errors='raise')
      self._state = PipelineState.DONE
    except:  # pylint: disable=broad-except
      self._state = PipelineState.FAILED
      raise
    return self._state

  def cancel(self) -> str:
    self._state = PipelineState.CANCELLING
    self.client.cancel(self.futures)
    self._state = PipelineState.CANCELLED
    return self._state

  def metrics(self):
    # TODO(alxr): Collect and return metrics...
    raise NotImplementedError('collecting metrics will come later!')


class DaskRunner(BundleBasedDirectRunner):
  """Executes a pipeline on a Dask distributed client."""
  @staticmethod
  def to_dask_bag_visitor() -> PipelineVisitor:
    from dask import bag as db

    @dataclasses.dataclass
    class DaskBagVisitor(PipelineVisitor):
      bags: t.Dict[AppliedPTransform,
                   db.Bag] = dataclasses.field(default_factory=dict)

      def visit_transform(self, transform_node: AppliedPTransform) -> None:
        op_class = TRANSLATIONS.get(transform_node.transform.__class__, NoOp)
        op = op_class(transform_node)

        inputs = list(transform_node.inputs)
        if inputs:
          bag_inputs = []
          for input_value in inputs:
            if isinstance(input_value, pvalue.PBegin):
              bag_inputs.append(None)

            prev_op = input_value.producer
            if prev_op in self.bags:
              bag_inputs.append(self.bags[prev_op])

          if len(bag_inputs) == 1:
            self.bags[transform_node] = op.apply(bag_inputs[0])
          else:
            self.bags[transform_node] = op.apply(bag_inputs)

        else:
          self.bags[transform_node] = op.apply(None)

    return DaskBagVisitor()

  @staticmethod
  def is_fnapi_compatible():
    return False

  def run_pipeline(self, pipeline, options):
    # TODO(alxr): Create interactive notebook support.
    if is_in_notebook():
      raise NotImplementedError('interactive support will come later!')

    try:
      import dask.distributed as ddist
    except ImportError:
      raise ImportError(
          'DaskRunner is not available. Please install apache_beam[dask].')

    dask_options = options.view_as(DaskOptions).get_all_options(
        drop_default=True)
    client = ddist.Client(**dask_options)

    pipeline.replace_all(dask_overrides())

    dask_visitor = self.to_dask_bag_visitor()
    pipeline.visit(dask_visitor)

    futures = client.compute(list(dask_visitor.bags.values()))
    return DaskRunnerResult(client, futures)
