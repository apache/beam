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


Ideas to explore / Notes:
- Write a PCollection subclass that wraps a Dask Bag.
  - Would be the input + return of the translation operators.
- The Ray runner is more focused on Task scheduling; This should focus more on graph translation.

- A bundle is a subset of elements in a PCollection. i.e. a small set of elements that are processed together.
- In Dask, it is probably the same as a partition. Thus, we probably don't need to worry about it; Dask should take
  care of it.
"""
import argparse
import dataclasses
import typing as t

from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pipeline import PipelineVisitor, AppliedPTransform
from apache_beam.runners.dask.overrides import dask_overrides
from apache_beam.runners.dask.transform_evaluator import TRANSLATIONS, NoOp
from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner
from apache_beam.utils.interactive_utils import is_in_notebook


class DaskOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser: argparse.ArgumentParser) -> None:
        # TODO: get Dask client options
        pass


class DaskRunner(BundleBasedDirectRunner):
    """Executes a pipeline on a Dask distributed client."""

    @staticmethod
    def to_dask_bag_visitor() -> 'DaskBagVisitor':

        from dask import bag as db

        @dataclasses.dataclass
        class DaskBagVisitor(PipelineVisitor):
            bags: t.Dict[AppliedPTransform, db.Bag] = dataclasses.field(default_factory=dict)

            def visit_transform(self, transform_node: AppliedPTransform) -> None:
                op_class = TRANSLATIONS.get(transform_node.transform.__class__, NoOp)
                op = op_class(transform_node)

                inputs = list(transform_node.inputs)
                if inputs:
                    for input_value in inputs:
                        if isinstance(input_value, pvalue.PBegin):
                            self.bags[transform_node] = op.apply(None)

                        prev_op = input_value.producer
                        if prev_op in self.bags:
                            self.bags[transform_node] = op.apply(self.bags[prev_op])
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
            import dask.bag as db
            import dask.distributed as ddist
        except ImportError:
            raise ImportError('DaskRunner is not available. Please install apache_beam[dask].')

        # TODO(alxr): Actually use this right.
        dask_options = options.view_as(DaskOptions).get_all_options(drop_default=True)
        client = ddist.Client(**dask_options)

        pipeline.replace_all(dask_overrides())

        dask_visitor = self.to_dask_bag_visitor()
        pipeline.visit(dask_visitor)

        for bag in dask_visitor.bags.values():
            bag.compute()

        # TODO(alxr): Return the proper thing...
        return None
        # if pipeline:
        #     pass
        # else:
        #     raise ValueError('Proto or FunctionAPI environments are not supported.')
        # if pipeline:
        #
        #     # Flatten / Optimize graph?
        #
        #     # Trigger a traversal of all reachable nodes.
        #     self.visit_transforms(pipeline, options)
        #
        # Get API Client?
