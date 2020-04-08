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

"""Module to build pipeline fragment that produces given PCollections.

For internal use only; no backwards-compatibility guarantees.
"""
from __future__ import absolute_import

import apache_beam as beam
from apache_beam.pipeline import PipelineVisitor
from apache_beam.testing.test_stream import TestStream


class PipelineFragment(object):
  """A fragment of a pipeline definition.

  A pipeline fragment is built from the original pipeline definition to include
  only PTransforms that are necessary to produce the given PCollections.
  """
  def __init__(self, pcolls, options=None):
    """Constructor of PipelineFragment.

    Args:
      pcolls: (List[PCollection]) a list of PCollections to build pipeline
          fragment for.
      options: (PipelineOptions) the pipeline options for the implicit
          pipeline run.
    """
    assert len(pcolls) > 0, (
        'Need at least 1 PCollection as the target data to build a pipeline '
        'fragment that produces it.')
    for pcoll in pcolls:
      assert isinstance(pcoll, beam.pvalue.PCollection), (
          '{} is not an apache_beam.pvalue.PCollection.'.format(pcoll))
    # No modification to self._user_pipeline is allowed.
    self._user_pipeline = pcolls[0].pipeline
    # These are user PCollections. Do not use them to deduce anything that
    # will be executed by any runner. Instead, use
    # `self._runner_pcolls_to_user_pcolls.keys()` to get copied PCollections.
    self._pcolls = set(pcolls)
    for pcoll in self._pcolls:
      assert pcoll.pipeline is self._user_pipeline, (
          '{} belongs to a different user pipeline than other PCollections '
          'given and cannot be used to build a pipeline fragment that produces '
          'the given PCollections.'.format(pcoll))
    self._options = options

    # A copied pipeline instance for modification without changing the user
    # pipeline instance held by the end user. This instance can be processed
    # into a pipeline fragment that later run by the underlying runner.
    self._runner_pipeline = self._build_runner_pipeline()
    _, self._context = self._runner_pipeline.to_runner_api(
        return_context=True, use_fake_coders=True)
    from apache_beam.runners.interactive import pipeline_instrument as instr
    self._runner_pcoll_to_id = instr.pcolls_to_pcoll_id(
        self._runner_pipeline, self._context)
    # Correlate components in the runner pipeline to components in the user
    # pipeline. The target pcolls are the pcolls given and defined in the user
    # pipeline.
    self._id_to_target_pcoll = self._calculate_target_pcoll_ids()
    self._label_to_user_transform = self._calculate_user_transform_labels()
    # Below will give us the 1:1 correlation between
    # PCollections/AppliedPTransforms from the copied runner pipeline and
    # PCollections/AppliedPTransforms from the user pipeline.
    # (Dict[PCollection, PCollection])
    (
        self._runner_pcolls_to_user_pcolls,
        # (Dict[AppliedPTransform, AppliedPTransform])
        self._runner_transforms_to_user_transforms
    ) = self._build_correlation_between_pipelines(
        self._runner_pcoll_to_id,
        self._id_to_target_pcoll,
        self._label_to_user_transform)

    # Below are operated on the runner pipeline.
    (self._necessary_transforms,
     self._necessary_pcollections) = self._mark_necessary_transforms_and_pcolls(
         self._runner_pcolls_to_user_pcolls)
    self._runner_pipeline = self._prune_runner_pipeline_to_fragment(
        self._runner_pipeline, self._necessary_transforms)

  def deduce_fragment(self):
    """Deduce the pipeline fragment as an apache_beam.Pipeline instance."""
    return beam.pipeline.Pipeline.from_runner_api(
        self._runner_pipeline.to_runner_api(use_fake_coders=True),
        self._runner_pipeline.runner,
        self._options)

  def run(self, display_pipeline_graph=False, use_cache=True, blocking=False):
    """Shorthand to run the pipeline fragment."""
    try:
      preserved_skip_display = self._runner_pipeline.runner._skip_display
      preserved_force_compute = self._runner_pipeline.runner._force_compute
      preserved_blocking = self._runner_pipeline.runner._blocking
      self._runner_pipeline.runner._skip_display = not display_pipeline_graph
      self._runner_pipeline.runner._force_compute = not use_cache
      self._runner_pipeline.runner._blocking = blocking
      return self.deduce_fragment().run()
    finally:
      self._runner_pipeline.runner._skip_display = preserved_skip_display
      self._runner_pipeline.runner._force_compute = preserved_force_compute
      self._runner_pipeline.runner._blocking = preserved_blocking

  def _build_runner_pipeline(self):
    return beam.pipeline.Pipeline.from_runner_api(
        self._user_pipeline.to_runner_api(use_fake_coders=True),
        self._user_pipeline.runner,
        self._options)

  def _calculate_target_pcoll_ids(self):
    pcoll_id_to_target_pcoll = {}
    for pcoll in self._pcolls:
      pcoll_id_to_target_pcoll[self._runner_pcoll_to_id.get(str(pcoll),
                                                            '')] = pcoll
    return pcoll_id_to_target_pcoll

  def _calculate_user_transform_labels(self):
    label_to_user_transform = {}

    class UserTransformVisitor(PipelineVisitor):
      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        if transform_node is not None:
          label_to_user_transform[transform_node.full_label] = transform_node

    v = UserTransformVisitor()
    self._runner_pipeline.visit(v)
    return label_to_user_transform

  def _build_correlation_between_pipelines(
      self, runner_pcoll_to_id, id_to_target_pcoll, label_to_user_transform):
    runner_pcolls_to_user_pcolls = {}
    runner_transforms_to_user_transforms = {}

    class CorrelationVisitor(PipelineVisitor):
      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        self._process_transform(transform_node)
        for in_pcoll in transform_node.inputs:
          self._process_pcoll(in_pcoll)
        for out_pcoll in transform_node.outputs.values():
          self._process_pcoll(out_pcoll)

      def _process_pcoll(self, pcoll):
        pcoll_id = runner_pcoll_to_id.get(str(pcoll), '')
        if pcoll_id in id_to_target_pcoll:
          runner_pcolls_to_user_pcolls[pcoll] = (id_to_target_pcoll[pcoll_id])

      def _process_transform(self, transform_node):
        if transform_node.full_label in label_to_user_transform:
          runner_transforms_to_user_transforms[transform_node] = (
              label_to_user_transform[transform_node.full_label])

    v = CorrelationVisitor()
    self._runner_pipeline.visit(v)
    return runner_pcolls_to_user_pcolls, runner_transforms_to_user_transforms

  def _mark_necessary_transforms_and_pcolls(self, runner_pcolls_to_user_pcolls):
    necessary_transforms = set()
    all_inputs = set()
    updated_all_inputs = set(runner_pcolls_to_user_pcolls.keys())
    # Do this until no more new PCollection is recorded.
    while len(updated_all_inputs) != len(all_inputs):
      all_inputs = set(updated_all_inputs)
      for pcoll in all_inputs:
        producer = pcoll.producer
        while producer:
          if producer in necessary_transforms:
            break
          # Mark the AppliedPTransform as necessary.
          necessary_transforms.add(producer)
          # Record all necessary input and side input PCollections.
          updated_all_inputs.update(producer.inputs)
          # pylint: disable=map-builtin-not-iterating
          side_input_pvalues = set(
              map(lambda side_input: side_input.pvalue, producer.side_inputs))
          updated_all_inputs.update(side_input_pvalues)
          # Go to its parent AppliedPTransform.
          producer = producer.parent
    return necessary_transforms, all_inputs

  def _prune_runner_pipeline_to_fragment(
      self, runner_pipeline, necessary_transforms):
    class PruneVisitor(PipelineVisitor):
      def enter_composite_transform(self, transform_node):
        if isinstance(transform_node.transform, TestStream):
          return

        pruned_parts = list(transform_node.parts)
        for part in transform_node.parts:
          if part not in necessary_transforms:
            pruned_parts.remove(part)
        transform_node.parts = tuple(pruned_parts)
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        if transform_node not in necessary_transforms:
          transform_node.parent = None

    v = PruneVisitor()
    runner_pipeline.visit(v)
    return runner_pipeline
