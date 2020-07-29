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

"""Pipeline, the top-level Beam object.

A pipeline holds a DAG of data transforms. Conceptually the nodes of the DAG
are transforms (:class:`~apache_beam.transforms.ptransform.PTransform` objects)
and the edges are values (mostly :class:`~apache_beam.pvalue.PCollection`
objects). The transforms take as inputs one or more PValues and output one or
more :class:`~apache_beam.pvalue.PValue` s.

The pipeline offers functionality to traverse the graph.  The actual operation
to be executed for each node visited is specified through a runner object.

Typical usage::

  # Create a pipeline object using a local runner for execution.
  with beam.Pipeline('DirectRunner') as p:

    # Add to the pipeline a "Create" transform. When executed this
    # transform will produce a PCollection object with the specified values.
    pcoll = p | 'Create' >> beam.Create([1, 2, 3])

    # Another transform could be applied to pcoll, e.g., writing to a text file.
    # For other transforms, refer to transforms/ directory.
    pcoll | 'Write' >> beam.io.WriteToText('./output')

    # run() will execute the DAG stored in the pipeline.  The execution of the
    # nodes visited is done using the specified local runner.

"""

# pytype: skip-file
# mypy: disallow-untyped-defs

from __future__ import absolute_import
from __future__ import print_function

import abc
import logging
import os
import re
import shutil
import sys
import tempfile
from builtins import object
from builtins import zip
from collections import defaultdict
from typing import TYPE_CHECKING
from typing import Dict
from typing import FrozenSet
from typing import Iterable
from typing import List
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple
from typing import Type
from typing import Union

from future.utils import with_metaclass
from past.builtins import unicode

from apache_beam import pvalue
from apache_beam.internal import pickler
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import CrossLanguageOptions
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import TypeOptions
from apache_beam.options.pipeline_options_validator import PipelineOptionsValidator
from apache_beam.portability import common_urns
from apache_beam.runners import PipelineRunner
from apache_beam.runners import create_runner
from apache_beam.transforms import ParDo
from apache_beam.transforms import ptransform
from apache_beam.transforms.core import RunnerAPIPTransformHolder
from apache_beam.transforms.sideinputs import SIDE_INPUT_PREFIX
from apache_beam.transforms.sideinputs import SIDE_INPUT_REGEX
from apache_beam.transforms.sideinputs import get_sideinput_index
from apache_beam.typehints import TypeCheckError
from apache_beam.typehints import typehints
from apache_beam.utils import proto_utils
from apache_beam.utils import subprocess_server
from apache_beam.utils.annotations import deprecated
from apache_beam.utils.interactive_utils import alter_label_if_ipython

if TYPE_CHECKING:
  from types import TracebackType
  from apache_beam.portability.api import beam_runner_api_pb2
  from apache_beam.runners.pipeline_context import PipelineContext
  from apache_beam.runners.runner import PipelineResult
  from apache_beam.transforms import environments

__all__ = ['Pipeline', 'PTransformOverride']


class Pipeline(object):
  """A pipeline object that manages a DAG of
  :class:`~apache_beam.pvalue.PValue` s and their
  :class:`~apache_beam.transforms.ptransform.PTransform` s.

  Conceptually the :class:`~apache_beam.pvalue.PValue` s are the DAG's nodes and
  the :class:`~apache_beam.transforms.ptransform.PTransform` s computing
  the :class:`~apache_beam.pvalue.PValue` s are the edges.

  All the transforms applied to the pipeline must have distinct full labels.
  If same transform instance needs to be applied then the right shift operator
  should be used to designate new names
  (e.g. ``input | "label" >> my_transform``).
  """
  @classmethod
  def runner_implemented_transforms(cls):
    # type: () -> FrozenSet[str]

    # This set should only contain transforms which are required to be
    # implemented by a runner.
    return frozenset([
        common_urns.primitives.GROUP_BY_KEY.urn,
        common_urns.primitives.IMPULSE.urn,
    ])

  def __init__(self, runner=None, options=None, argv=None):
    # type: (Optional[Union[str, PipelineRunner]], Optional[PipelineOptions], Optional[List[str]]) -> None

    """Initialize a pipeline object.

    Args:
      runner (~apache_beam.runners.runner.PipelineRunner): An object of
        type :class:`~apache_beam.runners.runner.PipelineRunner` that will be
        used to execute the pipeline. For registered runners, the runner name
        can be specified, otherwise a runner object must be supplied.
      options (~apache_beam.options.pipeline_options.PipelineOptions):
        A configured
        :class:`~apache_beam.options.pipeline_options.PipelineOptions` object
        containing arguments that should be used for running the Beam job.
      argv (List[str]): a list of arguments (such as :data:`sys.argv`)
        to be used for building a
        :class:`~apache_beam.options.pipeline_options.PipelineOptions` object.
        This will only be used if argument **options** is :data:`None`.

    Raises:
      ValueError: if either the runner or options argument is not
        of the expected type.
    """
    # Initializing logging configuration in case the user did not set it up.
    logging.basicConfig()

    if options is not None:
      if isinstance(options, PipelineOptions):
        self._options = options
      else:
        raise ValueError(
            'Parameter options, if specified, must be of type PipelineOptions. '
            'Received : %r' % options)
    elif argv is not None:
      if isinstance(argv, list):
        self._options = PipelineOptions(argv)
      else:
        raise ValueError(
            'Parameter argv, if specified, must be a list. Received : %r' %
            argv)
    else:
      self._options = PipelineOptions([])

    FileSystems.set_options(self._options)

    if runner is None:
      runner = self._options.view_as(StandardOptions).runner
      if runner is None:
        runner = StandardOptions.DEFAULT_RUNNER
        logging.info((
            'Missing pipeline option (runner). Executing pipeline '
            'using the default runner: %s.'),
                     runner)

    if isinstance(runner, str):
      runner = create_runner(runner)
    elif not isinstance(runner, PipelineRunner):
      raise TypeError(
          'Runner %s is not a PipelineRunner object or the '
          'name of a registered runner.' % runner)

    # Validate pipeline options
    errors = PipelineOptionsValidator(self._options, runner).validate()
    if errors:
      raise ValueError(
          'Pipeline has validations errors: \n' + '\n'.join(errors))

    # set default experiments for portable runners
    # (needs to occur prior to pipeline construction)
    if runner.is_fnapi_compatible():
      experiments = (self._options.view_as(DebugOptions).experiments or [])
      if not 'beam_fn_api' in experiments:
        experiments.append('beam_fn_api')
        self._options.view_as(DebugOptions).experiments = experiments

    self.local_tempdir = tempfile.mkdtemp(prefix='beam-pipeline-temp')

    # Default runner to be used.
    self.runner = runner
    # Stack of transforms generated by nested apply() calls. The stack will
    # contain a root node as an enclosing (parent) node for top transforms.
    self.transforms_stack = [AppliedPTransform(None, None, '', None)]
    # Set of transform labels (full labels) applied to the pipeline.
    # If a transform is applied and the full label is already in the set
    # then the transform will have to be cloned with a new label.
    self.applied_labels = set()  # type: Set[str]

    # Create a ComponentIdMap for assigning IDs to components. Ensures that any
    # components that receive an ID during pipeline construction (for example in
    # ExternalTransform), will receive the same component ID when generating the
    # full pipeline proto.
    self.component_id_map = ComponentIdMap()


  @property  # type: ignore[misc]  # decorated property not supported
  @deprecated(
      since='First stable release',
      extra_message='References to <pipeline>.options'
      ' will not be supported')
  def options(self):
    # type: () -> PipelineOptions
    return self._options

  def _current_transform(self):
    # type: () -> AppliedPTransform

    """Returns the transform currently on the top of the stack."""
    return self.transforms_stack[-1]

  def _root_transform(self):
    # type: () -> AppliedPTransform

    """Returns the root transform of the transform stack."""
    return self.transforms_stack[0]

  def _remove_labels_recursively(self, applied_transform):
    # type: (AppliedPTransform) -> None
    for part in applied_transform.parts:
      if part.full_label in self.applied_labels:
        self.applied_labels.remove(part.full_label)
        self._remove_labels_recursively(part)

  def _replace(self, override):
    # type: (PTransformOverride) -> None
    assert isinstance(override, PTransformOverride)

    # From original transform output --> replacement transform output
    output_map = {}  # type: Dict[pvalue.PValue, pvalue.PValue]
    output_replacements = {
    }  # type: Dict[AppliedPTransform, List[Tuple[pvalue.PValue, Optional[str]]]]
    input_replacements = {
    }  # type: Dict[AppliedPTransform, Sequence[Union[pvalue.PBegin, pvalue.PCollection]]]
    side_input_replacements = {
    }  # type: Dict[AppliedPTransform, List[pvalue.AsSideInput]]

    class TransformUpdater(PipelineVisitor):  # pylint: disable=used-before-assignment
      """"A visitor that replaces the matching PTransforms."""
      def __init__(self, pipeline):
        # type: (Pipeline) -> None
        self.pipeline = pipeline

      def _replace_if_needed(self, original_transform_node):
        # type: (AppliedPTransform) -> None
        if override.matches(original_transform_node):
          assert isinstance(original_transform_node, AppliedPTransform)
          replacement_transform = override.get_replacement_transform(
              original_transform_node.transform)
          if replacement_transform is original_transform_node.transform:
            return

          replacement_transform_node = AppliedPTransform(
              original_transform_node.parent,
              replacement_transform,
              original_transform_node.full_label,
              original_transform_node.inputs)

          # Transform execution could depend on order in which nodes are
          # considered. Hence we insert the replacement transform node to same
          # index as the original transform node. Note that this operation
          # removes the original transform node.
          if original_transform_node.parent:
            assert isinstance(original_transform_node.parent, AppliedPTransform)
            parent_parts = original_transform_node.parent.parts
            parent_parts[parent_parts.index(original_transform_node)] = (
                replacement_transform_node)
          else:
            # Original transform has to be a root.
            roots = self.pipeline.transforms_stack[0].parts
            assert original_transform_node in roots
            roots[roots.index(original_transform_node)] = (
                replacement_transform_node)

          inputs = replacement_transform_node.inputs
          # TODO:  Support replacing PTransforms with multiple inputs.
          if len(inputs) > 1:
            raise NotImplementedError(
                'PTransform overriding is only supported for PTransforms that '
                'have a single input. Tried to replace input of '
                'AppliedPTransform %r that has %d inputs' %
                (original_transform_node, len(inputs)))
          elif len(inputs) == 1:
            input_node = inputs[0]
          elif len(inputs) == 0:
            input_node = pvalue.PBegin(self.pipeline)

          try:
            # We have to add the new AppliedTransform to the stack before
            # expand() and pop it out later to make sure that parts get added
            # correctly.
            self.pipeline.transforms_stack.append(replacement_transform_node)

            # Keeping the same label for the replaced node but recursively
            # removing labels of child transforms of original transform since
            # they will be replaced during the expand below. This is needed in
            # case the replacement contains children that have labels that
            # conflicts with labels of the children of the original.
            self.pipeline._remove_labels_recursively(original_transform_node)

            new_output = replacement_transform.expand(input_node)
            assert isinstance(
                new_output, (dict, pvalue.PValue, pvalue.DoOutputsTuple))

            if isinstance(new_output, pvalue.PValue):
              new_output.element_type = None
              self.pipeline._infer_result_type(
                  replacement_transform, inputs, new_output)

            if isinstance(new_output, dict):
              for new_tag, new_pcoll in new_output.items():
                replacement_transform_node.add_output(new_pcoll, new_tag)
            elif isinstance(new_output, pvalue.DoOutputsTuple):
              replacement_transform_node.add_output(
                  new_output, new_output._main_tag)
            else:
              replacement_transform_node.add_output(new_output, new_output.tag)

            # Recording updated outputs. This cannot be done in the same
            # visitor since if we dynamically update output type here, we'll
            # run into errors when visiting child nodes.
            #
            # NOTE: When replacing multiple outputs, the replacement
            # PCollection tags must have a matching tag in the original
            # transform.
            if isinstance(new_output, pvalue.PValue):
              if not new_output.producer:
                new_output.producer = replacement_transform_node
              output_map[original_transform_node.outputs[new_output.tag]] = \
                  new_output
            elif isinstance(new_output, (pvalue.DoOutputsTuple, tuple)):
              for pcoll in new_output:
                if not pcoll.producer:
                  pcoll.producer = replacement_transform_node
                output_map[original_transform_node.outputs[pcoll.tag]] = pcoll
            elif isinstance(new_output, dict):
              for tag, pcoll in new_output.items():
                if not pcoll.producer:
                  pcoll.producer = replacement_transform_node
                output_map[original_transform_node.outputs[tag]] = pcoll
          finally:
            self.pipeline.transforms_stack.pop()

      def enter_composite_transform(self, transform_node):
        # type: (AppliedPTransform) -> None
        self._replace_if_needed(transform_node)

      def visit_transform(self, transform_node):
        # type: (AppliedPTransform) -> None
        self._replace_if_needed(transform_node)

    self.visit(TransformUpdater(self))

    # Adjusting inputs and outputs
    class InputOutputUpdater(PipelineVisitor):  # pylint: disable=used-before-assignment
      """"A visitor that records input and output values to be replaced.

      Input and output values that should be updated are recorded in maps
      input_replacements and output_replacements respectively.

      We cannot update input and output values while visiting since that results
      in validation errors.
      """
      def __init__(self, pipeline):
        # type: (Pipeline) -> None
        self.pipeline = pipeline

      def enter_composite_transform(self, transform_node):
        # type: (AppliedPTransform) -> None
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        # type: (AppliedPTransform) -> None
        replace_output = False
        for tag in transform_node.outputs:
          if transform_node.outputs[tag] in output_map:
            replace_output = True
            break

        replace_input = False
        for input in transform_node.inputs:
          if input in output_map:
            replace_input = True
            break

        replace_side_inputs = False
        for side_input in transform_node.side_inputs:
          if side_input.pvalue in output_map:
            replace_side_inputs = True
            break

        if replace_output:
          output_replacements[transform_node] = []
          for original, replacement in output_map.items():
            for tag, output in transform_node.outputs.items():
              if output == original:
                output_replacements[transform_node].append((tag, replacement))

        if replace_input:
          new_input = [
              input if not input in output_map else output_map[input]
              for input in transform_node.inputs
          ]
          input_replacements[transform_node] = new_input

        if replace_side_inputs:
          new_side_inputs = []
          for side_input in transform_node.side_inputs:
            if side_input.pvalue in output_map:
              side_input.pvalue = output_map[side_input.pvalue]
              new_side_inputs.append(side_input)
            else:
              new_side_inputs.append(side_input)
          side_input_replacements[transform_node] = new_side_inputs

    self.visit(InputOutputUpdater(self))

    for transform in output_replacements:
      for tag, output in output_replacements[transform]:
        transform.replace_output(output, tag=tag)

    for transform in input_replacements:
      transform.inputs = input_replacements[transform]

    for transform in side_input_replacements:
      transform.side_inputs = side_input_replacements[transform]

  def _check_replacement(self, override):
    # type: (PTransformOverride) -> None
    class ReplacementValidator(PipelineVisitor):
      def visit_transform(self, transform_node):
        # type: (AppliedPTransform) -> None
        if override.matches(transform_node):
          raise RuntimeError(
              'Transform node %r was not replaced as expected.' %
              transform_node)

    self.visit(ReplacementValidator())

  def replace_all(self, replacements):
    # type: (Iterable[PTransformOverride]) -> None

    """ Dynamically replaces PTransforms in the currently populated hierarchy.

    Currently this only works for replacements where input and output types
    are exactly the same.

    TODO: Update this to also work for transform overrides where input and
    output types are different.

    Args:
      replacements (List[~apache_beam.pipeline.PTransformOverride]): a list of
        :class:`~apache_beam.pipeline.PTransformOverride` objects.
    """
    for override in replacements:
      assert isinstance(override, PTransformOverride)
      self._replace(override)

    # Checking if the PTransforms have been successfully replaced. This will
    # result in a failure if a PTransform that was replaced in a given override
    # gets re-added in a subsequent override. This is not allowed and ordering
    # of PTransformOverride objects in 'replacements' is important.
    for override in replacements:
      self._check_replacement(override)

  def run(self, test_runner_api='AUTO'):
    # type: (Union[bool, str]) -> PipelineResult

    """Runs the pipeline. Returns whatever our runner returns after running."""

    try:
      if test_runner_api == 'AUTO':
        # Don't pay the cost of a round-trip if we're going to be going through
        # the FnApi anyway...
        test_runner_api = (
            not self.runner.is_fnapi_compatible() and (
                self.runner.__class__.__name__ != 'SwitchingDirectRunner' or
                self._options.view_as(StandardOptions).streaming))

      # When possible, invoke a round trip through the runner API.
      if test_runner_api and self._verify_runner_api_compatible():
        return Pipeline.from_runner_api(
            self.to_runner_api(use_fake_coders=True),
            self.runner,
            self._options,
            allow_proto_holders=True).run(False)

      if (self._options.view_as(TypeOptions).runtime_type_check and
          self._options.view_as(TypeOptions).performance_runtime_type_check):
        print(
            'You cannot turn on runtime_type_check '
            'and performance_runtime_type_check simultaneously. '
            'Beam will default to using performance_runtime_type_check.')
        self._options.view_as(TypeOptions).runtime_type_check = False

      if self._options.view_as(TypeOptions).runtime_type_check:
        from apache_beam.typehints import typecheck
        self.visit(typecheck.TypeCheckVisitor())

      if self._options.view_as(TypeOptions).performance_runtime_type_check:
        from apache_beam.typehints import typecheck
        self.visit(typecheck.PerformanceTypeCheckVisitor())

      if self._options.view_as(SetupOptions).save_main_session:
        # If this option is chosen, verify we can pickle the main session early.
        tmpdir = tempfile.mkdtemp()
        try:
          pickler.dump_session(os.path.join(tmpdir, 'main_session.pickle'))
        finally:
          shutil.rmtree(tmpdir)
      return self.runner.run_pipeline(self, self._options)
    finally:
      shutil.rmtree(self.local_tempdir, ignore_errors=True)

  def __enter__(self):
    # type: () -> Pipeline
    self._extra_context = subprocess_server.JavaJarServer.beam_services(
        self._options.view_as(CrossLanguageOptions).beam_services)
    self._extra_context.__enter__()
    return self

  def __exit__(
      self,
      exc_type,  # type: Optional[Type[BaseException]]
      exc_val,  # type: Optional[BaseException]
      exc_tb  # type: Optional[TracebackType]
  ):
    # type: (...) -> None

    try:
      if not exc_type:
        self.result = self.run()
        self.result.wait_until_finish()
    finally:
      self._extra_context.__exit__(exc_type, exc_val, exc_tb)

  def visit(self, visitor):
    # type: (PipelineVisitor) -> None

    """Visits depth-first every node of a pipeline's DAG.

    Runner-internal implementation detail; no backwards-compatibility guarantees

    Args:
      visitor (~apache_beam.pipeline.PipelineVisitor):
        :class:`~apache_beam.pipeline.PipelineVisitor` object whose callbacks
        will be called for each node visited. See
        :class:`~apache_beam.pipeline.PipelineVisitor` comments.

    Raises:
      TypeError: if node is specified and is not a
        :class:`~apache_beam.pvalue.PValue`.
      ~apache_beam.error.PipelineError: if node is specified and does not
        belong to this pipeline instance.
    """

    visited = set()  # type: Set[pvalue.PValue]
    self._root_transform().visit(visitor, self, visited)

  def apply(
      self,
      transform,  # type: ptransform.PTransform
      pvalueish=None,  # type: Optional[pvalue.PValue]
      label=None  # type: Optional[str]
  ):
    # type: (...) -> pvalue.PValue

    """Applies a custom transform using the pvalueish specified.

    Args:
      transform (~apache_beam.transforms.ptransform.PTransform): the
        :class:`~apache_beam.transforms.ptransform.PTransform` to apply.
      pvalueish (~apache_beam.pvalue.PCollection): the input for the
        :class:`~apache_beam.transforms.ptransform.PTransform` (typically a
        :class:`~apache_beam.pvalue.PCollection`).
      label (str): label of the
        :class:`~apache_beam.transforms.ptransform.PTransform`.

    Raises:
      TypeError: if the transform object extracted from the
        argument list is not a
        :class:`~apache_beam.transforms.ptransform.PTransform`.
      RuntimeError: if the transform object was already applied to
        this pipeline and needs to be cloned in order to apply again.
    """
    if isinstance(transform, ptransform._NamedPTransform):
      return self.apply(
          transform.transform, pvalueish, label or transform.label)

    if not isinstance(transform, ptransform.PTransform):
      raise TypeError("Expected a PTransform object, got %s" % transform)

    if label:
      # Fix self.label as it is inspected by some PTransform operations
      # (e.g. to produce error messages for type hint violations).
      try:
        old_label, transform.label = transform.label, label
        return self.apply(transform, pvalueish)
      finally:
        transform.label = old_label

    # Attempts to alter the label of the transform to be applied only when it's
    # a top-level transform so that the cell number will not be prepended to
    # every child transform in a composite.
    if self._current_transform() is self._root_transform():
      alter_label_if_ipython(transform, pvalueish)

    full_label = '/'.join(
        [self._current_transform().full_label, label or
         transform.label]).lstrip('/')
    if full_label in self.applied_labels:
      raise RuntimeError(
          'A transform with label "%s" already exists in the pipeline. '
          'To apply a transform with a specified label write '
          'pvalue | "label" >> transform' % full_label)
    self.applied_labels.add(full_label)

    pvalueish, inputs = transform._extract_input_pvalues(pvalueish)
    try:
      inputs = tuple(inputs)
      for leaf_input in inputs:
        if not isinstance(leaf_input, pvalue.PValue):
          raise TypeError
    except TypeError:
      raise NotImplementedError(
          'Unable to extract PValue inputs from %s; either %s does not accept '
          'inputs of this format, or it does not properly override '
          '_extract_input_pvalues' % (pvalueish, transform))

    current = AppliedPTransform(
        self._current_transform(), transform, full_label, inputs)
    self._current_transform().add_part(current)

    try:
      self.transforms_stack.append(current)

      type_options = self._options.view_as(TypeOptions)
      if type_options.pipeline_type_check:
        transform.type_check_inputs(pvalueish)

      pvalueish_result = self.runner.apply(transform, pvalueish, self._options)

      if type_options is not None and type_options.pipeline_type_check:
        transform.type_check_outputs(pvalueish_result)

      for tag, result in ptransform.get_named_nested_pvalues(pvalueish_result):
        assert isinstance(result, (pvalue.PValue, pvalue.DoOutputsTuple))

        # Make sure we set the producer only for a leaf node in the transform
        # DAG. This way we preserve the last transform of a composite transform
        # as being the real producer of the result.
        if result.producer is None:
          result.producer = current

        self._infer_result_type(transform, inputs, result)

        assert isinstance(result.producer.inputs, tuple)
        # The DoOutputsTuple adds the PCollection to the outputs when accessed
        # except for the main tag. Add the main tag here.
        if isinstance(result, pvalue.DoOutputsTuple):
          current.add_output(result, result._main_tag)
          continue

        # If there is already a tag with the same name, increase a counter for
        # the name. This can happen, for example, when a composite outputs a
        # list of PCollections where all the tags are None.
        base = tag
        counter = 0
        while tag in current.outputs:
          counter += 1
          tag = '%s_%d' % (base, counter)

        current.add_output(result, tag)

      if (type_options is not None and
          type_options.type_check_strictness == 'ALL_REQUIRED' and
          transform.get_type_hints().output_types is None):
        ptransform_name = '%s(%s)' % (transform.__class__.__name__, full_label)
        raise TypeCheckError(
            'Pipeline type checking is enabled, however no '
            'output type-hint was found for the '
            'PTransform %s' % ptransform_name)
    finally:
      self.transforms_stack.pop()
    return pvalueish_result

  def _infer_result_type(
      self,
      transform,  # type: ptransform.PTransform
      inputs,  # type: Sequence[Union[pvalue.PBegin, pvalue.PCollection]]
      result_pcollection  # type: Union[pvalue.PValue, pvalue.DoOutputsTuple]
  ):
    # type: (...) -> None
    # TODO(robertwb): Multi-input inference.
    type_options = self._options.view_as(TypeOptions)
    if type_options is None or not type_options.pipeline_type_check:
      return
    if (isinstance(result_pcollection, pvalue.PCollection) and
        (not result_pcollection.element_type
         # TODO(robertwb): Ideally we'd do intersection here.
         or result_pcollection.element_type == typehints.Any)):
      # Single-input, single-output inference.
      input_element_type = (
          inputs[0].element_type if len(inputs) == 1 else typehints.Any)
      type_hints = transform.get_type_hints()
      declared_output_type = type_hints.simple_output_type(transform.label)
      if declared_output_type:
        input_types = type_hints.input_types
        if input_types and input_types[0]:
          declared_input_type = input_types[0][0]
          result_pcollection.element_type = typehints.bind_type_variables(
              declared_output_type,
              typehints.match_type_variables(
                  declared_input_type, input_element_type))
        else:
          result_pcollection.element_type = declared_output_type
      else:
        result_pcollection.element_type = transform.infer_output_type(
            input_element_type)
    elif isinstance(result_pcollection, pvalue.DoOutputsTuple):
      # Single-input, multi-output inference.
      # TODO(BEAM-4132): Add support for tagged type hints.
      #   https://github.com/apache/beam/pull/9810#discussion_r338765251
      for pcoll in result_pcollection:
        if pcoll.element_type is None:
          pcoll.element_type = typehints.Any

  def __reduce__(self):
    # type: () -> Tuple[Type, Tuple[str, ...]]
    # Some transforms contain a reference to their enclosing pipeline,
    # which in turn reference all other transforms (resulting in quadratic
    # time/space to pickle each transform individually).  As we don't
    # require pickled pipelines to be executable, break the chain here.
    return str, ('Pickled pipeline stub.', )

  def _verify_runner_api_compatible(self):
    # type: () -> bool
    if self._options.view_as(TypeOptions).runtime_type_check:
      # This option is incompatible with the runner API as it requires
      # the runner to inspect non-serialized hints on the transform
      # itself.
      return False

    class Visitor(PipelineVisitor):  # pylint: disable=used-before-assignment
      ok = True  # Really a nonlocal.

      def enter_composite_transform(self, transform_node):
        # type: (AppliedPTransform) -> None
        pass

      def visit_transform(self, transform_node):
        # type: (AppliedPTransform) -> None
        try:
          # Transforms must be picklable.
          pickler.loads(
              pickler.dumps(transform_node.transform, enable_trace=False),
              enable_trace=False)
        except Exception:
          Visitor.ok = False

      def visit_value(self, value, _):
        # type: (pvalue.PValue, AppliedPTransform) -> None
        if isinstance(value, pvalue.PDone):
          Visitor.ok = False

    self.visit(Visitor())
    return Visitor.ok

  def to_runner_api(
      self,
      return_context=False,  # type: bool
      context=None,  # type: Optional[PipelineContext]
      use_fake_coders=False,  # type: bool
      default_environment=None  # type: Optional[environments.Environment]
  ):
    # type: (...) -> beam_runner_api_pb2.Pipeline

    """For internal use only; no backwards-compatibility guarantees."""
    from apache_beam.runners import pipeline_context
    from apache_beam.portability.api import beam_runner_api_pb2
    if context is None:
      context = pipeline_context.PipelineContext(
          use_fake_coders=use_fake_coders,
          component_id_map=self.component_id_map,
          default_environment=default_environment)
    elif default_environment is not None:
      raise ValueError(
          'Only one of context or default_environment may be specified.')

    # The RunnerAPI spec requires certain transforms and side-inputs to have KV
    # inputs (and corresponding outputs).
    # Currently we only upgrade to KV pairs.  If there is a need for more
    # general shapes, potential conflicts will have to be resolved.
    # We also only handle single-input, and (for fixing the output) single
    # output, which is sufficient.
    class ForceKvInputTypes(PipelineVisitor):
      def enter_composite_transform(self, transform_node):
        # type: (AppliedPTransform) -> None
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        # type: (AppliedPTransform) -> None
        if not transform_node.transform:
          return
        if transform_node.transform.runner_api_requires_keyed_input():
          pcoll = transform_node.inputs[0]
          pcoll.element_type = typehints.coerce_to_kv_type(
              pcoll.element_type, transform_node.full_label)
          if len(transform_node.outputs) == 1:
            # The runner often has expectations about the output types as well.
            output, = transform_node.outputs.values()
            if not output.element_type:
              output.element_type = transform_node.transform.infer_output_type(
                  pcoll.element_type)
        for side_input in transform_node.transform.side_inputs:
          if side_input.requires_keyed_input():
            side_input.pvalue.element_type = typehints.coerce_to_kv_type(
                side_input.pvalue.element_type,
                transform_node.full_label,
                side_input_producer=side_input.pvalue.producer.full_label)

    self.visit(ForceKvInputTypes())

    # Mutates context; placing inline would force dependence on
    # argument evaluation order.
    root_transform_id = context.transforms.get_id(self._root_transform())
    proto = beam_runner_api_pb2.Pipeline(
        root_transform_ids=[root_transform_id],
        components=context.to_runner_api(),
        requirements=context.requirements())
    proto.components.transforms[root_transform_id].unique_name = (
        root_transform_id)
    if return_context:
      return proto, context  # type: ignore  # too complicated for now
    else:
      return proto

  @staticmethod
  def from_runner_api(
      proto,  # type: beam_runner_api_pb2.Pipeline
      runner,  # type: PipelineRunner
      options,  # type: PipelineOptions
      return_context=False,  # type: bool
      allow_proto_holders=False  # type: bool
  ):
    # type: (...) -> Pipeline

    """For internal use only; no backwards-compatibility guarantees."""
    p = Pipeline(runner=runner, options=options)
    from apache_beam.runners import pipeline_context
    context = pipeline_context.PipelineContext(
        proto.components,
        allow_proto_holders=allow_proto_holders,
        requirements=proto.requirements)
    root_transform_id, = proto.root_transform_ids
    p.transforms_stack = [context.transforms.get_by_id(root_transform_id)]
    # TODO(robertwb): These are only needed to continue construction. Omit?
    p.applied_labels = {
        t.unique_name
        for t in proto.components.transforms.values()
    }
    for id in proto.components.pcollections:
      pcollection = context.pcollections.get_by_id(id)
      pcollection.pipeline = p
      if not pcollection.producer:
        raise ValueError('No producer for %s' % id)

    # Inject PBegin input where necessary.
    from apache_beam.io.iobase import Read
    from apache_beam.transforms.core import Create
    has_pbegin = [Read, Create]
    for id in proto.components.transforms:
      transform = context.transforms.get_by_id(id)
      if not transform.inputs and transform.transform.__class__ in has_pbegin:
        transform.inputs = (pvalue.PBegin(p), )

    if return_context:
      return p, context  # type: ignore  # too complicated for now
    else:
      return p


class PipelineVisitor(object):
  """For internal use only; no backwards-compatibility guarantees.

  Visitor pattern class used to traverse a DAG of transforms
  (used internally by Pipeline for bookeeping purposes).
  """
  def visit_value(self, value, producer_node):
    # type: (pvalue.PValue, AppliedPTransform) -> None

    """Callback for visiting a PValue in the pipeline DAG.

    Args:
      value: PValue visited (typically a PCollection instance).
      producer_node: AppliedPTransform object whose transform produced the
        pvalue.
    """
    pass

  def visit_transform(self, transform_node):
    # type: (AppliedPTransform) -> None

    """Callback for visiting a transform leaf node in the pipeline DAG."""
    pass

  def enter_composite_transform(self, transform_node):
    # type: (AppliedPTransform) -> None

    """Callback for entering traversal of a composite transform node."""
    pass

  def leave_composite_transform(self, transform_node):
    # type: (AppliedPTransform) -> None

    """Callback for leaving traversal of a composite transform node."""
    pass


class AppliedPTransform(object):
  """For internal use only; no backwards-compatibility guarantees.

  A transform node representing an instance of applying a PTransform
  (used internally by Pipeline for bookeeping purposes).
  """
  def __init__(
      self,
      parent,  # type:  Optional[AppliedPTransform]
      transform,  # type: Optional[ptransform.PTransform]
      full_label,  # type: str
      inputs,  # type: Optional[Sequence[Union[pvalue.PBegin, pvalue.PCollection]]]
      environment_id=None,  # type: Optional[str]
      input_tags_to_preserve=None,  # type: Dict[pvalue.PCollection, str]
  ):
    # type: (...) -> None
    self.parent = parent
    self.transform = transform
    # Note that we want the PipelineVisitor classes to use the full_label,
    # inputs, side_inputs, and outputs fields from this instance instead of the
    # ones of the PTransform instance associated with it. Doing this permits
    # reusing PTransform instances in different contexts (apply() calls) without
    # any interference. This is particularly useful for composite transforms.
    self.full_label = full_label
    self.inputs = inputs or ()

    self.side_inputs = tuple() if transform is None else transform.side_inputs
    self.outputs = {}  # type: Dict[Union[str, int, None], pvalue.PValue]
    self.parts = []  # type: List[AppliedPTransform]
    self.environment_id = environment_id if environment_id else None  # type: Optional[str]
    self.input_tags_to_preserve = input_tags_to_preserve or {}

  def __repr__(self):
    # type: () -> str
    return "%s(%s, %s)" % (
        self.__class__.__name__, self.full_label, type(self.transform).__name__)

  def replace_output(
      self,
      output,  # type: Union[pvalue.PValue, pvalue.DoOutputsTuple]
      tag=None  # type: Union[str, int, None]
  ):
    # type: (...) -> None

    """Replaces the output defined by the given tag with the given output.

    Args:
      output: replacement output
      tag: tag of the output to be replaced.
    """
    if isinstance(output, pvalue.DoOutputsTuple):
      self.replace_output(output[output._main_tag])
    elif isinstance(output, pvalue.PValue):
      self.outputs[tag] = output
    elif isinstance(output, dict):
      for output_tag, out in output.items():
        self.outputs[output_tag] = out
    else:
      raise TypeError("Unexpected output type: %s" % output)

  def add_output(
      self,
      output,  # type: Union[pvalue.DoOutputsTuple, pvalue.PValue]
      tag  # type: Union[str, int, None]
  ):
    # type: (...) -> None
    if isinstance(output, pvalue.DoOutputsTuple):
      self.add_output(output[tag], tag)
    elif isinstance(output, pvalue.PValue):
      assert tag not in self.outputs
      self.outputs[tag] = output
    else:
      raise TypeError("Unexpected output type: %s" % output)

  def add_part(self, part):
    # type: (AppliedPTransform) -> None
    assert isinstance(part, AppliedPTransform)
    self.parts.append(part)

  def is_composite(self):
    # type: () -> bool

    """Returns whether this is a composite transform.

    A composite transform has parts (inner transforms) or isn't the
    producer for any of its outputs. (An example of a transform that
    is not a producer is one that returns its inputs instead.)
    """
    return bool(self.parts) or all(
        pval.producer is not self for pval in self.outputs.values())

  def visit(
      self,
      visitor,  # type: PipelineVisitor
      pipeline,  # type: Pipeline
      visited  # type: Set[pvalue.PValue]
  ):
    # type: (...) -> None

    """Visits all nodes reachable from the current node."""

    for in_pval in self.inputs:
      if in_pval not in visited and not isinstance(in_pval, pvalue.PBegin):
        if in_pval.producer is not None:
          in_pval.producer.visit(visitor, pipeline, visited)
          # The value should be visited now since we visit outputs too.
          assert in_pval in visited, in_pval

    # Visit side inputs.
    for side_input in self.side_inputs:
      if isinstance(side_input, pvalue.AsSideInput) \
          and side_input.pvalue not in visited:
        pval = side_input.pvalue  # Unpack marker-object-wrapped pvalue.
        if pval.producer is not None:
          pval.producer.visit(visitor, pipeline, visited)
          # The value should be visited now since we visit outputs too.
          assert pval in visited
          # TODO(silviuc): Is there a way to signal that we are visiting a side
          # value? The issue is that the same PValue can be reachable through
          # multiple paths and therefore it is not guaranteed that the value
          # will be visited as a side value.

    # Visit a composite or primitive transform.
    if self.is_composite():
      visitor.enter_composite_transform(self)
      for part in self.parts:
        part.visit(visitor, pipeline, visited)
      visitor.leave_composite_transform(self)
    else:
      visitor.visit_transform(self)

    # Visit the outputs (one or more). It is essential to mark as visited the
    # tagged PCollections of the DoOutputsTuple object. A tagged PCollection is
    # connected directly with its producer (a multi-output ParDo), but the
    # output of such a transform is the containing DoOutputsTuple, not the
    # PCollection inside it. Without the code below a tagged PCollection will
    # not be marked as visited while visiting its producer.
    for out_pval in self.outputs.values():
      if isinstance(out_pval, pvalue.DoOutputsTuple):
        pvals = (v for v in out_pval)
      else:
        pvals = (out_pval, )
      for v in pvals:
        if v not in visited:
          visited.add(v)
          visitor.visit_value(v, self)

  def named_inputs(self):
    # type: () -> Dict[str, pvalue.PValue]
    # TODO(BEAM-1833): Push names up into the sdk construction.
    main_inputs = {
        str(ix): input
        for ix,
        input in enumerate(self.inputs)
        if isinstance(input, pvalue.PCollection)
    }
    side_inputs = {(SIDE_INPUT_PREFIX + '%s') % ix: si.pvalue
                   for (ix, si) in enumerate(self.side_inputs)}
    return dict(main_inputs, **side_inputs)

  def named_outputs(self):
    # type: () -> Dict[str, pvalue.PCollection]
    return {
        try_unicode(tag): output
        for tag,
        output in self.outputs.items()
        if isinstance(output, pvalue.PCollection)
    }

  def to_runner_api(self, context):
    # type: (PipelineContext) -> beam_runner_api_pb2.PTransform
    # External tranforms require more splicing than just setting the spec.
    from apache_beam.transforms import external
    if isinstance(self.transform, external.ExternalTransform):
      return self.transform.to_runner_api_transform(context, self.full_label)

    from apache_beam.portability.api import beam_runner_api_pb2

    def transform_to_runner_api(
        transform,  # type: Optional[ptransform.PTransform]
        context  # type: PipelineContext
    ):
      # type: (...) -> Optional[beam_runner_api_pb2.FunctionSpec]
      if transform is None:
        return None
      else:
        # We only populate inputs information to ParDo in order to expose
        # key_coder and window_coder to stateful DoFn.
        if isinstance(transform, ParDo):
          return transform.to_runner_api(
              context,
              has_parts=bool(self.parts),
              named_inputs=self.named_inputs())
        return transform.to_runner_api(context, has_parts=bool(self.parts))

    # Iterate over inputs and outputs by sorted key order, so that ids are
    # consistently generated for multiple runs of the same pipeline.
    transform_spec = transform_to_runner_api(self.transform, context)
    environment_id = self.environment_id
    transform_urn = transform_spec.urn if transform_spec else None
    if (not environment_id and
        (transform_urn not in Pipeline.runner_implemented_transforms())):
      environment_id = context.default_environment_id()

    def _maybe_preserve_tag(new_tag, pc, input_tags_to_preserve):
      # TODO(BEAM-1833): remove this after we update Python SDK and
      # DataflowRunner to construct pipelines using runner API.
      return input_tags_to_preserve[
          pc] if pc in input_tags_to_preserve else new_tag

    return beam_runner_api_pb2.PTransform(
        unique_name=self.full_label,
        spec=transform_spec,
        subtransforms=[
            context.transforms.get_id(part, label=part.full_label)
            for part in self.parts
        ],
        inputs={
            _maybe_preserve_tag(tag, pc, self.input_tags_to_preserve):
            context.pcollections.get_id(pc)
            for (tag, pc) in sorted(self.named_inputs().items())
        },
        outputs={
            tag: context.pcollections.get_id(out)
            for tag,
            out in sorted(self.named_outputs().items())
        },
        environment_id=environment_id,
        # TODO(BEAM-366): Add display_data.
        display_data=None)

  @staticmethod
  def from_runner_api(
      proto,  # type: beam_runner_api_pb2.PTransform
      context  # type: PipelineContext
  ):
    # type: (...) -> AppliedPTransform

    if common_urns.primitives.PAR_DO.urn == proto.spec.urn:
      # Preserving side input tags.
      from apache_beam.portability.api import beam_runner_api_pb2
      pardo_payload = (
          proto_utils.parse_Bytes(
              proto.spec.payload, beam_runner_api_pb2.ParDoPayload))
      side_input_tags = list(pardo_payload.side_inputs.keys())
    else:
      pardo_payload = None
      side_input_tags = []

    main_inputs = [
        context.pcollections.get_by_id(id) for tag,
        id in proto.inputs.items() if tag not in side_input_tags
    ]

    def is_python_side_input(tag):
      # type: (str) -> bool
      # As per named_inputs() above.
      return re.match(SIDE_INPUT_REGEX, tag)

    uses_python_sideinput_tags = (
        is_python_side_input(side_input_tags[0]) if side_input_tags else False)

    transform = ptransform.PTransform.from_runner_api(proto, context)
    if uses_python_sideinput_tags:
      # Ordering is important here.
      # TODO(BEAM-9635): use key, value pairs instead of depending on tags with
      # index as a suffix.
      indexed_side_inputs = [
          (get_sideinput_index(tag), context.pcollections.get_by_id(id))
          for tag,
          id in proto.inputs.items() if tag in side_input_tags
      ]
      side_inputs = [si for _, si in sorted(indexed_side_inputs)]
    else:
      # These must be set in the same order for subsequent zip to work.
      side_inputs = []
      transform_side_inputs = []

      for tag, id in proto.inputs.items():
        if tag in side_input_tags:
          pc = context.pcollections.get_by_id(id)
          side_inputs.append(pc)
          assert pardo_payload  # This must be a ParDo with side inputs.
          side_input_from_pardo = pardo_payload.side_inputs[tag]

          # TODO(BEAM-1833): use 'pvalue.SideInputData.from_runner_api' here
          # when that is updated to better represent runner API.
          if (common_urns.side_inputs.MULTIMAP.urn ==
              side_input_from_pardo.access_pattern.urn):
            transform_side_inputs.append(pvalue.AsMultiMap(pc))
          elif (common_urns.side_inputs.ITERABLE.urn ==
                side_input_from_pardo.access_pattern.urn):
            transform_side_inputs.append(pvalue.AsIter(pc))
          else:
            raise ValueError(
                'Unsupported side input access pattern %r' %
                side_input_from_pardo.access_pattern.urn)
      if transform:
        transform.side_inputs = transform_side_inputs

    if isinstance(transform, RunnerAPIPTransformHolder):
      # For external transforms that are ParDos, we have to preserve input tags.
      input_tags_to_preserve = {
          context.pcollections.get_by_id(id): tag
          for (tag, id) in proto.inputs.items()
      }
    else:
      input_tags_to_preserve = {}

    result = AppliedPTransform(
        parent=None,
        transform=transform,
        full_label=proto.unique_name,
        inputs=main_inputs,
        environment_id=proto.environment_id,
        input_tags_to_preserve=input_tags_to_preserve)

    if result.transform and result.transform.side_inputs:
      for si, pcoll in zip(result.transform.side_inputs, side_inputs):
        si.pvalue = pcoll
      result.side_inputs = tuple(result.transform.side_inputs)
    result.parts = []
    for transform_id in proto.subtransforms:
      part = context.transforms.get_by_id(transform_id)
      part.parent = result
      result.parts.append(part)
    result.outputs = {
        None if tag == 'None' else tag: context.pcollections.get_by_id(id)
        for tag,
        id in proto.outputs.items()
    }
    # This annotation is expected by some runners.
    if proto.spec.urn == common_urns.primitives.PAR_DO.urn:
      # TODO(BEAM-9168): Figure out what to do for RunnerAPIPTransformHolder.
      assert isinstance(result.transform, (ParDo, RunnerAPIPTransformHolder)),\
        type(result.transform)
      result.transform.output_tags = set(proto.outputs.keys()).difference(
          {'None'})
    if not result.parts:
      for tag, pcoll_id in proto.outputs.items():
        if pcoll_id not in proto.inputs.values():
          pc = context.pcollections.get_by_id(pcoll_id)
          pc.producer = result
          pc.tag = None if tag == 'None' else tag
    return result


class PTransformOverride(with_metaclass(abc.ABCMeta,
                                        object)):  # type: ignore[misc]
  """For internal use only; no backwards-compatibility guarantees.

  Gives a matcher and replacements for matching PTransforms.

  TODO: Update this to support cases where input and/our output types are
  different.
  """
  @abc.abstractmethod
  def matches(self, applied_ptransform):
    # type: (AppliedPTransform) -> bool

    """Determines whether the given AppliedPTransform matches.

    Note that the matching will happen *after* Runner API proto translation.
    If matching is done via type checks, to/from_runner_api[_parameter] methods
    must be implemented to preserve the type (and other data) through proto
    serialization.

    Consider URN-based translation instead.

    Args:
      applied_ptransform: AppliedPTransform to be matched.

    Returns:
      a bool indicating whether the given AppliedPTransform is a match.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def get_replacement_transform(self, ptransform):
    # type: (Optional[ptransform.PTransform]) -> ptransform.PTransform

    """Provides a runner specific override for a given PTransform.

    Args:
      ptransform: PTransform to be replaced.

    Returns:
      A PTransform that will be the replacement for the PTransform given as an
      argument.
    """
    # Returns a PTransformReplacement
    raise NotImplementedError


class ComponentIdMap(object):
  """A utility for assigning unique component ids to Beam components.

  Component ID assignments are only guaranteed to be unique and consistent
  within the scope of a ComponentIdMap instance.
  """
  def __init__(self, namespace="ref"):
    self.namespace = namespace
    self._counters = defaultdict(lambda: 0)  # type: Dict[type, int]
    self._obj_to_id = {}  # type: Dict[Any, str]

  def get_or_assign(self, obj=None, obj_type=None, label=None):
    if obj not in self._obj_to_id:
      self._obj_to_id[obj] = self._unique_ref(obj, obj_type, label)

    return self._obj_to_id[obj]

  def _unique_ref(self, obj=None, obj_type=None, label=None):
    self._counters[obj_type] += 1
    return "%s_%s_%s_%d" % (
        self.namespace,
        obj_type.__name__,
        label or type(obj).__name__,
        self._counters[obj_type])


if sys.version_info >= (3, ):
  try_unicode = str

else:

  def try_unicode(s):
    try:
      return unicode(s)
    except UnicodeDecodeError:
      return str(s).decode('ascii', 'replace')
