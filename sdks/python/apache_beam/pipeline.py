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

"""Pipeline, the top-level Dataflow object.

A pipeline holds a DAG of data transforms. Conceptually the nodes of the DAG
are transforms (PTransform objects) and the edges are values (mostly PCollection
objects). The transforms take as inputs one or more PValues and output one or
more PValues.

The pipeline offers functionality to traverse the graph.  The actual operation
to be executed for each node visited is specified through a runner object.

Typical usage:

  # Create a pipeline object using a local runner for execution.
  p = beam.Pipeline('DirectRunner')

  # Add to the pipeline a "Create" transform. When executed this
  # transform will produce a PCollection object with the specified values.
  pcoll = p | 'Create' >> beam.Create([1, 2, 3])

  # Another transform could be applied to pcoll, e.g., writing to a text file.
  # For other transforms, refer to transforms/ directory.
  pcoll | 'Write' >> beam.io.WriteToText('./output')

  # run() will execute the DAG stored in the pipeline.  The execution of the
  # nodes visited is done using the specified local runner.
  p.run()

"""

from __future__ import absolute_import

import collections
import logging
import os
import shutil
import tempfile

from apache_beam import pvalue
from apache_beam import typehints
from apache_beam.internal import pickler
from apache_beam.runners import create_runner
from apache_beam.runners import PipelineRunner
from apache_beam.transforms import ptransform
from apache_beam.typehints import TypeCheckError
from apache_beam.utils.pipeline_options import PipelineOptions
from apache_beam.utils.pipeline_options import SetupOptions
from apache_beam.utils.pipeline_options import StandardOptions
from apache_beam.utils.pipeline_options import TypeOptions
from apache_beam.utils.pipeline_options_validator import PipelineOptionsValidator
from apache_beam.utils.annotations import deprecated


class Pipeline(object):
  """A pipeline object that manages a DAG of PValues and their PTransforms.

  Conceptually the PValues are the DAG's nodes and the PTransforms computing
  the PValues are the edges.

  All the transforms applied to the pipeline must have distinct full labels.
  If same transform instance needs to be applied then a clone should be created
  with a new label (e.g., transform.clone('new label')).
  """

  def __init__(self, runner=None, options=None, argv=None):
    """Initialize a pipeline object.

    Args:
      runner: An object of type 'PipelineRunner' that will be used to execute
        the pipeline. For registered runners, the runner name can be specified,
        otherwise a runner object must be supplied.
      options: A configured 'PipelineOptions' object containing arguments
        that should be used for running the Dataflow job.
      argv: a list of arguments (such as sys.argv) to be used for building a
        'PipelineOptions' object. This will only be used if argument 'options'
        is None.

    Raises:
      ValueError: if either the runner or options argument is not of the
      expected type.
    """
    if options is not None:
      if isinstance(options, PipelineOptions):
        self._options = options
      else:
        raise ValueError(
            'Parameter options, if specified, must be of type PipelineOptions. '
            'Received : %r', options)
    elif argv is not None:
      if isinstance(argv, list):
        self._options = PipelineOptions(argv)
      else:
        raise ValueError(
            'Parameter argv, if specified, must be a list. Received : %r', argv)
    else:
      self._options = PipelineOptions([])

    if runner is None:
      runner = self._options.view_as(StandardOptions).runner
      if runner is None:
        runner = StandardOptions.DEFAULT_RUNNER
        logging.info(('Missing pipeline option (runner). Executing pipeline '
                      'using the default runner: %s.'), runner)

    if isinstance(runner, str):
      runner = create_runner(runner)
    elif not isinstance(runner, PipelineRunner):
      raise TypeError('Runner must be a PipelineRunner object or the '
                      'name of a registered runner.')

    # Validate pipeline options
    errors = PipelineOptionsValidator(self._options, runner).validate()
    if errors:
      raise ValueError(
          'Pipeline has validations errors: \n' + '\n'.join(errors))

    # Default runner to be used.
    self.runner = runner
    # Stack of transforms generated by nested apply() calls. The stack will
    # contain a root node as an enclosing (parent) node for top transforms.
    self.transforms_stack = [AppliedPTransform(None, None, '', None)]
    # Set of transform labels (full labels) applied to the pipeline.
    # If a transform is applied and the full label is already in the set
    # then the transform will have to be cloned with a new label.
    self.applied_labels = set()

  @property
  @deprecated(since='First stable release',
              extra_message='References to <pipeline>.options'
              ' will not be supported')
  def options(self):
    return self._options

  def _current_transform(self):
    """Returns the transform currently on the top of the stack."""
    return self.transforms_stack[-1]

  def _root_transform(self):
    """Returns the root transform of the transform stack."""
    return self.transforms_stack[0]

  def run(self, test_runner_api=True):
    """Runs the pipeline. Returns whatever our runner returns after running."""

    # When possible, invoke a round trip through the runner API.
    if test_runner_api and self._verify_runner_api_compatible():
      return Pipeline.from_runner_api(
          self.to_runner_api(), self.runner, self._options).run(False)

    if self._options.view_as(SetupOptions).save_main_session:
      # If this option is chosen, verify we can pickle the main session early.
      tmpdir = tempfile.mkdtemp()
      try:
        pickler.dump_session(os.path.join(tmpdir, 'main_session.pickle'))
      finally:
        shutil.rmtree(tmpdir)
    return self.runner.run(self)

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if not exc_type:
      self.run().wait_until_finish()

  def visit(self, visitor):
    """Visits depth-first every node of a pipeline's DAG.

    Args:
      visitor: PipelineVisitor object whose callbacks will be called for each
        node visited. See PipelineVisitor comments.

    Raises:
      TypeError: if node is specified and is not a PValue.
      pipeline.PipelineError: if node is specified and does not belong to this
        pipeline instance.
    """

    visited = set()
    self._root_transform().visit(visitor, self, visited)

  def apply(self, transform, pvalueish=None, label=None):
    """Applies a custom transform using the pvalueish specified.

    Args:
      transform: the PTranform to apply.
      pvalueish: the input for the PTransform (typically a PCollection).
      label: label of the PTransform.

    Raises:
      TypeError: if the transform object extracted from the argument list is
        not a PTransform.
      RuntimeError: if the transform object was already applied to this pipeline
        and needs to be cloned in order to apply again.
    """
    if isinstance(transform, ptransform._NamedPTransform):
      return self.apply(transform.transform, pvalueish,
                        label or transform.label)

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

    full_label = '/'.join([self._current_transform().full_label,
                           label or transform.label]).lstrip('/')
    if full_label in self.applied_labels:
      raise RuntimeError(
          'Transform "%s" does not have a stable unique label. '
          'This will prevent updating of pipelines. '
          'To apply a transform with a specified label write '
          'pvalue | "label" >> transform'
          % full_label)
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
    self.transforms_stack.append(current)

    type_options = self._options.view_as(TypeOptions)
    if type_options.pipeline_type_check:
      transform.type_check_inputs(pvalueish)

    pvalueish_result = self.runner.apply(transform, pvalueish)

    if type_options is not None and type_options.pipeline_type_check:
      transform.type_check_outputs(pvalueish_result)

    for result in ptransform.GetPValues().visit(pvalueish_result):
      assert isinstance(result, (pvalue.PValue, pvalue.DoOutputsTuple))

      # Make sure we set the producer only for a leaf node in the transform DAG.
      # This way we preserve the last transform of a composite transform as
      # being the real producer of the result.
      if result.producer is None:
        result.producer = current
      # TODO(robertwb): Multi-input, multi-output inference.
      # TODO(robertwb): Ideally we'd do intersection here.
      if (type_options is not None and type_options.pipeline_type_check
          and isinstance(result, pvalue.PCollection)
          and not result.element_type):
        input_element_type = (
            inputs[0].element_type
            if len(inputs) == 1
            else typehints.Any)
        type_hints = transform.get_type_hints()
        declared_output_type = type_hints.simple_output_type(transform.label)
        if declared_output_type:
          input_types = type_hints.input_types
          if input_types and input_types[0]:
            declared_input_type = input_types[0][0]
            result.element_type = typehints.bind_type_variables(
                declared_output_type,
                typehints.match_type_variables(declared_input_type,
                                               input_element_type))
          else:
            result.element_type = declared_output_type
        else:
          result.element_type = transform.infer_output_type(input_element_type)

      assert isinstance(result.producer.inputs, tuple)
      current.add_output(result)

    if (type_options is not None and
        type_options.type_check_strictness == 'ALL_REQUIRED' and
        transform.get_type_hints().output_types is None):
      ptransform_name = '%s(%s)' % (transform.__class__.__name__, full_label)
      raise TypeCheckError('Pipeline type checking is enabled, however no '
                           'output type-hint was found for the '
                           'PTransform %s' % ptransform_name)

    current.update_input_refcounts()
    self.transforms_stack.pop()
    return pvalueish_result

  def _verify_runner_api_compatible(self):
    class Visitor(PipelineVisitor):  # pylint: disable=used-before-assignment
      ok = True  # Really a nonlocal.

      def visit_transform(self, transform_node):
        if transform_node.side_inputs:
          # No side inputs (yet).
          Visitor.ok = False
        try:
          # Transforms must be picklable.
          pickler.loads(pickler.dumps(transform_node.transform,
                                      enable_trace=False),
                        enable_trace=False)
        except Exception:
          Visitor.ok = False

      def visit_value(self, value, _):
        if isinstance(value, pvalue.PDone):
          Visitor.ok = False

    self.visit(Visitor())
    return Visitor.ok

  def to_runner_api(self):
    from apache_beam.runners import pipeline_context
    from apache_beam.runners.api import beam_runner_api_pb2
    context = pipeline_context.PipelineContext()
    # Mutates context; placing inline would force dependence on
    # argument evaluation order.
    root_transform_id = context.transforms.get_id(self._root_transform())
    proto = beam_runner_api_pb2.Pipeline(
        root_transform_ids=[root_transform_id],
        components=context.to_runner_api())
    return proto

  @staticmethod
  def from_runner_api(proto, runner, options):
    p = Pipeline(runner=runner, options=options)
    from apache_beam.runners import pipeline_context
    context = pipeline_context.PipelineContext(proto.components)
    root_transform_id, = proto.root_transform_ids
    p.transforms_stack = [
        context.transforms.get_by_id(root_transform_id)]
    # TODO(robertwb): These are only needed to continue construction. Omit?
    p.applied_labels = set([
        t.unique_name for t in proto.components.transforms.values()])
    for id in proto.components.pcollections:
      context.pcollections.get_by_id(id).pipeline = p
    return p


class PipelineVisitor(object):
  """Visitor pattern class used to traverse a DAG of transforms.

  This is an internal class used for bookkeeping by a Pipeline.
  """

  def visit_value(self, value, producer_node):
    """Callback for visiting a PValue in the pipeline DAG.

    Args:
      value: PValue visited (typically a PCollection instance).
      producer_node: AppliedPTransform object whose transform produced the
        pvalue.
    """
    pass

  def visit_transform(self, transform_node):
    """Callback for visiting a transform node in the pipeline DAG."""
    pass

  def enter_composite_transform(self, transform_node):
    """Callback for entering traversal of a composite transform node."""
    pass

  def leave_composite_transform(self, transform_node):
    """Callback for leaving traversal of a composite transform node."""
    pass


class AppliedPTransform(object):
  """A transform node representing an instance of applying a PTransform.

  This is an internal class used for bookkeeping by a Pipeline.
  """

  def __init__(self, parent, transform, full_label, inputs):
    self.parent = parent
    self.transform = transform
    # Note that we want the PipelineVisitor classes to use the full_label,
    # inputs, side_inputs, and outputs fields from this instance instead of the
    # ones of the PTransform instance associated with it. Doing this permits
    # reusing PTransform instances in different contexts (apply() calls) without
    # any interference. This is particularly useful for composite transforms.
    self.full_label = full_label
    self.inputs = inputs or ()
    self.side_inputs = () if transform is None else tuple(transform.side_inputs)
    self.outputs = {}
    self.parts = []

    # Per tag refcount dictionary for PValues for which this node is a
    # root producer.
    self.refcounts = collections.defaultdict(int)

  def __repr__(self):
    return "%s(%s, %s)" % (self.__class__.__name__, self.full_label,
                           type(self.transform).__name__)

  def update_input_refcounts(self):
    """Increment refcounts for all transforms providing inputs."""

    def real_producer(pv):
      real = pv.producer
      while real.parts:
        real = real.parts[-1]
      return real

    if not self.is_composite():
      for main_input in self.inputs:
        if not isinstance(main_input, pvalue.PBegin):
          real_producer(main_input).refcounts[main_input.tag] += 1
      for side_input in self.side_inputs:
        real_producer(side_input.pvalue).refcounts[side_input.pvalue.tag] += 1

  def add_output(self, output, tag=None):
    if isinstance(output, pvalue.DoOutputsTuple):
      self.add_output(output[output._main_tag])
    elif isinstance(output, pvalue.PValue):
      # TODO(BEAM-1833): Require tags when calling this method.
      if tag is None and None in self.outputs:
        tag = len(self.outputs)
      assert tag not in self.outputs
      self.outputs[tag] = output
    else:
      raise TypeError("Unexpected output type: %s" % output)

  def add_part(self, part):
    assert isinstance(part, AppliedPTransform)
    self.parts.append(part)

  def is_composite(self):
    """Returns whether this is a composite transform.

    A composite transform has parts (inner transforms) or isn't the
    producer for any of its outputs. (An example of a transform that
    is not a producer is one that returns its inputs instead.)
    """
    return bool(self.parts) or all(
        pval.producer is not self for pval in self.outputs.values())

  def visit(self, visitor, pipeline, visited):
    """Visits all nodes reachable from the current node."""

    for pval in self.inputs:
      if pval not in visited and not isinstance(pval, pvalue.PBegin):
        assert pval.producer is not None
        pval.producer.visit(visitor, pipeline, visited)
        # The value should be visited now since we visit outputs too.
        assert pval in visited, pval

    # Visit side inputs.
    for pval in self.side_inputs:
      if isinstance(pval, pvalue.AsSideInput) and pval.pvalue not in visited:
        pval = pval.pvalue  # Unpack marker-object-wrapped pvalue.
        assert pval.producer is not None
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
    for pval in self.outputs.values():
      if isinstance(pval, pvalue.DoOutputsTuple):
        pvals = (v for v in pval)
      else:
        pvals = (pval,)
      for v in pvals:
        if v not in visited:
          visited.add(v)
          visitor.visit_value(v, self)

  def named_inputs(self):
    # TODO(BEAM-1833): Push names up into the sdk construction.
    return {str(ix): input for ix, input in enumerate(self.inputs)
            if isinstance(input, pvalue.PCollection)}

  def named_outputs(self):
    return {str(tag): output for tag, output in self.outputs.items()
            if isinstance(output, pvalue.PCollection)}

  def to_runner_api(self, context):
    from apache_beam.runners.api import beam_runner_api_pb2

    def transform_to_runner_api(transform, context):
      if transform is None:
        return None
      else:
        return transform.to_runner_api(context)
    return beam_runner_api_pb2.PTransform(
        unique_name=self.full_label,
        spec=transform_to_runner_api(self.transform, context),
        subtransforms=[context.transforms.get_id(part) for part in self.parts],
        # TODO(BEAM-115): Side inputs.
        inputs={tag: context.pcollections.get_id(pc)
                for tag, pc in self.named_inputs().items()},
        outputs={str(tag): context.pcollections.get_id(out)
                 for tag, out in self.named_outputs().items()},
        # TODO(BEAM-115): display_data
        display_data=None)

  @staticmethod
  def from_runner_api(proto, context):
    result = AppliedPTransform(
        parent=None,
        transform=ptransform.PTransform.from_runner_api(proto.spec, context),
        full_label=proto.unique_name,
        inputs=[
            context.pcollections.get_by_id(id) for id in proto.inputs.values()])
    result.parts = [
        context.transforms.get_by_id(id) for id in proto.subtransforms]
    result.outputs = {
        None if tag == 'None' else tag: context.pcollections.get_by_id(id)
        for tag, id in proto.outputs.items()}
    if not result.parts:
      for tag, pc in result.outputs.items():
        if pc not in result.inputs:
          pc.producer = result
          pc.tag = tag
    result.update_input_refcounts()
    return result
