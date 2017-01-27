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
  pipeline = Pipeline(runner=DirectPipelineRunner())

  # Add to the pipeline a "Create" transform. When executed this
  # transform will produce a PCollection object with the specified values.
  pcoll = pipeline.create('label', [1, 2, 3])

  # run() will execute the DAG stored in the pipeline.  The execution of the
  # nodes visited is done using the specified local runner.
  pipeline.run()

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
from apache_beam.transforms import format_full_label
from apache_beam.transforms import ptransform
from apache_beam.typehints import TypeCheckError
from apache_beam.utils.options import PipelineOptions
from apache_beam.utils.options import SetupOptions
from apache_beam.utils.options import StandardOptions
from apache_beam.utils.options import TypeOptions
from apache_beam.utils.pipeline_options_validator import PipelineOptionsValidator


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
        self.options = options
      else:
        raise ValueError(
            'Parameter options, if specified, must be of type PipelineOptions. '
            'Received : %r', options)
    elif argv is not None:
      if isinstance(argv, list):
        self.options = PipelineOptions(argv)
      else:
        raise ValueError(
            'Parameter argv, if specified, must be a list. Received : %r', argv)
    else:
      self.options = None

    if runner is None and self.options is not None:
      runner = self.options.view_as(StandardOptions).runner
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
    if self.options is not None:
      errors = PipelineOptionsValidator(self.options, runner).validate()
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
    # Store cache of views created from PCollections.  For reference, see
    # pvalue._cache_view().
    self._view_cache = {}

  def _current_transform(self):
    """Returns the transform currently on the top of the stack."""
    return self.transforms_stack[-1]

  def _root_transform(self):
    """Returns the root transform of the transform stack."""
    return self.transforms_stack[0]

  def run(self):
    """Runs the pipeline. Returns whatever our runner returns after running."""
    if not self.options or self.options.view_as(SetupOptions).save_main_session:
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
      self.run()

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

  def apply(self, transform, pvalueish=None):
    """Applies a custom transform using the pvalueish specified.

    Args:
      transform: the PTranform (or callable) to apply.
      pvalueish: the input for the PTransform (typically a PCollection).

    Raises:
      TypeError: if the transform object extracted from the argument list is
        not a callable type or a descendant from PTransform.
      RuntimeError: if the transform object was already applied to this pipeline
        and needs to be cloned in order to apply again.
    """
    if not isinstance(transform, ptransform.PTransform):
      transform = _CallableWrapperPTransform(transform)

    full_label = format_full_label(self._current_transform(), transform)
    if full_label in self.applied_labels:
      raise RuntimeError(
          'Transform "%s" does not have a stable unique label. '
          'This will prevent updating of pipelines. '
          'To clone a transform with a new label use: '
          'transform.clone("NEW LABEL").'
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
          '_extract_input_values' % (pvalueish, transform))

    current = AppliedPTransform(
        self._current_transform(), transform, full_label, inputs)
    self._current_transform().add_part(current)
    self.transforms_stack.append(current)

    if self.options is not None:
      type_options = self.options.view_as(TypeOptions)
    else:
      type_options = None

    if type_options is not None and type_options.pipeline_type_check:
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
      if (type_options is not None and type_options.pipeline_type_check and
          isinstance(result, (pvalue.PCollection, pvalue.PCollectionView))
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


class _CallableWrapperPTransform(ptransform.PTransform):

  def __init__(self, callee):
    assert callable(callee)
    super(_CallableWrapperPTransform, self).__init__(
        label=getattr(callee, '__name__', 'Callable'))
    self._callee = callee

  def apply(self, *args, **kwargs):
    return self._callee(*args, **kwargs)


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
        real_producer(side_input).refcounts[side_input.tag] += 1

  def add_output(self, output, tag=None):
    assert (isinstance(output, pvalue.PValue) or
            isinstance(output, pvalue.DoOutputsTuple))
    if tag is None:
      tag = len(self.outputs)
    assert tag not in self.outputs
    self.outputs[tag] = output

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
      if isinstance(pval, pvalue.PCollectionView) and pval not in visited:
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
