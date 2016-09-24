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

"""PTransform and descendants.

A PTransform is an object describing (not executing) a computation. The actual
execution semantics for a transform is captured by a runner object. A transform
object always belongs to a pipeline object.

A PTransform derived class needs to define the apply() method that describes
how one or more PValues are created by the transform.

The module defines a few standard transforms: FlatMap (parallel do),
GroupByKey (group by key), etc. Note that the apply() methods for these
classes contain code that will add nodes to the processing graph associated
with a pipeline.

As support for the FlatMap transform, the module also defines a DoFn
class and wrapper class that allows lambda functions to be used as
FlatMap processing functions.
"""

from __future__ import absolute_import

import copy
import inspect
import operator
import os
import sys

from apache_beam import error
from apache_beam import pvalue
from apache_beam import typehints
from apache_beam.internal import pickler
from apache_beam.internal import util
from apache_beam.typehints import getcallargs_forhints
from apache_beam.typehints import TypeCheckError
from apache_beam.typehints import validate_composite_type_param
from apache_beam.typehints import WithTypeHints
from apache_beam.typehints.trivial_inference import instance_to_type


class _PValueishTransform(object):
  """Visitor for PValueish objects.

  A PValueish is a PValue, or list, tuple, dict of PValuesish objects.

  This visits a PValueish, contstructing a (possibly mutated) copy.
  """
  def visit(self, node, *args):
    return getattr(
        self,
        'visit_' + node.__class__.__name__,
        lambda x, *args: x)(node, *args)

  def visit_list(self, node, *args):
    return [self.visit(x, *args) for x in node]

  def visit_tuple(self, node, *args):
    return tuple(self.visit(x, *args) for x in node)

  def visit_dict(self, node, *args):
    return {key: self.visit(value, *args) for (key, value) in node.items()}


class _SetInputPValues(_PValueishTransform):
  def visit(self, node, replacements):
    if id(node) in replacements:
      return replacements[id(node)]
    else:
      return super(_SetInputPValues, self).visit(node, replacements)


class _MaterializedDoOutputsTuple(pvalue.DoOutputsTuple):
  def __init__(self, deferred, pvalue_cache):
    super(_MaterializedDoOutputsTuple, self).__init__(
        None, None, deferred._tags, deferred._main_tag)
    self._deferred = deferred
    self._pvalue_cache = pvalue_cache

  def __getitem__(self, tag):
    return self._pvalue_cache.get_unwindowed_pvalue(self._deferred[tag])


class _MaterializePValues(_PValueishTransform):
  def __init__(self, pvalue_cache):
    self._pvalue_cache = pvalue_cache

  def visit(self, node):
    if isinstance(node, pvalue.PValue):
      return self._pvalue_cache.get_unwindowed_pvalue(node)
    elif isinstance(node, pvalue.DoOutputsTuple):
      return _MaterializedDoOutputsTuple(node, self._pvalue_cache)
    else:
      return super(_MaterializePValues, self).visit(node)


class GetPValues(_PValueishTransform):
  def visit(self, node, pvalues=None):
    if pvalues is None:
      pvalues = []
      self.visit(node, pvalues)
      return pvalues
    elif isinstance(node, (pvalue.PValue, pvalue.DoOutputsTuple)):
      pvalues.append(node)
    else:
      super(GetPValues, self).visit(node, pvalues)


class ZipPValues(_PValueishTransform):
  """Pairs each PValue in a pvalueish with a value in a parallel out sibling.

  Sibling should have the same nested structure as pvalueish.  Leaves in
  sibling are expanded across nested pvalueish lists, tuples, and dicts.
  For example

      ZipPValues().visit({'a': pc1, 'b': (pc2, pc3)},
                         {'a': 'A', 'b', 'B'})

  will return

      [('a', pc1, 'A'), ('b', pc2, 'B'), ('b', pc3, 'B')]
  """

  def visit(self, pvalueish, sibling, pairs=None, context=None):
    if pairs is None:
      pairs = []
      self.visit(pvalueish, sibling, pairs, context)
      return pairs
    elif isinstance(pvalueish, (pvalue.PValue, pvalue.DoOutputsTuple)):
      pairs.append((context, pvalueish, sibling))
    else:
      super(ZipPValues, self).visit(pvalueish, sibling, pairs, context)

  def visit_list(self, pvalueish, sibling, pairs, context):
    if isinstance(sibling, (list, tuple)):
      for ix, (p, s) in enumerate(zip(
          pvalueish, list(sibling) + [None] * len(pvalueish))):
        self.visit(p, s, pairs, 'position %s' % ix)
    else:
      for p in pvalueish:
        self.visit(p, sibling, pairs, context)

  def visit_tuple(self, pvalueish, sibling, pairs, context):
    self.visit_list(pvalueish, sibling, pairs, context)

  def visit_dict(self, pvalueish, sibling, pairs, context):
    if isinstance(sibling, dict):
      for key, p in pvalueish.items():
        self.visit(p, sibling.get(key), pairs, key)
    else:
      for p in pvalueish.values():
        self.visit(p, sibling, pairs, context)


class PTransform(WithTypeHints):
  """A transform object used to modify one or more PCollections.

  Subclasses must define an apply() method that will be used when the transform
  is applied to some arguments. Typical usage pattern will be:

    input | CustomTransform(...)

  The apply() method of the CustomTransform object passed in will be called
  with input as an argument.
  """
  # By default, transforms don't have any side inputs.
  side_inputs = ()

  # Used for nullary transforms.
  pipeline = None

  # Default is unset.
  _user_label = None

  def __init__(self, label=None):
    super(PTransform, self).__init__()
    self.label = label

  @property
  def label(self):
    return self._user_label or self.default_label()

  @label.setter
  def label(self, value):
    self._user_label = value

  def default_label(self):
    return self.__class__.__name__

  @classmethod
  def parse_label_and_arg(cls, args, kwargs, arg_name):
    """Parses a tuple of positional arguments into label, arg_name.

    The function is used by functions that take a (label, arg_name) list of
    parameters and in which first label could be optional even if the arg_name
    is not passed as a keyword. More specifically the following calling patterns
    are allowed::

      (value)
      ('label', value)
      (arg_name=value)
      ('label', arg_name=value)
      (value, label='label')
      (label='label', arg_name=value)

    Args:
      args: A tuple of position arguments.
      kwargs: A dictionary of keyword arguments.
      arg_name: The name of the second argument.

    Returns:
      A (label, value) tuple. The label will be the one passed in or one
      derived from the class name. The value will the corresponding value for
      the arg_name argument.

    Raises:
      ValueError: If the label and value cannot be deduced from args and kwargs
        and also if the label is not a string.
    """
    # TODO(robertwb): Fix to not silently drop extra arguments.
    kw_label = kwargs.get('label', None)
    kw_value = kwargs.get(arg_name, None)

    if kw_value is not None:
      value = kw_value
    else:
      value = args[1] if len(args) > 1 else args[0] if args else None

    if kw_label is not None:
      label = kw_label
    else:
      # We need to get a label from positional arguments. If we did not get a
      # keyword value for the arg_name either then expect that a one element
      # list will provide the value and the label will be derived from the class
      # name.
      num_args = len(args)
      if kw_value is None:
        label = args[0] if num_args >= 2 else cls.__name__
      else:
        label = args[0] if num_args >= 1 else cls.__name__

    if label is None or value is None or not isinstance(label, basestring):
      raise ValueError(
          '%s expects a (label, %s) or (%s) argument list '
          'instead of args=%s, kwargs=%s' % (
              cls.__name__, arg_name, arg_name, args, kwargs))
    return label, value

  def with_input_types(self, input_type_hint):
    """Annotates the input type of a PTransform with a type-hint.

    Args:
      input_type_hint: An instance of an allowed built-in type, a custom class,
        or an instance of a typehints.TypeConstraint.

    Raises:
      TypeError: If 'type_hint' is not a valid type-hint. See
        typehints.validate_composite_type_param for further details.

    Returns:
      A reference to the instance of this particular PTransform object. This
      allows chaining type-hinting related methods.
    """
    validate_composite_type_param(input_type_hint,
                                  'Type hints for a PTransform')
    return super(PTransform, self).with_input_types(input_type_hint)

  def with_output_types(self, type_hint):
    """Annotates the output type of a PTransform with a type-hint.

    Args:
      type_hint: An instance of an allowed built-in type, a custom class, or a
        typehints.TypeConstraint.

    Raises:
      TypeError: If 'type_hint' is not a valid type-hint. See
        typehints.validate_composite_type_param for further details.

    Returns:
      A reference to the instance of this particular PTransform object. This
      allows chaining type-hinting related methods.
    """
    validate_composite_type_param(type_hint, 'Type hints for a PTransform')
    return super(PTransform, self).with_output_types(type_hint)

  def type_check_inputs(self, pvalueish):
    self.type_check_inputs_or_outputs(pvalueish, 'input')

  def infer_output_type(self, unused_input_type):
    return self.get_type_hints().simple_output_type(self.label) or typehints.Any

  def type_check_outputs(self, pvalueish):
    self.type_check_inputs_or_outputs(pvalueish, 'output')

  def type_check_inputs_or_outputs(self, pvalueish, input_or_output):
    hints = getattr(self.get_type_hints(), input_or_output + '_types')
    if not hints:
      return
    arg_hints, kwarg_hints = hints
    if arg_hints and kwarg_hints:
      raise TypeCheckError(
          'PTransform cannot have both positional and keyword type hints '
          'without overriding %s._type_check_%s()' % (
              self.__class__, input_or_output))
    root_hint = (
        arg_hints[0] if len(arg_hints) == 1 else arg_hints or kwarg_hints)
    for context, pvalue_, hint in ZipPValues().visit(pvalueish, root_hint):
      if pvalue_.element_type is None:
        # TODO(robertwb): It's a bug that we ever get here. (typecheck)
        continue
      if hint and not typehints.is_consistent_with(pvalue_.element_type, hint):
        at_context = ' %s %s' % (input_or_output, context) if context else ''
        raise TypeCheckError(
            '%s type hint violation at %s%s: expected %s, got %s' % (
                input_or_output.title(), self.label, at_context, hint,
                pvalue_.element_type))

  def _infer_output_coder(self, input_type=None, input_coder=None):
    """Returns the output coder to use for output of this transform.

    Note: this API is experimental and is subject to change; please do not rely
    on behavior induced by this method.

    The Coder returned here should not be wrapped in a WindowedValueCoder
    wrapper.

    Args:
      input_type: An instance of an allowed built-in type, a custom class, or a
        typehints.TypeConstraint for the input type, or None if not available.
      input_coder: Coder object for encoding input to this PTransform, or None
        if not available.

    Returns:
      Coder object for encoding output of this PTransform or None if unknown.
    """
    # TODO(ccy): further refine this API.
    return None

  def clone(self, new_label):
    """Clones the current transform instance under a new label."""
    transform = copy.copy(self)
    transform.label = new_label
    return transform

  def apply(self, input_or_inputs):
    raise NotImplementedError

  def __str__(self):
    return '<%s>' % self._str_internal()

  def __repr__(self):
    return '<%s at %s>' % (self._str_internal(), hex(id(self)))

  def _str_internal(self):
    return '%s(PTransform)%s%s%s' % (
        self.__class__.__name__,
        ' label=[%s]' % self.label if (hasattr(self, 'label') and
                                       self.label) else '',
        ' inputs=%s' % str(self.inputs) if (hasattr(self, 'inputs') and
                                            self.inputs) else '',
        ' side_inputs=%s' % str(self.side_inputs) if self.side_inputs else '')

  def _check_pcollection(self, pcoll):
    if not isinstance(pcoll, pvalue.PCollection):
      raise error.TransformError('Expecting a PCollection argument.')
    if not pcoll.pipeline:
      raise error.TransformError('PCollection not part of a pipeline.')

  def get_windowing(self, inputs):
    """Returns the window function to be associated with transform's output.

    By default most transforms just return the windowing function associated
    with the input PCollection (or the first input if several).
    """
    # TODO(robertwb): Assert all input WindowFns compatible.
    return inputs[0].windowing

  def __rrshift__(self, label):
    return _NamedPTransform(self, label)

  def __or__(self, right):
    """Used to compose PTransforms, e.g., ptransform1 | ptransform2."""
    if isinstance(right, PTransform):
      return ChainedPTransform(self, right)
    else:
      return NotImplemented

  def __ror__(self, left, label=None):
    """Used to apply this PTransform to non-PValues, e.g., a tuple."""
    pvalueish, pvalues = self._extract_input_pvalues(left)
    pipelines = [v.pipeline for v in pvalues if isinstance(v, pvalue.PValue)]
    if pvalues and not pipelines:
      deferred = False
      # pylint: disable=wrong-import-order, wrong-import-position
      from apache_beam import pipeline
      from apache_beam.utils.options import PipelineOptions
      # pylint: enable=wrong-import-order, wrong-import-position
      p = pipeline.Pipeline(
          'DirectPipelineRunner', PipelineOptions(sys.argv))
    else:
      if not pipelines:
        if self.pipeline is not None:
          p = self.pipeline
        else:
          raise ValueError('"%s" requires a pipeline to be specified '
                           'as there are no deferred inputs.'% self.label)
      else:
        p = self.pipeline or pipelines[0]
        for pp in pipelines:
          if p != pp:
            raise ValueError(
                'Mixing value from different pipelines not allowed.')
      deferred = not getattr(p.runner, 'is_eager', False)
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.transforms.core import Create
    # pylint: enable=wrong-import-order, wrong-import-position
    replacements = {id(v): p | Create('CreatePInput%s' % ix, v)
                    for ix, v in enumerate(pvalues)
                    if not isinstance(v, pvalue.PValue) and v is not None}
    pvalueish = _SetInputPValues().visit(pvalueish, replacements)
    self.pipeline = p
    result = p.apply(self, pvalueish, label)
    if deferred:
      return result
    else:
      # Get a reference to the runners internal cache, otherwise runner may
      # clean it after run.
      cache = p.runner.cache
      p.run()
      return _MaterializePValues(cache).visit(result)

  def _extract_input_pvalues(self, pvalueish):
    """Extract all the pvalues contained in the input pvalueish.

    Returns pvalueish as well as the flat inputs list as the input may have to
    be copied as inspection may be destructive.

    By default, recursively extracts tuple components and dict values.

    Generally only needs to be overriden for multi-input PTransforms.
    """
    # pylint: disable=wrong-import-order
    from apache_beam import pipeline
    # pylint: enable=wrong-import-order
    if isinstance(pvalueish, pipeline.Pipeline):
      pvalueish = pvalue.PBegin(pvalueish)

    def _dict_tuple_leaves(pvalueish):
      if isinstance(pvalueish, tuple):
        for a in pvalueish:
          for p in _dict_tuple_leaves(a):
            yield p
      elif isinstance(pvalueish, dict):
        for a in pvalueish.values():
          for p in _dict_tuple_leaves(a):
            yield p
      else:
        yield pvalueish
    return pvalueish, tuple(_dict_tuple_leaves(pvalueish))


class ChainedPTransform(PTransform):

  def __init__(self, *parts):
    super(ChainedPTransform, self).__init__(label=self._chain_label(parts))
    self._parts = parts

  def _chain_label(self, parts):
    return '|'.join(p.label for p in parts)

  def __or__(self, right):
    if isinstance(right, PTransform):
      # Create a flat list rather than a nested tree of composite
      # transforms for better monitoring, etc.
      return ChainedPTransform(*(self._parts + (right,)))
    else:
      return NotImplemented

  def apply(self, pval):
    return reduce(operator.or_, self._parts, pval)


class PTransformWithSideInputs(PTransform):
  """A superclass for any PTransform (e.g. FlatMap or Combine)
  invoking user code.

  PTransforms like FlatMap invoke user-supplied code in some kind of
  package (e.g. a DoFn) and optionally provide arguments and side inputs
  to that code. This internal-use-only class contains common functionality
  for PTransforms that fit this model.
  """

  def __init__(self, fn_or_label, *args, **kwargs):
    if fn_or_label is None or isinstance(fn_or_label, basestring):
      label = fn_or_label
      fn, args = args[0], args[1:]
    else:
      label = None
      fn = fn_or_label
    if isinstance(fn, type) and issubclass(fn, typehints.WithTypeHints):
      # Don't treat Fn class objects as callables.
      raise ValueError('Use %s() not %s.' % (fn.__name__, fn.__name__))
    self.fn = self.make_fn(fn)
    # Now that we figure out the label, initialize the super-class.
    super(PTransformWithSideInputs, self).__init__(label=label)

    if (any([isinstance(v, pvalue.PCollection) for v in args]) or
        any([isinstance(v, pvalue.PCollection) for v in kwargs.itervalues()])):
      raise error.SideInputError(
          'PCollection used directly as side input argument. Specify '
          'AsIter(pcollection) or AsSingleton(pcollection) to indicate how the '
          'PCollection is to be used.')
    self.args, self.kwargs, self.side_inputs = util.remove_objects_from_args(
        args, kwargs, pvalue.PCollectionView)
    self.raw_side_inputs = args, kwargs

    # Prevent name collisions with fns of the form '<function <lambda> at ...>'
    self._cached_fn = self.fn

    # Ensure fn and side inputs are picklable for remote execution.
    self.fn = pickler.loads(pickler.dumps(self.fn))
    self.args = pickler.loads(pickler.dumps(self.args))
    self.kwargs = pickler.loads(pickler.dumps(self.kwargs))

    # For type hints, because loads(dumps(class)) != class.
    self.fn = self._cached_fn

  def with_input_types(
      self, input_type_hint, *side_inputs_arg_hints, **side_input_kwarg_hints):
    """Annotates the types of main inputs and side inputs for the PTransform.

    Args:
      input_type_hint: An instance of an allowed built-in type, a custom class,
        or an instance of a typehints.TypeConstraint.
      *side_inputs_arg_hints: A variable length argument composed of
        of an allowed built-in type, a custom class, or a
        typehints.TypeConstraint.
      **side_input_kwarg_hints: A dictionary argument composed of
        of an allowed built-in type, a custom class, or a
        typehints.TypeConstraint.

    Example of annotating the types of side-inputs:
      FlatMap().with_input_types(int, int, bool)

    Raises:
      TypeError: If 'type_hint' is not a valid type-hint. See
        typehints.validate_composite_type_param for further details.

    Returns:
      A reference to the instance of this particular PTransform object. This
      allows chaining type-hinting related methods.
    """
    super(PTransformWithSideInputs, self).with_input_types(input_type_hint)

    for si in side_inputs_arg_hints:
      validate_composite_type_param(si, 'Type hints for a PTransform')
    for si in side_input_kwarg_hints.values():
      validate_composite_type_param(si, 'Type hints for a PTransform')

    self.side_inputs_types = side_inputs_arg_hints
    return WithTypeHints.with_input_types(
        self, input_type_hint, *side_inputs_arg_hints, **side_input_kwarg_hints)

  def type_check_inputs(self, pvalueish):
    type_hints = self.get_type_hints().input_types
    if type_hints:
      args, kwargs = self.raw_side_inputs

      def element_type(side_input):
        if isinstance(side_input, pvalue.PCollectionView):
          return side_input.element_type
        else:
          return instance_to_type(side_input)
      arg_types = [pvalueish.element_type] + [element_type(v) for v in args]
      kwargs_types = {k: element_type(v) for (k, v) in kwargs.items()}
      argspec_fn = self.process_argspec_fn()
      bindings = getcallargs_forhints(argspec_fn, *arg_types, **kwargs_types)
      hints = getcallargs_forhints(argspec_fn, *type_hints[0], **type_hints[1])
      for arg, hint in hints.items():
        if arg.startswith('%unknown%'):
          continue
        if hint is None:
          continue
        if not typehints.is_consistent_with(
            bindings.get(arg, typehints.Any), hint):
          raise typehints.TypeCheckError(
              'Type hint violation for \'%s\': requires %s but got %s for %s'
              % (self.label, hint, bindings[arg], arg))

  def process_argspec_fn(self):
    """Returns an argspec of the function actually consuming the data.
    """
    raise NotImplementedError

  def make_fn(self, fn):
    # TODO(silviuc): Add comment describing that this is meant to be overriden
    # by methods detecting callables and wrapping them in DoFns.
    return fn

  def default_label(self):
    return '%s(%s)' % (self.__class__.__name__, self.fn.default_label())


class CallablePTransform(PTransform):
  """A class wrapper for a function-based transform."""

  def __init__(self, fn):
    # pylint: disable=super-init-not-called
    # This  is a helper class for a function decorator. Only when the class
    # is called (and __call__ invoked) we will have all the information
    # needed to initialize the super class.
    self.fn = fn
    self._args = ()
    self._kwargs = {}

  def __call__(self, *args, **kwargs):
    if args and args[0] is None:
      label, self._args = None, args[1:]
    elif args and isinstance(args[0], str):
      label, self._args = args[0], args[1:]
    else:
      label, self._args = None, args
    self._kwargs = kwargs
    # We know the label now, so initialize the super-class.
    super(CallablePTransform, self).__init__(label=label)
    return self

  def apply(self, pcoll):
    # Since the PTransform will be implemented entirely as a function
    # (once called), we need to pass through any type-hinting information that
    # may have been annotated via the .with_input_types() and
    # .with_output_types() methods.
    kwargs = dict(self._kwargs)
    args = tuple(self._args)
    try:
      if 'type_hints' in inspect.getargspec(self.fn).args:
        args = (self.get_type_hints(),) + args
    except TypeError:
      # Might not be a function.
      pass
    return self.fn(pcoll, *args, **kwargs)

  def default_label(self):
    if self._args:
      return '%s(%s)' % (
          label_from_callable(self.fn), label_from_callable(self._args[0]))
    else:
      return label_from_callable(self.fn)


def ptransform_fn(fn):
  """A decorator for a function-based PTransform.

  Args:
    fn: A function implementing a custom PTransform.

  Returns:
    A CallablePTransform instance wrapping the function-based PTransform.

  This wrapper provides an alternative, simpler way to define a PTransform.
  The standard method is to subclass from PTransform and override the apply()
  method. An equivalent effect can be obtained by defining a function that
  an input PCollection and additional optional arguments and returns a
  resulting PCollection. For example::

    @ptransform_fn
    def CustomMapper(pcoll, mapfn):
      return pcoll | ParDo(mapfn)

  The equivalent approach using PTransform subclassing::

    class CustomMapper(PTransform):

      def __init__(self, mapfn):
        super(CustomMapper, self).__init__()
        self.mapfn = mapfn

      def apply(self, pcoll):
        return pcoll | ParDo(self.mapfn)

  With either method the custom PTransform can be used in pipelines as if
  it were one of the "native" PTransforms::

    result_pcoll = input_pcoll | 'label' >> CustomMapper(somefn)

  Note that for both solutions the underlying implementation of the pipe
  operator (i.e., `|`) will inject the pcoll argument in its proper place
  (first argument if no label was specified and second argument otherwise).
  """
  return CallablePTransform(fn)


def label_from_callable(fn):
  if hasattr(fn, 'default_label'):
    return fn.default_label()
  elif hasattr(fn, '__name__'):
    if fn.__name__ == '<lambda>':
      return '<lambda at %s:%s>' % (
          os.path.basename(fn.func_code.co_filename),
          fn.func_code.co_firstlineno)
    else:
      return fn.__name__
  else:
    return str(fn)


class _NamedPTransform(PTransform):

  def __init__(self, transform, label):
    super(_NamedPTransform, self).__init__(label)
    self.transform = transform

  def __ror__(self, pvalueish):
    return self.transform.__ror__(pvalueish, self.label)

  def apply(self, pvalue):
    raise RuntimeError("Should never be applied directly.")
