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

"""Runtime type checking support."""

import collections
import inspect
import sys
import types

from apache_beam.pvalue import TaggedOutput
from apache_beam.transforms.core import DoFn
from apache_beam.transforms.window import WindowedValue
from apache_beam.typehints import check_constraint
from apache_beam.typehints import CompositeTypeHintError
from apache_beam.typehints import GeneratorWrapper
from apache_beam.typehints import SimpleTypeHintError
from apache_beam.typehints import TypeCheckError
from apache_beam.typehints.decorators import _check_instance_type
from apache_beam.typehints.decorators import getcallargs_forhints


class AbstractDoFnWrapper(DoFn):
  """An abstract class to create wrapper around DoFn"""

  def __init__(self, dofn):
    super(AbstractDoFnWrapper, self).__init__()
    self.dofn = dofn

  def _inspect_start_bundle(self):
    return self.dofn.get_function_arguments('start_bundle')

  def _inspect_process(self):
    return self.dofn.get_function_arguments('process')

  def _inspect_finish_bundle(self):
    return self.dofn.get_function_arguments('finish_bundle')

  def wrapper(self, method, args, kwargs):
    return method(*args, **kwargs)

  def start_bundle(self, *args, **kwargs):
    return self.wrapper(self.dofn.start_bundle, args, kwargs)

  def process(self, *args, **kwargs):
    return self.wrapper(self.dofn.process, args, kwargs)

  def finish_bundle(self, *args, **kwargs):
    return self.wrapper(self.dofn.finish_bundle, args, kwargs)

  def is_process_bounded(self):
    return self.dofn.is_process_bounded()


class OutputCheckWrapperDoFn(AbstractDoFnWrapper):
  """A DoFn that verifies against common errors in the output type."""

  def __init__(self, dofn, full_label):
    super(OutputCheckWrapperDoFn, self).__init__(dofn)
    self.full_label = full_label

  def wrapper(self, method, args, kwargs):
    try:
      result = method(*args, **kwargs)
    except TypeCheckError as e:
      error_msg = ('Runtime type violation detected within ParDo(%s): '
                   '%s' % (self.full_label, e))
      raise TypeCheckError, error_msg, sys.exc_info()[2]
    else:
      return self._check_type(result)

  def _check_type(self, output):
    if output is None:
      return output
    elif isinstance(output, (dict, basestring)):
      object_type = type(output).__name__
      raise TypeCheckError('Returning a %s from a ParDo or FlatMap is '
                           'discouraged. Please use list("%s") if you really '
                           'want this behavior.' %
                           (object_type, output))
    elif not isinstance(output, collections.Iterable):
      raise TypeCheckError('FlatMap and ParDo must return an '
                           'iterable. %s was returned instead.'
                           % type(output))
    return output


class TypeCheckWrapperDoFn(AbstractDoFnWrapper):
  """A wrapper around a DoFn which performs type-checking of input and output.
  """

  def __init__(self, dofn, type_hints, label=None):
    super(TypeCheckWrapperDoFn, self).__init__(dofn)
    self.dofn = dofn
    self._process_fn = self.dofn.process_argspec_fn()
    if type_hints.input_types:
      input_args, input_kwargs = type_hints.input_types
      self._input_hints = getcallargs_forhints(
          self._process_fn, *input_args, **input_kwargs)
    else:
      self._input_hints = None
    # TODO(robertwb): Multi-output.
    self._output_type_hint = type_hints.simple_output_type(label)

  def wrapper(self, method, args, kwargs):
    result = method(*args, **kwargs)
    return self._type_check_result(result)

  def process(self, *args, **kwargs):
    if self._input_hints:
      actual_inputs = inspect.getcallargs(self._process_fn, *args, **kwargs)
      for var, hint in self._input_hints.items():
        if hint is actual_inputs[var]:
          # self parameter
          continue
        _check_instance_type(hint, actual_inputs[var], var, True)
    return self._type_check_result(self.dofn.process(*args, **kwargs))

  def _type_check_result(self, transform_results):
    if self._output_type_hint is None or transform_results is None:
      return transform_results

    def type_check_output(o):
      # TODO(robertwb): Multi-output.
      x = o.value if isinstance(o, (TaggedOutput, WindowedValue)) else o
      self._type_check(self._output_type_hint, x, is_input=False)

    # If the return type is a generator, then we will need to interleave our
    # type-checking with its normal iteration so we don't deplete the
    # generator initially just by type-checking its yielded contents.
    if isinstance(transform_results, types.GeneratorType):
      return GeneratorWrapper(transform_results, type_check_output)
    for o in transform_results:
      type_check_output(o)
    return transform_results

  def _type_check(self, type_constraint, datum, is_input):
    """Typecheck a PTransform related datum according to a type constraint.

    This function is used to optionally type-check either an input or an output
    to a PTransform.

    Args:
        type_constraint: An instance of a typehints.TypeContraint, one of the
          white-listed builtin Python types, or a custom user class.
        datum: An instance of a Python object.
        is_input: True if 'datum' is an input to a PTransform's DoFn. False
          otherwise.

    Raises:
      TypeError: If 'datum' fails to type-check according to 'type_constraint'.
    """
    datum_type = 'input' if is_input else 'output'

    try:
      check_constraint(type_constraint, datum)
    except CompositeTypeHintError as e:
      raise TypeCheckError, e.message, sys.exc_info()[2]
    except SimpleTypeHintError:
      error_msg = ("According to type-hint expected %s should be of type %s. "
                   "Instead, received '%s', an instance of type %s."
                   % (datum_type, type_constraint, datum, type(datum)))
      raise TypeCheckError, error_msg, sys.exc_info()[2]
