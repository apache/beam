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

"""Type coders registration.

This module contains functionality to define and use coders for custom classes.
Let's say we have a class Xyz and we are processing a PCollection with elements
of type Xyz. If we do not register a coder for Xyz, a default pickle-based
fallback coder will be used. This can be undesirable for two reasons. First, we
may want a faster coder or a more space efficient one. Second, the pickle-based
coder is not deterministic in the sense that objects like dictionaries or sets
are not guaranteed to be encoded in the same way every time (elements are not
really ordered).

Two (sometimes three) steps are needed to define and use a custom coder:
  - define the coder class
  - associate the code with the class (a.k.a. coder registration)
  - typehint DoFns or transforms with the new class or composite types using
    the class.

A coder class is defined by subclassing from CoderBase and defining the
encode_to_bytes and decode_from_bytes methods. The framework uses duck-typing
for coders so it is not strictly required to subclass from CoderBase as long as
the encode/decode methods are defined.

Registering a coder class is made with a register_coder() call::

  from apache_beam import coders
  ...
  coders.registry.register_coder(Xyz, XyzCoder)

Additionally, DoFns and PTransforms may need type hints. This is not always
necessary since there is functionality to infer the return types of DoFns by
analyzing the code. For instance, for the function below the return type of
'Xyz' will be inferred::

  def MakeXyzs(v):
    return Xyz(v)

If Xyz is inferred then its coder will be used whenever the framework needs to
serialize data (e.g., writing to the shuffler subsystem responsible for group by
key operations). If a typehint is needed it can be specified by decorating the
DoFns or using with_input_types/with_output_types methods on PTransforms. For
example, the above function can be decorated::

  @with_output_types(Xyz)
  def MakeXyzs(v):
    return complex_operation_returning_Xyz(v)

See apache_beam.typehints.decorators module for more details.
"""

import logging

from apache_beam.coders import coders
from apache_beam.typehints import typehints


class CoderRegistry(object):
  """A coder registry for typehint/coder associations."""

  def __init__(self, fallback_coder=None):
    self._coders = {}
    self.custom_types = []
    self.register_standard_coders(fallback_coder)

  def register_standard_coders(self, fallback_coder):
    """Register coders for all basic and composite types."""
    self._register_coder_internal(int, coders.VarIntCoder)
    self._register_coder_internal(float, coders.FloatCoder)
    self._register_coder_internal(str, coders.BytesCoder)
    self._register_coder_internal(bytes, coders.BytesCoder)
    self._register_coder_internal(unicode, coders.StrUtf8Coder)
    self._register_coder_internal(typehints.TupleConstraint, coders.TupleCoder)
    self._fallback_coder = fallback_coder or coders.FastPrimitivesCoder
    self._register_coder_internal(typehints.AnyTypeConstraint,
                                  self._fallback_coder)

  def _register_coder_internal(self, typehint_type, typehint_coder_class):
    self._coders[typehint_type] = typehint_coder_class

  def register_coder(self, typehint_type, typehint_coder_class):
    if not isinstance(typehint_coder_class, type):
      raise TypeError('Coder registration requires a coder class object. '
                      'Received %r instead.' % typehint_coder_class)
    if typehint_type not in self.custom_types:
      self.custom_types.append(typehint_type)
    self._register_coder_internal(typehint_type, typehint_coder_class)

  def get_coder(self, typehint):
    coder = self._coders.get(
        typehint.__class__ if isinstance(typehint, typehints.TypeConstraint)
        else typehint, None)
    if isinstance(typehint, typehints.TypeConstraint) and coder is not None:
      return coder.from_type_hint(typehint, self)
    if coder is None:
      # We use the fallback coder when there is no coder registered for a
      # typehint. For example a user defined class with no coder specified.
      if not hasattr(self, '_fallback_coder'):
        raise RuntimeError(
            'Coder registry has no fallback coder. This can happen if the '
            'fast_coders module could not be imported.')
      if isinstance(typehint, typehints.IterableTypeConstraint):
        # In this case, we suppress the warning message for using the fallback
        # coder, since Iterable is hinted as the output of a GroupByKey
        # operation and that direct output will not be coded.
        # TODO(ccy): refine this behavior.
        pass
      elif typehint is None:
        # In some old code, None is used for Any.
        # TODO(robertwb): Clean this up.
        pass
      elif isinstance(typehint, typehints.TypeVariable):
        # TODO(robertwb): Clean this up when type inference is fully enabled.
        pass
      else:
        logging.warning('Using fallback coder for typehint: %r.', typehint)
      coder = self._fallback_coder
    return coder.from_type_hint(typehint, self)

  def get_custom_type_coder_tuples(self, types):
    """Returns type/coder tuples for all custom types passed in."""
    return [(t, self._coders[t]) for t in types if t in self.custom_types]

  def verify_deterministic(self, key_coder, op_name, silent=True):
    if not key_coder.is_deterministic():
      error_msg = ('The key coder "%s" for %s '
                   'is not deterministic. This may result in incorrect '
                   'pipeline output. This can be fixed by adding a type '
                   'hint to the operation preceding the GroupByKey step, '
                   'and for custom key classes, by writing a '
                   'deterministic custom Coder. Please see the '
                   'documentation for more details.' % (key_coder, op_name))
      if isinstance(key_coder, (coders.PickleCoder, self._fallback_coder)):
        if not silent:
          logging.warning(error_msg)
        return coders.DeterministicPickleCoder(key_coder, op_name)
      else:
        raise ValueError(error_msg)
    else:
      return key_coder

registry = CoderRegistry()
