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

"""Collection of useful coders.

Only those coders listed in __all__ are part of the public API of this module.

## On usage of `pickle`, `dill` and `pickler` in Beam

In Beam, we generally we use `pickle` for pipeline elements and `dill` for
more complex types, like user functions.

`pickler` is Beam's own wrapping of dill + compression + error handling.
It serves also as an API to mask the actual encoding layer (so we can
change it from `dill` if necessary).

We created `_MemoizingPickleCoder` to improve performance when serializing
complex user types for the execution of SDF. Specifically to address
BEAM-12781, where many identical `BoundedSource` instances are being
encoded.

"""
# pytype: skip-file

import base64
import decimal
import pickle
from functools import lru_cache
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import overload

import google.protobuf.wrappers_pb2
import proto
from google.protobuf import message

from apache_beam.coders import coder_impl
from apache_beam.coders.avro_record import AvroRecord
from apache_beam.internal import cloudpickle_pickler
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.typehints import typehints
from apache_beam.utils import proto_utils
from apache_beam.utils import windowed_value

if TYPE_CHECKING:
  from apache_beam.coders.typecoders import CoderRegistry
  from apache_beam.runners.pipeline_context import PipelineContext

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from .stream import get_varint_size
except ImportError:
  from .slow_stream import get_varint_size
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

# pylint: disable=wrong-import-order, wrong-import-position
# Avoid dependencies on the full SDK.
try:
  # Import dill from the pickler module to make sure our monkey-patching of dill
  # occurs.
  from apache_beam.internal.dill_pickler import dill
except ImportError:
  dill = None

__all__ = [
    'Coder',
    'AvroGenericCoder',
    'BooleanCoder',
    'BytesCoder',
    'CloudpickleCoder',
    'DillCoder',
    'FastPrimitivesCoder',
    'FloatCoder',
    'IterableCoder',
    'ListCoder',
    'MapCoder',
    'NullableCoder',
    'PickleCoder',
    'ProtoCoder',
    'ProtoPlusCoder',
    'ShardedKeyCoder',
    'SinglePrecisionFloatCoder',
    'SingletonCoder',
    'StrUtf8Coder',
    'TimestampCoder',
    'TupleCoder',
    'TupleSequenceCoder',
    'VarIntCoder',
    'WindowedValueCoder',
    'ParamWindowedValueCoder',
    'BigIntegerCoder',
    'DecimalCoder',
    'PaneInfoCoder'
]

T = TypeVar('T')
CoderT = TypeVar('CoderT', bound='Coder')
ProtoCoderT = TypeVar('ProtoCoderT', bound='ProtoCoder')
ConstructorFn = Callable[[Optional[Any], List['Coder'], 'PipelineContext'], Any]


def serialize_coder(coder):
  from apache_beam.internal import pickler
  return b'%s$%s' % (
      coder.__class__.__name__.encode('utf-8'),
      pickler.dumps(coder, use_zlib=True))


def deserialize_coder(serialized):
  from apache_beam.internal import pickler
  return pickler.loads(serialized.split(b'$', 1)[1], use_zlib=True)


# pylint: enable=wrong-import-order, wrong-import-position


class Coder(object):
  """Base class for coders."""
  def encode(self, value):
    # type: (Any) -> bytes

    """Encodes the given object into a byte string."""
    raise NotImplementedError('Encode not implemented: %s.' % self)

  def decode(self, encoded):
    """Decodes the given byte string into the corresponding object."""
    raise NotImplementedError('Decode not implemented: %s.' % self)

  def encode_nested(self, value):
    """Uses the underlying implementation to encode in nested format."""
    return self.get_impl().encode_nested(value)

  def decode_nested(self, encoded):
    """Uses the underlying implementation to decode in nested format."""
    return self.get_impl().decode_nested(encoded)

  def is_deterministic(self):
    # type: () -> bool

    """Whether this coder is guaranteed to encode values deterministically.

    A deterministic coder is required for key coders in GroupByKey operations
    to produce consistent results.

    For example, note that the default coder, the PickleCoder, is not
    deterministic: the ordering of picked entries in maps may vary across
    executions since there is no defined order, and such a coder is not in
    general suitable for usage as a key coder in GroupByKey operations, since
    each instance of the same key may be encoded differently.

    Returns:
      Whether coder is deterministic.
    """
    return False

  def as_deterministic_coder(self, step_label, error_message=None):
    """Returns a deterministic version of self, if possible.

    Otherwise raises a value error.
    """
    if self.is_deterministic():
      return self
    else:
      raise ValueError(
          error_message or
          "%s cannot be made deterministic for '%s'." % (self, step_label))

  def estimate_size(self, value):
    """Estimates the encoded size of the given value, in bytes.

    Dataflow estimates the encoded size of a PCollection processed in a pipeline
    step by using the estimated size of a random sample of elements in that
    PCollection.

    The default implementation encodes the given value and returns its byte
    size.  If a coder can provide a fast estimate of the encoded size of a value
    (e.g., if the encoding has a fixed size), it can provide its estimate here
    to improve performance.

    Arguments:
      value: the value whose encoded size is to be estimated.

    Returns:
      The estimated encoded size of the given value.
    """
    return len(self.encode(value))

  # ===========================================================================
  # Methods below are internal SDK details that don't need to be modified for
  # user-defined coders.
  # ===========================================================================

  def _create_impl(self):
    # type: () -> coder_impl.CoderImpl

    """Creates a CoderImpl to do the actual encoding and decoding.
    """
    return coder_impl.CallbackCoderImpl(
        self.encode, self.decode, self.estimate_size)

  def get_impl(self):
    """For internal use only; no backwards-compatibility guarantees.

    Returns the CoderImpl backing this Coder.
    """
    if not hasattr(self, '_impl'):
      self._impl = self._create_impl()
      assert isinstance(self._impl, coder_impl.CoderImpl)
    return self._impl

  def __getstate__(self):
    return self._dict_without_impl()

  def _dict_without_impl(self):
    if hasattr(self, '_impl'):
      d = dict(self.__dict__)
      del d['_impl']
      return d
    return self.__dict__

  def to_type_hint(self):
    # TODO: After https://github.com/apache/beam/issues/18490 we should be
    # able to infer the type hint rather than require every subclass define
    # it
    raise NotImplementedError

  @classmethod
  def from_type_hint(cls, unused_typehint, unused_registry):
    # type: (Type[CoderT], Any, CoderRegistry) -> CoderT
    # If not overridden, just construct the coder without arguments.
    return cls()

  def is_kv_coder(self):
    # type: () -> bool
    return False

  def key_coder(self):
    # type: () -> Coder
    if self.is_kv_coder():
      raise NotImplementedError('key_coder: %s' % self)
    else:
      raise ValueError('Not a KV coder: %s.' % self)

  def value_coder(self):
    # type: () -> Coder
    if self.is_kv_coder():
      raise NotImplementedError('value_coder: %s' % self)
    else:
      raise ValueError('Not a KV coder: %s.' % self)

  def _get_component_coders(self):
    # type: () -> Sequence[Coder]

    """For internal use only; no backwards-compatibility guarantees.

    Returns the internal component coders of this coder."""
    # This is an internal detail of the Coder API and does not need to be
    # refined in user-defined Coders.
    return []

  def __repr__(self):
    return self.__class__.__name__

  # pylint: disable=protected-access
  def __eq__(self, other):
    return (
        self.__class__ == other.__class__ and
        self._dict_without_impl() == other._dict_without_impl())

  # pylint: enable=protected-access

  def __hash__(self):
    return hash(type(self))

  _known_urns = {}  # type: Dict[str, Tuple[type, ConstructorFn]]

  @classmethod
  @overload
  def register_urn(
      cls,
      urn,  # type: str
      parameter_type,  # type: Optional[Type[T]]
  ):
    # type: (...) -> Callable[[Callable[[T, List[Coder], PipelineContext], Any]], Callable[[T, List[Coder], PipelineContext], Any]]
    pass

  @classmethod
  @overload
  def register_urn(
      cls,
      urn,  # type: str
      parameter_type,  # type: Optional[Type[T]]
      fn  # type: Callable[[T, List[Coder], PipelineContext], Any]
  ):
    # type: (...) -> None
    pass

  @classmethod
  def register_urn(cls, urn, parameter_type, fn=None):
    """Registers a urn with a constructor.

    For example, if 'beam:fn:foo' had parameter type FooPayload, one could
    write `RunnerApiFn.register_urn('bean:fn:foo', FooPayload, foo_from_proto)`
    where foo_from_proto took as arguments a FooPayload and a PipelineContext.
    This function can also be used as a decorator rather than passing the
    callable in as the final parameter.

    A corresponding to_runner_api_parameter method would be expected that
    returns the tuple ('beam:fn:foo', FooPayload)
    """
    def register(fn):
      cls._known_urns[urn] = parameter_type, fn
      return fn

    if fn:
      # Used as a statement.
      register(fn)
    else:
      # Used as a decorator.
      return register

  def to_runner_api(self, context):
    # type: (PipelineContext) -> beam_runner_api_pb2.Coder
    urn, typed_param, components = self.to_runner_api_parameter(context)
    return beam_runner_api_pb2.Coder(
        spec=beam_runner_api_pb2.FunctionSpec(
            urn=urn,
            payload=typed_param if isinstance(typed_param, (bytes, type(None)))
            else typed_param.SerializeToString()),
        component_coder_ids=[context.coders.get_id(c) for c in components])

  @classmethod
  def from_runner_api(cls, coder_proto, context):
    # type: (Type[CoderT], beam_runner_api_pb2.Coder, PipelineContext) -> CoderT

    """Converts from an FunctionSpec to a Fn object.

    Prefer registering a urn with its parameter type and constructor.
    """
    parameter_type, constructor = cls._known_urns[coder_proto.spec.urn]
    return constructor(
        proto_utils.parse_Bytes(coder_proto.spec.payload, parameter_type),
        [context.coders.get_by_id(c) for c in coder_proto.component_coder_ids],
        context)

  def to_runner_api_parameter(self, context):
    # type: (Optional[PipelineContext]) -> Tuple[str, Any, Sequence[Coder]]
    return (
        python_urns.PICKLED_CODER,
        google.protobuf.wrappers_pb2.BytesValue(value=serialize_coder(self)),
        ())

  @staticmethod
  def register_structured_urn(urn, cls):
    # type: (str, Type[Coder]) -> None

    """Register a coder that's completely defined by its urn and its
    component(s), if any, which are passed to construct the instance.
    """
    setattr(
        cls,
        'to_runner_api_parameter', lambda self, unused_context:
        (urn, None, self._get_component_coders()))

    # pylint: disable=unused-variable
    @Coder.register_urn(urn, None)
    def from_runner_api_parameter(unused_payload, components, unused_context):
      if components:
        return cls(*components)
      else:
        return cls()

  def version_tag(self) -> str:
    """For internal use. Appends a version tag to the coder key in the pipeline
    proto. Some runners (e.g. DataflowRunner) use coder key/id to verify if a
    pipeline is update compatible. If the implementation of a coder changed
    in an update incompatible way a version tag can be added to fail
    compatibility checks.
    """
    return ""


@Coder.register_urn(
    python_urns.PICKLED_CODER, google.protobuf.wrappers_pb2.BytesValue)
def _pickle_from_runner_api_parameter(payload, components, context):
  return deserialize_coder(payload.value)


class StrUtf8Coder(Coder):
  """A coder used for reading and writing strings as UTF-8."""
  def encode(self, value):
    return value.encode('utf-8')

  def decode(self, value):
    return value.decode('utf-8')

  def is_deterministic(self):
    # type: () -> bool
    return True

  def to_type_hint(self):
    return str


Coder.register_structured_urn(common_urns.coders.STRING_UTF8.urn, StrUtf8Coder)


class ToBytesCoder(Coder):
  """A default string coder used if no sink coder is specified."""
  def encode(self, value):
    return value if isinstance(value, bytes) else str(value).encode('utf-8')

  def decode(self, _):
    raise NotImplementedError('ToBytesCoder cannot be used for decoding.')

  def is_deterministic(self):
    # type: () -> bool
    return True


# alias to the old class name for a courtesy to users who reference it
ToStringCoder = ToBytesCoder


class FastCoder(Coder):
  """Coder subclass used when a (faster) CoderImpl is supplied directly.

  The Coder class defines _create_impl in terms of encode() and decode();
  this class inverts that by defining encode() and decode() in terms of
  _create_impl().
  """
  def encode(self, value):
    """Encodes the given object into a byte string."""
    return self.get_impl().encode(value)

  def decode(self, encoded):
    """Decodes the given byte string into the corresponding object."""
    return self.get_impl().decode(encoded)

  def estimate_size(self, value):
    return self.get_impl().estimate_size(value)

  def _create_impl(self):
    raise NotImplementedError


class BytesCoder(FastCoder):
  """Byte string coder."""
  def _create_impl(self):
    return coder_impl.BytesCoderImpl()

  def is_deterministic(self):
    # type: () -> bool
    return True

  def to_type_hint(self):
    return bytes

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))


Coder.register_structured_urn(common_urns.coders.BYTES.urn, BytesCoder)


class BooleanCoder(FastCoder):
  def _create_impl(self):
    return coder_impl.BooleanCoderImpl()

  def is_deterministic(self):
    # type: () -> bool
    return True

  def to_type_hint(self):
    return bool

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))


Coder.register_structured_urn(common_urns.coders.BOOL.urn, BooleanCoder)


class MapCoder(FastCoder):
  def __init__(self, key_coder, value_coder):
    # type: (Coder, Coder) -> None
    self._key_coder = key_coder
    self._value_coder = value_coder

  def _create_impl(self):
    return coder_impl.MapCoderImpl(
        self._key_coder.get_impl(), self._value_coder.get_impl())

  @classmethod
  def from_type_hint(cls, typehint, registry):
    # type: (typehints.DictConstraint, CoderRegistry) -> MapCoder
    return cls(
        registry.get_coder(typehint.key_type),
        registry.get_coder(typehint.value_type))

  def to_type_hint(self):
    return typehints.Dict[self._key_coder.to_type_hint(),
                          self._value_coder.to_type_hint()]

  def is_deterministic(self):
    # type: () -> bool
    # Map ordering is non-deterministic
    return False

  def as_deterministic_coder(self, step_label, error_message=None):
    return DeterministicMapCoder(
        self._key_coder.as_deterministic_coder(step_label, error_message),
        self._value_coder.as_deterministic_coder(step_label, error_message))

  def __eq__(self, other):
    return (
        type(self) == type(other) and self._key_coder == other._key_coder and
        self._value_coder == other._value_coder)

  def __hash__(self):
    return hash(type(self)) + hash(self._key_coder) + hash(self._value_coder)

  def __repr__(self):
    return 'MapCoder[%s, %s]' % (self._key_coder, self._value_coder)


# This is a separate class from MapCoder as the former is a standard coder with
# no way to carry the is_deterministic bit.
class DeterministicMapCoder(FastCoder):
  def __init__(self, key_coder, value_coder):
    # type: (Coder, Coder) -> None
    assert key_coder.is_deterministic()
    assert value_coder.is_deterministic()
    self._key_coder = key_coder
    self._value_coder = value_coder

  def _create_impl(self):
    return coder_impl.MapCoderImpl(
        self._key_coder.get_impl(), self._value_coder.get_impl(), True)

  def is_deterministic(self):
    return True

  def __eq__(self, other):
    return (
        type(self) == type(other) and self._key_coder == other._key_coder and
        self._value_coder == other._value_coder)

  def __hash__(self):
    return hash(type(self)) + hash(self._key_coder) + hash(self._value_coder)

  def __repr__(self):
    return 'DeterministicMapCoder[%s, %s]' % (
        self._key_coder, self._value_coder)


class NullableCoder(FastCoder):
  def __init__(self, value_coder):
    # type: (Coder) -> None
    self._value_coder = value_coder

  def _create_impl(self):
    return coder_impl.NullableCoderImpl(self._value_coder.get_impl())

  def to_type_hint(self):
    return typehints.Optional[self._value_coder.to_type_hint()]

  def _get_component_coders(self):
    # type: () -> List[Coder]
    return [self._value_coder]

  @classmethod
  def from_type_hint(cls, typehint, registry):
    if typehints.is_nullable(typehint):
      return cls(
          registry.get_coder(
              typehints.get_concrete_type_from_nullable(typehint)))
    else:
      raise TypeError(
          'Typehint is not of nullable type, '
          'and cannot be converted to a NullableCoder',
          typehint)

  def is_deterministic(self):
    # type: () -> bool
    return self._value_coder.is_deterministic()

  def as_deterministic_coder(self, step_label, error_message=None):
    if self.is_deterministic():
      return self
    else:
      deterministic_value_coder = self._value_coder.as_deterministic_coder(
          step_label, error_message)
      return NullableCoder(deterministic_value_coder)

  def __eq__(self, other):
    return (
        type(self) == type(other) and self._value_coder == other._value_coder)

  def __hash__(self):
    return hash(type(self)) + hash(self._value_coder)

  def __repr__(self):
    return 'NullableCoder[%s]' % self._value_coder


Coder.register_structured_urn(common_urns.coders.NULLABLE.urn, NullableCoder)


class VarIntCoder(FastCoder):
  """Variable-length integer coder  matches Java SDK's VarLongCoder."""
  def _create_impl(self):
    return coder_impl.VarIntCoderImpl()

  def is_deterministic(self):
    # type: () -> bool
    return True

  def to_type_hint(self):
    return int

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))


Coder.register_structured_urn(common_urns.coders.VARINT.urn, VarIntCoder)


class VarInt32Coder(FastCoder):
  """Variable-length integer coder matches Java SDK's VarIntCoder."""
  def _create_impl(self):
    return coder_impl.VarInt32CoderImpl()

  def is_deterministic(self):
    # type: () -> bool
    return True

  def to_type_hint(self):
    return int

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))


class BigEndianShortCoder(FastCoder):
  """A coder used for big-endian int16 values."""
  def _create_impl(self):
    return coder_impl.BigEndianShortCoderImpl()

  def is_deterministic(self):
    # type: () -> bool
    return True

  def to_type_hint(self):
    return int

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))


class SinglePrecisionFloatCoder(FastCoder):
  """A coder used for single-precision floating-point values."""
  def _create_impl(self):
    return coder_impl.SinglePrecisionFloatCoderImpl()

  def is_deterministic(self):
    # type: () -> bool
    return True

  def to_type_hint(self):
    return float

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))


class FloatCoder(FastCoder):
  """A coder used for **double-precision** floating-point values.

  Note that the name "FloatCoder" is in reference to Python's ``float`` built-in
  which is generally implemented using C doubles. See
  :class:`SinglePrecisionFloatCoder` for a single-precision version of this
  coder.
  """
  def _create_impl(self):
    return coder_impl.FloatCoderImpl()

  def is_deterministic(self):
    # type: () -> bool
    return True

  def to_type_hint(self):
    return float

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))


Coder.register_structured_urn(common_urns.coders.DOUBLE.urn, FloatCoder)


class TimestampCoder(FastCoder):
  """A coder used for timeutil.Timestamp values."""
  def _create_impl(self):
    return coder_impl.TimestampCoderImpl()

  def is_deterministic(self):
    # type: () -> bool
    return True

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))


class _TimerCoder(FastCoder):
  """A coder used for timer values.

  For internal use."""
  def __init__(self, key_coder, window_coder):
    # type: (Coder, Coder) -> None
    self._key_coder = key_coder
    self._window_coder = window_coder

  def _get_component_coders(self):
    # type: () -> List[Coder]
    return [self._key_coder, self._window_coder]

  def _create_impl(self):
    return coder_impl.TimerCoderImpl(
        self._key_coder.get_impl(), self._window_coder.get_impl())

  def is_deterministic(self):
    # type: () -> bool
    return (
        self._key_coder.is_deterministic() and
        self._window_coder.is_deterministic())

  def __eq__(self, other):
    return (
        type(self) == type(other) and self._key_coder == other._key_coder and
        self._window_coder == other._window_coder)

  def __hash__(self):
    return hash(type(self)) + hash(self._key_coder) + hash(self._window_coder)


Coder.register_structured_urn(common_urns.coders.TIMER.urn, _TimerCoder)


class SingletonCoder(FastCoder):
  """A coder that always encodes exactly one value."""
  def __init__(self, value):
    self._value = value

  def _create_impl(self):
    return coder_impl.SingletonCoderImpl(self._value)

  def is_deterministic(self):
    # type: () -> bool
    return True

  def __eq__(self, other):
    return type(self) == type(other) and self._value == other._value

  def __hash__(self):
    return hash(self._value)


def maybe_dill_dumps(o):
  """Pickle using cPickle or the Dill pickler as a fallback."""
  # We need to use the dill pickler for objects of certain custom classes,
  # including, for example, ones that contain lambdas.
  try:
    return pickle.dumps(o, pickle.HIGHEST_PROTOCOL)
  except Exception:  # pylint: disable=broad-except
    return dill.dumps(o)


def maybe_dill_loads(o):
  """Unpickle using cPickle or the Dill pickler as a fallback."""
  try:
    return pickle.loads(o)
  except Exception:  # pylint: disable=broad-except
    return dill.loads(o)


class _PickleCoderBase(FastCoder):
  """Base class for pickling coders."""
  def is_deterministic(self):
    # type: () -> bool
    # Note that the default coder, the PickleCoder, is not deterministic (for
    # example, the ordering of picked entries in maps may vary across
    # executions), and so is not in general suitable for usage as a key coder in
    # GroupByKey operations.
    return False

  # We allow .key_coder() and .value_coder() to be called on PickleCoder since
  # we can't always infer the return values of lambdas in ParDo operations, the
  # result of which may be used in a GroupBykey.
  def is_kv_coder(self):
    # type: () -> bool
    return True

  def key_coder(self):
    return self

  def value_coder(self):
    return self

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))


class _MemoizingPickleCoder(_PickleCoderBase):
  """Coder using Python's pickle functionality with memoization."""
  def __init__(self, cache_size=16):
    super().__init__()
    self.cache_size = cache_size

  def _create_impl(self):
    from apache_beam.internal import pickler
    dumps = pickler.dumps

    mdumps = lru_cache(maxsize=self.cache_size, typed=True)(dumps)

    def _nonhashable_dumps(x):
      try:
        return mdumps(x)
      except TypeError:
        return dumps(x)

    return coder_impl.CallbackCoderImpl(_nonhashable_dumps, pickler.loads)

  def as_deterministic_coder(self, step_label, error_message=None):
    return FastPrimitivesCoder(self, requires_deterministic=step_label)

  def to_type_hint(self):
    return Any


class PickleCoder(_PickleCoderBase):
  """Coder using Python's pickle functionality."""
  def _create_impl(self):
    dumps = pickle.dumps
    protocol = pickle.HIGHEST_PROTOCOL
    return coder_impl.CallbackCoderImpl(
        lambda x: dumps(x, protocol), pickle.loads)

  def as_deterministic_coder(self, step_label, error_message=None):
    return FastPrimitivesCoder(self, requires_deterministic=step_label)

  def to_type_hint(self):
    return Any


class DillCoder(_PickleCoderBase):
  """Coder using dill's pickle functionality."""
  def __init__(self):
    if not dill:
      raise RuntimeError(
          "This pipeline contains a DillCoder which requires "
          "the dill package. Install the dill package with the dill extra "
          "e.g. apache-beam[dill]")

  def _create_impl(self):
    return coder_impl.CallbackCoderImpl(maybe_dill_dumps, maybe_dill_loads)


class CloudpickleCoder(_PickleCoderBase):
  """Coder using Apache Beam's vendored Cloudpickle pickler."""
  def _create_impl(self):
    return coder_impl.CallbackCoderImpl(
        cloudpickle_pickler.dumps, cloudpickle_pickler.loads)


class DeterministicFastPrimitivesCoderV2(FastCoder):
  """Throws runtime errors when encoding non-deterministic values."""
  def __init__(self, coder, step_label, update_compatibility_version=None):
    self._underlying_coder = coder
    self._step_label = step_label
    self._use_relative_filepaths = True
    self._version_tag = "v2_69"
    from apache_beam.transforms.util import is_v1_prior_to_v2
    # Versions prior to 2.69.0 did not use relative filepaths.
    if update_compatibility_version and is_v1_prior_to_v2(
        v1=update_compatibility_version, v2="2.69.0"):
      self._version_tag = ""
      self._use_relative_filepaths = False

  def _create_impl(self):
    return coder_impl.FastPrimitivesCoderImpl(
        self._underlying_coder.get_impl(),
        requires_deterministic_step_label=self._step_label,
        force_use_dill=False,
        use_relative_filepaths=self._use_relative_filepaths)

  def is_deterministic(self):
    # type: () -> bool
    return True

  def is_kv_coder(self):
    # type: () -> bool
    return True

  def key_coder(self):
    return self

  def value_coder(self):
    return self

  def to_type_hint(self):
    return Any

  def to_runner_api_parameter(self, context):
    # type: (Optional[PipelineContext]) -> Tuple[str, Any, Sequence[Coder]]
    return (
        python_urns.PICKLED_CODER,
        google.protobuf.wrappers_pb2.BytesValue(value=serialize_coder(self)),
        ())

  def version_tag(self):
    return self._version_tag


class DeterministicFastPrimitivesCoder(FastCoder):
  """Throws runtime errors when encoding non-deterministic values."""
  def __init__(self, coder, step_label):
    self._underlying_coder = coder
    self._step_label = step_label

  def _create_impl(self):
    return coder_impl.FastPrimitivesCoderImpl(
        self._underlying_coder.get_impl(),
        requires_deterministic_step_label=self._step_label,
        force_use_dill=True)

  def is_deterministic(self):
    # type: () -> bool
    return True

  def is_kv_coder(self):
    # type: () -> bool
    return True

  def key_coder(self):
    return self

  def value_coder(self):
    return self

  def to_type_hint(self):
    return Any


def _should_force_use_dill(registry):
  # force_dill_deterministic_coders is for testing purposes. If there is a
  # DeterministicFastPrimitivesCoder in the pipeline graph but the dill
  # encoding path is not really triggered dill does not have to be installed.
  # and this check can be skipped.
  if getattr(registry, 'force_dill_deterministic_coders', False):
    return True

  from apache_beam.transforms.util import is_v1_prior_to_v2
  update_compat_version = registry.update_compatibility_version
  if not update_compat_version:
    return False

  if not is_v1_prior_to_v2(v1=update_compat_version, v2="2.68.0"):
    return False

  try:
    import dill
    assert dill.__version__ == "0.3.1.1"
  except Exception as e:
    raise RuntimeError("This pipeline runs with the pipeline option " \
    "--update_compatibility_version=2.67.0 or earlier. When running with " \
    "this option on SDKs 2.68.0 or later, you must ensure dill==0.3.1.1 " \
    f"is installed. Error {e}")
  return True


def _update_compatible_deterministic_fast_primitives_coder(coder, step_label):
  """ Returns the update compatible version of DeterministicFastPrimitivesCoder
   The differences are in how "special types" e.g. NamedTuples, Dataclasses are
   deterministically encoded.

   - In SDK version <= 2.67.0 dill is used to encode "special types"
   - In SDK version 2.68.0 cloudpickle is used to encode "special types" with
   absolute filepaths in code objects and dynamic functions.
   - In SDK version 2.69.0 cloudpickle is used to encode "special types" with
   relative filepaths in code objects and dynamic functions.
  """
  from apache_beam.coders import typecoders

  if _should_force_use_dill(typecoders.registry):
    return DeterministicFastPrimitivesCoder(coder, step_label)
  return DeterministicFastPrimitivesCoderV2(
      coder, step_label, typecoders.registry.update_compatibility_version)


class FastPrimitivesCoder(FastCoder):
  """Encodes simple primitives (e.g. str, int) efficiently.

  For unknown types, falls back to another coder (e.g. PickleCoder).
  """
  def __init__(self, fallback_coder=PickleCoder()):
    # type: (Coder) -> None
    self._fallback_coder = fallback_coder

  def _create_impl(self):
    return coder_impl.FastPrimitivesCoderImpl(self._fallback_coder.get_impl())

  def is_deterministic(self):
    # type: () -> bool
    return self._fallback_coder.is_deterministic()

  def as_deterministic_coder(self, step_label, error_message=None):
    if self.is_deterministic():
      return self
    else:
      return _update_compatible_deterministic_fast_primitives_coder(
          self, step_label)

  def to_type_hint(self):
    return Any

  # We allow .key_coder() and .value_coder() to be called on FastPrimitivesCoder
  # since we can't always infer the return values of lambdas in ParDo
  # operations, the result of which may be used in a GroupBykey.
  def is_kv_coder(self):
    # type: () -> bool
    return True

  def key_coder(self):
    return self

  def value_coder(self):
    return self

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))


class FakeDeterministicFastPrimitivesCoder(FastPrimitivesCoder):
  """A FastPrimitivesCoder that claims to be deterministic.

  This can be registered as a fallback coder to go back to the behavior before
  deterministic encoding was enforced (BEAM-11719).
  """
  def is_deterministic(self):
    return True


class Base64PickleCoder(Coder):
  """Coder of objects by Python pickle, then base64 encoding."""

  # TODO(robertwb): Do base64 encoding where it's needed (e.g. in json) rather
  # than via a special Coder.

  def encode(self, value):
    return base64.b64encode(pickle.dumps(value, pickle.HIGHEST_PROTOCOL))

  def decode(self, encoded):
    return pickle.loads(base64.b64decode(encoded))

  def is_deterministic(self):
    # type: () -> bool
    # Note that the Base64PickleCoder is not deterministic.  See the
    # corresponding comments for PickleCoder above.
    return False

  # We allow .key_coder() and .value_coder() to be called on Base64PickleCoder
  # since we can't always infer the return values of lambdas in ParDo
  # operations, the result of which may be used in a GroupBykey.
  #
  # TODO(ccy): this is currently only used for KV values from Create transforms.
  # Investigate a way to unify this with PickleCoder.
  def is_kv_coder(self):
    # type: () -> bool
    return True

  def key_coder(self):
    return self

  def value_coder(self):
    return self


class ProtoCoder(FastCoder):
  """A Coder for Google Protocol Buffers.

  It supports both Protocol Buffers syntax versions 2 and 3. However,
  the runtime version of the python protobuf library must exactly match the
  version of the protoc compiler what was used to generate the protobuf
  messages.

  ProtoCoder is registered in the global CoderRegistry as the default coder for
  any protobuf Message object.

  """
  def __init__(self, proto_message_type):
    # type: (Type[google.protobuf.message.Message]) -> None
    self.proto_message_type = proto_message_type

  def _create_impl(self):
    return coder_impl.ProtoCoderImpl(self.proto_message_type)

  def is_deterministic(self):
    # type: () -> bool
    # TODO(vikasrk): A proto message can be deterministic if it does not contain
    # a Map.
    return False

  def as_deterministic_coder(self, step_label, error_message=None):
    return DeterministicProtoCoder(self.proto_message_type)

  def __eq__(self, other):
    return (
        type(self) == type(other) and
        self.proto_message_type == other.proto_message_type)

  def __hash__(self):
    return hash(self.proto_message_type)

  @classmethod
  def from_type_hint(cls, typehint, unused_registry):
    # The typehint must be a strict subclass of google.protobuf.message.Message.
    # ProtoCoder cannot work with message.Message itself, as deserialization of
    # a serialized proto requires knowledge of the desired concrete proto
    # subclass which is not stored in the encoded bytes themselves. If this
    # occurs, an error is raised and the system defaults to other fallback
    # coders.
    if (issubclass(typehint, proto_utils.message_types) and
        typehint != message.Message):
      return cls(typehint)
    else:
      raise ValueError((
          'Expected a strict subclass of google.protobuf.message.Message'
          ', but got a %s' % typehint))

  def to_type_hint(self):
    return self.proto_message_type


class DeterministicProtoCoder(ProtoCoder):
  """A deterministic Coder for Google Protocol Buffers.

  It supports both Protocol Buffers syntax versions 2 and 3. However,
  the runtime version of the python protobuf library must exactly match the
  version of the protoc compiler what was used to generate the protobuf
  messages.
  """
  def _create_impl(self):
    return coder_impl.DeterministicProtoCoderImpl(self.proto_message_type)

  def is_deterministic(self):
    # type: () -> bool
    return True

  def as_deterministic_coder(self, step_label, error_message=None):
    return self


class ProtoPlusCoder(FastCoder):
  """A Coder for Google Protocol Buffers wrapped using the proto-plus library.

  ProtoPlusCoder is registered in the global CoderRegistry as the default coder
  for any proto.Message object.
  """
  def __init__(self, proto_plus_message_type):
    # type: (Type[proto.Message]) -> None
    self.proto_plus_message_type = proto_plus_message_type

  def _create_impl(self):
    return coder_impl.ProtoPlusCoderImpl(self.proto_plus_message_type)

  def is_deterministic(self):
    return True

  def __eq__(self, other):
    return (
        type(self) == type(other) and
        self.proto_plus_message_type == other.proto_plus_message_type)

  def __hash__(self):
    return hash(self.proto_plus_message_type)

  @classmethod
  def from_type_hint(cls, typehint, unused_registry):
    if issubclass(typehint, proto.Message):
      return cls(typehint)
    else:
      raise ValueError(
          'Expected a subclass of proto.Message, but got a %s' % typehint)

  def to_type_hint(self):
    return self.proto_plus_message_type


AVRO_GENERIC_CODER_URN = "beam:coder:avro:generic:v1"


class AvroGenericCoder(FastCoder):
  """A coder used for AvroRecord values."""
  def __init__(self, schema):
    self.schema = schema

  def _create_impl(self):
    return coder_impl.AvroCoderImpl(self.schema)

  def is_deterministic(self):
    # TODO(https://github.com/apache/beam/issues/19628): need to confirm if
    # it's deterministic
    return False

  def __eq__(self, other):
    return type(self) == type(other) and self.schema == other.schema

  def __hash__(self):
    return hash(self.schema)

  def to_type_hint(self):
    return AvroRecord

  def to_runner_api_parameter(self, context):
    return AVRO_GENERIC_CODER_URN, self.schema.encode('utf-8'), ()

  @staticmethod
  @Coder.register_urn(AVRO_GENERIC_CODER_URN, bytes)
  def from_runner_api_parameter(payload, unused_components, unused_context):
    return AvroGenericCoder(payload.decode('utf-8'))


class TupleCoder(FastCoder):
  """Coder of tuple objects."""
  def __init__(self, components):
    # type: (Iterable[Coder]) -> None
    self._coders = tuple(components)

  def _create_impl(self):
    return coder_impl.TupleCoderImpl([c.get_impl() for c in self._coders])

  def is_deterministic(self):
    # type: () -> bool
    return all(c.is_deterministic() for c in self._coders)

  def as_deterministic_coder(self, step_label, error_message=None):
    if self.is_deterministic():
      return self
    else:
      return TupleCoder([
          c.as_deterministic_coder(step_label, error_message)
          for c in self._coders
      ])

  def to_type_hint(self):
    return typehints.Tuple[tuple(c.to_type_hint() for c in self._coders)]

  @classmethod
  def from_type_hint(cls, typehint, registry):
    # type: (typehints.TupleConstraint, CoderRegistry) -> TupleCoder
    return cls([registry.get_coder(t) for t in typehint.tuple_types])

  def _get_component_coders(self):
    # type: () -> Tuple[Coder, ...]
    return self.coders()

  def coders(self):
    # type: () -> Tuple[Coder, ...]
    return self._coders

  def is_kv_coder(self):
    # type: () -> bool
    return len(self._coders) == 2

  def key_coder(self):
    # type: () -> Coder
    if len(self._coders) != 2:
      raise ValueError('TupleCoder does not have exactly 2 components.')
    return self._coders[0]

  def value_coder(self):
    # type: () -> Coder
    if len(self._coders) != 2:
      raise ValueError('TupleCoder does not have exactly 2 components.')
    return self._coders[1]

  def __repr__(self):
    return 'TupleCoder[%s]' % ', '.join(str(c) for c in self._coders)

  def __eq__(self, other):
    return type(self) == type(other) and self._coders == other.coders()

  def __hash__(self):
    return hash(self._coders)

  def to_runner_api_parameter(self, context):
    if self.is_kv_coder():
      return common_urns.coders.KV.urn, None, self.coders()
    else:
      return python_urns.TUPLE_CODER, None, self.coders()

  @staticmethod
  @Coder.register_urn(common_urns.coders.KV.urn, None)
  @Coder.register_urn(python_urns.TUPLE_CODER, None)
  def from_runner_api_parameter(unused_payload, components, unused_context):
    return TupleCoder(components)


class TupleSequenceCoder(FastCoder):
  """Coder of homogeneous tuple objects."""
  def __init__(self, elem_coder):
    # type: (Coder) -> None
    self._elem_coder = elem_coder

  def value_coder(self):
    return self._elem_coder

  def _create_impl(self):
    return coder_impl.TupleSequenceCoderImpl(self._elem_coder.get_impl())

  def is_deterministic(self):
    # type: () -> bool
    return self._elem_coder.is_deterministic()

  def as_deterministic_coder(self, step_label, error_message=None):
    if self.is_deterministic():
      return self
    else:
      return TupleSequenceCoder(
          self._elem_coder.as_deterministic_coder(step_label, error_message))

  @classmethod
  def from_type_hint(cls, typehint, registry):
    # type: (Any, CoderRegistry) -> TupleSequenceCoder
    return cls(registry.get_coder(typehint.inner_type))

  def _get_component_coders(self):
    # type: () -> Tuple[Coder, ...]
    return (self._elem_coder, )

  def __repr__(self):
    return 'TupleSequenceCoder[%r]' % self._elem_coder

  def __eq__(self, other):
    return (
        type(self) == type(other) and self._elem_coder == other.value_coder())

  def __hash__(self):
    return hash((type(self), self._elem_coder))


class ListLikeCoder(FastCoder):
  """Coder of iterables of homogeneous objects."""
  def __init__(self, elem_coder):
    # type: (Coder) -> None
    self._elem_coder = elem_coder

  def _create_impl(self):
    return coder_impl.IterableCoderImpl(self._elem_coder.get_impl())

  def is_deterministic(self):
    # type: () -> bool
    return self._elem_coder.is_deterministic()

  def as_deterministic_coder(self, step_label, error_message=None):
    if self.is_deterministic():
      return self
    else:
      return type(self)(
          self._elem_coder.as_deterministic_coder(step_label, error_message))

  def value_coder(self):
    return self._elem_coder

  @classmethod
  def from_type_hint(cls, typehint, registry):
    # type: (Any, CoderRegistry) -> ListLikeCoder
    return cls(registry.get_coder(typehint.inner_type))

  def _get_component_coders(self):
    # type: () -> Tuple[Coder, ...]
    return (self._elem_coder, )

  def __repr__(self):
    return '%s[%r]' % (self.__class__.__name__, self._elem_coder)

  def __eq__(self, other):
    return (
        type(self) == type(other) and self._elem_coder == other.value_coder())

  def __hash__(self):
    return hash((type(self), self._elem_coder))


class IterableCoder(ListLikeCoder):
  """Coder of iterables of homogeneous objects."""
  def to_type_hint(self):
    return typehints.Iterable[self._elem_coder.to_type_hint()]


Coder.register_structured_urn(common_urns.coders.ITERABLE.urn, IterableCoder)


class ListCoder(ListLikeCoder):
  """Coder of Python lists."""
  def to_type_hint(self):
    return typehints.List[self._elem_coder.to_type_hint()]

  def _create_impl(self):
    return coder_impl.ListCoderImpl(self._elem_coder.get_impl())


class GlobalWindowCoder(SingletonCoder):
  """Coder for global windows."""
  def __init__(self):
    from apache_beam.transforms import window
    super().__init__(window.GlobalWindow())


Coder.register_structured_urn(
    common_urns.coders.GLOBAL_WINDOW.urn, GlobalWindowCoder)


class IntervalWindowCoder(FastCoder):
  """Coder for an window defined by a start timestamp and a duration."""
  def _create_impl(self):
    return coder_impl.IntervalWindowCoderImpl()

  def is_deterministic(self):
    # type: () -> bool
    return True

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))


Coder.register_structured_urn(
    common_urns.coders.INTERVAL_WINDOW.urn, IntervalWindowCoder)


class _OrderedUnionCoder(FastCoder):
  def __init__(
      self, *coder_types: Tuple[type, Coder], fallback_coder: Optional[Coder]):
    self._coder_types = coder_types
    self._fallback_coder = fallback_coder

  def _create_impl(self):
    return coder_impl._OrderedUnionCoderImpl(
        [(t, c.get_impl()) for t, c in self._coder_types],
        fallback_coder_impl=self._fallback_coder.get_impl()
        if self._fallback_coder else None)

  def is_deterministic(self) -> bool:
    return (
        all(c.is_deterministic for _, c in self._coder_types) and (
            self._fallback_coder is None or
            self._fallback_coder.is_deterministic()))

  def to_type_hint(self):
    return Any

  def __eq__(self, other):
    return (
        type(self) == type(other) and
        self._coder_types == other._coder_types and
        self._fallback_coder == other._fallback_coder)

  def __hash__(self):
    return hash((type(self), tuple(self._coder_types), self._fallback_coder))


class WindowedValueCoder(FastCoder):
  """Coder for windowed values."""
  def __init__(self, wrapped_value_coder, window_coder=None):
    # type: (Coder, Optional[Coder]) -> None
    if not window_coder:
      # Avoid circular imports.
      from apache_beam.transforms import window
      window_coder = _OrderedUnionCoder(
          (window.GlobalWindow, GlobalWindowCoder()),
          (window.IntervalWindow, IntervalWindowCoder()),
          fallback_coder=PickleCoder())
    self.wrapped_value_coder = wrapped_value_coder
    self.timestamp_coder = TimestampCoder()
    self.window_coder = window_coder

  def _create_impl(self):
    return coder_impl.WindowedValueCoderImpl(
        self.wrapped_value_coder.get_impl(),
        self.timestamp_coder.get_impl(),
        self.window_coder.get_impl())

  def is_deterministic(self):
    # type: () -> bool
    return all(
        c.is_deterministic() for c in
        [self.wrapped_value_coder, self.timestamp_coder, self.window_coder])

  def _get_component_coders(self):
    # type: () -> List[Coder]
    return [self.wrapped_value_coder, self.window_coder]

  def is_kv_coder(self):
    # type: () -> bool
    return self.wrapped_value_coder.is_kv_coder()

  def key_coder(self):
    # type: () -> Coder
    return self.wrapped_value_coder.key_coder()

  def value_coder(self):
    # type: () -> Coder
    return self.wrapped_value_coder.value_coder()

  def __repr__(self):
    return (
        f'WindowedValueCoder[window_coder={self.window_coder}, '
        f'value_coder={self.value_coder()}]')

  def __eq__(self, other):
    return (
        type(self) == type(other) and
        self.wrapped_value_coder == other.wrapped_value_coder and
        self.timestamp_coder == other.timestamp_coder and
        self.window_coder == other.window_coder)

  def __hash__(self):
    return hash(
        (self.wrapped_value_coder, self.timestamp_coder, self.window_coder))

  @classmethod
  def from_type_hint(cls, typehint, registry):
    # type: (Any, CoderRegistry) -> WindowedValueCoder
    # Ideally this'd take two parameters so that one could hint at
    # the window type as well instead of falling back to the
    # pickle coders.
    return cls(registry.get_coder(typehint.inner_type))

  def to_type_hint(self):
    return typehints.WindowedValue[self.wrapped_value_coder.to_type_hint()]


Coder.register_structured_urn(
    common_urns.coders.WINDOWED_VALUE.urn, WindowedValueCoder)


class ParamWindowedValueCoder(WindowedValueCoder):
  """A coder used for parameterized windowed values."""
  def __init__(self, payload, components):
    super().__init__(components[0], components[1])
    self.payload = payload

  def _create_impl(self):
    return coder_impl.ParamWindowedValueCoderImpl(
        self.wrapped_value_coder.get_impl(),
        self.window_coder.get_impl(),
        self.payload)

  def is_deterministic(self):
    # type: () -> bool
    return self.wrapped_value_coder.is_deterministic()

  def __repr__(self):
    return 'ParamWindowedValueCoder[%s]' % self.wrapped_value_coder

  def __eq__(self, other):
    return (
        type(self) == type(other) and
        self.wrapped_value_coder == other.wrapped_value_coder and
        self.window_coder == other.window_coder and
        self.payload == other.payload)

  def __hash__(self):
    return hash((self.wrapped_value_coder, self.window_coder, self.payload))

  @staticmethod
  @Coder.register_urn(common_urns.coders.PARAM_WINDOWED_VALUE.urn, bytes)
  def from_runner_api_parameter(payload, components, unused_context):
    return ParamWindowedValueCoder(payload, components)

  def to_runner_api_parameter(self, context):
    return (
        common_urns.coders.PARAM_WINDOWED_VALUE.urn,
        self.payload, (self.wrapped_value_coder, self.window_coder))


class LengthPrefixCoder(FastCoder):
  """For internal use only; no backwards-compatibility guarantees.

  Coder which prefixes the length of the encoded object in the stream."""
  def __init__(self, value_coder):
    # type: (Coder) -> None
    self._value_coder = value_coder

  def _create_impl(self):
    return coder_impl.LengthPrefixCoderImpl(self._value_coder.get_impl())

  def is_deterministic(self):
    # type: () -> bool
    return self._value_coder.is_deterministic()

  def estimate_size(self, value):
    value_size = self._value_coder.estimate_size(value)
    return get_varint_size(value_size) + value_size

  def value_coder(self):
    return self._value_coder

  def _get_component_coders(self):
    # type: () -> Tuple[Coder, ...]
    return (self._value_coder, )

  def __repr__(self):
    return 'LengthPrefixCoder[%r]' % self._value_coder

  def __eq__(self, other):
    return (
        type(self) == type(other) and self._value_coder == other._value_coder)

  def __hash__(self):
    return hash((type(self), self._value_coder))

  def to_type_hint(length_prefix_coder):
    return length_prefix_coder.value_coder().to_type_hint()


Coder.register_structured_urn(
    common_urns.coders.LENGTH_PREFIX.urn, LengthPrefixCoder)


class StateBackedIterableCoder(FastCoder):
  DEFAULT_WRITE_THRESHOLD = 1

  def __init__(
      self,
      element_coder,  # type: Coder
      read_state=None,  # type: Optional[coder_impl.IterableStateReader]
      write_state=None,  # type: Optional[coder_impl.IterableStateWriter]
      write_state_threshold=DEFAULT_WRITE_THRESHOLD):
    self._element_coder = element_coder
    self._read_state = read_state
    self._write_state = write_state
    self._write_state_threshold = write_state_threshold

  def _create_impl(self):
    return coder_impl.IterableCoderImpl(
        self._element_coder.get_impl(),
        self._read_state,
        self._write_state,
        self._write_state_threshold)

  def is_deterministic(self):
    # type: () -> bool
    return False

  def _get_component_coders(self):
    # type: () -> Tuple[Coder, ...]
    return (self._element_coder, )

  def __repr__(self):
    return 'StateBackedIterableCoder[%r]' % self._element_coder

  def __eq__(self, other):
    return (
        type(self) == type(other) and
        self._element_coder == other._element_coder and
        self._write_state_threshold == other._write_state_threshold)

  def __hash__(self):
    return hash((type(self), self._element_coder, self._write_state_threshold))

  def to_runner_api_parameter(self, context):
    # type: (Optional[PipelineContext]) -> Tuple[str, Any, Sequence[Coder]]
    return (
        common_urns.coders.STATE_BACKED_ITERABLE.urn,
        str(self._write_state_threshold).encode('ascii'),
        self._get_component_coders())

  @staticmethod
  @Coder.register_urn(common_urns.coders.STATE_BACKED_ITERABLE.urn, bytes)
  def from_runner_api_parameter(payload, components, context):
    return StateBackedIterableCoder(
        components[0],
        read_state=context.iterable_state_read,
        write_state=context.iterable_state_write,
        write_state_threshold=int(payload)
        if payload else StateBackedIterableCoder.DEFAULT_WRITE_THRESHOLD)


class ShardedKeyCoder(FastCoder):
  """A coder for sharded key."""
  def __init__(self, key_coder):
    # type: (Coder) -> None
    self._key_coder = key_coder

  def _get_component_coders(self):
    # type: () -> List[Coder]
    return [self._key_coder]

  def _create_impl(self):
    return coder_impl.ShardedKeyCoderImpl(self._key_coder.get_impl())

  def is_deterministic(self):
    # type: () -> bool
    return self._key_coder.is_deterministic()

  def to_type_hint(self):
    from apache_beam.typehints import sharded_key_type
    return sharded_key_type.ShardedKeyTypeConstraint(
        self._key_coder.to_type_hint())

  @classmethod
  def from_type_hint(cls, typehint, registry):
    from apache_beam.typehints import sharded_key_type
    if isinstance(typehint, sharded_key_type.ShardedKeyTypeConstraint):
      return cls(registry.get_coder(typehint.key_type))
    else:
      raise ValueError((
          'Expected an instance of ShardedKeyTypeConstraint'
          ', but got a %s' % typehint))

  def __eq__(self, other):
    return type(self) == type(other) and self._key_coder == other._key_coder

  def __hash__(self):
    return hash(type(self)) + hash(self._key_coder)

  def __repr__(self):
    return 'ShardedKeyCoder[%s]' % self._key_coder


Coder.register_structured_urn(
    common_urns.coders.SHARDED_KEY.urn, ShardedKeyCoder)


class TimestampPrefixingWindowCoder(FastCoder):
  """For internal use only; no backwards-compatibility guarantees.

  Coder which prefixes the max timestamp of arbitrary window to its encoded
  form."""
  def __init__(self, window_coder: Coder) -> None:
    self._window_coder = window_coder

  def _create_impl(self):
    return coder_impl.TimestampPrefixingWindowCoderImpl(
        self._window_coder.get_impl())

  def to_type_hint(self):
    return self._window_coder.to_type_hint()

  def _get_component_coders(self) -> List[Coder]:
    return [self._window_coder]

  def is_deterministic(self) -> bool:
    return self._window_coder.is_deterministic()

  def __repr__(self):
    return 'TimestampPrefixingWindowCoder[%r]' % self._window_coder

  def __eq__(self, other):
    return (
        type(self) == type(other) and self._window_coder == other._window_coder)

  def __hash__(self):
    return hash((type(self), self._window_coder))


Coder.register_structured_urn(
    common_urns.coders.CUSTOM_WINDOW.urn, TimestampPrefixingWindowCoder)


class TimestampPrefixingOpaqueWindowCoder(FastCoder):
  """For internal use only; no backwards-compatibility guarantees.

  Coder which decodes windows as bytes."""
  def __init__(self) -> None:
    pass

  def _create_impl(self):
    return coder_impl.TimestampPrefixingOpaqueWindowCoderImpl()

  def is_deterministic(self) -> bool:
    return True

  def __repr__(self):
    return 'TimestampPrefixingOpaqueWindowCoder'

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash((type(self)))


Coder.register_structured_urn(
    python_urns.TIMESTAMP_PREFIXED_OPAQUE_WINDOW_CODER,
    TimestampPrefixingOpaqueWindowCoder)


class BigIntegerCoder(FastCoder):
  def _create_impl(self):
    return coder_impl.BigIntegerCoderImpl()

  def is_deterministic(self):
    # type: () -> bool
    return True

  def to_type_hint(self):
    return int

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))


class PaneInfoCoder(FastCoder):
  def _create_impl(self):
    return coder_impl.PaneInfoCoderImpl()

  def is_deterministic(self):
    # type: () -> bool
    return True

  def to_type_hint(self):
    return windowed_value.PaneInfo

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))


class DecimalCoder(FastCoder):
  def _create_impl(self):
    return coder_impl.DecimalCoderImpl()

  def is_deterministic(self):
    # type: () -> bool
    return True

  def to_type_hint(self):
    return decimal.Decimal

  def __eq__(self, other):
    return type(self) == type(other)

  def __hash__(self):
    return hash(type(self))
