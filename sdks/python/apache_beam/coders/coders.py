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

"""Collection of useful coders."""

import base64
import collections
import cPickle as pickle

from apache_beam.coders import coder_impl


# pylint: disable=wrong-import-order, wrong-import-position
# Avoid dependencies on the full SDK.
try:
  # Import dill from the pickler module to make sure our monkey-patching of dill
  # occurs.
  from apache_beam.internal.pickler import dill
  from apache_beam.transforms.timeutil import Timestamp
except ImportError:
  # We fall back to using the stock dill library in tests that don't use the
  # full Python SDK.
  import dill
  Timestamp = collections.namedtuple('Timestamp', 'micros')


def serialize_coder(coder):
  from apache_beam.internal import pickler
  return '%s$%s' % (coder.__class__.__name__, pickler.dumps(coder))


def deserialize_coder(serialized):
  from apache_beam.internal import pickler
  return pickler.loads(serialized.split('$', 1)[1])
# pylint: enable=wrong-import-order, wrong-import-position


class Coder(object):
  """Base class for coders."""

  def encode(self, value):
    """Encodes the given object into a byte string."""
    raise NotImplementedError('Encode not implemented: %s.' % self)

  def decode(self, encoded):
    """Decodes the given byte string into the corresponding object."""
    raise NotImplementedError('Decode not implemented: %s.' % self)

  def is_deterministic(self):
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
    """Creates a CoderImpl to do the actual encoding and decoding.
    """
    return coder_impl.CallbackCoderImpl(self.encode, self.decode,
                                        self.estimate_size)

  def get_impl(self):
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
    else:
      return self.__dict__

  @classmethod
  def from_type_hint(cls, unused_typehint, unused_registry):
    # If not overridden, just construct the coder without arguments.
    return cls()

  def is_kv_coder(self):
    return False

  def key_coder(self):
    if self.is_kv_coder():
      raise NotImplementedError('key_coder: %s' % self)
    else:
      raise ValueError('Not a KV coder: %s.' % self)

  def value_coder(self):
    if self.is_kv_coder():
      raise NotImplementedError('value_coder: %s' % self)
    else:
      raise ValueError('Not a KV coder: %s.' % self)

  def _get_component_coders(self):
    """Returns the internal component coders of this coder."""
    # This is an internal detail of the Coder API and does not need to be
    # refined in user-defined Coders.
    return []

  def as_cloud_object(self):
    """Returns Google Cloud Dataflow API description of this coder."""
    # This is an internal detail of the Coder API and does not need to be
    # refined in user-defined Coders.

    value = {
        # We pass coders in the form "<coder_name>$<pickled_data>" to make the
        # job description JSON more readable.  Data before the $ is ignored by
        # the worker.
        '@type': serialize_coder(self),
        'component_encodings': list(
            component.as_cloud_object()
            for component in self._get_component_coders())
    }
    return value

  def __repr__(self):
    return self.__class__.__name__

  def __eq__(self, other):
    # pylint: disable=protected-access
    return (self.__class__ == other.__class__
            and self._dict_without_impl() == other._dict_without_impl())
    # pylint: enable=protected-access


class StrUtf8Coder(Coder):
  """A coder used for reading and writing strings as UTF-8."""

  def encode(self, value):
    return value.encode('utf-8')

  def decode(self, value):
    return value.decode('utf-8')

  def is_deterministic(self):
    return True


class ToStringCoder(Coder):
  """A default string coder used if no sink coder is specified."""

  def encode(self, value):
    if isinstance(value, unicode):
      return value.encode('utf-8')
    elif isinstance(value, str):
      return value
    else:
      return str(value)

  def decode(self, _):
    raise NotImplementedError('ToStringCoder cannot be used for decoding.')

  def is_deterministic(self):
    return True


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
    return True


class VarIntCoder(FastCoder):
  """Variable-length integer coder."""

  def _create_impl(self):
    return coder_impl.VarIntCoderImpl()

  def is_deterministic(self):
    return True


class FloatCoder(FastCoder):
  """A coder used for floating-point values."""

  def _create_impl(self):
    return coder_impl.FloatCoderImpl()

  def is_deterministic(self):
    return True


class TimestampCoder(FastCoder):
  """A coder used for timeutil.Timestamp values."""

  def _create_impl(self):
    return coder_impl.TimestampCoderImpl(Timestamp)

  def is_deterministic(self):
    return True


class SingletonCoder(FastCoder):
  """A coder that always encodes exactly one value."""

  def __init__(self, value):
    self._value = value

  def _create_impl(self):
    return coder_impl.SingletonCoderImpl(self._value)

  def is_deterministic(self):
    return True


def maybe_dill_dumps(o):
  """Pickle using cPickle or the Dill pickler as a fallback."""
  # We need to use the dill pickler for objects of certain custom classes,
  # including, for example, ones that contain lambdas.
  try:
    return pickle.dumps(o)
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
    # Note that the default coder, the PickleCoder, is not deterministic (for
    # example, the ordering of picked entries in maps may vary across
    # executions), and so is not in general suitable for usage as a key coder in
    # GroupByKey operations.
    return False

  def as_cloud_object(self, is_pair_like=True):
    value = super(_PickleCoderBase, self).as_cloud_object()
    # We currently use this coder in places where we cannot infer the coder to
    # use for the value type in a more granular way.  In places where the
    # service expects a pair, it checks for the "is_pair_like" key, in which
    # case we would fail without the hack below.
    if is_pair_like:
      value['is_pair_like'] = True
      value['component_encodings'] = [
          self.as_cloud_object(is_pair_like=False),
          self.as_cloud_object(is_pair_like=False)
      ]

    return value

  # We allow .key_coder() and .value_coder() to be called on PickleCoder since
  # we can't always infer the return values of lambdas in ParDo operations, the
  # result of which may be used in a GroupBykey.
  def is_kv_coder(self):
    return True

  def key_coder(self):
    return self

  def value_coder(self):
    return self


class PickleCoder(_PickleCoderBase):
  """Coder using Python's pickle functionality."""

  def _create_impl(self):
    return coder_impl.CallbackCoderImpl(pickle.dumps, pickle.loads)


class DillCoder(_PickleCoderBase):
  """Coder using dill's pickle functionality."""

  def _create_impl(self):
    return coder_impl.CallbackCoderImpl(maybe_dill_dumps, maybe_dill_loads)


class DeterministicPickleCoder(FastCoder):
  """Throws runtime errors when pickling non-deterministic values."""

  def __init__(self, pickle_coder, step_label):
    self._pickle_coder = pickle_coder
    self._step_label = step_label

  def _create_impl(self):
    return coder_impl.DeterministicPickleCoderImpl(
        self._pickle_coder.get_impl(), self._step_label)

  def is_deterministic(self):
    return True

  def is_kv_coder(self):
    return True

  def key_coder(self):
    return self

  def value_coder(self):
    return self


class FastPrimitivesCoder(FastCoder):
  """Encodes simple primitives (e.g. str, int) efficiently.

  For unknown types, falls back to another coder (e.g. PickleCoder).
  """
  def __init__(self, fallback_coder=PickleCoder()):
    self._fallback_coder = fallback_coder

  def _create_impl(self):
    return coder_impl.FastPrimitivesCoderImpl(
        self._fallback_coder.get_impl())

  def is_deterministic(self):
    return self._fallback_coder.is_deterministic()

  def as_cloud_object(self, is_pair_like=True):
    value = super(FastCoder, self).as_cloud_object()
    # We currently use this coder in places where we cannot infer the coder to
    # use for the value type in a more granular way.  In places where the
    # service expects a pair, it checks for the "is_pair_like" key, in which
    # case we would fail without the hack below.
    if is_pair_like:
      value['is_pair_like'] = True
      value['component_encodings'] = [
          self.as_cloud_object(is_pair_like=False),
          self.as_cloud_object(is_pair_like=False)
      ]

    return value

  # We allow .key_coder() and .value_coder() to be called on FastPrimitivesCoder
  # since we can't always infer the return values of lambdas in ParDo
  # operations, the result of which may be used in a GroupBykey.
  def is_kv_coder(self):
    return True

  def key_coder(self):
    return self

  def value_coder(self):
    return self


class Base64PickleCoder(Coder):
  """Coder of objects by Python pickle, then base64 encoding."""
  # TODO(robertwb): Do base64 encoding where it's needed (e.g. in json) rather
  # than via a special Coder.

  def encode(self, value):
    return base64.b64encode(pickle.dumps(value))

  def decode(self, encoded):
    return pickle.loads(base64.b64decode(encoded))

  def is_deterministic(self):
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
    return True

  def key_coder(self):
    return self

  def value_coder(self):
    return self


class TupleCoder(FastCoder):
  """Coder of tuple objects."""

  def __init__(self, components):
    self._coders = tuple(components)

  def _create_impl(self):
    return coder_impl.TupleCoderImpl([c.get_impl() for c in self._coders])

  def is_deterministic(self):
    return all(c.is_deterministic() for c in self._coders)

  @staticmethod
  def from_type_hint(typehint, registry):
    return TupleCoder([registry.get_coder(t) for t in typehint.tuple_types])

  def as_cloud_object(self):
    value = super(TupleCoder, self).as_cloud_object()
    value['is_pair_like'] = True
    return value

  def _get_component_coders(self):
    return self.coders()

  def coders(self):
    return self._coders

  def is_kv_coder(self):
    return len(self._coders) == 2

  def key_coder(self):
    if len(self._coders) != 2:
      raise ValueError('TupleCoder does not have exactly 2 components.')
    return self._coders[0]

  def value_coder(self):
    if len(self._coders) != 2:
      raise ValueError('TupleCoder does not have exactly 2 components.')
    return self._coders[1]

  def __repr__(self):
    return 'TupleCoder[%s]' % ', '.join(str(c) for c in self._coders)


class TupleSequenceCoder(FastCoder):
  """Coder of homogeneous tuple objects."""

  def __init__(self, elem_coder):
    self._elem_coder = elem_coder

  def _create_impl(self):
    return coder_impl.TupleSequenceCoderImpl(self._elem_coder.get_impl())

  def is_deterministic(self):
    return self._elem_coder.is_deterministic()

  @staticmethod
  def from_type_hint(typehint, registry):
    return TupleSequenceCoder(registry.get_coder(typehint.inner_type))

  def _get_component_coders(self):
    return (self._elem_coder,)

  def __repr__(self):
    return 'TupleSequenceCoder[%r]' % self._elem_coder


class WindowCoder(PickleCoder):
  """Coder for windows in windowed values."""

  def _create_impl(self):
    return coder_impl.CallbackCoderImpl(pickle.dumps, pickle.loads)

  def is_deterministic(self):
    # Note that WindowCoder as implemented is not deterministic because the
    # implementation simply pickles windows.  See the corresponding comments
    # on PickleCoder for more details.
    return False

  def as_cloud_object(self):
    return super(WindowCoder, self).as_cloud_object(is_pair_like=False)


class WindowedValueCoder(FastCoder):
  """Coder for windowed values."""

  def __init__(self, wrapped_value_coder, timestamp_coder=None,
               window_coder=None):
    if not timestamp_coder:
      timestamp_coder = TimestampCoder()
    if not window_coder:
      window_coder = PickleCoder()
    self.wrapped_value_coder = wrapped_value_coder
    self.timestamp_coder = timestamp_coder
    self.window_coder = window_coder

  def _create_impl(self):
    return coder_impl.WindowedValueCoderImpl(
        self.wrapped_value_coder.get_impl(),
        self.timestamp_coder.get_impl(),
        self.window_coder.get_impl())

  def is_deterministic(self):
    return all(c.is_deterministic() for c in [self.wrapped_value_coder,
                                              self.timestamp_coder,
                                              self.window_coder])

  def as_cloud_object(self):
    value = super(WindowedValueCoder, self).as_cloud_object()
    value['is_wrapper'] = True
    return value

  def _get_component_coders(self):
    return [self.wrapped_value_coder, self.timestamp_coder, self.window_coder]

  def is_kv_coder(self):
    return self.wrapped_value_coder.is_kv_coder()

  def key_coder(self):
    return self.wrapped_value_coder.key_coder()

  def value_coder(self):
    return self.wrapped_value_coder.value_coder()

  def __repr__(self):
    return 'WindowedValueCoder[%s]' % self.wrapped_value_coder
