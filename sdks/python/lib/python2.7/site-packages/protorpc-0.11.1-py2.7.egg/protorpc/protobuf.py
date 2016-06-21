#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Protocol buffer support for message types.

For more details about protocol buffer encoding and decoding please see:

  http://code.google.com/apis/protocolbuffers/docs/encoding.html

Public Exceptions:
  DecodeError: Raised when a decode error occurs from incorrect protobuf format.

Public Functions:
  encode_message: Encodes a message in to a protocol buffer string.
  decode_message: Decode from a protocol buffer string to a message.
"""
import six

__author__ = 'rafek@google.com (Rafe Kaplan)'


import array

from . import message_types
from . import messages
from . import util
from .google_imports import ProtocolBuffer


__all__ = ['ALTERNATIVE_CONTENT_TYPES',
           'CONTENT_TYPE',
           'encode_message',
           'decode_message',
          ]

CONTENT_TYPE = 'application/octet-stream'

ALTERNATIVE_CONTENT_TYPES = ['application/x-google-protobuf']


class _Encoder(ProtocolBuffer.Encoder):
  """Extension of protocol buffer encoder.

  Original protocol buffer encoder does not have complete set of methods
  for handling required encoding.  This class adds them.
  """

  # TODO(rafek): Implement the missing encoding types.
  def no_encoding(self, value):
    """No encoding available for type.

    Args:
      value: Value to encode.

    Raises:
      NotImplementedError at all times.
    """
    raise NotImplementedError()

  def encode_enum(self, value):
    """Encode an enum value.

    Args:
      value: Enum to encode.
    """
    self.putVarInt32(value.number)

  def encode_message(self, value):
    """Encode a Message in to an embedded message.

    Args:
      value: Message instance to encode.
    """
    self.putPrefixedString(encode_message(value))


  def encode_unicode_string(self, value):
    """Helper to properly pb encode unicode strings to UTF-8.

    Args:
      value: String value to encode.
    """
    if isinstance(value, six.text_type):
      value = value.encode('utf-8')
    self.putPrefixedString(value)


class _Decoder(ProtocolBuffer.Decoder):
  """Extension of protocol buffer decoder.

  Original protocol buffer decoder does not have complete set of methods
  for handling required decoding.  This class adds them.
  """

  # TODO(rafek): Implement the missing encoding types.
  def no_decoding(self):
    """No decoding available for type.

    Raises:
      NotImplementedError at all times.
    """
    raise NotImplementedError()

  def decode_string(self):
    """Decode a unicode string.

    Returns:
      Next value in stream as a unicode string.
    """
    return self.getPrefixedString().decode('UTF-8')

  def decode_boolean(self):
    """Decode a boolean value.

    Returns:
      Next value in stream as a boolean.
    """
    return bool(self.getBoolean())


# Number of bits used to describe a protocol buffer bits used for the variant.
_WIRE_TYPE_BITS = 3
_WIRE_TYPE_MASK = 7


# Maps variant to underlying wire type.  Many variants map to same type.
_VARIANT_TO_WIRE_TYPE = {
    messages.Variant.DOUBLE: _Encoder.DOUBLE,
    messages.Variant.FLOAT: _Encoder.FLOAT,
    messages.Variant.INT64: _Encoder.NUMERIC,
    messages.Variant.UINT64: _Encoder.NUMERIC,
    messages.Variant.INT32:  _Encoder.NUMERIC,
    messages.Variant.BOOL: _Encoder.NUMERIC,
    messages.Variant.STRING: _Encoder.STRING,
    messages.Variant.MESSAGE: _Encoder.STRING,
    messages.Variant.BYTES: _Encoder.STRING,
    messages.Variant.UINT32: _Encoder.NUMERIC,
    messages.Variant.ENUM:  _Encoder.NUMERIC,
    messages.Variant.SINT32: _Encoder.NUMERIC,
    messages.Variant.SINT64: _Encoder.NUMERIC,
}


# Maps variant to encoder method.
_VARIANT_TO_ENCODER_MAP = {
    messages.Variant.DOUBLE: _Encoder.putDouble,
    messages.Variant.FLOAT: _Encoder.putFloat,
    messages.Variant.INT64: _Encoder.putVarInt64,
    messages.Variant.UINT64: _Encoder.putVarUint64,
    messages.Variant.INT32: _Encoder.putVarInt32,
    messages.Variant.BOOL: _Encoder.putBoolean,
    messages.Variant.STRING: _Encoder.encode_unicode_string,
    messages.Variant.MESSAGE: _Encoder.encode_message,
    messages.Variant.BYTES: _Encoder.encode_unicode_string,
    messages.Variant.UINT32: _Encoder.no_encoding,
    messages.Variant.ENUM: _Encoder.encode_enum,
    messages.Variant.SINT32: _Encoder.no_encoding,
    messages.Variant.SINT64: _Encoder.no_encoding,
}


# Basic wire format decoders.  Used for reading unknown values.
_WIRE_TYPE_TO_DECODER_MAP = {
  _Encoder.NUMERIC: _Decoder.getVarInt64,
  _Encoder.DOUBLE: _Decoder.getDouble,
  _Encoder.STRING: _Decoder.getPrefixedString,
  _Encoder.FLOAT: _Decoder.getFloat,
}


# Map wire type to variant.  Used to find a variant for unknown values.
_WIRE_TYPE_TO_VARIANT_MAP = {
  _Encoder.NUMERIC: messages.Variant.INT64,
  _Encoder.DOUBLE: messages.Variant.DOUBLE,
  _Encoder.STRING: messages.Variant.STRING,
  _Encoder.FLOAT: messages.Variant.FLOAT,
}


# Wire type to name mapping for error messages.
_WIRE_TYPE_NAME = {
  _Encoder.NUMERIC: 'NUMERIC',
  _Encoder.DOUBLE: 'DOUBLE',
  _Encoder.STRING: 'STRING',
  _Encoder.FLOAT: 'FLOAT',
}


# Maps variant to decoder method.
_VARIANT_TO_DECODER_MAP = {
    messages.Variant.DOUBLE: _Decoder.getDouble,
    messages.Variant.FLOAT: _Decoder.getFloat,
    messages.Variant.INT64: _Decoder.getVarInt64,
    messages.Variant.UINT64: _Decoder.getVarUint64,
    messages.Variant.INT32:  _Decoder.getVarInt32,
    messages.Variant.BOOL: _Decoder.decode_boolean,
    messages.Variant.STRING: _Decoder.decode_string,
    messages.Variant.MESSAGE: _Decoder.getPrefixedString,
    messages.Variant.BYTES: _Decoder.getPrefixedString,
    messages.Variant.UINT32: _Decoder.no_decoding,
    messages.Variant.ENUM:  _Decoder.getVarInt32,
    messages.Variant.SINT32: _Decoder.no_decoding,
    messages.Variant.SINT64: _Decoder.no_decoding,
}


def encode_message(message):
  """Encode Message instance to protocol buffer.

  Args:
    Message instance to encode in to protocol buffer.

  Returns:
    String encoding of Message instance in protocol buffer format.

  Raises:
    messages.ValidationError if message is not initialized.
  """
  message.check_initialized()
  encoder = _Encoder()

  # Get all fields, from the known fields we parsed and the unknown fields
  # we saved.  Note which ones were known, so we can process them differently.
  all_fields = [(field.number, field) for field in message.all_fields()]
  all_fields.extend((key, None)
                    for key in message.all_unrecognized_fields()
                    if isinstance(key, six.integer_types))
  all_fields.sort()
  for field_num, field in all_fields:
    if field:
      # Known field.
      value = message.get_assigned_value(field.name)
      if value is None:
        continue
      variant = field.variant
      repeated = field.repeated
    else:
      # Unrecognized field.
      value, variant = message.get_unrecognized_field_info(field_num)
      if not isinstance(variant, messages.Variant):
        continue
      repeated = isinstance(value, (list, tuple))

    tag = ((field_num << _WIRE_TYPE_BITS) | _VARIANT_TO_WIRE_TYPE[variant])

    # Write value to wire.
    if repeated:
      values = value
    else:
      values = [value]
    for next in values:
      encoder.putVarInt32(tag)
      if isinstance(field, messages.MessageField):
        next = field.value_to_message(next)
      field_encoder = _VARIANT_TO_ENCODER_MAP[variant]
      field_encoder(encoder, next)

  return encoder.buffer().tostring()


def decode_message(message_type, encoded_message):
  """Decode protocol buffer to Message instance.

  Args:
    message_type: Message type to decode data to.
    encoded_message: Encoded version of message as string.

  Returns:
    Decoded instance of message_type.

  Raises:
    DecodeError if an error occurs during decoding, such as incompatible
      wire format for a field.
    messages.ValidationError if merged message is not initialized.
  """
  message = message_type()
  message_array = array.array('B')
  message_array.fromstring(encoded_message)
  try:
    decoder = _Decoder(message_array, 0, len(message_array))

    while decoder.avail() > 0:
      # Decode tag and variant information.
      encoded_tag = decoder.getVarInt32()
      tag = encoded_tag >> _WIRE_TYPE_BITS
      wire_type = encoded_tag & _WIRE_TYPE_MASK
      try:
        found_wire_type_decoder = _WIRE_TYPE_TO_DECODER_MAP[wire_type]
      except:
        raise messages.DecodeError('No such wire type %d' % wire_type)

      if tag < 1:
        raise messages.DecodeError('Invalid tag value %d' % tag)

      try:
        field = message.field_by_number(tag)
      except KeyError:
        # Unexpected tags are ok.
        field = None
        wire_type_decoder = found_wire_type_decoder
      else:
        expected_wire_type = _VARIANT_TO_WIRE_TYPE[field.variant]
        if expected_wire_type != wire_type:
          raise messages.DecodeError('Expected wire type %s but found %s' % (
              _WIRE_TYPE_NAME[expected_wire_type],
              _WIRE_TYPE_NAME[wire_type]))

        wire_type_decoder = _VARIANT_TO_DECODER_MAP[field.variant]

      value = wire_type_decoder(decoder)

      # Save unknown fields and skip additional processing.
      if not field:
        # When saving this, save it under the tag number (which should
        # be unique), and set the variant and value so we know how to
        # interpret the value later.
        variant = _WIRE_TYPE_TO_VARIANT_MAP.get(wire_type)
        if variant:
          message.set_unrecognized_field(tag, value, variant)
        continue

      # Special case Enum and Message types.
      if isinstance(field, messages.EnumField):
        try:
          value = field.type(value)
        except TypeError:
          raise messages.DecodeError('Invalid enum value %s' % value)
      elif isinstance(field, messages.MessageField):
        value = decode_message(field.message_type, value)
        value = field.value_from_message(value)

      # Merge value in to message.
      if field.repeated:
        values = getattr(message, field.name)
        if values is None:
          setattr(message, field.name, [value])
        else:
          values.append(value)
      else:
        setattr(message, field.name, value)
  except ProtocolBuffer.ProtocolBufferDecodeError as err:
    raise messages.DecodeError('Decoding error: %s' % str(err))

  message.check_initialized()
  return message
