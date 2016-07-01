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

"""Tests for protorpc.protobuf."""

__author__ = 'rafek@google.com (Rafe Kaplan)'


import datetime
import unittest

from protorpc import message_types
from protorpc import messages
from protorpc import protobuf
from protorpc import protorpc_test_pb2
from protorpc import test_util
from protorpc import util

# TODO: Add DateTimeFields to protorpc_test.proto when definition.py
# supports date time fields.
class HasDateTimeMessage(messages.Message):
  value = message_types.DateTimeField(1)

class NestedDateTimeMessage(messages.Message):
  value = messages.MessageField(message_types.DateTimeMessage, 1)


class ModuleInterfaceTest(test_util.ModuleInterfaceTest,
                          test_util.TestCase):

  MODULE = protobuf


class EncodeMessageTest(test_util.TestCase,
                        test_util.ProtoConformanceTestBase):
  """Test message to protocol buffer encoding."""

  PROTOLIB = protobuf

  def assertErrorIs(self, exception, message, function, *params, **kwargs):
    try:
      function(*params, **kwargs)
      self.fail('Expected to raise exception %s but did not.' % exception)
    except exception as err:
      self.assertEquals(message, str(err))

  @property
  def encoded_partial(self):
    proto = protorpc_test_pb2.OptionalMessage()
    proto.double_value = 1.23
    proto.int64_value = -100000000000
    proto.int32_value = 1020
    proto.string_value = u'a string'
    proto.enum_value = protorpc_test_pb2.OptionalMessage.VAL2

    return proto.SerializeToString()

  @property
  def encoded_full(self):
    proto = protorpc_test_pb2.OptionalMessage()
    proto.double_value = 1.23
    proto.float_value = -2.5
    proto.int64_value = -100000000000
    proto.uint64_value = 102020202020
    proto.int32_value = 1020
    proto.bool_value = True
    proto.string_value = u'a string\u044f'
    proto.bytes_value = b'a bytes\xff\xfe'
    proto.enum_value = protorpc_test_pb2.OptionalMessage.VAL2

    return proto.SerializeToString()

  @property
  def encoded_repeated(self):
    proto = protorpc_test_pb2.RepeatedMessage()
    proto.double_value.append(1.23)
    proto.double_value.append(2.3)
    proto.float_value.append(-2.5)
    proto.float_value.append(0.5)
    proto.int64_value.append(-100000000000)
    proto.int64_value.append(20)
    proto.uint64_value.append(102020202020)
    proto.uint64_value.append(10)
    proto.int32_value.append(1020)
    proto.int32_value.append(718)
    proto.bool_value.append(True)
    proto.bool_value.append(False)
    proto.string_value.append(u'a string\u044f')
    proto.string_value.append(u'another string')
    proto.bytes_value.append(b'a bytes\xff\xfe')
    proto.bytes_value.append(b'another bytes')
    proto.enum_value.append(protorpc_test_pb2.RepeatedMessage.VAL2)
    proto.enum_value.append(protorpc_test_pb2.RepeatedMessage.VAL1)

    return proto.SerializeToString()

  @property
  def encoded_nested(self):
    proto = protorpc_test_pb2.HasNestedMessage()
    proto.nested.a_value = 'a string'

    return proto.SerializeToString()

  @property
  def encoded_repeated_nested(self):
    proto = protorpc_test_pb2.HasNestedMessage()
    proto.repeated_nested.add().a_value = 'a string'
    proto.repeated_nested.add().a_value = 'another string'

    return proto.SerializeToString()

  unexpected_tag_message = (
        chr((15 << protobuf._WIRE_TYPE_BITS) | protobuf._Encoder.NUMERIC) +
        chr(5))

  @property
  def encoded_default_assigned(self):
    proto = protorpc_test_pb2.HasDefault()
    proto.a_value = test_util.HasDefault.a_value.default
    return proto.SerializeToString()

  @property
  def encoded_nested_empty(self):
    proto = protorpc_test_pb2.HasOptionalNestedMessage()
    proto.nested.Clear()
    return proto.SerializeToString()

  @property
  def encoded_repeated_nested_empty(self):
    proto = protorpc_test_pb2.HasOptionalNestedMessage()
    proto.repeated_nested.add()
    proto.repeated_nested.add()
    return proto.SerializeToString()

  @property
  def encoded_extend_message(self):
    proto = protorpc_test_pb2.RepeatedMessage()
    proto.add_int64_value(400)
    proto.add_int64_value(50)
    proto.add_int64_value(6000)
    return proto.SerializeToString()

  @property
  def encoded_string_types(self):
    proto = protorpc_test_pb2.OptionalMessage()
    proto.string_value = u'Latin'
    return proto.SerializeToString()

  @property
  def encoded_invalid_enum(self):
    encoder = protobuf._Encoder()
    field_num = test_util.OptionalMessage.enum_value.number
    tag = (field_num << protobuf._WIRE_TYPE_BITS) | encoder.NUMERIC
    encoder.putVarInt32(tag)
    encoder.putVarInt32(1000)
    return encoder.buffer().tostring()

  def testDecodeWrongWireFormat(self):
    """Test what happens when wrong wire format found in protobuf."""
    class ExpectedProto(messages.Message):
      value = messages.StringField(1)

    class WrongVariant(messages.Message):
      value = messages.IntegerField(1)

    original = WrongVariant()
    original.value = 10
    self.assertErrorIs(messages.DecodeError,
                       'Expected wire type STRING but found NUMERIC',
                       protobuf.decode_message,
                       ExpectedProto,
                       protobuf.encode_message(original))

  def testDecodeBadWireType(self):
    """Test what happens when non-existant wire type found in protobuf."""
    # Message has tag 1, type 3 which does not exist.
    bad_wire_type_message = chr((1 << protobuf._WIRE_TYPE_BITS) | 3)

    self.assertErrorIs(messages.DecodeError,
                       'No such wire type 3',
                       protobuf.decode_message,
                       test_util.OptionalMessage,
                       bad_wire_type_message)

  def testUnexpectedTagBelowOne(self):
    """Test that completely invalid tags generate an error."""
    # Message has tag 0, type NUMERIC.
    invalid_tag_message = chr(protobuf._Encoder.NUMERIC)

    self.assertErrorIs(messages.DecodeError,
                       'Invalid tag value 0',
                       protobuf.decode_message,
                       test_util.OptionalMessage,
                       invalid_tag_message)

  def testProtocolBufferDecodeError(self):
    """Test what happens when there a ProtocolBufferDecodeError.

    This is what happens when the underlying ProtocolBuffer library raises
    it's own decode error.
    """
    # Message has tag 1, type DOUBLE, missing value.
    truncated_message = (
        chr((1 << protobuf._WIRE_TYPE_BITS) | protobuf._Encoder.DOUBLE))

    self.assertErrorIs(messages.DecodeError,
                       'Decoding error: truncated',
                       protobuf.decode_message,
                       test_util.OptionalMessage,
                       truncated_message)

  def testProtobufUnrecognizedField(self):
    """Test that unrecognized fields are serialized and can be accessed."""
    decoded = protobuf.decode_message(test_util.OptionalMessage,
                                      self.unexpected_tag_message)
    self.assertEquals(1, len(decoded.all_unrecognized_fields()))
    self.assertEquals(15, decoded.all_unrecognized_fields()[0])
    self.assertEquals((5, messages.Variant.INT64),
                      decoded.get_unrecognized_field_info(15))

  def testUnrecognizedFieldWrongFormat(self):
    """Test that unrecognized fields in the wrong format are skipped."""

    class SimpleMessage(messages.Message):
      value = messages.IntegerField(1)

    message = SimpleMessage(value=3)
    message.set_unrecognized_field('from_json', 'test', messages.Variant.STRING)

    encoded = protobuf.encode_message(message)
    expected = (
        chr((1 << protobuf._WIRE_TYPE_BITS) | protobuf._Encoder.NUMERIC) +
        chr(3))
    self.assertEquals(encoded, expected)

  def testProtobufDecodeDateTimeMessage(self):
    """Test what happens when decoding a DateTimeMessage."""

    nested = NestedDateTimeMessage()
    nested.value = message_types.DateTimeMessage(milliseconds=2500)
    value = protobuf.decode_message(HasDateTimeMessage,
                                    protobuf.encode_message(nested)).value
    self.assertEqual(datetime.datetime(1970, 1, 1, 0, 0, 2, 500000), value)

  def testProtobufDecodeDateTimeMessageWithTimeZone(self):
    """Test what happens when decoding a DateTimeMessage with a time zone."""
    nested = NestedDateTimeMessage()
    nested.value = message_types.DateTimeMessage(milliseconds=12345678,
                                                 time_zone_offset=60)
    value = protobuf.decode_message(HasDateTimeMessage,
                                    protobuf.encode_message(nested)).value
    self.assertEqual(datetime.datetime(1970, 1, 1, 3, 25, 45, 678000,
                                       tzinfo=util.TimeZoneOffset(60)),
                     value)

  def testProtobufEncodeDateTimeMessage(self):
    """Test what happens when encoding a DateTimeField."""
    mine = HasDateTimeMessage(value=datetime.datetime(1970, 1, 1))
    nested = NestedDateTimeMessage()
    nested.value = message_types.DateTimeMessage(milliseconds=0)

    my_encoded = protobuf.encode_message(mine)
    encoded = protobuf.encode_message(nested)
    self.assertEquals(my_encoded, encoded)

  def testProtobufEncodeDateTimeMessageWithTimeZone(self):
    """Test what happens when encoding a DateTimeField with a time zone."""
    for tz_offset in (30, -30, 8 * 60, 0):
      mine = HasDateTimeMessage(value=datetime.datetime(
          1970, 1, 1, tzinfo=util.TimeZoneOffset(tz_offset)))
      nested = NestedDateTimeMessage()
      nested.value = message_types.DateTimeMessage(
          milliseconds=0, time_zone_offset=tz_offset)

      my_encoded = protobuf.encode_message(mine)
      encoded = protobuf.encode_message(nested)
      self.assertEquals(my_encoded, encoded)


def main():
  unittest.main()


if __name__ == '__main__':
  main()
