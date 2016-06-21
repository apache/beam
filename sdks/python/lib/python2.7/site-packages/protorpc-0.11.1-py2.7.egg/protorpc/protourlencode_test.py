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

"""Tests for protorpc.protourlencode."""

__author__ = 'rafek@google.com (Rafe Kaplan)'


import cgi
import logging
import unittest
import urllib

from protorpc import message_types
from protorpc import messages
from protorpc import protourlencode
from protorpc import test_util


class ModuleInterfaceTest(test_util.ModuleInterfaceTest,
                          test_util.TestCase):

  MODULE = protourlencode



class SuperMessage(messages.Message):
  """A test message with a nested message field."""

  sub_message = messages.MessageField(test_util.OptionalMessage, 1)
  sub_messages = messages.MessageField(test_util.OptionalMessage,
                                       2,
                                       repeated=True)


class SuperSuperMessage(messages.Message):
  """A test message with two levels of nested."""

  sub_message = messages.MessageField(SuperMessage, 1)
  sub_messages = messages.MessageField(SuperMessage, 2, repeated=True)


class URLEncodedRequestBuilderTest(test_util.TestCase):
  """Test the URL Encoded request builder."""

  def testMakePath(self):
    builder = protourlencode.URLEncodedRequestBuilder(SuperSuperMessage(),
                                                      prefix='pre.')

    self.assertEquals(None, builder.make_path(''))
    self.assertEquals(None, builder.make_path('no_such_field'))
    self.assertEquals(None, builder.make_path('pre.no_such_field'))

    # Missing prefix.
    self.assertEquals(None, builder.make_path('sub_message'))

    # Valid parameters.
    self.assertEquals((('sub_message', None),),
                       builder.make_path('pre.sub_message'))
    self.assertEquals((('sub_message', None), ('sub_messages', 1)),
                       builder.make_path('pre.sub_message.sub_messages-1'))
    self.assertEquals(
        (('sub_message', None),
         ('sub_messages', 1),
         ('int64_value', None)),
        builder.make_path('pre.sub_message.sub_messages-1.int64_value'))

    # Missing index.
    self.assertEquals(
        None,
        builder.make_path('pre.sub_message.sub_messages.integer_field'))

    # Has unexpected index.
    self.assertEquals(
        None,
        builder.make_path('pre.sub_message.sub_message-1.integer_field'))

  def testAddParameter_SimpleAttributes(self):
    message = test_util.OptionalMessage()
    builder = protourlencode.URLEncodedRequestBuilder(message, prefix='pre.')

    self.assertTrue(builder.add_parameter('pre.int64_value', ['10']))
    self.assertTrue(builder.add_parameter('pre.string_value', ['a string']))
    self.assertTrue(builder.add_parameter('pre.enum_value', ['VAL1']))
    self.assertEquals(10, message.int64_value)
    self.assertEquals('a string', message.string_value)
    self.assertEquals(test_util.OptionalMessage.SimpleEnum.VAL1,
                      message.enum_value)

  def testAddParameter_InvalidAttributes(self):
    message = SuperSuperMessage()
    builder = protourlencode.URLEncodedRequestBuilder(message, prefix='pre.')

    def assert_empty():
      self.assertEquals(None, getattr(message, 'sub_message'))
      self.assertEquals([], getattr(message, 'sub_messages'))

    self.assertFalse(builder.add_parameter('pre.nothing', ['x']))
    assert_empty()

    self.assertFalse(builder.add_parameter('pre.sub_messages', ['x']))
    self.assertFalse(builder.add_parameter('pre.sub_messages-1.nothing', ['x']))
    assert_empty()

  def testAddParameter_NestedAttributes(self):
    message = SuperSuperMessage()
    builder = protourlencode.URLEncodedRequestBuilder(message, prefix='pre.')

    # Set an empty message fields.
    self.assertTrue(builder.add_parameter('pre.sub_message', ['']))
    self.assertTrue(isinstance(message.sub_message, SuperMessage))

    # Add a basic attribute.
    self.assertTrue(builder.add_parameter(
        'pre.sub_message.sub_message.int64_value', ['10']))
    self.assertTrue(builder.add_parameter(
        'pre.sub_message.sub_message.string_value', ['hello']))

    self.assertTrue(10, message.sub_message.sub_message.int64_value)
    self.assertTrue('hello', message.sub_message.sub_message.string_value)


  def testAddParameter_NestedMessages(self):
    message = SuperSuperMessage()
    builder = protourlencode.URLEncodedRequestBuilder(message, prefix='pre.')

    # Add a repeated empty message.
    self.assertTrue(builder.add_parameter(
        'pre.sub_message.sub_messages-0', ['']))
    sub_message = message.sub_message.sub_messages[0]
    self.assertTrue(1, len(message.sub_message.sub_messages))
    self.assertTrue(isinstance(sub_message,
                               test_util.OptionalMessage))
    self.assertEquals(None, getattr(sub_message, 'int64_value'))
    self.assertEquals(None, getattr(sub_message, 'string_value'))
    self.assertEquals(None, getattr(sub_message, 'enum_value'))

    # Add a repeated message with value.
    self.assertTrue(builder.add_parameter(
        'pre.sub_message.sub_messages-1.int64_value', ['10']))
    self.assertTrue(2, len(message.sub_message.sub_messages))
    self.assertTrue(10, message.sub_message.sub_messages[1].int64_value)

    # Add another value to the same nested message.
    self.assertTrue(builder.add_parameter(
        'pre.sub_message.sub_messages-1.string_value', ['a string']))
    self.assertTrue(2, len(message.sub_message.sub_messages))
    self.assertEquals(10, message.sub_message.sub_messages[1].int64_value)
    self.assertEquals('a string',
                      message.sub_message.sub_messages[1].string_value)

  def testAddParameter_RepeatedValues(self):
    message = test_util.RepeatedMessage()
    builder = protourlencode.URLEncodedRequestBuilder(message, prefix='pre.')

    self.assertTrue(builder.add_parameter('pre.int64_value-0', ['20']))
    self.assertTrue(builder.add_parameter('pre.int64_value-1', ['30']))
    self.assertEquals([20, 30], message.int64_value)

    self.assertTrue(builder.add_parameter('pre.string_value-0', ['hi']))
    self.assertTrue(builder.add_parameter('pre.string_value-1', ['lo']))
    self.assertTrue(builder.add_parameter('pre.string_value-1', ['dups overwrite']))
    self.assertEquals(['hi', 'dups overwrite'], message.string_value)

  def testAddParameter_InvalidValuesMayRepeat(self):
    message = test_util.OptionalMessage()
    builder = protourlencode.URLEncodedRequestBuilder(message, prefix='pre.')

    self.assertFalse(builder.add_parameter('nothing', [1, 2, 3]))

  def testAddParameter_RepeatedParameters(self):
    message = test_util.OptionalMessage()
    builder = protourlencode.URLEncodedRequestBuilder(message, prefix='pre.')

    self.assertRaises(messages.DecodeError,
                      builder.add_parameter,
                      'pre.int64_value',
                      [1, 2, 3])
    self.assertRaises(messages.DecodeError,
                      builder.add_parameter,
                      'pre.int64_value',
                      [])

  def testAddParameter_UnexpectedNestedValue(self):
    """Test getting a nested value on a non-message sub-field."""
    message = test_util.HasNestedMessage()
    builder = protourlencode.URLEncodedRequestBuilder(message, 'pre.')

    self.assertFalse(builder.add_parameter('pre.nested.a_value.whatever',
                                           ['1']))

  def testInvalidFieldFormat(self):
    message = test_util.OptionalMessage()
    builder = protourlencode.URLEncodedRequestBuilder(message, prefix='pre.')

    self.assertFalse(builder.add_parameter('pre.illegal%20', ['1']))

  def testAddParameter_UnexpectedNestedValue(self):
    """Test getting a nested value on a non-message sub-field

    There is an odd corner case where if trying to insert a repeated value
    on an nested repeated message that would normally succeed in being created
    should fail.  This case can only be tested when the first message of the
    nested messages already exists.

    Another case is trying to access an indexed value nested within a
    non-message field.
    """
    class HasRepeated(messages.Message):

      values = messages.IntegerField(1, repeated=True)

    class HasNestedRepeated(messages.Message):

      nested = messages.MessageField(HasRepeated, 1, repeated=True)


    message = HasNestedRepeated()
    builder = protourlencode.URLEncodedRequestBuilder(message, prefix='pre.')

    self.assertTrue(builder.add_parameter('pre.nested-0.values-0', ['1']))
    # Try to create an indexed value on a non-message field.
    self.assertFalse(builder.add_parameter('pre.nested-0.values-0.unknown-0',
                                           ['1']))
    # Try to create an out of range indexed field on an otherwise valid
    # repeated message field.
    self.assertFalse(builder.add_parameter('pre.nested-1.values-1', ['1']))


class ProtourlencodeConformanceTest(test_util.TestCase,
                                    test_util.ProtoConformanceTestBase):

  PROTOLIB = protourlencode

  encoded_partial = urllib.urlencode([('double_value', 1.23),
                                      ('int64_value', -100000000000),
                                      ('int32_value', 1020),
                                      ('string_value', u'a string'),
                                      ('enum_value', 'VAL2'),
                                     ])

  encoded_full = urllib.urlencode([('double_value', 1.23),
                                   ('float_value', -2.5),
                                   ('int64_value', -100000000000),
                                   ('uint64_value', 102020202020),
                                   ('int32_value', 1020),
                                   ('bool_value', 'true'),
                                   ('string_value',
                                    u'a string\u044f'.encode('utf-8')),
                                   ('bytes_value', b'a bytes\xff\xfe'),
                                   ('enum_value', 'VAL2'),
                                  ])

  encoded_repeated = urllib.urlencode([('double_value-0', 1.23),
                                       ('double_value-1', 2.3),
                                       ('float_value-0', -2.5),
                                       ('float_value-1', 0.5),
                                       ('int64_value-0', -100000000000),
                                       ('int64_value-1', 20),
                                       ('uint64_value-0', 102020202020),
                                       ('uint64_value-1', 10),
                                       ('int32_value-0', 1020),
                                       ('int32_value-1', 718),
                                       ('bool_value-0', 'true'),
                                       ('bool_value-1', 'false'),
                                       ('string_value-0',
                                        u'a string\u044f'.encode('utf-8')),
                                       ('string_value-1',
                                        u'another string'.encode('utf-8')),
                                       ('bytes_value-0', b'a bytes\xff\xfe'),
                                       ('bytes_value-1', b'another bytes'),
                                       ('enum_value-0', 'VAL2'),
                                       ('enum_value-1', 'VAL1'),
                                      ])

  encoded_nested = urllib.urlencode([('nested.a_value', 'a string'),
                                    ])

  encoded_repeated_nested = urllib.urlencode(
      [('repeated_nested-0.a_value', 'a string'),
       ('repeated_nested-1.a_value', 'another string'),
       ])

  unexpected_tag_message = 'unexpected=whatever'

  encoded_default_assigned = urllib.urlencode([('a_value', 'a default'),
                                              ])

  encoded_nested_empty = urllib.urlencode([('nested', '')])

  encoded_repeated_nested_empty = urllib.urlencode([('repeated_nested-0', ''),
                                                    ('repeated_nested-1', '')])

  encoded_extend_message = urllib.urlencode([('int64_value-0', 400),
                                             ('int64_value-1', 50),
                                             ('int64_value-2', 6000)])

  encoded_string_types = urllib.urlencode(
    [('string_value', 'Latin')])

  encoded_invalid_enum = urllib.urlencode([('enum_value', 'undefined')])

  def testParameterPrefix(self):
    """Test using the 'prefix' parameter to encode_message."""
    class MyMessage(messages.Message):
      number = messages.IntegerField(1)
      names = messages.StringField(2, repeated=True)

    message = MyMessage()
    message.number = 10
    message.names = [u'Fred', u'Lisa']

    encoded_message = protourlencode.encode_message(message, prefix='prefix-')
    self.assertEquals({'prefix-number': ['10'],
                       'prefix-names-0': ['Fred'],
                       'prefix-names-1': ['Lisa'],
                      },
                      cgi.parse_qs(encoded_message))

    self.assertEquals(message, protourlencode.decode_message(MyMessage,
                                                             encoded_message,
                                                             prefix='prefix-'))

  def testProtourlencodeUnrecognizedField(self):
    """Test that unrecognized fields are saved and can be accessed."""

    class MyMessage(messages.Message):
      number = messages.IntegerField(1)

    decoded = protourlencode.decode_message(MyMessage,
                                            self.unexpected_tag_message)
    self.assertEquals(1, len(decoded.all_unrecognized_fields()))
    self.assertEquals('unexpected', decoded.all_unrecognized_fields()[0])
    # Unknown values set to a list of however many values had that name.
    self.assertEquals((['whatever'], messages.Variant.STRING),
                      decoded.get_unrecognized_field_info('unexpected'))

    repeated_unknown = urllib.urlencode([('repeated', 400),
                                         ('repeated', 'test'),
                                         ('repeated', '123.456')])
    decoded2 = protourlencode.decode_message(MyMessage, repeated_unknown)
    self.assertEquals((['400', 'test', '123.456'], messages.Variant.STRING),
                      decoded2.get_unrecognized_field_info('repeated'))

  def testDecodeInvalidDateTime(self):

    class MyMessage(messages.Message):
      a_datetime = message_types.DateTimeField(1)

    self.assertRaises(messages.DecodeError, protourlencode.decode_message,
                      MyMessage, 'a_datetime=invalid')


if __name__ == '__main__':
  unittest.main()
