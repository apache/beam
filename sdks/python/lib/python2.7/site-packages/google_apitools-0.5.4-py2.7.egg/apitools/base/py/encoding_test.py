#
# Copyright 2015 Google Inc.
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

import base64
import datetime
import json
import sys

import unittest2

from apitools.base.protorpclite import message_types
from apitools.base.protorpclite import messages
from apitools.base.protorpclite import util
from apitools.base.py import encoding
from apitools.base.py import exceptions
from apitools.base.py import extra_types


class SimpleMessage(messages.Message):
    field = messages.StringField(1)
    repfield = messages.StringField(2, repeated=True)


class BytesMessage(messages.Message):
    field = messages.BytesField(1)
    repfield = messages.BytesField(2, repeated=True)


class TimeMessage(messages.Message):
    timefield = message_types.DateTimeField(3)


@encoding.MapUnrecognizedFields('additional_properties')
class AdditionalPropertiesMessage(messages.Message):

    class AdditionalProperty(messages.Message):
        key = messages.StringField(1)
        value = messages.StringField(2)

    additional_properties = messages.MessageField(
        'AdditionalProperty', 1, repeated=True)


@encoding.MapUnrecognizedFields('additional_properties')
class UnrecognizedEnumMessage(messages.Message):

    class ThisEnum(messages.Enum):
        VALUE_ONE = 1
        VALUE_TWO = 2

    class AdditionalProperty(messages.Message):
        key = messages.StringField(1)
        value = messages.EnumField('UnrecognizedEnumMessage.ThisEnum', 2)

    additional_properties = messages.MessageField(
        AdditionalProperty, 1, repeated=True)


class CompoundPropertyType(messages.Message):
    index = messages.IntegerField(1)
    name = messages.StringField(2)


class MessageWithEnum(messages.Message):

    class ThisEnum(messages.Enum):
        VALUE_ONE = 1
        VALUE_TWO = 2

    field_one = messages.EnumField(ThisEnum, 1)
    field_two = messages.EnumField(ThisEnum, 2, default=ThisEnum.VALUE_TWO)
    ignored_field = messages.EnumField(ThisEnum, 3)


@encoding.MapUnrecognizedFields('additional_properties')
class AdditionalMessagePropertiesMessage(messages.Message):

    class AdditionalProperty(messages.Message):
        key = messages.StringField(1)
        value = messages.MessageField(CompoundPropertyType, 2)

    additional_properties = messages.MessageField(
        'AdditionalProperty', 1, repeated=True)


class HasNestedMessage(messages.Message):
    nested = messages.MessageField(AdditionalPropertiesMessage, 1)
    nested_list = messages.StringField(2, repeated=True)


class ExtraNestedMessage(messages.Message):
    nested = messages.MessageField(HasNestedMessage, 1)


class MessageWithRemappings(messages.Message):

    class SomeEnum(messages.Enum):
        enum_value = 1
        second_value = 2

    enum_field = messages.EnumField(SomeEnum, 1)
    double_encoding = messages.EnumField(SomeEnum, 2)
    another_field = messages.StringField(3)
    repeated_enum = messages.EnumField(SomeEnum, 4, repeated=True)
    repeated_field = messages.StringField(5, repeated=True)


@encoding.MapUnrecognizedFields('additional_properties')
class RepeatedJsonValueMessage(messages.Message):

    class AdditionalProperty(messages.Message):
        key = messages.StringField(1)
        value = messages.MessageField(extra_types.JsonValue, 2, repeated=True)

    additional_properties = messages.MessageField('AdditionalProperty', 1,
                                                  repeated=True)


encoding.AddCustomJsonEnumMapping(MessageWithRemappings.SomeEnum,
                                  'enum_value', 'wire_name')
encoding.AddCustomJsonFieldMapping(MessageWithRemappings,
                                   'double_encoding', 'doubleEncoding')
encoding.AddCustomJsonFieldMapping(MessageWithRemappings,
                                   'another_field', 'anotherField')
encoding.AddCustomJsonFieldMapping(MessageWithRemappings,
                                   'repeated_field', 'repeatedField')


class EncodingTest(unittest2.TestCase):

    def testCopyProtoMessage(self):
        msg = SimpleMessage(field='abc')
        new_msg = encoding.CopyProtoMessage(msg)
        self.assertEqual(msg.field, new_msg.field)
        msg.field = 'def'
        self.assertNotEqual(msg.field, new_msg.field)

    def testBytesEncoding(self):
        b64_str = 'AAc+'
        b64_msg = '{"field": "%s"}' % b64_str
        urlsafe_b64_str = 'AAc-'
        urlsafe_b64_msg = '{"field": "%s"}' % urlsafe_b64_str
        data = base64.b64decode(b64_str)
        msg = BytesMessage(field=data)
        self.assertEqual(
            msg, encoding.JsonToMessage(BytesMessage, urlsafe_b64_msg))
        self.assertEqual(msg, encoding.JsonToMessage(BytesMessage, b64_msg))
        self.assertEqual(urlsafe_b64_msg, encoding.MessageToJson(msg))

        enc_rep_msg = '{"repfield": ["%(b)s", "%(b)s"]}' % {
            'b': urlsafe_b64_str}
        rep_msg = BytesMessage(repfield=[data, data])
        self.assertEqual(
            rep_msg, encoding.JsonToMessage(BytesMessage, enc_rep_msg))
        self.assertEqual(enc_rep_msg, encoding.MessageToJson(rep_msg))

    def testIncludeFields(self):
        msg = SimpleMessage()
        self.assertEqual('{}', encoding.MessageToJson(msg))
        self.assertEqual(
            '{"field": null}',
            encoding.MessageToJson(msg, include_fields=['field']))
        self.assertEqual(
            '{"repfield": []}',
            encoding.MessageToJson(msg, include_fields=['repfield']))

    def testNestedIncludeFields(self):
        msg = HasNestedMessage(
            nested=AdditionalPropertiesMessage(
                additional_properties=[]))
        self.assertEqual(
            '{"nested": null}',
            encoding.MessageToJson(msg, include_fields=['nested']))
        self.assertEqual(
            '{"nested": {"additional_properties": []}}',
            encoding.MessageToJson(
                msg, include_fields=['nested.additional_properties']))
        # pylint: disable=redefined-variable-type
        msg = ExtraNestedMessage(nested=msg)
        self.assertEqual(
            '{"nested": {"nested": null}}',
            encoding.MessageToJson(msg, include_fields=['nested.nested']))
        self.assertEqual(
            '{"nested": {"nested_list": []}}',
            encoding.MessageToJson(msg, include_fields=['nested.nested_list']))
        self.assertEqual(
            '{"nested": {"nested": {"additional_properties": []}}}',
            encoding.MessageToJson(
                msg, include_fields=['nested.nested.additional_properties']))

    def testAdditionalPropertyMapping(self):
        msg = AdditionalPropertiesMessage()
        msg.additional_properties = [
            AdditionalPropertiesMessage.AdditionalProperty(
                key='key_one', value='value_one'),
            AdditionalPropertiesMessage.AdditionalProperty(
                key='key_two', value='value_two'),
        ]

        encoded_msg = encoding.MessageToJson(msg)
        self.assertEqual(
            {'key_one': 'value_one', 'key_two': 'value_two'},
            json.loads(encoded_msg))

        new_msg = encoding.JsonToMessage(type(msg), encoded_msg)
        self.assertEqual(
            set(('key_one', 'key_two')),
            set([x.key for x in new_msg.additional_properties]))
        self.assertIsNot(msg, new_msg)

        new_msg.additional_properties.pop()
        self.assertEqual(1, len(new_msg.additional_properties))
        self.assertEqual(2, len(msg.additional_properties))

    def testNumericPropertyName(self):
        json_msg = '{"nested": {"123": "def"}}'
        msg = encoding.JsonToMessage(HasNestedMessage, json_msg)
        self.assertEqual(1, len(msg.nested.additional_properties))

    def testAdditionalMessageProperties(self):
        json_msg = '{"input": {"index": 0, "name": "output"}}'
        result = encoding.JsonToMessage(
            AdditionalMessagePropertiesMessage, json_msg)
        self.assertEqual(1, len(result.additional_properties))
        self.assertEqual(0, result.additional_properties[0].value.index)

    def testUnrecognizedEnum(self):
        json_msg = '{"input": "VALUE_ONE"}'
        result = encoding.JsonToMessage(
            UnrecognizedEnumMessage, json_msg)
        self.assertEqual(1, len(result.additional_properties))
        self.assertEqual(UnrecognizedEnumMessage.ThisEnum.VALUE_ONE,
                         result.additional_properties[0].value)

    def testNestedFieldMapping(self):
        nested_msg = AdditionalPropertiesMessage()
        nested_msg.additional_properties = [
            AdditionalPropertiesMessage.AdditionalProperty(
                key='key_one', value='value_one'),
            AdditionalPropertiesMessage.AdditionalProperty(
                key='key_two', value='value_two'),
        ]
        msg = HasNestedMessage(nested=nested_msg)

        encoded_msg = encoding.MessageToJson(msg)
        self.assertEqual(
            {'nested': {'key_one': 'value_one', 'key_two': 'value_two'}},
            json.loads(encoded_msg))

        new_msg = encoding.JsonToMessage(type(msg), encoded_msg)
        self.assertEqual(
            set(('key_one', 'key_two')),
            set([x.key for x in new_msg.nested.additional_properties]))

        new_msg.nested.additional_properties.pop()
        self.assertEqual(1, len(new_msg.nested.additional_properties))
        self.assertEqual(2, len(msg.nested.additional_properties))

    def testValidEnums(self):
        message_json = '{"field_one": "VALUE_ONE"}'
        message = encoding.JsonToMessage(MessageWithEnum, message_json)
        self.assertEqual(MessageWithEnum.ThisEnum.VALUE_ONE, message.field_one)
        self.assertEqual(MessageWithEnum.ThisEnum.VALUE_TWO, message.field_two)
        self.assertEqual(json.loads(message_json),
                         json.loads(encoding.MessageToJson(message)))

    def testIgnoredEnums(self):
        json_with_typo = '{"field_one": "VALUE_OEN"}'
        message = encoding.JsonToMessage(MessageWithEnum, json_with_typo)
        self.assertEqual(None, message.field_one)
        self.assertEqual(('VALUE_OEN', messages.Variant.ENUM),
                         message.get_unrecognized_field_info('field_one'))
        self.assertEqual(json.loads(json_with_typo),
                         json.loads(encoding.MessageToJson(message)))

        empty_json = ''
        message = encoding.JsonToMessage(MessageWithEnum, empty_json)
        self.assertEqual(None, message.field_one)

    def testIgnoredEnumsWithDefaults(self):
        json_with_typo = '{"field_two": "VALUE_OEN"}'
        message = encoding.JsonToMessage(MessageWithEnum, json_with_typo)
        self.assertEqual(MessageWithEnum.ThisEnum.VALUE_TWO, message.field_two)
        self.assertEqual(json.loads(json_with_typo),
                         json.loads(encoding.MessageToJson(message)))

    def testUnknownNestedRoundtrip(self):
        json_message = '{"field": "abc", "submessage": {"a": 1, "b": "foo"}}'
        message = encoding.JsonToMessage(SimpleMessage, json_message)
        self.assertEqual(json.loads(json_message),
                         json.loads(encoding.MessageToJson(message)))

    def testJsonDatetime(self):
        msg = TimeMessage(timefield=datetime.datetime(
            2014, 7, 2, 23, 33, 25, 541000,
            tzinfo=util.TimeZoneOffset(datetime.timedelta(0))))
        self.assertEqual(
            '{"timefield": "2014-07-02T23:33:25.541000+00:00"}',
            encoding.MessageToJson(msg))

    def testEnumRemapping(self):
        msg = MessageWithRemappings(
            enum_field=MessageWithRemappings.SomeEnum.enum_value)
        json_message = encoding.MessageToJson(msg)
        self.assertEqual('{"enum_field": "wire_name"}', json_message)
        self.assertEqual(
            msg, encoding.JsonToMessage(MessageWithRemappings, json_message))

    def testRepeatedEnumRemapping(self):
        msg = MessageWithRemappings(
            repeated_enum=[
                MessageWithRemappings.SomeEnum.enum_value,
                MessageWithRemappings.SomeEnum.second_value,
            ])
        json_message = encoding.MessageToJson(msg)
        self.assertEqual('{"repeated_enum": ["wire_name", "second_value"]}',
                         json_message)
        self.assertEqual(
            msg, encoding.JsonToMessage(MessageWithRemappings, json_message))

    def testFieldRemapping(self):
        msg = MessageWithRemappings(another_field='abc')
        json_message = encoding.MessageToJson(msg)
        self.assertEqual('{"anotherField": "abc"}', json_message)
        self.assertEqual(
            msg, encoding.JsonToMessage(MessageWithRemappings, json_message))

    def testRepeatedFieldRemapping(self):
        msg = MessageWithRemappings(repeated_field=['abc', 'def'])
        json_message = encoding.MessageToJson(msg)
        self.assertEqual('{"repeatedField": ["abc", "def"]}', json_message)
        self.assertEqual(
            msg, encoding.JsonToMessage(MessageWithRemappings, json_message))

    def testMultipleRemapping(self):
        msg = MessageWithRemappings(
            double_encoding=MessageWithRemappings.SomeEnum.enum_value)
        json_message = encoding.MessageToJson(msg)
        self.assertEqual('{"doubleEncoding": "wire_name"}', json_message)
        self.assertEqual(
            msg, encoding.JsonToMessage(MessageWithRemappings, json_message))

    def testRepeatedRemapping(self):
        # Should allow remapping if the mapping remains the same.
        encoding.AddCustomJsonEnumMapping(MessageWithRemappings.SomeEnum,
                                          'enum_value', 'wire_name')
        encoding.AddCustomJsonFieldMapping(MessageWithRemappings,
                                           'double_encoding', 'doubleEncoding')
        encoding.AddCustomJsonFieldMapping(MessageWithRemappings,
                                           'another_field', 'anotherField')
        encoding.AddCustomJsonFieldMapping(MessageWithRemappings,
                                           'repeated_field', 'repeatedField')

        # Should raise errors if the remapping changes the mapping.
        self.assertRaises(
            exceptions.InvalidDataError,
            encoding.AddCustomJsonFieldMapping,
            MessageWithRemappings, 'double_encoding', 'something_else')
        self.assertRaises(
            exceptions.InvalidDataError,
            encoding.AddCustomJsonFieldMapping,
            MessageWithRemappings, 'enum_field', 'anotherField')
        self.assertRaises(
            exceptions.InvalidDataError,
            encoding.AddCustomJsonEnumMapping,
            MessageWithRemappings.SomeEnum, 'enum_value', 'another_name')
        self.assertRaises(
            exceptions.InvalidDataError,
            encoding.AddCustomJsonEnumMapping,
            MessageWithRemappings.SomeEnum, 'second_value', 'wire_name')

    def testMessageToRepr(self):
        # Using the same string returned by MessageToRepr, with the
        # module names fixed.
        # pylint: disable=bad-whitespace
        msg = SimpleMessage(field='field', repfield=['field', 'field', ],)
        # pylint: enable=bad-whitespace
        self.assertEqual(
            encoding.MessageToRepr(msg),
            r"%s.SimpleMessage(field='field',repfield=['field','field',],)" % (
                __name__,))
        self.assertEqual(
            encoding.MessageToRepr(msg, no_modules=True),
            r"SimpleMessage(field='field',repfield=['field','field',],)")

    def testMessageToReprWithTime(self):
        msg = TimeMessage(timefield=datetime.datetime(
            2014, 7, 2, 23, 33, 25, 541000,
            tzinfo=util.TimeZoneOffset(datetime.timedelta(0))))
        self.assertEqual(
            encoding.MessageToRepr(msg, multiline=True),
            ('%s.TimeMessage(\n    '
             'timefield=datetime.datetime(2014, 7, 2, 23, 33, 25, 541000, '
             'tzinfo=apitools.base.protorpclite.util.TimeZoneOffset('
             'datetime.timedelta(0))),\n)') % __name__)
        self.assertEqual(
            encoding.MessageToRepr(msg, multiline=True, no_modules=True),
            'TimeMessage(\n    '
            'timefield=datetime.datetime(2014, 7, 2, 23, 33, 25, 541000, '
            'tzinfo=TimeZoneOffset(datetime.timedelta(0))),\n)')

    def testPackageMappingsNoPackage(self):
        this_module_name = util.get_package_for_module(__name__)
        full_type_name = 'MessageWithEnum.ThisEnum'
        full_key = '%s.%s' % (this_module_name, full_type_name)
        self.assertEqual(full_key,
                         encoding._GetTypeKey(MessageWithEnum.ThisEnum, ''))

    def testPackageMappingsWithPackage(self):
        this_module_name = util.get_package_for_module(__name__)
        full_type_name = 'MessageWithEnum.ThisEnum'
        full_key = '%s.%s' % (this_module_name, full_type_name)
        this_module = sys.modules[__name__]
        new_package = 'new_package'
        try:
            setattr(this_module, 'package', new_package)
            new_key = '%s.%s' % (new_package, full_type_name)
            self.assertEqual(
                new_key,
                encoding._GetTypeKey(MessageWithEnum.ThisEnum, ''))
            self.assertEqual(
                full_key,
                encoding._GetTypeKey(MessageWithEnum.ThisEnum, new_package))
        finally:
            delattr(this_module, 'package')

    def testRepeatedJsonValuesAsRepeatedProperty(self):
        encoded_msg = '{"a": [{"one": 1}]}'
        msg = encoding.JsonToMessage(RepeatedJsonValueMessage, encoded_msg)
        self.assertEqual(encoded_msg, encoding.MessageToJson(msg))
