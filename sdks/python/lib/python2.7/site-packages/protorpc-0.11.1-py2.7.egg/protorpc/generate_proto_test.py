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

"""Tests for protorpc.generate_proto_test."""


import os
import shutil
import cStringIO
import sys
import tempfile
import unittest

from protorpc import descriptor
from protorpc import generate_proto
from protorpc import test_util
from protorpc import util


class ModuleInterfaceTest(test_util.ModuleInterfaceTest,
                          test_util.TestCase):

  MODULE = generate_proto


class FormatProtoFileTest(test_util.TestCase):

  def setUp(self):
    self.file_descriptor = descriptor.FileDescriptor()
    self.output = cStringIO.StringIO()

  @property
  def result(self):
    return self.output.getvalue()

  def MakeMessage(self, name='MyMessage', fields=[]):
    message = descriptor.MessageDescriptor()
    message.name = name
    message.fields = fields

    messages_list = getattr(self.file_descriptor, 'fields', [])
    messages_list.append(message)
    self.file_descriptor.message_types = messages_list

  def testBlankPackage(self):
    self.file_descriptor.package = None
    generate_proto.format_proto_file(self.file_descriptor, self.output)
    self.assertEquals('', self.result)

  def testEmptyPackage(self):
    self.file_descriptor.package = 'my_package'
    generate_proto.format_proto_file(self.file_descriptor, self.output)
    self.assertEquals('package my_package;\n', self.result)

  def testSingleField(self):
    field = descriptor.FieldDescriptor()
    field.name = 'integer_field'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.INT64

    self.MakeMessage(fields=[field])

    generate_proto.format_proto_file(self.file_descriptor, self.output)
    self.assertEquals('\n\n'
                      'message MyMessage {\n'
                      '  optional int64 integer_field = 1;\n'
                      '}\n',
                      self.result)

  def testSingleFieldWithDefault(self):
    field = descriptor.FieldDescriptor()
    field.name = 'integer_field'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.INT64
    field.default_value = '10'

    self.MakeMessage(fields=[field])

    generate_proto.format_proto_file(self.file_descriptor, self.output)
    self.assertEquals('\n\n'
                      'message MyMessage {\n'
                      '  optional int64 integer_field = 1 [default=10];\n'
                      '}\n',
                      self.result)

  def testRepeatedFieldWithDefault(self):
    field = descriptor.FieldDescriptor()
    field.name = 'integer_field'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.REPEATED
    field.variant = descriptor.FieldDescriptor.Variant.INT64
    field.default_value = '[10, 20]'

    self.MakeMessage(fields=[field])

    generate_proto.format_proto_file(self.file_descriptor, self.output)
    self.assertEquals('\n\n'
                      'message MyMessage {\n'
                      '  repeated int64 integer_field = 1;\n'
                      '}\n',
                      self.result)

  def testSingleFieldWithDefaultString(self):
    field = descriptor.FieldDescriptor()
    field.name = 'string_field'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.STRING
    field.default_value = 'hello'

    self.MakeMessage(fields=[field])

    generate_proto.format_proto_file(self.file_descriptor, self.output)
    self.assertEquals('\n\n'
                      'message MyMessage {\n'
                      "  optional string string_field = 1 [default='hello'];\n"
                      '}\n',
                      self.result)

  def testSingleFieldWithDefaultEmptyString(self):
    field = descriptor.FieldDescriptor()
    field.name = 'string_field'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.STRING
    field.default_value = ''

    self.MakeMessage(fields=[field])

    generate_proto.format_proto_file(self.file_descriptor, self.output)
    self.assertEquals('\n\n'
                      'message MyMessage {\n'
                      "  optional string string_field = 1 [default=''];\n"
                      '}\n',
                      self.result)

  def testSingleFieldWithDefaultMessage(self):
    field = descriptor.FieldDescriptor()
    field.name = 'message_field'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.MESSAGE
    field.type_name = 'MyNestedMessage'
    field.default_value = 'not valid'

    self.MakeMessage(fields=[field])

    generate_proto.format_proto_file(self.file_descriptor, self.output)
    self.assertEquals('\n\n'
                      'message MyMessage {\n'
                      "  optional MyNestedMessage message_field = 1;\n"
                      '}\n',
                      self.result)

  def testSingleFieldWithDefaultEnum(self):
    field = descriptor.FieldDescriptor()
    field.name = 'enum_field'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.ENUM
    field.type_name = 'my_package.MyEnum'
    field.default_value = '17'

    self.MakeMessage(fields=[field])

    generate_proto.format_proto_file(self.file_descriptor, self.output)
    self.assertEquals('\n\n'
                      'message MyMessage {\n'
                      "  optional my_package.MyEnum enum_field = 1 "
                      "[default=17];\n"
                      '}\n',
                      self.result)


def main():
  unittest.main()


if __name__ == '__main__':
  main()

