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

"""Tests for protorpc.generate_python_test."""

__author__ = 'rafek@google.com (Rafe Kaplan)'


import os
import shutil
import sys
import tempfile
import unittest

from protorpc import descriptor
from protorpc import generate_python
from protorpc import test_util
from protorpc import util


class ModuleInterfaceTest(test_util.ModuleInterfaceTest,
                          test_util.TestCase):

  MODULE = generate_python


class FormatPythonFileTest(test_util.TestCase):

  def setUp(self):
    self.original_path = list(sys.path)
    self.original_modules = dict(sys.modules)
    sys.path = list(sys.path)
    self.file_descriptor = descriptor.FileDescriptor()

    # Create temporary directory and add to Python path so that generated
    # Python code can be easily parsed, imported and executed.
    self.temp_dir = tempfile.mkdtemp()
    sys.path.append(self.temp_dir)

  def tearDown(self):
    # Reset path.
    sys.path[:] = []
    sys.path.extend(self.original_path)

    # Reset modules.
    sys.modules.clear()
    sys.modules.update(self.original_modules)

    # Remove temporary directory.
    try:
      shutil.rmtree(self.temp_dir)
    except IOError:
      pass

  def DoPythonTest(self, file_descriptor):
    """Execute python test based on a FileDescriptor object.

    The full test of the Python code generation is to generate a Python source
    code file, import the module and regenerate the FileDescriptor from it.
    If the generated FileDescriptor is the same as the original, it means that
    the generated source code correctly implements the actual FileDescriptor.
    """
    file_name = os.path.join(self.temp_dir,
                             '%s.py' % (file_descriptor.package or 'blank',))
    source_file = open(file_name, 'wt')
    try:
      generate_python.format_python_file(file_descriptor, source_file)
    finally:
      source_file.close()

    module_to_import = file_descriptor.package or 'blank'
    module = __import__(module_to_import)

    if not file_descriptor.package:
      self.assertFalse(hasattr(module, 'package'))
      module.package = ''  # Create package name so that comparison will work.

    reloaded_descriptor = descriptor.describe_file(module)

    # Need to sort both message_types fields because document order is never
    # Ensured.
    # TODO(rafek): Ensure document order.
    if reloaded_descriptor.message_types:
      reloaded_descriptor.message_types = sorted(
        reloaded_descriptor.message_types, key=lambda v: v.name)

    if file_descriptor.message_types:
      file_descriptor.message_types = sorted(
        file_descriptor.message_types, key=lambda v: v.name)

    self.assertEquals(file_descriptor, reloaded_descriptor)

  @util.positional(2)
  def DoMessageTest(self,
                    field_descriptors,
                    message_types=None,
                    enum_types=None):
    """Execute message generation test based on FieldDescriptor objects.

    Args:
      field_descriptor: List of FieldDescriptor object to generate and test.
      message_types: List of other MessageDescriptor objects that the new
        Message class depends on.
      enum_types: List of EnumDescriptor objects that the new Message class
        depends on.
    """
    file_descriptor = descriptor.FileDescriptor()
    file_descriptor.package = 'my_package'

    message_descriptor = descriptor.MessageDescriptor()
    message_descriptor.name = 'MyMessage'

    message_descriptor.fields = list(field_descriptors)

    file_descriptor.message_types = message_types or []
    file_descriptor.message_types.append(message_descriptor)

    if enum_types:
      file_descriptor.enum_types = list(enum_types)

    self.DoPythonTest(file_descriptor)

  def testBlankPackage(self):
    self.DoPythonTest(descriptor.FileDescriptor())

  def testEmptyPackage(self):
    file_descriptor = descriptor.FileDescriptor()
    file_descriptor.package = 'mypackage'
    self.DoPythonTest(file_descriptor)

  def testSingleField(self):
    field = descriptor.FieldDescriptor()
    field.name = 'integer_field'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.INT64

    self.DoMessageTest([field])

  def testMessageField_InternalReference(self):
    other_message = descriptor.MessageDescriptor()
    other_message.name = 'OtherMessage'

    field = descriptor.FieldDescriptor()
    field.name = 'message_field'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.MESSAGE
    field.type_name = 'my_package.OtherMessage'

    self.DoMessageTest([field], message_types=[other_message])

  def testMessageField_ExternalReference(self):
    field = descriptor.FieldDescriptor()
    field.name = 'message_field'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.MESSAGE
    field.type_name = 'protorpc.registry.GetFileSetResponse'

    self.DoMessageTest([field])

  def testEnumField_InternalReference(self):
    enum = descriptor.EnumDescriptor()
    enum.name = 'Color'

    field = descriptor.FieldDescriptor()
    field.name = 'color'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.ENUM
    field.type_name = 'my_package.Color'

    self.DoMessageTest([field], enum_types=[enum])

  def testEnumField_ExternalReference(self):
    field = descriptor.FieldDescriptor()
    field.name = 'color'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.ENUM
    field.type_name = 'protorpc.descriptor.FieldDescriptor.Label'

    self.DoMessageTest([field])

  def testDateTimeField(self):
    field = descriptor.FieldDescriptor()
    field.name = 'timestamp'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.MESSAGE
    field.type_name = 'protorpc.message_types.DateTimeMessage'

    self.DoMessageTest([field])

  def testNonDefaultVariant(self):
    field = descriptor.FieldDescriptor()
    field.name = 'integer_field'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.UINT64

    self.DoMessageTest([field])

  def testRequiredField(self):
    field = descriptor.FieldDescriptor()
    field.name = 'integer_field'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.REQUIRED
    field.variant = descriptor.FieldDescriptor.Variant.INT64

    self.DoMessageTest([field])

  def testRepeatedField(self):
    field = descriptor.FieldDescriptor()
    field.name = 'integer_field'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.REPEATED
    field.variant = descriptor.FieldDescriptor.Variant.INT64

    self.DoMessageTest([field])

  def testIntegerDefaultValue(self):
    field = descriptor.FieldDescriptor()
    field.name = 'integer_field'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.INT64
    field.default_value = '10'

    self.DoMessageTest([field])

  def testFloatDefaultValue(self):
    field = descriptor.FieldDescriptor()
    field.name = 'float_field'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.DOUBLE
    field.default_value = '10.1'

    self.DoMessageTest([field])

  def testStringDefaultValue(self):
    field = descriptor.FieldDescriptor()
    field.name = 'string_field'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.STRING
    field.default_value = u'a nice lovely string\'s "string"'

    self.DoMessageTest([field])

  def testEnumDefaultValue(self):
    field = descriptor.FieldDescriptor()
    field.name = 'label'
    field.number = 1
    field.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field.variant = descriptor.FieldDescriptor.Variant.ENUM
    field.type_name = 'protorpc.descriptor.FieldDescriptor.Label'
    field.default_value = '2'

    self.DoMessageTest([field])

  def testMultiFields(self):
    field1 = descriptor.FieldDescriptor()
    field1.name = 'integer_field'
    field1.number = 1
    field1.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field1.variant = descriptor.FieldDescriptor.Variant.INT64

    field2 = descriptor.FieldDescriptor()
    field2.name = 'string_field'
    field2.number = 2
    field2.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field2.variant = descriptor.FieldDescriptor.Variant.STRING

    field3 = descriptor.FieldDescriptor()
    field3.name = 'unsigned_integer_field'
    field3.number = 3
    field3.label = descriptor.FieldDescriptor.Label.OPTIONAL
    field3.variant = descriptor.FieldDescriptor.Variant.UINT64

    self.DoMessageTest([field1, field2, field3])

  def testNestedMessage(self):
    message = descriptor.MessageDescriptor()
    message.name = 'OuterMessage'

    inner_message = descriptor.MessageDescriptor()
    inner_message.name = 'InnerMessage'

    inner_inner_message = descriptor.MessageDescriptor()
    inner_inner_message.name = 'InnerInnerMessage'

    inner_message.message_types = [inner_inner_message]

    message.message_types = [inner_message]

    file_descriptor = descriptor.FileDescriptor()
    file_descriptor.message_types = [message]

    self.DoPythonTest(file_descriptor)

  def testNestedEnum(self):
    message = descriptor.MessageDescriptor()
    message.name = 'OuterMessage'

    inner_enum = descriptor.EnumDescriptor()
    inner_enum.name = 'InnerEnum'

    message.enum_types = [inner_enum]

    file_descriptor = descriptor.FileDescriptor()
    file_descriptor.message_types = [message]

    self.DoPythonTest(file_descriptor)

  def testService(self):
    service = descriptor.ServiceDescriptor()
    service.name = 'TheService'

    method1 = descriptor.MethodDescriptor()
    method1.name = 'method1'
    method1.request_type = 'protorpc.descriptor.FileDescriptor'
    method1.response_type = 'protorpc.descriptor.MethodDescriptor'

    service.methods = [method1]

    file_descriptor = descriptor.FileDescriptor()
    file_descriptor.service_types = [service]

    self.DoPythonTest(file_descriptor)

    # Test to make sure that implementation methods raise an exception.
    import blank
    service_instance = blank.TheService()
    self.assertRaisesWithRegexpMatch(NotImplementedError,
                                     'Method method1 is not implemented',
                                     service_instance.method1,
                                     descriptor.FileDescriptor())
  

def main():
  unittest.main()


if __name__ == '__main__':
  main()
