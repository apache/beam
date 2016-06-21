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

from __future__ import with_statement

__author__ = 'rafek@google.com (Rafe Kaplan)'

from . import descriptor
from . import generate
from . import message_types
from . import messages
from . import util


__all__ = ['format_python_file']

_MESSAGE_FIELD_MAP = {
    message_types.DateTimeMessage.definition_name(): message_types.DateTimeField,
}


def _write_enums(enum_descriptors, out):
  """Write nested and non-nested Enum types.

  Args:
    enum_descriptors: List of EnumDescriptor objects from which to generate
      enums.
    out: Indent writer used for generating text.
  """
  # Write enums.
  for enum in enum_descriptors or []:
    out << ''
    out << ''
    out << 'class %s(messages.Enum):' % enum.name
    out << ''

    with out.indent():
      if not enum.values:
        out << 'pass'
      else:
        for enum_value in enum.values:
          out << '%s = %s' % (enum_value.name, enum_value.number)


def _write_fields(field_descriptors, out):
  """Write fields for Message types.

  Args:
    field_descriptors: List of FieldDescriptor objects from which to generate
      fields.
    out: Indent writer used for generating text.
  """
  out << ''
  for field in field_descriptors or []:
    type_format = ''
    label_format = ''

    message_field = _MESSAGE_FIELD_MAP.get(field.type_name)
    if message_field:
      module = 'message_types'
      field_type = message_field
    else:
      module = 'messages'
      field_type = messages.Field.lookup_field_type_by_variant(field.variant)

    if field_type in (messages.EnumField, messages.MessageField):
      type_format = '\'%s\', ' % field.type_name

    if field.label == descriptor.FieldDescriptor.Label.REQUIRED:
      label_format = ', required=True'

    elif field.label == descriptor.FieldDescriptor.Label.REPEATED:
      label_format = ', repeated=True'

    if field_type.DEFAULT_VARIANT != field.variant:
      variant_format = ', variant=messages.Variant.%s' % field.variant
    else:
      variant_format = ''

    if field.default_value:
      if field_type in [messages.BytesField,
                        messages.StringField,
                       ]:
        default_value = repr(field.default_value)
      elif field_type is messages.EnumField:
        try:
          default_value = str(int(field.default_value))
        except ValueError:
          default_value = repr(field.default_value)
      else:
        default_value = field.default_value

      default_format = ', default=%s' % (default_value,)
    else:
      default_format = ''

    out << '%s = %s.%s(%s%s%s%s%s)' % (field.name,
                                       module,
                                       field_type.__name__,
                                       type_format,
                                       field.number,
                                       label_format,
                                       variant_format,
                                       default_format)


def _write_messages(message_descriptors, out):
  """Write nested and non-nested Message types.

  Args:
    message_descriptors: List of MessageDescriptor objects from which to
      generate messages.
    out: Indent writer used for generating text.
  """
  for message in message_descriptors or []:
    out << ''
    out << ''
    out << 'class %s(messages.Message):' % message.name

    with out.indent():
      if not (message.enum_types or message.message_types or message.fields):
        out << ''
        out << 'pass'
      else:
        _write_enums(message.enum_types, out)
        _write_messages(message.message_types, out)
        _write_fields(message.fields, out)


def _write_methods(method_descriptors, out):
  """Write methods of Service types.

  All service method implementations raise NotImplementedError.

  Args:
    method_descriptors: List of MethodDescriptor objects from which to
      generate methods.
    out: Indent writer used for generating text.
  """
  for method in method_descriptors:
    out << ''
    out << "@remote.method('%s', '%s')" % (method.request_type,
                                           method.response_type)
    out << 'def %s(self, request):' % (method.name,)
    with out.indent():
      out << ('raise NotImplementedError'
              "('Method %s is not implemented')" % (method.name))


def _write_services(service_descriptors, out):
  """Write Service types.

  Args:
    service_descriptors: List of ServiceDescriptor instances from which to
      generate services.
    out: Indent writer used for generating text.
  """
  for service in service_descriptors or []:
    out << ''
    out << ''
    out << 'class %s(remote.Service):' % service.name

    with out.indent():
      if service.methods:
        _write_methods(service.methods, out)
      else:
        out << ''
        out << 'pass'


@util.positional(2)
def format_python_file(file_descriptor, output, indent_space=2):
  """Format FileDescriptor object as a single Python module.

  Services generated by this function will raise NotImplementedError.

  All Python classes generated by this function use delayed binding for all
  message fields, enum fields and method parameter types.  For example a
  service method might be generated like so:

    class MyService(remote.Service):

      @remote.method('my_package.MyRequestType', 'my_package.MyResponseType')
      def my_method(self, request):
        raise NotImplementedError('Method my_method is not implemented')

  Args:
    file_descriptor: FileDescriptor instance to format as python module.
    output: File-like object to write module source code to.
    indent_space: Number of spaces for each level of Python indentation.
  """
  out = generate.IndentWriter(output, indent_space=indent_space)

  out << 'from protorpc import message_types'
  out << 'from protorpc import messages'
  if file_descriptor.service_types:
    out << 'from protorpc import remote'

  if file_descriptor.package:
    out << "package = '%s'" % file_descriptor.package

  _write_enums(file_descriptor.enum_types, out)
  _write_messages(file_descriptor.message_types, out)
  _write_services(file_descriptor.service_types, out)
