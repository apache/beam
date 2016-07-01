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

import logging

from . import descriptor
from . import generate
from . import messages
from . import util


__all__ = ['format_proto_file']


@util.positional(2)
def format_proto_file(file_descriptor, output, indent_space=2):
  out = generate.IndentWriter(output, indent_space=indent_space)

  if file_descriptor.package:
    out << 'package %s;' % file_descriptor.package

  def write_enums(enum_descriptors):
    """Write nested and non-nested Enum types.

    Args:
      enum_descriptors: List of EnumDescriptor objects from which to generate
        enums.
    """
    # Write enums.
    for enum in enum_descriptors or []:
      out << ''
      out << ''
      out << 'enum %s {' % enum.name
      out << ''

      with out.indent():
        if enum.values:
          for enum_value in enum.values:
            out << '%s = %s;' % (enum_value.name, enum_value.number)

      out << '}'

  write_enums(file_descriptor.enum_types)

  def write_fields(field_descriptors):
    """Write fields for Message types.

    Args:
      field_descriptors: List of FieldDescriptor objects from which to generate
        fields.
    """
    for field in field_descriptors or []:
      default_format = ''
      if field.default_value is not None:
        if field.label == descriptor.FieldDescriptor.Label.REPEATED:
          logging.warning('Default value for repeated field %s is not being '
                          'written to proto file' % field.name)
        else:
          # Convert default value to string.
          if field.variant == messages.Variant.MESSAGE:
            logging.warning(
              'Message field %s should not have default values' % field.name)
            default = None
          elif field.variant == messages.Variant.STRING:
            default = repr(field.default_value.encode('utf-8'))
          elif field.variant == messages.Variant.BYTES:
            default = repr(field.default_value)
          else:
            default = str(field.default_value)

          if default is not None:
            default_format = ' [default=%s]' % default

      if field.variant in (messages.Variant.MESSAGE, messages.Variant.ENUM):
        field_type = field.type_name
      else:
        field_type = str(field.variant).lower()
            
      out << '%s %s %s = %s%s;' % (str(field.label).lower(),
                                   field_type,
                                   field.name,
                                   field.number,
                                   default_format)

  def write_messages(message_descriptors):
    """Write nested and non-nested Message types.

    Args:
      message_descriptors: List of MessageDescriptor objects from which to
        generate messages.
    """
    for message in message_descriptors or []:
      out << ''
      out << ''
      out << 'message %s {' % message.name

      with out.indent():
        if message.enum_types:
          write_enums(message.enum_types)

        if message.message_types:
          write_messages(message.message_types)

        if message.fields:
          write_fields(message.fields)

      out << '}'

  write_messages(file_descriptor.message_types)
