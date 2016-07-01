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

"""Stub library."""
import six

__author__ = 'rafek@google.com (Rafe Kaplan)'

import sys
import types

from . import descriptor
from . import message_types
from . import messages
from . import protobuf
from . import remote
from . import util

__all__ = [
    'define_enum',
    'define_field',
    'define_file',
    'define_message',
    'define_service',
    'import_file',
    'import_file_set',
]


# Map variant back to message field classes.
def _build_variant_map():
  """Map variants to fields.

  Returns:
    Dictionary mapping field variant to its associated field type.
  """
  result = {}
  for name in dir(messages):
    value = getattr(messages, name)
    if isinstance(value, type) and issubclass(value, messages.Field):
      for variant in getattr(value, 'VARIANTS', []):
        result[variant] = value
  return result

_VARIANT_MAP = _build_variant_map()

_MESSAGE_TYPE_MAP = {
  message_types.DateTimeMessage.definition_name(): message_types.DateTimeField,
}


def _get_or_define_module(full_name, modules):
  """Helper method for defining new modules.

  Args:
    full_name: Fully qualified name of module to create or return.
    modules: Dictionary of all modules.  Defaults to sys.modules.

  Returns:
    Named module if found in 'modules', else creates new module and inserts in
    'modules'.  Will also construct parent modules if necessary.
  """
  module = modules.get(full_name)
  if not module:
    module = types.ModuleType(full_name)
    modules[full_name] = module

    split_name = full_name.rsplit('.', 1)
    if len(split_name) > 1:
      parent_module_name, sub_module_name = split_name
      parent_module = _get_or_define_module(parent_module_name, modules)
      setattr(parent_module, sub_module_name, module)

  return module


def define_enum(enum_descriptor, module_name):
  """Define Enum class from descriptor.

  Args:
    enum_descriptor: EnumDescriptor to build Enum class from.
    module_name: Module name to give new descriptor class.

  Returns:
    New messages.Enum sub-class as described by enum_descriptor.
  """
  enum_values = enum_descriptor.values or []

  class_dict = dict((value.name, value.number) for value in enum_values)
  class_dict['__module__'] = module_name
  return type(str(enum_descriptor.name), (messages.Enum,), class_dict)


def define_field(field_descriptor):
  """Define Field instance from descriptor.

  Args:
    field_descriptor: FieldDescriptor class to build field instance from.

  Returns:
    New field instance as described by enum_descriptor.
  """
  field_class = _VARIANT_MAP[field_descriptor.variant]
  params = {'number': field_descriptor.number,
            'variant': field_descriptor.variant,
           }

  if field_descriptor.label == descriptor.FieldDescriptor.Label.REQUIRED:
    params['required'] = True
  elif field_descriptor.label == descriptor.FieldDescriptor.Label.REPEATED:
    params['repeated'] = True

  message_type_field = _MESSAGE_TYPE_MAP.get(field_descriptor.type_name)
  if message_type_field:
    return message_type_field(**params)
  elif field_class in (messages.EnumField, messages.MessageField):
    return field_class(field_descriptor.type_name, **params)
  else:
    if field_descriptor.default_value:
      value = field_descriptor.default_value
      try:
        value = descriptor._DEFAULT_FROM_STRING_MAP[field_class](value)
      except (TypeError, ValueError, KeyError):
        pass  # Let the value pass to the constructor.
      params['default'] = value
    return field_class(**params)


def define_message(message_descriptor, module_name):
  """Define Message class from descriptor.

  Args:
    message_descriptor: MessageDescriptor to describe message class from.
    module_name: Module name to give to new descriptor class.

  Returns:
    New messages.Message sub-class as described by message_descriptor.
  """
  class_dict = {'__module__': module_name}

  for enum in message_descriptor.enum_types or []:
    enum_instance = define_enum(enum, module_name)
    class_dict[enum.name] = enum_instance

  # TODO(rafek): support nested messages when supported by descriptor.

  for field in message_descriptor.fields or []:
    field_instance = define_field(field)
    class_dict[field.name] = field_instance

  class_name = message_descriptor.name.encode('utf-8')
  return type(class_name, (messages.Message,), class_dict)


def define_service(service_descriptor, module):
  """Define a new service proxy.

  Args:
    service_descriptor: ServiceDescriptor class that describes the service.
    module: Module to add service to.  Request and response types are found
      relative to this module.

  Returns:
    Service class proxy capable of communicating with a remote server.
  """
  class_dict = {'__module__': module.__name__}
  class_name = service_descriptor.name.encode('utf-8')

  for method_descriptor in service_descriptor.methods or []:
    request_definition = messages.find_definition(
        method_descriptor.request_type, module)
    response_definition = messages.find_definition(
        method_descriptor.response_type, module)

    method_name = method_descriptor.name.encode('utf-8')
    def remote_method(self, request):
      """Actual service method."""
      raise NotImplementedError('Method is not implemented')
    remote_method.__name__ = method_name
    remote_method_decorator = remote.method(request_definition,
                                            response_definition)

    class_dict[method_name] = remote_method_decorator(remote_method)

  service_class = type(class_name, (remote.Service,), class_dict)
  return service_class


def define_file(file_descriptor, module=None):
  """Define module from FileDescriptor.

  Args:
    file_descriptor: FileDescriptor instance to describe module from.
    module: Module to add contained objects to.  Module name overrides value
      in file_descriptor.package.  Definitions are added to existing
      module if provided.

  Returns:
    If no module provided, will create a new module with its name set to the
    file descriptor's package.  If a module is provided, returns the same
    module.
  """
  if module is None:
    module = types.ModuleType(file_descriptor.package)

  for enum_descriptor in file_descriptor.enum_types or []:
    enum_class = define_enum(enum_descriptor, module.__name__)
    setattr(module, enum_descriptor.name, enum_class)

  for message_descriptor in file_descriptor.message_types or []:
    message_class = define_message(message_descriptor, module.__name__)
    setattr(module, message_descriptor.name, message_class)

  for service_descriptor in file_descriptor.service_types or []:
    service_class = define_service(service_descriptor, module)
    setattr(module, service_descriptor.name, service_class)

  return module


@util.positional(1)
def import_file(file_descriptor, modules=None):
  """Import FileDescriptor in to module space.

  This is like define_file except that a new module and any required parent
  modules are created and added to the modules parameter or sys.modules if not
  provided.

  Args:
    file_descriptor: FileDescriptor instance to describe module from.
    modules: Dictionary of modules to update.  Modules and their parents that
      do not exist will be created.  If an existing module is found that
      matches file_descriptor.package, that module is updated with the
      FileDescriptor contents.

  Returns:
    Module found in modules, else a new module.
  """
  if not file_descriptor.package:
    raise ValueError('File descriptor must have package name')

  if modules is None:
    modules = sys.modules

  module = _get_or_define_module(file_descriptor.package.encode('utf-8'),
                                 modules)

  return define_file(file_descriptor, module)


@util.positional(1)
def import_file_set(file_set, modules=None, _open=open):
  """Import FileSet in to module space.

  Args:
    file_set: If string, open file and read serialized FileSet.  Otherwise,
      a FileSet instance to import definitions from.
    modules: Dictionary of modules to update.  Modules and their parents that
      do not exist will be created.  If an existing module is found that
      matches file_descriptor.package, that module is updated with the
      FileDescriptor contents.
    _open: Used for dependency injection during tests.
  """
  if isinstance(file_set, six.string_types):
    encoded_file = _open(file_set, 'rb')
    try:
      encoded_file_set = encoded_file.read()
    finally:
      encoded_file.close()

    file_set = protobuf.decode_message(descriptor.FileSet, encoded_file_set)

  for file_descriptor in file_set.files:
    # Do not reload built in protorpc classes.
    if not file_descriptor.package.startswith('protorpc.'):
      import_file(file_descriptor, modules=modules)
