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

"""Service regsitry for service discovery.

The registry service can be deployed on a server in order to provide a
central place where remote clients can discover available.

On the server side, each service is registered by their name which is unique
to the registry.  Typically this name provides enough information to identify
the service and locate it within a server.  For example, for an HTTP based
registry the name is the URL path on the host where the service is invocable.

The registry is also able to resolve the full descriptor.FileSet necessary to
describe the service and all required data-types (messages and enums).

A configured registry is itself a remote service and should reference itself.
"""

import sys

from . import descriptor
from . import messages
from . import remote
from . import util


__all__ = [
  'ServiceMapping',
  'ServicesResponse',
  'GetFileSetRequest',
  'GetFileSetResponse',
  'RegistryService',
]


class ServiceMapping(messages.Message):
  """Description of registered service.

  Fields:
    name: Name of service.  On HTTP based services this will be the
      URL path used for invocation.
    definition: Fully qualified name of the service definition.  Useful
      for clients that can look up service definitions based on an existing
      repository of definitions.
  """

  name = messages.StringField(1, required=True)
  definition = messages.StringField(2, required=True)


class ServicesResponse(messages.Message):
  """Response containing all registered services.

  May also contain complete descriptor file-set for all services known by the
  registry.

  Fields:
    services: Service mappings for all registered services in registry.
    file_set: Descriptor file-set describing all services, messages and enum
      types needed for use with all requested services if asked for in the
      request.
  """

  services = messages.MessageField(ServiceMapping, 1, repeated=True)


class GetFileSetRequest(messages.Message):
  """Request for service descriptor file-set.

  Request to retrieve file sets for specific services.

  Fields:
    names: Names of services to retrieve file-set for.
  """

  names = messages.StringField(1, repeated=True)


class GetFileSetResponse(messages.Message):
  """Descriptor file-set for all names in GetFileSetRequest.

  Fields:
    file_set: Descriptor file-set containing all descriptors for services,
      messages and enum types needed for listed names in request.
  """

  file_set = messages.MessageField(descriptor.FileSet, 1, required=True)


class RegistryService(remote.Service):
  """Registry service.

  Maps names to services and is able to describe all descriptor file-sets
  necessary to use contined services.

  On an HTTP based server, the name is the URL path to the service.
  """

  @util.positional(2)
  def __init__(self, registry, modules=None):
    """Constructor.

    Args:
      registry: Map of name to service class.  This map is not copied and may
        be modified after the reigstry service has been configured.
      modules: Module dict to draw descriptors from.  Defaults to sys.modules.
    """
    # Private Attributes:
    #   __registry: Map of name to service class.  Refers to same instance as
    #     registry parameter.
    #   __modules: Mapping of module name to module.
    #   __definition_to_modules: Mapping of definition types to set of modules
    #     that they refer to.  This cache is used to make repeated look-ups
    #     faster and to prevent circular references from causing endless loops.

    self.__registry = registry
    if modules is None:
      modules = sys.modules
    self.__modules = modules
    # This cache will only last for a single request.
    self.__definition_to_modules = {}

  def __find_modules_for_message(self, message_type):
    """Find modules referred to by a message type.

    Determines the entire list of modules ultimately referred to by message_type
    by iterating over all of its message and enum fields.  Includes modules
    referred to fields within its referred messages.

    Args:
      message_type: Message type to find all referring modules for.

    Returns:
      Set of modules referred to by message_type by traversing all its
      message and enum fields.
    """
    # TODO(rafek): Maybe this should be a method on Message and Service?
    def get_dependencies(message_type, seen=None):
      """Get all dependency definitions of a message type.

      This function works by collecting the types of all enumeration and message
      fields defined within the message type.  When encountering a message
      field, it will recursivly find all of the associated message's
      dependencies.  It will terminate on circular dependencies by keeping track
      of what definitions it already via the seen set.

      Args:
        message_type: Message type to get dependencies for.
        seen: Set of definitions that have already been visited.

      Returns:
        All dependency message and enumerated types associated with this message
        including the message itself.
      """
      if seen is None:
        seen = set()
      seen.add(message_type)

      for field in message_type.all_fields():
        if isinstance(field, messages.MessageField):
          if field.message_type not in seen:
            get_dependencies(field.message_type, seen)
        elif isinstance(field, messages.EnumField):
          seen.add(field.type)

      return seen

    found_modules = self.__definition_to_modules.setdefault(message_type, set())
    if not found_modules:
      dependencies = get_dependencies(message_type)
      found_modules.update(self.__modules[definition.__module__]
                           for definition in dependencies)

    return found_modules

  def __describe_file_set(self, names):
    """Get file-set for named services.

    Args:
      names: List of names to get file-set for.

    Returns:
      descriptor.FileSet containing all the descriptors for all modules
      ultimately referred to by all service types request by names parameter.
    """
    service_modules = set()
    if names:
      for service in (self.__registry[name] for name in names):
        found_modules = self.__definition_to_modules.setdefault(service, set())
        if not found_modules:
          found_modules.add(self.__modules[service.__module__])
          for method_name in service.all_remote_methods():
            method = getattr(service, method_name)
            for message_type in (method.remote.request_type,
                                 method.remote.response_type):
              found_modules.update(
                self.__find_modules_for_message(message_type))
        service_modules.update(found_modules)

    return descriptor.describe_file_set(service_modules)

  @property
  def registry(self):
    """Get service registry associated with this service instance."""
    return self.__registry

  @remote.method(response_type=ServicesResponse)
  def services(self, request):
    """Get all registered services."""
    response = ServicesResponse()
    response.services = []
    for name, service_class in self.__registry.items():
      mapping = ServiceMapping()
      mapping.name = name.decode('utf-8')
      mapping.definition = service_class.definition_name().decode('utf-8')
      response.services.append(mapping)

    return response

  @remote.method(GetFileSetRequest, GetFileSetResponse)
  def get_file_set(self, request):
    """Get file-set for registered servies."""
    response = GetFileSetResponse()
    response.file_set = self.__describe_file_set(request.names)
    return response
