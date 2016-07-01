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

"""Tests for protorpc.message."""

__author__ = 'rafek@google.com (Rafe Kaplan)'


import sys
import unittest

from protorpc import descriptor
from protorpc import message_types
from protorpc import messages
from protorpc import registry
from protorpc import remote
from protorpc import test_util


class ModuleInterfaceTest(test_util.ModuleInterfaceTest,
                          test_util.TestCase):

  MODULE = registry


class MyService1(remote.Service):
  """Test service that refers to messages in another module."""

  @remote.method(test_util.NestedMessage, test_util.NestedMessage)
  def a_method(self, request):
    pass


class MyService2(remote.Service):
  """Test service that does not refer to messages in another module."""


class RegistryServiceTest(test_util.TestCase):

  def setUp(self):
    self.registry = {
        'my-service1': MyService1,
        'my-service2': MyService2,
    }

    self.modules = {
        __name__: sys.modules[__name__],
        test_util.__name__: test_util,
    }

    self.registry_service = registry.RegistryService(self.registry,
                                                     modules=self.modules)

  def CheckServiceMappings(self, mappings):
    module_name = test_util.get_module_name(RegistryServiceTest)
    service1_mapping = registry.ServiceMapping()
    service1_mapping.name = 'my-service1'
    service1_mapping.definition = '%s.MyService1' % module_name

    service2_mapping = registry.ServiceMapping()
    service2_mapping.name = 'my-service2'
    service2_mapping.definition = '%s.MyService2' % module_name

    self.assertIterEqual(mappings, [service1_mapping, service2_mapping])

  def testServices(self):
    response = self.registry_service.services(message_types.VoidMessage())

    self.CheckServiceMappings(response.services)

  def testGetFileSet_All(self):
    request = registry.GetFileSetRequest()
    request.names = ['my-service1', 'my-service2']
    response = self.registry_service.get_file_set(request)

    expected_file_set = descriptor.describe_file_set(list(self.modules.values()))
    self.assertIterEqual(expected_file_set.files, response.file_set.files)

  def testGetFileSet_None(self):
    request = registry.GetFileSetRequest()
    response = self.registry_service.get_file_set(request)

    self.assertEquals(descriptor.FileSet(),
                      response.file_set)

  def testGetFileSet_ReferenceOtherModules(self):
    request = registry.GetFileSetRequest()
    request.names = ['my-service1']
    response = self.registry_service.get_file_set(request)

    # Will suck in and describe the test_util module.
    expected_file_set = descriptor.describe_file_set(list(self.modules.values()))
    self.assertIterEqual(expected_file_set.files, response.file_set.files)

  def testGetFileSet_DoNotReferenceOtherModules(self):
    request = registry.GetFileSetRequest()
    request.names = ['my-service2']
    response = self.registry_service.get_file_set(request)

    # Service does not reference test_util, so will only describe this module.
    expected_file_set = descriptor.describe_file_set([self.modules[__name__]])
    self.assertIterEqual(expected_file_set.files, response.file_set.files)


def main():
  unittest.main()


if __name__ == '__main__':
  main()
