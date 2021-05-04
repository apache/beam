# -- coding: utf-8 --
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Unit tests for the transform.environments classes."""

# pytype: skip-file

import logging
import unittest

from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.portability import common_urns
from apache_beam.runners import pipeline_context
from apache_beam.transforms import environments
from apache_beam.transforms.environments import DockerEnvironment
from apache_beam.transforms.environments import EmbeddedPythonEnvironment
from apache_beam.transforms.environments import EmbeddedPythonGrpcEnvironment
from apache_beam.transforms.environments import Environment
from apache_beam.transforms.environments import ExternalEnvironment
from apache_beam.transforms.environments import ProcessEnvironment
from apache_beam.transforms.environments import SubprocessSDKEnvironment


class RunnerApiTest(unittest.TestCase):
  def test_environment_encoding(self):
    for environment in (DockerEnvironment(),
                        DockerEnvironment(container_image='img'),
                        DockerEnvironment(capabilities=['x, y, z']),
                        ProcessEnvironment('run.sh'),
                        ProcessEnvironment('run.sh',
                                           os='linux',
                                           arch='amd64',
                                           env={'k1': 'v1'}),
                        ExternalEnvironment('localhost:8080'),
                        ExternalEnvironment('localhost:8080',
                                            params={'k1': 'v1'}),
                        EmbeddedPythonEnvironment(),
                        EmbeddedPythonGrpcEnvironment(),
                        EmbeddedPythonGrpcEnvironment(
                            state_cache_size=0, data_buffer_time_limit_ms=0),
                        SubprocessSDKEnvironment(command_string=u'foö')):
      context = pipeline_context.PipelineContext()
      proto = environment.to_runner_api(context)
      reconstructed = Environment.from_runner_api(proto, context)
      self.assertEqual(environment, reconstructed)
      self.assertEqual(proto, reconstructed.to_runner_api(context))

  def test_sdk_capabilities(self):
    sdk_capabilities = environments.python_sdk_capabilities()
    self.assertIn(common_urns.coders.LENGTH_PREFIX.urn, sdk_capabilities)
    self.assertIn(
        common_urns.protocols.HARNESS_MONITORING_INFOS.urn, sdk_capabilities)
    self.assertIn(common_urns.protocols.WORKER_STATUS.urn, sdk_capabilities)
    self.assertIn(
        common_urns.sdf_components.TRUNCATE_SIZED_RESTRICTION.urn,
        sdk_capabilities)
    self.assertIn(common_urns.primitives.TO_STRING.urn, sdk_capabilities)

  def test_default_capabilities(self):
    environment = DockerEnvironment.from_options(
        PortableOptions(sdk_location='container'))
    context = pipeline_context.PipelineContext()
    proto = environment.to_runner_api(context)
    self.assertEqual(
        set(proto.capabilities), set(environments.python_sdk_capabilities()))


class EnvironmentOptionsTest(unittest.TestCase):
  def test_process_variables_empty(self):
    options = PortableOptions([
        '--environment_type=PROCESS',
        '--environment_option=process_command=foo',
        '--sdk_location=container'
    ])
    environment = ProcessEnvironment.from_options(options)
    self.assertEqual(environment.command, 'foo')
    self.assertEqual(environment.env, {})

  def test_process_variables_set(self):
    options = PortableOptions([
        '--environment_type=PROCESS',
        '--environment_option=process_command=foo',
        '--environment_option=process_variables='
        'BASH_VARIABLE_ONE=spam,BASH_VARIABLE_TWO=ham',
        '--sdk_location=container'
    ])
    environment = ProcessEnvironment.from_options(options)
    self.assertEqual(environment.command, 'foo')
    self.assertEqual(
        environment.env, {
            'BASH_VARIABLE_ONE': 'spam', 'BASH_VARIABLE_TWO': 'ham'
        })

  def test_process_variables_missing_rvalue(self):
    with self.assertRaises(ValueError):
      options = PortableOptions([
          '--environment_type=PROCESS',
          '--environment_option=process_command=foo',
          '--environment_option=process_variables='
          'BASH_VARIABLE_ONE=spam,MISSING_RVALUE',
          '--sdk_location=container'
      ])
      ProcessEnvironment.from_options(options)

  def test_environments_with_same_hints_are_equal(self):
    options = PortableOptions([
        '--environment_type=PROCESS',
        '--environment_option=process_command=foo',
        '--sdk_location=container',
        '--resource_hint=accelerator=gpu',
    ])
    environment1 = ProcessEnvironment.from_options(options)
    environment2 = ProcessEnvironment.from_options(options)
    self.assertEqual(environment1, environment2)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
