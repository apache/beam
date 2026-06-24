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

"""Unit tests for the maven_repository_url functionality."""

import unittest
from unittest import mock

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.external import MANAGED_TRANSFORM_URN_TO_JAR_TARGET_MAPPING  # pylint: disable=line-too-long
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import JavaJarExpansionService
from apache_beam.transforms.managed import _resolve_expansion_service
from apache_beam.utils.subprocess_server import JavaJarServer


class MavenRepositoryUrlTest(unittest.TestCase):
  """Test cases for maven_repository_url functionality."""
  def test_beam_jar_expansion_service_with_maven_repository_url(self):
    """Test that BeamJarExpansionService accepts and uses
    maven_repository_url."""
    custom_repo_url = "https://custom.maven.repo/"
    custom_user_agent = "test-user-agent/1.0"

    with mock.patch.object(JavaJarServer, 'path_to_beam_jar') as \
        mock_path_to_beam_jar:
      mock_path_to_beam_jar.return_value = "/path/to/beam.jar"

      service = BeamJarExpansionService(
          gradle_target="dummy:target",
          maven_repository_url=custom_repo_url,
          user_agent=custom_user_agent)

      # Verify that path_to_beam_jar was called with the custom repository
      # URL
      mock_path_to_beam_jar.assert_called_once()
      call_args = mock_path_to_beam_jar.call_args
      self.assertEqual(call_args[1]['maven_repository_url'], custom_repo_url)

      # Verify that the user_agent is stored
      self.assertEqual(service._user_agent, custom_user_agent)

  def test_java_jar_expansion_service_with_maven_repository_url(self):
    """Test that JavaJarExpansionService accepts and uses
    maven_repository_url."""
    custom_repo_url = "https://custom.maven.repo/"
    custom_user_agent = "test-user-agent/1.0"

    service = JavaJarExpansionService(
        "dummy.jar",
        maven_repository_url=custom_repo_url,
        user_agent=custom_user_agent)

    # Verify that the maven_repository_url is stored
    self.assertEqual(service._maven_repository_url, custom_repo_url)

    # Verify that the user_agent is stored
    self.assertEqual(service._user_agent, custom_user_agent)

  def test_expand_jars_with_maven_repository_url(self):
    """Test that JavaJarExpansionService passes maven_repository_url to
    _expand_jars."""
    custom_repo_url = "https://custom.maven.repo/"
    custom_user_agent = "test-user-agent/1.0"

    # Test with a Maven artifact format in classpath
    with mock.patch(
        'apache_beam.transforms.external.JavaJarExpansionService._expand_jars'
    ) as mock_expand_jars:
      mock_expand_jars.return_value = ["/path/to/expanded.jar"]

      # Create service with maven_repository_url and user_agent
      service = JavaJarExpansionService(
          "dummy.jar",
          classpath=["group:artifact:1.0"],
          maven_repository_url=custom_repo_url,
          user_agent=custom_user_agent)

      # Call _default_args which should trigger _expand_jars
      service._default_args()

      # Verify that _expand_jars was called with the custom repository URL
      # and user_agent
      # Note: The actual call uses positional arguments, not keyword
      # arguments
      mock_expand_jars.assert_called_with(
          'group:artifact:1.0',
          custom_user_agent,  # user_agent
          custom_repo_url  # maven_repository_url
      )

  @mock.patch.dict(
      MANAGED_TRANSFORM_URN_TO_JAR_TARGET_MAPPING,
      {'test:identifier': 'test:gradle:target'})
  def test_resolve_expansion_service_with_pipeline_options(self):
    """Test that _resolve_expansion_service uses maven_repository_url and
    user_agent from pipeline options."""
    custom_repo_url = "https://custom.maven.repo/"
    custom_user_agent = "test-user-agent/1.0"

    # Create pipeline options with maven_repository_url and user_agent
    options = PipelineOptions()
    setup_options = options.view_as(SetupOptions)
    setup_options.maven_repository_url = custom_repo_url
    setup_options.user_agent = custom_user_agent

    with mock.patch.object(JavaJarServer, 'path_to_beam_jar') as \
        mock_path_to_beam_jar:
      mock_path_to_beam_jar.return_value = "/path/to/beam.jar"

      # Call _resolve_expansion_service with pipeline options
      service = _resolve_expansion_service(
          "test_source", "test:identifier", None, pipeline_options=options)

      # Verify that the returned service has the correct parameters
      self.assertIsInstance(service, BeamJarExpansionService)
      self.assertEqual(service._maven_repository_url, custom_repo_url)
      self.assertEqual(service._user_agent, custom_user_agent)

      # Verify that path_to_beam_jar was called with the custom repository
      # URL
      mock_path_to_beam_jar.assert_called_once()
      call_args = mock_path_to_beam_jar.call_args
      self.assertEqual(call_args[1]['maven_repository_url'], custom_repo_url)

  @mock.patch.dict(
      MANAGED_TRANSFORM_URN_TO_JAR_TARGET_MAPPING,
      {'test:identifier': 'test:gradle:target'})
  def test_resolve_expansion_service_without_maven_repository_url(self):
    """Test that _resolve_expansion_service works without
    maven_repository_url."""
    # Create pipeline options without maven_repository_url
    options = PipelineOptions()

    with mock.patch.object(JavaJarServer, 'path_to_beam_jar') as \
        mock_path_to_beam_jar:
      mock_path_to_beam_jar.return_value = "/path/to/beam.jar"

      # Call _resolve_expansion_service with pipeline options
      _ = _resolve_expansion_service(
          "test_source", "test:identifier", None, pipeline_options=options)

      # Verify that path_to_beam_jar was called without maven_repository_url
      mock_path_to_beam_jar.assert_called_once()
      call_args = mock_path_to_beam_jar.call_args
      self.assertIsNone(call_args[1].get('maven_repository_url'))

  @mock.patch.dict(
      MANAGED_TRANSFORM_URN_TO_JAR_TARGET_MAPPING,
      {'test:identifier': 'test:gradle:target'})
  def test_resolve_expansion_service_without_pipeline_options(self):
    """Test that _resolve_expansion_service works without pipeline
    options."""
    with mock.patch.object(JavaJarServer, 'path_to_beam_jar') as \
        mock_path_to_beam_jar:
      mock_path_to_beam_jar.return_value = "/path/to/beam.jar"

      # Call _resolve_expansion_service without pipeline options
      _ = _resolve_expansion_service(
          "test_source", "test:identifier", None, pipeline_options=None)

      # Verify that path_to_beam_jar was called without maven_repository_url
      mock_path_to_beam_jar.assert_called_once()
      call_args = mock_path_to_beam_jar.call_args
      self.assertIsNone(call_args[1].get('maven_repository_url'))

  def test_user_agent_only_beam_jar_expansion_service(self):
    """Test BeamJarExpansionService with only user_agent parameter."""
    custom_user_agent = "test-user-agent/1.0"

    with mock.patch.object(JavaJarServer, 'path_to_beam_jar') as \
        mock_path_to_beam_jar:
      mock_path_to_beam_jar.return_value = "/path/to/beam.jar"

      service = BeamJarExpansionService(
          "dummy.jar", user_agent=custom_user_agent)

      # Verify that the user_agent is stored
      self.assertEqual(service._user_agent, custom_user_agent)
      # Verify that maven_repository_url is None (default)
      self.assertIsNone(service._maven_repository_url)

  def test_user_agent_only_java_jar_expansion_service(self):
    """Test JavaJarExpansionService with only user_agent parameter."""
    custom_user_agent = "test-user-agent/1.0"

    service = JavaJarExpansionService("dummy.jar", user_agent=custom_user_agent)

    # Verify that the user_agent is stored
    self.assertEqual(service._user_agent, custom_user_agent)
    # Verify that maven_repository_url is None (default)
    self.assertIsNone(service._maven_repository_url)

  def test_default_user_agent_values(self):
    """Test that services have None as default user_agent."""
    with mock.patch.object(JavaJarServer, 'path_to_beam_jar') as \
        mock_path_to_beam_jar:
      mock_path_to_beam_jar.return_value = "/path/to/beam.jar"

      beam_service = BeamJarExpansionService("dummy.jar")
      java_service = JavaJarExpansionService("dummy.jar")

      # Verify that user_agent defaults to None
      self.assertIsNone(beam_service._user_agent)
      self.assertIsNone(java_service._user_agent)


if __name__ == '__main__':
  unittest.main()
