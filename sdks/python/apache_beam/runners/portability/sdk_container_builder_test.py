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

"""Unit tests for the sdk container builder module."""

# pytype: skip-file

import gc
import logging
import unittest
import unittest.mock

from apache_beam.options import pipeline_options
from apache_beam.runners.portability import sdk_container_builder


class SdkContainerBuilderTest(unittest.TestCase):
  def tearDown(self):
    # Ensures SdkContainerImageBuilder subclasses are cleared
    gc.collect()

  def test_can_find_local_builder(self):
    local_builder = (
        sdk_container_builder.SdkContainerImageBuilder._get_subclass_by_key(
            'local_docker'))
    self.assertEqual(
        local_builder, sdk_container_builder._SdkContainerImageLocalBuilder)

  def test_can_find_cloud_builder(self):
    local_builder = (
        sdk_container_builder.SdkContainerImageBuilder._get_subclass_by_key(
            'cloud_build'))
    self.assertEqual(
        local_builder, sdk_container_builder._SdkContainerImageCloudBuilder)

  def test_missing_builder_key_throws_value_error(self):
    with self.assertRaises(ValueError):
      sdk_container_builder.SdkContainerImageBuilder._get_subclass_by_key(
          'missing-id')

  def test_multiple_matchings_keys_throws_value_error(self):
    # pylint: disable=unused-variable
    class _PluginSdkBuilder(sdk_container_builder.SdkContainerImageBuilder):
      @classmethod
      def _builder_key(cls):
        return 'test-id'

    class _PluginSdkBuilder2(sdk_container_builder.SdkContainerImageBuilder):
      @classmethod
      def _builder_key(cls):
        return 'test-id'

    # pylint: enable=unused-variable

    with self.assertRaises(ValueError):
      sdk_container_builder.SdkContainerImageBuilder._get_subclass_by_key(
          'test-id')

  def test_can_find_new_subclass(self):
    class _PluginSdkBuilder(sdk_container_builder.SdkContainerImageBuilder):
      pass

    expected_key = f'{_PluginSdkBuilder.__module__}._PluginSdkBuilder'
    local_builder = (
        sdk_container_builder.SdkContainerImageBuilder._get_subclass_by_key(
            expected_key))
    self.assertEqual(local_builder, _PluginSdkBuilder)

  @unittest.mock.patch(
      'apache_beam.runners.portability.sdk_container_builder._SdkContainerImageLocalBuilder'  # pylint: disable=line-too-long
  )
  @unittest.mock.patch.object(
      sdk_container_builder.SdkContainerImageBuilder, "_get_subclass_by_key")
  def test_build_container_image_locates_subclass_invokes_build(
      self, mock_get_subclass, mocked_local_builder):
    mock_get_subclass.return_value = mocked_local_builder
    options = pipeline_options.PipelineOptions()
    sdk_container_builder.SdkContainerImageBuilder.build_container_image(
        options)
    mocked_local_builder.assert_called_once_with(options)
    mocked_local_builder.return_value._build.assert_called_once_with()


if __name__ == '__main__':
  # Run the tests.
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
