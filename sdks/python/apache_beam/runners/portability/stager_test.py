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

"""Unit tests for the stager module."""

# pytype: skip-file

import io
import logging
import os
import shutil
import sys
import tempfile
import unittest
from typing import List

import mock
import pytest

from apache_beam.internal import pickler
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.internal import names
from apache_beam.runners.portability import stager

_LOGGER = logging.getLogger(__name__)


class StagerTest(unittest.TestCase):
  def setUp(self):
    self._temp_dir = None
    self.stager = TestStager()

  def tearDown(self):
    if self._temp_dir:
      shutil.rmtree(self._temp_dir)
    self.stager = None
    # [https://github.com/apache/beam/issues/21457] set pickler to dill by
    # default.
    pickler.set_library(pickler.DEFAULT_PICKLE_LIB)

  def make_temp_dir(self):
    if self._temp_dir is None:
      self._temp_dir = tempfile.mkdtemp()
    return tempfile.mkdtemp(dir=self._temp_dir)

  def update_options(self, options):
    setup_options = options.view_as(SetupOptions)
    setup_options.sdk_location = ''

  def create_temp_file(self, path, contents):
    with open(path, 'w') as f:
      f.write(contents)
      return f.name

  # We can not rely on actual remote file systems paths hence making
  # '/tmp/remote/' a new remote path.
  def is_remote_path(self, path):
    return path.startswith('/tmp/remote/')

  remote_copied_files = []  # type: List[str]

  def file_copy(self, from_path, to_path):
    if self.is_remote_path(from_path):
      self.remote_copied_files.append(from_path)
      _, from_name = os.path.split(from_path)
      if os.path.isdir(to_path):
        to_path = os.path.join(to_path, from_name)
      self.create_temp_file(to_path, 'nothing')
      _LOGGER.info('Fake copied remote file: %s to %s', from_path, to_path)
    elif self.is_remote_path(to_path):
      _LOGGER.info('Faking upload_file(%s, %s)', from_path, to_path)
    else:
      shutil.copyfile(from_path, to_path)

  def populate_requirements_cache(
      self, requirements_file, cache_dir, populate_cache_with_sdists=False):
    _ = requirements_file
    self.create_temp_file(os.path.join(cache_dir, 'abc.txt'), 'nothing')
    self.create_temp_file(os.path.join(cache_dir, 'def.txt'), 'nothing')

  def build_fake_pip_download_command_handler(self, has_wheels):
    """A stub for apache_beam.utils.processes.check_output that imitates pip.

      Args:
        has_wheels: Whether pip fake should have a whl distribution of packages.
      """
    def pip_fake(args):
      """Fakes fetching a package from pip by creating a temporary file.

          Args:
            args: a complete list of command line arguments to invoke pip.
              The fake is sensitive to the order of the arguments.
              Supported commands:

              1) Download SDK sources file:
              python pip -m download --dest /tmp/dir apache-beam==2.0.0 \
                  --no-deps --no-binary :all:

              2) Download SDK binary wheel file:
              python pip -m download --dest /tmp/dir apache-beam==2.0.0 \
                  --no-deps --no-binary :all: --python-version 27 \
                  --implementation cp --abi cp27mu --platform manylinux1_x86_64
          """
      package_file = None
      if len(args) >= 8:
        # package_name==x.y.z
        if '==' in args[6]:
          distribution_name = args[6][0:args[6].find('==')]
          distribution_version = args[6][args[6].find('==') + 2:]

          if args[8] == '--no-binary':
            package_file = '%s-%s.zip' % (
                distribution_name, distribution_version)
          elif args[8] == '--only-binary' and len(args) >= 18:
            if not has_wheels:
              # Imitate the case when desired wheel distribution is not in PyPI.
              raise RuntimeError('No matching distribution.')

            # Per PEP-0427 in wheel filenames non-alphanumeric characters
            # in distribution name are replaced with underscore.
            distribution_name = distribution_name.replace('-', '_')
            package_file = '%s-%s-%s%s-%s-%s.whl' % (
                distribution_name,
                distribution_version,
                args[13],  # implementation
                args[11],  # python version
                args[15],  # abi tag
                args[17]  # platform
            )

      assert package_file, 'Pip fake does not support the command: ' + str(args)
      self.create_temp_file(
          FileSystems.join(args[5], package_file), 'Package content.')

    return pip_fake

  @mock.patch('apache_beam.runners.portability.stager.open')
  @mock.patch('apache_beam.runners.portability.stager.get_new_http')
  def test_download_file_https(self, mock_new_http, mock_open):
    from_url = 'https://from_url'
    to_path = '/tmp/http_file/'
    mock_new_http.return_value.request.return_value = ({
        'status': 200
    },
                                                       'file_content')
    self.stager._download_file(from_url, to_path)
    assert mock_open.mock_calls == [
        mock.call('/tmp/http_file/', 'wb'),
        mock.call().__enter__(),
        mock.call().__enter__().write('file_content'),
        mock.call().__exit__(None, None, None)
    ]

  @mock.patch('apache_beam.runners.portability.stager.open')
  @mock.patch('apache_beam.runners.portability.stager.get_new_http')
  @mock.patch.object(FileSystems, 'open')
  def test_download_file_non_http(self, mock_fs_open, mock_new_http, mock_open):
    from_url = 'gs://bucket/from_url'
    to_path = '/tmp/file/'
    mock_fs_open.return_value = io.BytesIO(b"file_content")
    self.stager._download_file(from_url, to_path)
    assert not mock_new_http.called
    mock_fs_open.assert_called_with(
        from_url, compression_type=CompressionTypes.UNCOMPRESSED)
    assert mock_open.mock_calls == [
        mock.call('/tmp/file/', 'wb'),
        mock.call().__enter__(),
        mock.call().__enter__().write(b'file_content'),
        mock.call().__exit__(None, None, None)
    ]

  @mock.patch('apache_beam.runners.portability.stager.os.mkdir')
  @mock.patch('apache_beam.runners.portability.stager.shutil.copyfile')
  @mock.patch('apache_beam.runners.portability.stager.get_new_http')
  def test_download_file_unrecognized(
      self, mock_new_http, mock_copyfile, mock_mkdir):
    from_url = '/tmp/from_file'
    to_path = '/tmp/to_file/'
    with mock.patch('apache_beam.runners.portability.stager.os.path.isdir',
                    return_value=True):
      self.stager._download_file(from_url, to_path)
      assert not mock_new_http.called
      mock_copyfile.assert_called_with(from_url, to_path)

    with mock.patch('apache_beam.runners.portability.stager.os.path.isdir',
                    return_value=False):
      self.stager._download_file(from_url, to_path)
      assert mock_mkdir.called

  def test_no_staging_location(self):
    with self.assertRaises(RuntimeError) as cm:
      self.stager.stage_job_resources([], staging_location=None)
    self.assertEqual(
        'The staging_location must be specified.', cm.exception.args[0])

  def test_no_main_session(self):
    staging_dir = self.make_temp_dir()
    options = PipelineOptions()

    options.view_as(SetupOptions).save_main_session = False
    self.update_options(options)

    self.assertEqual([],
                     self.stager.create_and_stage_job_resources(
                         options, staging_location=staging_dir)[1])

  # xdist adds unpicklable modules to the main session.
  @pytest.mark.no_xdist
  @unittest.skipIf(
      sys.platform == "win32" and sys.version_info < (3, 8),
      'https://github.com/apache/beam/issues/20659: pytest on Windows pulls '
      'in a zipimporter, unpicklable before py3.8')
  def test_with_main_session(self):
    staging_dir = self.make_temp_dir()
    options = PipelineOptions()

    options.view_as(SetupOptions).save_main_session = True
    options.view_as(SetupOptions).pickle_library = pickler.USE_DILL
    self.update_options(options)

    self.assertEqual([names.PICKLED_MAIN_SESSION_FILE],
                     self.stager.create_and_stage_job_resources(
                         options, staging_location=staging_dir)[1])
    self.assertTrue(
        os.path.isfile(
            os.path.join(staging_dir, names.PICKLED_MAIN_SESSION_FILE)))

  # (https://github.com/apache/beam/issues/21457): Remove the decorator once
  # cloudpickle is default pickle library
  @pytest.mark.no_xdist
  def test_main_session_not_staged_when_using_cloudpickle(self):
    staging_dir = self.make_temp_dir()
    options = PipelineOptions()

    options.view_as(SetupOptions).save_main_session = True
    # even if the save main session is on, no pickle file for main
    # session is saved when pickle_library==cloudpickle.
    options.view_as(SetupOptions).pickle_library = pickler.USE_CLOUDPICKLE
    self.update_options(options)
    self.assertEqual([],
                     self.stager.create_and_stage_job_resources(
                         options, staging_location=staging_dir)[1])

  def test_default_resources(self):
    staging_dir = self.make_temp_dir()
    options = PipelineOptions()
    self.update_options(options)

    self.assertEqual([],
                     self.stager.create_and_stage_job_resources(
                         options, staging_location=staging_dir)[1])

  def test_with_requirements_file(self):
    staging_dir = self.make_temp_dir()
    requirements_cache_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).requirements_cache = requirements_cache_dir
    options.view_as(SetupOptions).requirements_file = os.path.join(
        source_dir, stager.REQUIREMENTS_FILE)
    self.create_temp_file(
        os.path.join(source_dir, stager.REQUIREMENTS_FILE), 'nothing')
    self.assertEqual(
        sorted([stager.REQUIREMENTS_FILE, 'abc.txt', 'def.txt']),
        sorted(
            self.stager.create_and_stage_job_resources(
                options,
                populate_requirements_cache=self.populate_requirements_cache,
                staging_location=staging_dir)[1]))
    self.assertTrue(
        os.path.isfile(os.path.join(staging_dir, stager.REQUIREMENTS_FILE)))

  def test_with_pypi_requirements(self):
    staging_dir = self.make_temp_dir()
    requirements_cache_dir = self.make_temp_dir()

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).requirements_cache = requirements_cache_dir
    resources = self.stager.create_and_stage_job_resources(
        options,
        pypi_requirements=['nothing>=1.0,<2.0'],
        populate_requirements_cache=self.populate_requirements_cache,
        staging_location=staging_dir)[1]
    self.assertEqual(3, len(resources))
    self.assertTrue({'abc.txt', 'def.txt'} <= set(resources))
    generated_requirements = (set(resources) - {'abc.txt', 'def.txt'}).pop()
    with open(os.path.join(staging_dir, generated_requirements)) as f:
      data = f.read()
    self.assertEqual('nothing>=1.0,<2.0', data)
    self.assertTrue(os.path.isfile(os.path.join(staging_dir, 'abc.txt')))
    self.assertTrue(os.path.isfile(os.path.join(staging_dir, 'def.txt')))

  def test_requirements_file_not_present(self):
    staging_dir = self.make_temp_dir()
    with self.assertRaises(RuntimeError) as cm:
      options = PipelineOptions()
      self.update_options(options)
      options.view_as(SetupOptions).requirements_file = 'nosuchfile'
      self.stager.create_and_stage_job_resources(
          options,
          populate_requirements_cache=self.populate_requirements_cache,
          staging_location=staging_dir)
    self.assertEqual(
        cm.exception.args[0],
        'The file %s cannot be found. It was specified in the '
        '--requirements_file command line option.' % 'nosuchfile')

  def test_with_requirements_file_and_cache(self):
    staging_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).requirements_file = os.path.join(
        source_dir, stager.REQUIREMENTS_FILE)
    options.view_as(SetupOptions).requirements_cache = self.make_temp_dir()
    self.create_temp_file(
        os.path.join(source_dir, stager.REQUIREMENTS_FILE), 'nothing')
    self.assertEqual(
        sorted([stager.REQUIREMENTS_FILE, 'abc.txt', 'def.txt']),
        sorted(
            self.stager.create_and_stage_job_resources(
                options,
                populate_requirements_cache=self.populate_requirements_cache,
                staging_location=staging_dir)[1]))
    self.assertTrue(
        os.path.isfile(os.path.join(staging_dir, stager.REQUIREMENTS_FILE)))
    self.assertTrue(os.path.isfile(os.path.join(staging_dir, 'abc.txt')))
    self.assertTrue(os.path.isfile(os.path.join(staging_dir, 'def.txt')))

  def test_requirements_cache_not_populated_when_cache_disabled(self):
    staging_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).requirements_file = os.path.join(
        source_dir, stager.REQUIREMENTS_FILE)
    options.view_as(
        SetupOptions).requirements_cache = stager.SKIP_REQUIREMENTS_CACHE
    self.create_temp_file(
        os.path.join(source_dir, stager.REQUIREMENTS_FILE), 'nothing')
    with mock.patch(
        'apache_beam.runners.portability.stager_test.StagerTest.'
        'populate_requirements_cache') as (populate_requirements_cache):
      resources = self.stager.create_and_stage_job_resources(
          options,
          populate_requirements_cache=self.populate_requirements_cache,
          staging_location=staging_dir)[1]
      assert not populate_requirements_cache.called
      self.assertEqual([stager.REQUIREMENTS_FILE], resources)
      self.assertTrue(not os.path.isfile(os.path.join(staging_dir, 'abc.txt')))
      self.assertTrue(not os.path.isfile(os.path.join(staging_dir, 'def.txt')))

  def test_with_pypi_requirements_skipping_cache(self):
    staging_dir = self.make_temp_dir()

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(
        SetupOptions).requirements_cache = stager.SKIP_REQUIREMENTS_CACHE

    resources = self.stager.create_and_stage_job_resources(
        options,
        pypi_requirements=['nothing>=1.0,<2.0'],
        populate_requirements_cache=self.populate_requirements_cache,
        staging_location=staging_dir)[1]
    with open(os.path.join(staging_dir, resources[0])) as f:
      data = f.read()
    self.assertEqual('nothing>=1.0,<2.0', data)
    self.assertTrue(not os.path.isfile(os.path.join(staging_dir, 'abc.txt')))
    self.assertTrue(not os.path.isfile(os.path.join(staging_dir, 'def.txt')))

  def test_setup_file_not_present(self):
    staging_dir = self.make_temp_dir()

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).setup_file = 'nosuchfile'

    with self.assertRaises(RuntimeError) as cm:
      self.stager.create_and_stage_job_resources(
          options, staging_location=staging_dir)
    self.assertEqual(
        cm.exception.args[0],
        'The file %s cannot be found. It was specified in the '
        '--setup_file command line option.' % 'nosuchfile')

  def test_setup_file_not_named_setup_dot_py(self):
    staging_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).setup_file = (
        os.path.join(source_dir, 'xyz-setup.py'))

    self.create_temp_file(os.path.join(source_dir, 'xyz-setup.py'), 'notused')
    with self.assertRaises(RuntimeError) as cm:
      self.stager.create_and_stage_job_resources(
          options, staging_location=staging_dir)
    self.assertTrue(
        cm.exception.args[0].startswith(
            'The --setup_file option expects the full path to a file named '
            'setup.py instead of '))

  def test_sdk_location_default(self):
    staging_dir = self.make_temp_dir()
    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).sdk_location = 'default'

    with mock.patch(
        'apache_beam.utils.processes.check_output',
        self.build_fake_pip_download_command_handler(has_wheels=False)):
      _, staged_resources = self.stager.create_and_stage_job_resources(
          options, temp_dir=self.make_temp_dir(), staging_location=staging_dir)

    self.assertEqual([names.STAGED_SDK_SOURCES_FILENAME], staged_resources)

    with open(os.path.join(staging_dir,
                           names.STAGED_SDK_SOURCES_FILENAME)) as f:
      self.assertEqual(f.read(), 'Package content.')

  def test_sdk_location_default_with_wheels(self):
    staging_dir = self.make_temp_dir()

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).sdk_location = 'default'

    with mock.patch(
        'apache_beam.utils.processes.check_output',
        self.build_fake_pip_download_command_handler(has_wheels=True)):
      _, staged_resources = self.stager.create_and_stage_job_resources(
          options, temp_dir=self.make_temp_dir(), staging_location=staging_dir)

      self.assertEqual(len(staged_resources), 2)
      self.assertEqual(staged_resources[0], names.STAGED_SDK_SOURCES_FILENAME)
      # Exact name depends on the version of the SDK.
      self.assertTrue(staged_resources[1].endswith('whl'))
      for name in staged_resources:
        with open(os.path.join(staging_dir, name)) as f:
          self.assertEqual(f.read(), 'Package content.')

  def test_sdk_location_local_directory(self):
    staging_dir = self.make_temp_dir()
    sdk_location = self.make_temp_dir()
    self.create_temp_file(
        os.path.join(sdk_location, names.STAGED_SDK_SOURCES_FILENAME),
        'Package content.')

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).sdk_location = sdk_location

    self.assertEqual([names.STAGED_SDK_SOURCES_FILENAME],
                     self.stager.create_and_stage_job_resources(
                         options, staging_location=staging_dir)[1])
    tarball_path = os.path.join(staging_dir, names.STAGED_SDK_SOURCES_FILENAME)
    with open(tarball_path) as f:
      self.assertEqual(f.read(), 'Package content.')

  def test_sdk_location_local_source_file(self):
    staging_dir = self.make_temp_dir()
    sdk_directory = self.make_temp_dir()
    sdk_filename = 'apache-beam-3.0.0.tar.gz'
    sdk_location = os.path.join(sdk_directory, sdk_filename)
    self.create_temp_file(sdk_location, 'Package content.')

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).sdk_location = sdk_location

    self.assertEqual([names.STAGED_SDK_SOURCES_FILENAME],
                     self.stager.create_and_stage_job_resources(
                         options, staging_location=staging_dir)[1])
    tarball_path = os.path.join(staging_dir, names.STAGED_SDK_SOURCES_FILENAME)
    with open(tarball_path) as f:
      self.assertEqual(f.read(), 'Package content.')

  def test_sdk_location_local_wheel_file(self):
    staging_dir = self.make_temp_dir()
    sdk_directory = self.make_temp_dir()
    sdk_filename = 'apache_beam-1.0.0-cp27-cp27mu-manylinux1_x86_64.whl'
    sdk_location = os.path.join(sdk_directory, sdk_filename)
    self.create_temp_file(sdk_location, 'Package content.')

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).sdk_location = sdk_location

    self.assertEqual([sdk_filename],
                     self.stager.create_and_stage_job_resources(
                         options, staging_location=staging_dir)[1])
    tarball_path = os.path.join(staging_dir, sdk_filename)
    with open(tarball_path) as f:
      self.assertEqual(f.read(), 'Package content.')

  def test_sdk_location_local_directory_not_present(self):
    staging_dir = self.make_temp_dir()
    sdk_location = 'nosuchdir'
    with self.assertRaises(RuntimeError) as cm:
      options = PipelineOptions()
      self.update_options(options)
      options.view_as(SetupOptions).sdk_location = sdk_location

      self.stager.create_and_stage_job_resources(
          options, staging_location=staging_dir)
    self.assertEqual(
        'The file "%s" cannot be found. Its '
        'location was specified by the --sdk_location command-line option.' %
        sdk_location,
        cm.exception.args[0])

  @mock.patch(
      'apache_beam.runners.portability.stager_test.TestStager.stage_artifact')
  @mock.patch(
      'apache_beam.runners.portability.stager_test.stager.Stager._download_file'
  )
  def test_sdk_location_remote_source_file(self, *unused_mocks):
    staging_dir = self.make_temp_dir()
    sdk_location = 'gs://my-gcs-bucket/tarball.tar.gz'

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).sdk_location = sdk_location

    self.assertEqual([names.STAGED_SDK_SOURCES_FILENAME],
                     self.stager.create_and_stage_job_resources(
                         options, staging_location=staging_dir)[1])

  def test_sdk_location_remote_wheel_file(self, *unused_mocks):
    staging_dir = self.make_temp_dir()
    sdk_filename = 'apache_beam-1.0.0-cp27-cp27mu-manylinux1_x86_64.whl'
    sdk_location = 'https://storage.googleapis.com/my-gcs-bucket/' + \
                   sdk_filename

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).sdk_location = sdk_location

    def file_download(_, to_path):
      with open(to_path, 'w') as f:
        f.write('Package content.')
      return to_path

    with mock.patch('apache_beam.runners.portability.stager_test'
                    '.stager.Stager._download_file',
                    staticmethod(file_download)):
      self.assertEqual([sdk_filename],
                       self.stager.create_and_stage_job_resources(
                           options, staging_location=staging_dir)[1])

    wheel_file_path = os.path.join(staging_dir, sdk_filename)
    with open(wheel_file_path) as f:
      self.assertEqual(f.read(), 'Package content.')

  def test_sdk_location_http(self):
    staging_dir = self.make_temp_dir()
    sdk_location = 'http://storage.googleapis.com/my-gcs-bucket/tarball.tar.gz'

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).sdk_location = sdk_location

    def file_download(_, to_path):
      with open(to_path, 'w') as f:
        f.write('Package content.')
      return to_path

    with mock.patch('apache_beam.runners.portability.stager_test'
                    '.stager.Stager._download_file',
                    staticmethod(file_download)):
      self.assertEqual([names.STAGED_SDK_SOURCES_FILENAME],
                       self.stager.create_and_stage_job_resources(
                           options, staging_location=staging_dir)[1])

    tarball_path = os.path.join(staging_dir, names.STAGED_SDK_SOURCES_FILENAME)
    with open(tarball_path) as f:
      self.assertEqual(f.read(), 'Package content.')

  def test_with_extra_packages(self):
    staging_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()
    self.create_temp_file(os.path.join(source_dir, 'abc.tar.gz'), 'nothing')
    self.create_temp_file(os.path.join(source_dir, 'xyz.tar.gz'), 'nothing')
    self.create_temp_file(os.path.join(source_dir, 'xyz2.tar'), 'nothing')
    self.create_temp_file(os.path.join(source_dir, 'whl.whl'), 'nothing')
    self.create_temp_file(
        os.path.join(source_dir, stager.EXTRA_PACKAGES_FILE), 'nothing')

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).extra_packages = [
        os.path.join(source_dir, 'abc.tar.gz'),
        os.path.join(source_dir, 'xyz.tar.gz'),
        os.path.join(source_dir, 'xyz2.tar'),
        os.path.join(source_dir, 'whl.whl'),
        '/tmp/remote/remote_file.tar.gz'
    ]

    self.remote_copied_files = []

    with mock.patch('apache_beam.runners.portability.stager_test'
                    '.stager.Stager._download_file',
                    staticmethod(self.file_copy)):
      with mock.patch('apache_beam.runners.portability.stager_test'
                      '.stager.Stager._is_remote_path',
                      staticmethod(self.is_remote_path)):
        self.assertEqual([
            'abc.tar.gz',
            'xyz.tar.gz',
            'xyz2.tar',
            'whl.whl',
            'remote_file.tar.gz',
            stager.EXTRA_PACKAGES_FILE
        ],
                         self.stager.create_and_stage_job_resources(
                             options, staging_location=staging_dir)[1])
    with open(os.path.join(staging_dir, stager.EXTRA_PACKAGES_FILE)) as f:
      self.assertEqual([
          'abc.tar.gz\n',
          'xyz.tar.gz\n',
          'xyz2.tar\n',
          'whl.whl\n',
          'remote_file.tar.gz\n'
      ],
                       f.readlines())
    self.assertEqual(['/tmp/remote/remote_file.tar.gz'],
                     self.remote_copied_files)

  def test_with_extra_packages_missing_files(self):
    staging_dir = self.make_temp_dir()
    with self.assertRaises(RuntimeError) as cm:

      options = PipelineOptions()
      self.update_options(options)
      options.view_as(SetupOptions).extra_packages = ['nosuchfile.tar.gz']

      self.stager.create_and_stage_job_resources(
          options, staging_location=staging_dir)
    self.assertEqual(
        cm.exception.args[0],
        'The file %s cannot be found. It was specified in the '
        '--extra_packages command line option.' % 'nosuchfile.tar.gz')

  def test_with_extra_packages_invalid_file_name(self):
    staging_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()
    self.create_temp_file(os.path.join(source_dir, 'abc.tgz'), 'nothing')
    with self.assertRaises(RuntimeError) as cm:
      options = PipelineOptions()
      self.update_options(options)
      options.view_as(SetupOptions).extra_packages = [
          os.path.join(source_dir, 'abc.tgz')
      ]
      self.stager.create_and_stage_job_resources(
          options, staging_location=staging_dir)
    self.assertEqual(
        cm.exception.args[0],
        'The --extra_package option expects a full path ending with '
        '".tar", ".tar.gz", ".whl" or ".zip" '
        'instead of %s' % os.path.join(source_dir, 'abc.tgz'))

  def test_with_jar_packages_missing_files(self):
    staging_dir = self.make_temp_dir()
    with self.assertRaises(RuntimeError) as cm:

      options = PipelineOptions()
      self.update_options(options)
      options.view_as(DebugOptions).experiments = [
          'jar_packages=nosuchfile.jar'
      ]
      self.stager.create_and_stage_job_resources(
          options, staging_location=staging_dir)
    self.assertEqual(
        cm.exception.args[0],
        'The file %s cannot be found. It was specified in the '
        '--experiment=\'jar_packages=\' command line option.' %
        'nosuchfile.jar')

  def test_with_jar_packages_invalid_file_name(self):
    staging_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()
    self.create_temp_file(os.path.join(source_dir, 'abc.tgz'), 'nothing')
    with self.assertRaises(RuntimeError) as cm:
      options = PipelineOptions()
      self.update_options(options)
      options.view_as(DebugOptions).experiments = [
          'jar_packages=' + os.path.join(source_dir, 'abc.tgz')
      ]
      self.stager.create_and_stage_job_resources(
          options, staging_location=staging_dir)
    self.assertEqual(
        cm.exception.args[0],
        'The --experiment=\'jar_packages=\' option expects a full path ending '
        'with ".jar" instead of %s' % os.path.join(source_dir, 'abc.tgz'))

  def test_with_jar_packages(self):
    staging_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()
    self.create_temp_file(os.path.join(source_dir, 'abc.jar'), 'nothing')
    self.create_temp_file(os.path.join(source_dir, 'xyz.jar'), 'nothing')
    self.create_temp_file(os.path.join(source_dir, 'ijk.jar'), 'nothing')

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(DebugOptions).experiments = [
        'jar_packages=%s,%s,%s,%s' % (
            os.path.join(source_dir, 'abc.jar'),
            os.path.join(source_dir, 'xyz.jar'),
            os.path.join(source_dir, 'ijk.jar'),
            '/tmp/remote/remote.jar')
    ]

    self.remote_copied_files = []

    with mock.patch('apache_beam.runners.portability.stager_test'
                    '.stager.Stager._download_file',
                    staticmethod(self.file_copy)):
      with mock.patch('apache_beam.runners.portability.stager_test'
                      '.stager.Stager._is_remote_path',
                      staticmethod(self.is_remote_path)):
        self.assertEqual(['abc.jar', 'xyz.jar', 'ijk.jar', 'remote.jar'],
                         self.stager.create_and_stage_job_resources(
                             options, staging_location=staging_dir)[1])
    self.assertEqual(['/tmp/remote/remote.jar'], self.remote_copied_files)

  def test_remove_dependency_from_requirements(self):
    requirements_cache_dir = self.make_temp_dir()
    requirements = ['apache_beam\n', 'avro-python3\n', 'fastavro\n', 'numpy\n']
    with open(os.path.join(requirements_cache_dir, 'abc.txt'), 'w') as f:
      for i in range(len(requirements)):
        f.write(requirements[i])

    tmp_req_filename = self.stager._remove_dependency_from_requirements(
        requirements_file=os.path.join(requirements_cache_dir, 'abc.txt'),
        dependency_to_remove='apache_beam',
        temp_directory_path=requirements_cache_dir)
    with open(tmp_req_filename, 'r') as tf:
      lines = tf.readlines()
    self.assertEqual(['avro-python3\n', 'fastavro\n', 'numpy\n'], sorted(lines))

    tmp_req_filename = self.stager._remove_dependency_from_requirements(
        requirements_file=os.path.join(requirements_cache_dir, 'abc.txt'),
        dependency_to_remove='fastavro',
        temp_directory_path=requirements_cache_dir)

    with open(tmp_req_filename, 'r') as tf:
      lines = tf.readlines()
    self.assertEqual(['apache_beam\n', 'avro-python3\n', 'numpy\n'],
                     sorted(lines))

  def _populate_requitements_cache_fake(
      self, requirements_file, temp_dir, populate_cache_with_sdists):
    if not populate_cache_with_sdists:
      self.create_temp_file(os.path.join(temp_dir, 'nothing.whl'), 'Fake whl')
    self.create_temp_file(
        os.path.join(temp_dir, 'nothing.tar.gz'), 'Fake tarball')

  # requirements cache will popultated with bdist/whl if present
  # else source would be downloaded.
  def test_populate_requirements_cache_with_bdist(self):
    staging_dir = self.make_temp_dir()
    requirements_cache_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()

    options = PipelineOptions()
    self.update_options(options)

    options.view_as(SetupOptions).requirements_cache = requirements_cache_dir
    options.view_as(SetupOptions).requirements_file = os.path.join(
        source_dir, stager.REQUIREMENTS_FILE)
    self.create_temp_file(
        os.path.join(source_dir, stager.REQUIREMENTS_FILE), 'nothing')
    # for default container image, the sdk_container_image option would be none
    with mock.patch('apache_beam.runners.portability.stager_test'
                    '.stager.Stager._populate_requirements_cache',
                    staticmethod(self._populate_requitements_cache_fake)):
      options.view_as(SetupOptions).requirements_cache_only_sources = False
      resources = self.stager.create_and_stage_job_resources(
          options, staging_location=staging_dir)[1]
      for f in resources:
        if f != stager.REQUIREMENTS_FILE:
          self.assertTrue(('.tar.gz' in f) or ('.whl' in f))

  # requirements cache will populated only with sdists/sources
  def test_populate_requirements_cache_with_sdist(self):
    staging_dir = self.make_temp_dir()
    requirements_cache_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()

    options = PipelineOptions()
    self.update_options(options)

    options.view_as(SetupOptions).requirements_cache = requirements_cache_dir
    options.view_as(SetupOptions).requirements_file = os.path.join(
        source_dir, stager.REQUIREMENTS_FILE)
    self.create_temp_file(
        os.path.join(source_dir, stager.REQUIREMENTS_FILE), 'nothing')
    with mock.patch('apache_beam.runners.portability.stager_test'
                    '.stager.Stager._populate_requirements_cache',
                    staticmethod(self._populate_requitements_cache_fake)):
      options.view_as(SetupOptions).requirements_cache_only_sources = True
      resources = self.stager.create_and_stage_job_resources(
          options, staging_location=staging_dir)[1]

      for f in resources:
        if f != stager.REQUIREMENTS_FILE:
          self.assertTrue('.tar.gz' in f)
          self.assertTrue('.whl' not in f)


class TestStager(stager.Stager):
  def stage_artifact(self, local_path_to_artifact, artifact_name, sha256):
    _LOGGER.info(
        'File copy from %s to %s.', local_path_to_artifact, artifact_name)
    shutil.copyfile(local_path_to_artifact, artifact_name)

  def commit_manifest(self):
    pass


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
