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

from __future__ import absolute_import

import logging
import os
import shutil
import sys
import tempfile
import unittest
from typing import List

import mock
import pytest

from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.dataflow.internal import names
from apache_beam.runners.internal import names as shared_names
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

  remote_copied_files: List[str] = []

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

  def populate_requirements_cache(self, requirements_file, cache_dir):
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
  def test_with_main_session(self):
    staging_dir = self.make_temp_dir()
    options = PipelineOptions()

    options.view_as(SetupOptions).save_main_session = True
    self.update_options(options)

    self.assertEqual([shared_names.PICKLED_MAIN_SESSION_FILE],
                     self.stager.create_and_stage_job_resources(
                         options, staging_location=staging_dir)[1])
    self.assertTrue(
        os.path.isfile(
            os.path.join(staging_dir, shared_names.PICKLED_MAIN_SESSION_FILE)))

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

  @unittest.skipIf(
      sys.version_info[0] == 3,
      'This test is not hermetic '
      'and halts test suite execution on Python 3. '
      'TODO: BEAM-5502')
  def test_with_setup_file(self):
    staging_dir = self.make_temp_dir()
    source_dir = self.make_temp_dir()
    self.create_temp_file(os.path.join(source_dir, 'setup.py'), 'notused')

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).setup_file = os.path.join(
        source_dir, 'setup.py')

    self.assertEqual(
        [stager.WORKFLOW_TARBALL_FILE],
        self.stager.create_and_stage_job_resources(
            options,
            # We replace the build setup command because a realistic one would
            # require the setuptools package to be installed. Note that we can't
            # use "touch" here to create the expected output tarball file, since
            # touch is not available on Windows, so we invoke python to produce
            # equivalent behavior.
            build_setup_args=[
                'python',
                '-c',
                'open(__import__("sys").argv[1], "a")',
                os.path.join(source_dir, stager.WORKFLOW_TARBALL_FILE)
            ],
            temp_dir=source_dir,
            staging_location=staging_dir)[1])
    self.assertTrue(
        os.path.isfile(os.path.join(staging_dir, stager.WORKFLOW_TARBALL_FILE)))

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

    self.assertEqual([names.DATAFLOW_SDK_TARBALL_FILE], staged_resources)

    with open(os.path.join(staging_dir, names.DATAFLOW_SDK_TARBALL_FILE)) as f:
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
      self.assertEqual(staged_resources[0], names.DATAFLOW_SDK_TARBALL_FILE)
      # Exact name depends on the version of the SDK.
      self.assertTrue(staged_resources[1].endswith('whl'))
      for name in staged_resources:
        with open(os.path.join(staging_dir, name)) as f:
          self.assertEqual(f.read(), 'Package content.')

  def test_sdk_location_local_directory(self):
    staging_dir = self.make_temp_dir()
    sdk_location = self.make_temp_dir()
    self.create_temp_file(
        os.path.join(sdk_location, names.DATAFLOW_SDK_TARBALL_FILE),
        'Package content.')

    options = PipelineOptions()
    self.update_options(options)
    options.view_as(SetupOptions).sdk_location = sdk_location

    self.assertEqual([names.DATAFLOW_SDK_TARBALL_FILE],
                     self.stager.create_and_stage_job_resources(
                         options, staging_location=staging_dir)[1])
    tarball_path = os.path.join(staging_dir, names.DATAFLOW_SDK_TARBALL_FILE)
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

    self.assertEqual([names.DATAFLOW_SDK_TARBALL_FILE],
                     self.stager.create_and_stage_job_resources(
                         options, staging_location=staging_dir)[1])
    tarball_path = os.path.join(staging_dir, names.DATAFLOW_SDK_TARBALL_FILE)
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

    self.assertEqual([names.DATAFLOW_SDK_TARBALL_FILE],
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
      self.assertEqual([names.DATAFLOW_SDK_TARBALL_FILE],
                       self.stager.create_and_stage_job_resources(
                           options, staging_location=staging_dir)[1])

    tarball_path = os.path.join(staging_dir, names.DATAFLOW_SDK_TARBALL_FILE)
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


class TestStager(stager.Stager):
  def stage_artifact(self, local_path_to_artifact, artifact_name):
    _LOGGER.info(
        'File copy from %s to %s.', local_path_to_artifact, artifact_name)
    shutil.copyfile(local_path_to_artifact, artifact_name)

  def commit_manifest(self):
    pass


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
