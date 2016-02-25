# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for the setup module."""

import logging
import os
import shutil
import tempfile
import unittest

from google.cloud.dataflow import utils
from google.cloud.dataflow.utils import dependency
from google.cloud.dataflow.utils import names
from google.cloud.dataflow.version import __version__


class SetupTest(unittest.TestCase):

  class FakeOptions(object):
    """Options object with expected attributes for stage_job_resources()."""

    def __init__(self, **kwargs):
      self.requirements_file = None
      self.setup_file = None
      self.extra_packages = None
      self.save_main_session = True
      self.staging_location = None
      self.sdk_location = ''
      have_temp_location = False
      for k, v in kwargs.iteritems():
        if k == 'temp_location':
          have_temp_location = True
        setattr(self, k, v)
      if not have_temp_location:
        self.temp_location = self.staging_location

  def create_temp_file(self, path, contents):
    with open(path, 'w') as f:
      f.write(contents)
      return f.name

  def test_no_staging_location(self):
    with self.assertRaises(RuntimeError) as cm:
      dependency.stage_job_resources(
          SetupTest.FakeOptions())
    self.assertEqual('The --staging_location option must be specified.',
                     cm.exception.message)

  def test_no_temp_location(self):
    staging_dir = tempfile.mkdtemp()
    with self.assertRaises(RuntimeError) as cm:
      dependency.stage_job_resources(
          SetupTest.FakeOptions(
              staging_location=staging_dir,
              temp_location=None))
    self.assertEqual('The --temp_location option must be specified.',
                     cm.exception.message)

  def test_no_main_session(self):
    staging_dir = tempfile.mkdtemp()
    self.assertEqual(
        [],
        dependency.stage_job_resources(
            SetupTest.FakeOptions(
                staging_location=staging_dir,
                save_main_session=False)))

  def test_default_resources(self):
    staging_dir = tempfile.mkdtemp()
    self.assertEqual(
        [names.PICKLED_MAIN_SESSION_FILE],
        dependency.stage_job_resources(
            SetupTest.FakeOptions(staging_location=staging_dir)))
    self.assertTrue(
        os.path.isfile(
            os.path.join(staging_dir, names.PICKLED_MAIN_SESSION_FILE)))

  def test_with_requirements_file(self):
    staging_dir = tempfile.mkdtemp()
    source_dir = tempfile.mkdtemp()
    self.create_temp_file(
        os.path.join(source_dir, dependency.REQUIREMENTS_FILE), 'nothing')
    self.assertEqual(
        [dependency.REQUIREMENTS_FILE,
         names.PICKLED_MAIN_SESSION_FILE],
        dependency.stage_job_resources(
            SetupTest.FakeOptions(
                staging_location=staging_dir,
                requirements_file=os.path.join(
                    source_dir, dependency.REQUIREMENTS_FILE))))
    self.assertTrue(
        os.path.isfile(
            os.path.join(staging_dir, dependency.REQUIREMENTS_FILE)))

  def test_requirements_file_not_present(self):
    staging_dir = tempfile.mkdtemp()
    with self.assertRaises(RuntimeError) as cm:
      dependency.stage_job_resources(
          SetupTest.FakeOptions(
              staging_location=staging_dir,
              requirements_file='nosuchfile'))
    self.assertEqual(
        cm.exception.message,
        'The file %s cannot be found. It was specified in the '
        '--requirements_file command line option.' % 'nosuchfile')

  def test_with_setup_file(self):
    staging_dir = tempfile.mkdtemp()
    source_dir = tempfile.mkdtemp()
    self.create_temp_file(
        os.path.join(source_dir, 'setup.py'), 'notused')
    self.assertEqual(
        [dependency.WORKFLOW_TARBALL_FILE,
         names.PICKLED_MAIN_SESSION_FILE],
        dependency.stage_job_resources(
            SetupTest.FakeOptions(
                staging_location=staging_dir,
                setup_file=os.path.join(source_dir, 'setup.py')),
            # We replace the build setup command because a realistic one would
            # require the setuptools package to be installed. Note that we can't
            # use "touch" here to create the expected output tarball file, since
            # touch is not available on Windows, so we invoke python to produce
            # equivalent behavior.
            build_setup_args=[
                'python', '-c', 'open(__import__("sys").argv[1], "a")',
                os.path.join(source_dir, dependency.WORKFLOW_TARBALL_FILE)],
            temp_dir=source_dir))
    self.assertTrue(
        os.path.isfile(
            os.path.join(staging_dir, dependency.WORKFLOW_TARBALL_FILE)))

  def test_setup_file_not_present(self):
    staging_dir = tempfile.mkdtemp()
    with self.assertRaises(RuntimeError) as cm:
      dependency.stage_job_resources(
          SetupTest.FakeOptions(
              staging_location=staging_dir,
              setup_file='nosuchfile'))
    self.assertEqual(
        cm.exception.message,
        'The file %s cannot be found. It was specified in the '
        '--setup_file command line option.' % 'nosuchfile')

  def test_setup_file_not_named_setup_dot_py(self):
    staging_dir = tempfile.mkdtemp()
    source_dir = tempfile.mkdtemp()
    self.create_temp_file(
        os.path.join(source_dir, 'xyz-setup.py'), 'notused')
    with self.assertRaises(RuntimeError) as cm:
      dependency.stage_job_resources(
          SetupTest.FakeOptions(
              staging_location=staging_dir,
              setup_file=os.path.join(source_dir, 'xyz-setup.py')))
    self.assertTrue(
        cm.exception.message.startswith(
            'The --setup_file option expects the full path to a file named '
            'setup.py instead of '))

  def override_file_copy(self, expected_from_path, expected_to_dir):
    def file_copy(from_path, to_path):
      if not from_path.endswith(names.PICKLED_MAIN_SESSION_FILE):
        self.assertEqual(expected_from_path, from_path)
        self.assertEqual(utils.path.join(expected_to_dir,
                                         names.DATAFLOW_SDK_TARBALL_FILE),
                         to_path)
      if from_path.startswith('gs://') or to_path.startswith('gs://'):
        logging.info('Faking file_copy(%s, %s)', from_path, to_path)
      else:
        shutil.copyfile(from_path, to_path)
    dependency._dependency_file_copy = file_copy

  def test_sdk_location_default(self):
    staging_dir = tempfile.mkdtemp()
    expected_from_path = utils.path.join(
        'gs://dataflow-sdk-for-python',
        'google-cloud-dataflow-python-sdk-%s.tgz' % __version__)
    self.override_file_copy(expected_from_path, staging_dir)
    self.assertEqual(
        [names.PICKLED_MAIN_SESSION_FILE,
         names.DATAFLOW_SDK_TARBALL_FILE],
        dependency.stage_job_resources(
            SetupTest.FakeOptions(
                staging_location=staging_dir,
                sdk_location='default'),
            file_copy=dependency._dependency_file_copy))

  def test_sdk_location_local(self):
    staging_dir = tempfile.mkdtemp()
    sdk_location = tempfile.mkdtemp()
    self.create_temp_file(
        os.path.join(
            sdk_location,
            names.DATAFLOW_SDK_TARBALL_FILE),
        'contents')
    self.assertEqual(
        [names.PICKLED_MAIN_SESSION_FILE,
         names.DATAFLOW_SDK_TARBALL_FILE],
        dependency.stage_job_resources(
            SetupTest.FakeOptions(
                staging_location=staging_dir,
                sdk_location=sdk_location)))
    tarball_path = os.path.join(
        staging_dir, names.DATAFLOW_SDK_TARBALL_FILE)
    with open(tarball_path) as f:
      self.assertEqual(f.read(), 'contents')

  def test_sdk_location_local_not_present(self):
    staging_dir = tempfile.mkdtemp()
    sdk_location = 'nosuchdir'
    with self.assertRaises(RuntimeError) as cm:
      dependency.stage_job_resources(
          SetupTest.FakeOptions(
              staging_location=staging_dir,
              sdk_location=sdk_location))
    self.assertEqual(
        'The file "%s" cannot be found. Its '
        'directory was specified by the --sdk_location command-line option.' %
        os.path.join(sdk_location, names.DATAFLOW_SDK_TARBALL_FILE),
        cm.exception.message)

  def test_sdk_location_gcs(self):
    staging_dir = tempfile.mkdtemp()
    sdk_location = 'gs://my-gcs-bucket'
    expected_from_path = utils.path.join(
        sdk_location,
        'google-cloud-dataflow-python-sdk-%s.tgz' % __version__)
    self.override_file_copy(expected_from_path, staging_dir)
    self.assertEqual(
        [names.PICKLED_MAIN_SESSION_FILE,
         names.DATAFLOW_SDK_TARBALL_FILE],
        dependency.stage_job_resources(
            SetupTest.FakeOptions(
                staging_location=staging_dir,
                sdk_location=sdk_location)))

  def test_with_extra_packages(self):
    staging_dir = tempfile.mkdtemp()
    source_dir = tempfile.mkdtemp()
    self.create_temp_file(
        os.path.join(source_dir, 'abc.tar.gz'), 'nothing')
    self.create_temp_file(
        os.path.join(source_dir, 'xyz.tar.gz'), 'nothing')
    self.create_temp_file(
        os.path.join(source_dir, dependency.EXTRA_PACKAGES_FILE), 'nothing')
    self.assertEqual(
        ['abc.tar.gz', 'xyz.tar.gz', dependency.EXTRA_PACKAGES_FILE,
         names.PICKLED_MAIN_SESSION_FILE],
        dependency.stage_job_resources(
            SetupTest.FakeOptions(
                staging_location=staging_dir,
                extra_packages=[
                    os.path.join(source_dir, 'abc.tar.gz'),
                    os.path.join(source_dir, 'xyz.tar.gz')])))
    with open(os.path.join(staging_dir, dependency.EXTRA_PACKAGES_FILE)) as f:
      self.assertEqual(['abc.tar.gz\n', 'xyz.tar.gz\n'], f.readlines())

  def test_with_extra_packages_missing_files(self):
    staging_dir = tempfile.mkdtemp()
    with self.assertRaises(RuntimeError) as cm:
      dependency.stage_job_resources(
          SetupTest.FakeOptions(
              staging_location=staging_dir,
              extra_packages=['nosuchfile']))
    self.assertEqual(
        cm.exception.message,
        'The file %s cannot be found. It was specified in the '
        '--extra_packages command line option.' % 'nosuchfile')

  def test_with_extra_packages_invalid_file_name(self):
    staging_dir = tempfile.mkdtemp()
    source_dir = tempfile.mkdtemp()
    self.create_temp_file(
        os.path.join(source_dir, 'abc.tgz'), 'nothing')
    with self.assertRaises(RuntimeError) as cm:
      dependency.stage_job_resources(
          SetupTest.FakeOptions(
              staging_location=staging_dir,
              extra_packages=[os.path.join(source_dir, 'abc.tgz')]))
    self.assertEqual(
        cm.exception.message,
        'The --extra_packages option expects a full path ending with '
        '\'.tar.gz\' instead of %s' % os.path.join(source_dir, 'abc.tgz'))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
