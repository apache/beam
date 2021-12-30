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

"""Unit tests for the processes module."""

# pytype: skip-file

import subprocess
import unittest

import mock

from apache_beam.utils import processes


class Exec(unittest.TestCase):
  def setUp(self):
    pass

  @mock.patch('apache_beam.utils.processes.subprocess')
  def test_method_forwarding_not_windows(self, *unused_mocks):
    # Test that the correct calls are being forwarded to the subprocess module
    # when we are not on Windows.
    processes.force_shell = False

    processes.call(['subprocess', 'call'], shell=False, other_arg=True)
    processes.subprocess.call.assert_called_once_with(['subprocess', 'call'],
                                                      shell=False,
                                                      other_arg=True)

    processes.check_call(['subprocess', 'check_call'],
                         shell=False,
                         other_arg=True)
    processes.subprocess.check_call.assert_called_once_with(
        ['subprocess', 'check_call'], shell=False, other_arg=True)

    processes.check_output(['subprocess', 'check_output'],
                           shell=False,
                           other_arg=True)
    processes.subprocess.check_output.assert_called_once_with(
        ['subprocess', 'check_output'], shell=False, other_arg=True)

    processes.Popen(['subprocess', 'Popen'], shell=False, other_arg=True)
    processes.subprocess.Popen.assert_called_once_with(['subprocess', 'Popen'],
                                                       shell=False,
                                                       other_arg=True)

  @mock.patch('apache_beam.utils.processes.subprocess')
  def test_method_forwarding_windows(self, *unused_mocks):
    # Test that the correct calls are being forwarded to the subprocess module
    # and that the shell=True flag is added when we are on Windows.
    processes.force_shell = True

    processes.call(['subprocess', 'call'], shell=False, other_arg=True)
    processes.subprocess.call.assert_called_once_with(['subprocess', 'call'],
                                                      shell=True,
                                                      other_arg=True)

    processes.check_call(['subprocess', 'check_call'],
                         shell=False,
                         other_arg=True)
    processes.subprocess.check_call.assert_called_once_with(
        ['subprocess', 'check_call'], shell=True, other_arg=True)

    processes.check_output(['subprocess', 'check_output'],
                           shell=False,
                           other_arg=True)
    processes.subprocess.check_output.assert_called_once_with(
        ['subprocess', 'check_output'], shell=True, other_arg=True)

    processes.Popen(['subprocess', 'Popen'], shell=False, other_arg=True)
    processes.subprocess.Popen.assert_called_once_with(['subprocess', 'Popen'],
                                                       shell=True,
                                                       other_arg=True)


class TestErrorHandlingCheckCall(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.mock_get_patcher = mock.patch(\
      'apache_beam.utils.processes.subprocess.check_call')
    cls.mock_get = cls.mock_get_patcher.start()

  @classmethod
  def tearDownClass(cls):
    cls.mock_get_patcher.stop()

  def test_oserror_check_call(self):
    # Configure the mock to return a response with an OK status code.
    self.mock_get.side_effect = OSError("Test OSError")
    with self.assertRaises(RuntimeError):
      processes.check_call(["lls"])

  def test_oserror_check_call_message(self):
    # Configure the mock to return a response with an OK status code.
    self.mock_get.side_effect = OSError()
    cmd = ["lls"]
    try:
      processes.check_call(cmd)
    except RuntimeError as error:
      self.assertIn('Executable {} not found'.format(str(cmd)),\
      error.args[0])

  def test_check_call_pip_install_non_existing_package(self):
    returncode = 1
    package = "non-exsisting-package"
    cmd = ['python', '-m', 'pip', 'download', '--dest', '/var',\
        '{}'.format(package),\
        '--no-deps', '--no-binary', ':all:']
    output = "Collecting {}".format(package)
    self.mock_get.side_effect = subprocess.CalledProcessError(returncode,\
      cmd, output=output)
    try:
      output = processes.check_call(cmd)
      self.fail(
          "The test failed due to that\
        no error was raised when calling process.check_call")
    except RuntimeError as error:
      self.assertIn("Output from execution of subprocess: {}".format(output),\
        error.args[0])
      self.assertIn("Pip install failed for package: {}".format(package),\
        error.args[0])


class TestErrorHandlingCheckOutput(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.mock_get_patcher = mock.patch(\
        'apache_beam.utils.processes.subprocess.check_output')
    cls.mock_get = cls.mock_get_patcher.start()

  @classmethod
  def tearDownClass(cls):
    cls.mock_get_patcher.stop()

  def test_oserror_check_output_message(self):
    self.mock_get.side_effect = OSError()
    cmd = ["lls"]
    try:
      processes.check_output(cmd)
    except RuntimeError as error:
      self.assertIn('Executable {} not found'.format(str(cmd)),\
      error.args[0])

  def test_check_output_pip_install_non_existing_package(self):
    returncode = 1
    package = "non-exsisting-package"
    cmd = ['python', '-m', 'pip', 'download', '--dest', '/var',\
      '{}'.format(package),\
      '--no-deps', '--no-binary', ':all:']
    output = "Collecting {}".format(package)
    self.mock_get.side_effect = subprocess.CalledProcessError(returncode,\
         cmd, output=output)
    try:
      output = processes.check_output(cmd)
      self.fail(
          "The test failed due to that\
      no error was raised when calling process.check_call")
    except RuntimeError as error:
      self.assertIn("Output from execution of subprocess: {}".format(output),\
        error.args[0])
      self.assertIn("Pip install failed for package: {}".format(package),\
        error.args[0])


class TestErrorHandlingCall(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.mock_get_patcher = mock.patch(\
      'apache_beam.utils.processes.subprocess.call')
    cls.mock_get = cls.mock_get_patcher.start()

  @classmethod
  def tearDownClass(cls):
    cls.mock_get_patcher.stop()

  def test_oserror_check_output_message(self):
    self.mock_get.side_effect = OSError()
    cmd = ["lls"]
    try:
      processes.call(cmd)
    except RuntimeError as error:
      self.assertIn('Executable {} not found'.format(str(cmd)),\
      error.args[0])

  def test_check_output_pip_install_non_existing_package(self):
    returncode = 1
    package = "non-exsisting-package"
    cmd = ['python', '-m', 'pip', 'download', '--dest', '/var',\
      '{}'.format(package),\
      '--no-deps', '--no-binary', ':all:']
    output = "Collecting {}".format(package)
    self.mock_get.side_effect = subprocess.CalledProcessError(returncode,\
         cmd, output=output)
    try:
      output = processes.call(cmd)
      self.fail(
          "The test failed due to that\
        no error was raised when calling process.check_call")
    except RuntimeError as error:
      self.assertIn("Output from execution of subprocess: {}".format(output),\
        error.args[0])
      self.assertIn("Pip install failed for package: {}".format(package),\
        error.args[0])


if __name__ == '__main__':
  unittest.main()
