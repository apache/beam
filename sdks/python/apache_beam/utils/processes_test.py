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

from __future__ import absolute_import

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
    processes.subprocess.call.assert_called_once_with(
        ['subprocess', 'call'],
        shell=False,
        other_arg=True)

    processes.check_call(
        ['subprocess', 'check_call'],
        shell=False,
        other_arg=True)
    processes.subprocess.check_call.assert_called_once_with(
        ['subprocess', 'check_call'],
        shell=False,
        other_arg=True)

    processes.check_output(
        ['subprocess', 'check_output'],
        shell=False,
        other_arg=True)
    processes.subprocess.check_output.assert_called_once_with(
        ['subprocess', 'check_output'],
        shell=False,
        other_arg=True)

    processes.Popen(['subprocess', 'Popen'], shell=False, other_arg=True)
    processes.subprocess.Popen.assert_called_once_with(
        ['subprocess', 'Popen'],
        shell=False,
        other_arg=True)

  @mock.patch('apache_beam.utils.processes.subprocess')
  def test_method_forwarding_windows(self, *unused_mocks):
    # Test that the correct calls are being forwarded to the subprocess module
    # and that the shell=True flag is added when we are on Windows.
    processes.force_shell = True

    processes.call(['subprocess', 'call'], shell=False, other_arg=True)
    processes.subprocess.call.assert_called_once_with(
        ['subprocess', 'call'],
        shell=True,
        other_arg=True)

    processes.check_call(
        ['subprocess', 'check_call'],
        shell=False,
        other_arg=True)
    processes.subprocess.check_call.assert_called_once_with(
        ['subprocess', 'check_call'],
        shell=True,
        other_arg=True)

    processes.check_output(
        ['subprocess', 'check_output'],
        shell=False,
        other_arg=True)
    processes.subprocess.check_output.assert_called_once_with(
        ['subprocess', 'check_output'],
        shell=True,
        other_arg=True)

    processes.Popen(['subprocess', 'Popen'], shell=False, other_arg=True)
    processes.subprocess.Popen.assert_called_once_with(
        ['subprocess', 'Popen'],
        shell=True,
        other_arg=True)


if __name__ == '__main__':
  unittest.main()
