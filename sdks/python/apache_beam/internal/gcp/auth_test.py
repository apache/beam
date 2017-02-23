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
"""Unit tests for the auth module."""

import os
import sys
import unittest

import mock

from apache_beam.internal.gcp import auth


class AuthTest(unittest.TestCase):

  def test_create_application_client(self):
    try:
      test_args = [
          'test', '--service_account_name', 'abc', '--service_account_key_file',
          os.path.join(os.path.dirname(__file__), '..', '..', 'tests',
                       'data', 'privatekey.p12')]
      with mock.patch.object(sys, 'argv', test_args):
        credentials = auth.get_service_credentials()
        self.assertIsNotNone(credentials)
    except NotImplementedError:
      self.skipTest('service account tests require pyOpenSSL module.')


if __name__ == '__main__':
  unittest.main()
