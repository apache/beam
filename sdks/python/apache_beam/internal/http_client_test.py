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

"""Unit tests for the http_client module."""
# pytype: skip-file

import os
import unittest

import mock
from httplib2 import ProxyInfo

from apache_beam.internal.http_client import DEFAULT_HTTP_TIMEOUT_SECONDS
from apache_beam.internal.http_client import get_new_http
from apache_beam.internal.http_client import proxy_info_from_environment_var


class HttpClientTest(unittest.TestCase):
  def test_proxy_from_env_http_with_port(self):
    with mock.patch.dict(os.environ, http_proxy='http://localhost:9000'):
      proxy_info = proxy_info_from_environment_var('http_proxy')
      expected = ProxyInfo(3, 'localhost', 9000)
      self.assertEqual(str(expected), str(proxy_info))

  def test_proxy_from_env_https_with_port(self):
    with mock.patch.dict(os.environ, https_proxy='https://localhost:9000'):
      proxy_info = proxy_info_from_environment_var('https_proxy')
      expected = ProxyInfo(3, 'localhost', 9000)
      self.assertEqual(str(expected), str(proxy_info))

  def test_proxy_from_env_http_without_port(self):
    with mock.patch.dict(os.environ, http_proxy='http://localhost'):
      proxy_info = proxy_info_from_environment_var('http_proxy')
      expected = ProxyInfo(3, 'localhost', 80)
      self.assertEqual(str(expected), str(proxy_info))

  def test_proxy_from_env_https_without_port(self):
    with mock.patch.dict(os.environ, https_proxy='https://localhost'):
      proxy_info = proxy_info_from_environment_var('https_proxy')
      expected = ProxyInfo(3, 'localhost', 443)
      self.assertEqual(str(expected), str(proxy_info))

  def test_proxy_from_env_http_without_method(self):
    with mock.patch.dict(os.environ, http_proxy='localhost:8000'):
      proxy_info = proxy_info_from_environment_var('http_proxy')
      expected = ProxyInfo(3, 'localhost', 8000)
      self.assertEqual(str(expected), str(proxy_info))

  def test_proxy_from_env_https_without_method(self):
    with mock.patch.dict(os.environ, https_proxy='localhost:8000'):
      proxy_info = proxy_info_from_environment_var('https_proxy')
      expected = ProxyInfo(3, 'localhost', 8000)
      self.assertEqual(str(expected), str(proxy_info))

  def test_proxy_from_env_http_without_port_without_method(self):
    with mock.patch.dict(os.environ, http_proxy='localhost'):
      proxy_info = proxy_info_from_environment_var('http_proxy')
      expected = ProxyInfo(3, 'localhost', 80)
      self.assertEqual(str(expected), str(proxy_info))

  def test_proxy_from_env_https_without_port_without_method(self):
    with mock.patch.dict(os.environ, https_proxy='localhost'):
      proxy_info = proxy_info_from_environment_var('https_proxy')
      expected = ProxyInfo(3, 'localhost', 443)
      self.assertEqual(str(expected), str(proxy_info))

  def test_proxy_from_env_invalid_var(self):
    proxy_info = proxy_info_from_environment_var('http_proxy_host')
    expected = None
    self.assertEqual(str(expected), str(proxy_info))

  def test_proxy_from_env_wrong_method_in_var_name(self):
    with mock.patch.dict(os.environ, smtp_proxy='localhost'):
      with self.assertRaises(KeyError):
        proxy_info_from_environment_var('smtp_proxy')

  def test_proxy_from_env_wrong_method_in_url(self):
    with mock.patch.dict(os.environ, http_proxy='smtp://localhost:8000'):
      proxy_info = proxy_info_from_environment_var('http_proxy')
      expected = ProxyInfo(3, 'smtp', 80)  # wrong proxy info generated
      self.assertEqual(str(expected), str(proxy_info))

  def test_get_new_http_proxy_info(self):
    with mock.patch.dict(os.environ, http_proxy='localhost'):
      http = get_new_http()
      expected = ProxyInfo(3, 'localhost', 80)
      self.assertEqual(str(http.proxy_info), str(expected))

  def test_get_new_http_timeout(self):
    http = get_new_http()
    self.assertEqual(http.timeout, DEFAULT_HTTP_TIMEOUT_SECONDS)


if __name__ == '__main__':
  unittest.main()
