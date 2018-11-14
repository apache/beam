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

"""Utility functions used for creating a common Http client from httplib2.

For internal use only. No backwards compatibility guarantees.
"""
from __future__ import absolute_import

import logging
import os
import re

import httplib2

# This is the number of seconds the library will wait for GCS operations to
# complete.
DEFAULT_HTTP_TIMEOUT_SECONDS = 60


def proxy_info_from_environment_var(proxy_env_var):
  """Reads proxy info from the environment and converts to httplib2.ProxyInfo.

  Args:
    proxy_env_var: environment variable string to read, http_proxy or
       https_proxy (in lower case).
       Example: http://myproxy.domain.com:8080

  Returns:
    httplib2.ProxyInfo constructed from the environment string.
  """
  proxy_url = os.environ.get(proxy_env_var)
  if not proxy_url:
    return None
  proxy_protocol = proxy_env_var.lower().split('_')[0]
  if not re.match('^https?://', proxy_url, flags=re.IGNORECASE):
    logging.warn("proxy_info_from_url requires a protocol, which is always "
                 "http or https.")
    proxy_url = proxy_protocol + '://' + proxy_url
  return httplib2.proxy_info_from_url(proxy_url, method=proxy_protocol)


def get_new_http():
  """Creates and returns a new httplib2.Http instance.

  Returns:
    An initialized httplib2.Http instance.
  """
  proxy_info = None
  for proxy_env_var in ['http_proxy', 'https_proxy']:
    if os.environ.get(proxy_env_var):
      proxy_info = proxy_info_from_environment_var(proxy_env_var)
      break
  # Use a non-infinite SSL timeout to avoid hangs during network flakiness.
  return httplib2.Http(proxy_info=proxy_info,
                       timeout=DEFAULT_HTTP_TIMEOUT_SECONDS)
