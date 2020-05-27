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

"""HTTP client
"""

# pytype: skip-file

from __future__ import absolute_import

import errno
import io
import logging
import re
import time
import traceback
from builtins import object

from apache_beam.io.filesystemio import Downloader
from apache_beam.io.filesystemio import DownloaderStream
from apache_beam.io.filesystemio import Uploader
from apache_beam.io.filesystemio import UploaderStream
from apache_beam.internal.http_client import get_new_http
from apache_beam.utils import retry
import sys

class HttpIO(object):
  """HTTP I/O."""
  def __init__(self, client = None):
    if sys.version_info[0] != 3:
      raise RuntimeError("HttpIO only supports Python 3.")
    self._client = client or get_new_http()
    pass
  
  def open(
      self,
      uri,
      mode='r',
      read_buffer_size=16 * 1024 * 1024):
    downloader = HttpDownloader(uri, self._client)
    if mode != 'r':
      raise Exception("only r mode is supported.")
    return io.BufferedReader(
      DownloaderStream(downloader, mode=mode), buffer_size=read_buffer_size)

  def size(self, uri):
    try:
      # Pass in "" for "Accept-Encoding" because we want the non-gzipped content-length.
      resp, content = self._client.request(uri, method='HEAD', headers={"Accept-Encoding": ""})
      if resp.status != 200:
        raise Exception(resp.status, resp.reason)
      return int(resp["content-length"])
    except Exception as e:
      # Server doesn't support HEAD method;
      # use GET method instead to prefetch the result.
      resp, content = self._client.request(uri, method='GET')
      if resp.status != 200:
        raise Exception(resp.status, resp.reason)
      return int(resp["content-length"])

class HttpDownloader(Downloader):
  def __init__(self, uri, client):
    self._uri = uri
    self._client = client

    resp, content = self._client.request(self._uri, method='GET')
    if resp.status != 200:
      raise Exception(resp.status, resp.reason)
    self._size = int(resp["content-length"])
    self._content = content

  @property
  def size(self):
    return self._size

  def get_range(self, start, end):
    return self._content[start:end]