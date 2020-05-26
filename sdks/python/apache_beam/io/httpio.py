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

"""AWS S3 client
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

class HttpIO(object):
  """HTTP I/O."""
  def __init__(self):
    pass
  
  def open(
      self,
      uri,
      mode='r',
      read_buffer_size=16 * 1024 * 1024):
    downloader = HttpDownloader(uri)
    if mode != 'r':
      raise Exception("only r mode is supported.")
    return io.BufferedReader(
      DownloaderStream(downloader, mode=mode), buffer_size=read_buffer_size)



class HttpDownloader(Downloader):
  def __init__(self, uri):
    self._uri = uri
    self._client = get_new_http()
    self._content = None
    self._size = None

    try:
      resp, content = self._client.request(self._uri, method='HEAD')
      self._size = resp["content-length"]
    except Exception as e:
      # Server doesn't support HEAD method; use GET method instead.
      resp, content = self._client.request(self._uri, method='GET')
      self._size = resp["content-length"]
      self._content = content

  @property
  def size(self):
    return self._size

  def get_range(self, start, end):
    # If we've already fetched the content at the beginning (for servers that
    # don't support HEAD requests), just use that data.
    if self._content:
      return self._content[start:end]
    
    try:
      headers = {"Range": "bytes=%d-%d" % (start, end)}
      resp, content = self._client.request(self._uri, method='GET', headers=headers)
      self._size = resp["content-length"]
    except Exception as e:
      raise e
