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

"""This class implements methods to interact with files at HTTP URLs.

This I/O only implements methods to read with files at HTTP URLs, because
of the variability in methods by which HTTP content can be written
to a server. If you need to write your results to an HTTP endpoint,
you might want to make your own I/O or use another, more specific,
I/O connector.

"""

# pytype: skip-file

from __future__ import absolute_import

import io
from builtins import object

from apache_beam.io.filesystemio import Downloader
from apache_beam.io.filesystemio import DownloaderStream
from apache_beam.internal.http_client import get_new_http
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
      """Open a URL for reading or writing.

      Args:
        uri (str): HTTP URL in the form ``http://[path]`` or ``https://[path]``.
        mode (str): ``'r'`` or ``'rb'`` for reading.
        read_buffer_size (int): Buffer size to use during read operations.

      Returns:
        A file object representing the response.

      Raises:
        ValueError: Invalid open file mode.
      """
    if mode == 'r' or mode == 'rb':
      downloader = HttpDownloader(uri, self._client)
      return io.BufferedReader(
        DownloaderStream(downloader, mode=mode), buffer_size=read_buffer_size)
    else:
      raise ValueError('Invalid file open mode: %s.' % mode)

  def list_prefix(self, path):
    """Lists files matching the prefix.
    
    Because there is no common standard for listing files at a given
    HTTP URL, this method just returns a single file at the given URL.
    This means that listing files only works with an exact path, not
    with a glob expression.

    Args:
      path: HTTP URL in the form http://[path] or https://[path].

    Returns:
      Dictionary of file name -> size.
    """
    return {path: self.size(path)}

  def size(self, uri):
    """Returns the size of a single file stored at a HTTP URL.

    First, the client attempts to make a HEAD request for a non-gzipped version of the file,
    and uses the Content-Length header to retrieve the size. If that fails because the server
    does not attempt HEAD requests, the client just does a GET requuest to retrieve the length. 

    Args:
      path: HTTP URL in the form http://[path] or https://[path].

    Returns:
      Size of the HTTP file in bytes.
    """
    try:
      # Pass in "" for "Accept-Encoding" because we want the non-gzipped content-length.
      resp, content = self._client.request(uri, method='HEAD', headers={"Accept-Encoding": ""})
      if resp.status != 200:
        raise Exception(resp.status, resp.reason)
      return int(resp["content-length"])
    except Exception:
      # Server doesn't support HEAD method;
      # use GET method instead to prefetch the result.
      resp, content = self._client.request(uri, method='GET')
      if resp.status != 200:
        raise Exception(resp.status, resp.reason)
      return int(resp["content-length"])

  def exists(self, uri):
    """Returns whether the file at the given HTTP URL exists.

    The client attempts to make a HEAD request, and if that fails, a GET request.
    If the server returns 404, this function returns false, and it returns
    true only if the server returns 200.

    Args:
      path: HTTP URL in the form http://[path] or https://[path].

    Returns:
      Size of the HTTP file in bytes.
    """
    try:
      resp, content = self._client.request(uri, method='HEAD')
      if resp.status == 200:
        return True
      elif resp.status == 404:
        return False
      else:
        raise Exception(resp.status, resp.reason)
    except Exception:
      # Server doesn't support HEAD method;
      # use GET method instead to prefetch the result.
      resp, content = self._client.request(uri, method='GET')
      if resp.status == 200:
        return True
      elif resp.status == 404:
        return False
      else:
        raise Exception(resp.status, resp.reason)


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
