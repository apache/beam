#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Transport library for ProtoRPC.

Contains underlying infrastructure used for communicating RPCs over low level
transports such as HTTP.

Includes HTTP transport built over urllib2.
"""

import six.moves.http_client
import logging
import os
import socket
import sys
import urlparse

from . import messages
from . import protobuf
from . import remote
from . import util
import six

__all__ = [
  'RpcStateError',

  'HttpTransport',
  'LocalTransport',
  'Rpc',
  'Transport',
]


class RpcStateError(messages.Error):
  """Raised when trying to put RPC in to an invalid state."""


class Rpc(object):
  """Represents a client side RPC.

  An RPC is created by the transport class and is used with a single RPC.  While
  an RPC is still in process, the response is set to None.  When it is complete
  the response will contain the response message.
  """

  def __init__(self, request):
    """Constructor.

    Args:
      request: Request associated with this RPC.
    """
    self.__request = request
    self.__response = None
    self.__state = remote.RpcState.RUNNING
    self.__error_message = None
    self.__error_name = None

  @property
  def request(self):
    """Request associated with RPC."""
    return self.__request

  @property
  def response(self):
    """Response associated with RPC."""
    self.wait()
    self.__check_status()
    return self.__response

  @property
  def state(self):
    """State associated with RPC."""
    return self.__state

  @property
  def error_message(self):
    """Error, if any, associated with RPC."""
    self.wait()
    return self.__error_message

  @property
  def error_name(self):
    """Error name, if any, associated with RPC."""
    self.wait()
    return self.__error_name

  def wait(self):
    """Wait for an RPC to finish."""
    if self.__state == remote.RpcState.RUNNING:
      self._wait_impl()

  def _wait_impl(self):
    """Implementation for wait()."""
    raise NotImplementedError()

  def __check_status(self):
    error_class = remote.RpcError.from_state(self.__state)
    if error_class is not None:
      if error_class is remote.ApplicationError:
        raise error_class(self.__error_message, self.__error_name)
      else:
        raise error_class(self.__error_message)

  def __set_state(self, state, error_message=None, error_name=None):
    if self.__state != remote.RpcState.RUNNING:
      raise RpcStateError(
        'RPC must be in RUNNING state to change to %s' % state)
    if state == remote.RpcState.RUNNING:
      raise RpcStateError('RPC is already in RUNNING state')
    self.__state = state
    self.__error_message = error_message
    self.__error_name = error_name

  def set_response(self, response):
    # TODO: Even more specific type checking.
    if not isinstance(response, messages.Message):
      raise TypeError('Expected Message type, received %r' % (response))

    self.__response = response
    self.__set_state(remote.RpcState.OK)

  def set_status(self, status):
    status.check_initialized()
    self.__set_state(status.state, status.error_message, status.error_name)


class Transport(object):
  """Transport base class.

  Provides basic support for implementing a ProtoRPC transport such as one
  that can send and receive messages over HTTP.

  Implementations override _start_rpc.  This method receives a RemoteInfo
  instance and a request Message. The transport is expected to set the rpc
  response or raise an exception before termination.
  """

  @util.positional(1)
  def __init__(self, protocol=protobuf):
    """Constructor.

    Args:
      protocol: If string, will look up a protocol from the default Protocols
        instance by name.  Can also be an instance of remote.ProtocolConfig.
        If neither, it must be an object that implements a protocol interface
        by implementing encode_message, decode_message and set CONTENT_TYPE.
        For example, the modules protobuf and protojson can be used directly.
    """
    if isinstance(protocol, six.string_types):
      protocols = remote.Protocols.get_default()
      try:
        protocol = protocols.lookup_by_name(protocol)
      except KeyError:
        protocol = protocols.lookup_by_content_type(protocol)
    if isinstance(protocol, remote.ProtocolConfig):
      self.__protocol = protocol.protocol
      self.__protocol_config = protocol
    else:
      self.__protocol = protocol
      self.__protocol_config = remote.ProtocolConfig(
        protocol, 'default', default_content_type=protocol.CONTENT_TYPE)

  @property
  def protocol(self):
    """Protocol associated with this transport."""
    return self.__protocol

  @property
  def protocol_config(self):
    """Protocol associated with this transport."""
    return self.__protocol_config

  def send_rpc(self, remote_info, request):
    """Initiate sending an RPC over the transport.

    Args:
      remote_info: RemoteInfo instance describing remote method.
      request: Request message to send to service.

    Returns:
      An Rpc instance intialized with the request..
    """
    request.check_initialized()

    rpc = self._start_rpc(remote_info, request)

    return rpc

  def _start_rpc(self, remote_info, request):
    """Start a remote procedure call.

    Args:
      remote_info: RemoteInfo instance describing remote method.
      request: Request message to send to service.

    Returns:
      An Rpc instance initialized with the request.
    """
    raise NotImplementedError()


class HttpTransport(Transport):
  """Transport for communicating with HTTP servers."""

  @util.positional(2)
  def __init__(self,
               service_url,
               protocol=protobuf):
    """Constructor.

    Args:
      service_url: URL where the service is located.  All communication via
        the transport will go to this URL.
      protocol: The protocol implementation.  Must implement encode_message and
        decode_message.  Can also be an instance of remote.ProtocolConfig.
    """
    super(HttpTransport, self).__init__(protocol=protocol)
    self.__service_url = service_url

  def __get_rpc_status(self, response, content):
    """Get RPC status from HTTP response.

    Args:
      response: HTTPResponse object.
      content: Content read from HTTP response.

    Returns:
      RpcStatus object parsed from response, else an RpcStatus with a generic
      HTTP error.
    """
    # Status above 400 may have RpcStatus content.
    if response.status >= 400:
      content_type = response.getheader('content-type')
      if content_type == self.protocol_config.default_content_type:
        try:
          rpc_status = self.protocol.decode_message(remote.RpcStatus, content)
        except Exception as decode_err:
          logging.warning(
            'An error occurred trying to parse status: %s\n%s',
            str(decode_err), content)
        else:
          if rpc_status.is_initialized():
            return rpc_status
          else:
            logging.warning(
              'Body does not result in an initialized RpcStatus message:\n%s',
              content)

    # If no RpcStatus message present, attempt to forward any content.  If empty
    # use standard error message.
    if not content.strip():
      content = six.moves.http_client.responses.get(response.status, 'Unknown Error')
    return remote.RpcStatus(state=remote.RpcState.SERVER_ERROR,
                            error_message='HTTP Error %s: %s' % (
                              response.status, content or 'Unknown Error'))

  def __set_response(self, remote_info, connection, rpc):
    """Set response on RPC.

    Sets response or status from HTTP request.  Implements the wait method of
    Rpc instance.

    Args:
      remote_info: Remote info for invoked RPC.
      connection: HTTPConnection that is making request.
      rpc: Rpc instance.
    """
    try:
      response = connection.getresponse()

      content = response.read()

      if response.status == six.moves.http_client.OK:
        response = self.protocol.decode_message(remote_info.response_type,
                                                content)
        rpc.set_response(response)
      else:
        status = self.__get_rpc_status(response, content)
        rpc.set_status(status)
    finally:
      connection.close()

  def _start_rpc(self, remote_info, request):
    """Start a remote procedure call.

    Args:
      remote_info: A RemoteInfo instance for this RPC.
      request: The request message for this RPC.

    Returns:
      An Rpc instance initialized with a Request.
    """
    method_url = '%s.%s' % (self.__service_url, remote_info.method.__name__)
    encoded_request = self.protocol.encode_message(request)

    url = urlparse.urlparse(method_url)
    if url.scheme == 'https':
      connection_type = six.moves.http_client.HTTPSConnection
    else:
      connection_type = six.moves.http_client.HTTPConnection
    connection = connection_type(url.hostname, url.port)
    try:
      self._send_http_request(connection, url.path, encoded_request)
      rpc = Rpc(request)
    except remote.RpcError:
      # Pass through all ProtoRPC errors
      connection.close()
      raise
    except socket.error as err:
      connection.close()
      raise remote.NetworkError('Socket error: %s %r' % (type(err).__name__,
                                                         err.args),
                                err)
    except Exception as err:
      connection.close()
      raise remote.NetworkError('Error communicating with HTTP server',
                                err)
    else:
      wait_impl = lambda: self.__set_response(remote_info, connection, rpc)
      rpc._wait_impl = wait_impl

      return rpc

  def _send_http_request(self, connection, http_path, encoded_request):
    connection.request(
        'POST',
        http_path,
        encoded_request,
        headers={'Content-type': self.protocol_config.default_content_type,
                 'Content-length': len(encoded_request)})


class LocalTransport(Transport):
  """Local transport that sends messages directly to services.

  Useful in tests or creating code that can work with either local or remote
  services.  Using LocalTransport is preferrable to simply instantiating a
  single instance of a service and reusing it.  The entire request process
  involves instantiating a new instance of a service, initializing it with
  request state and then invoking the remote method for every request.
  """

  def __init__(self, service_factory):
    """Constructor.

    Args:
      service_factory: Service factory or class.
    """
    super(LocalTransport, self).__init__()
    self.__service_class = getattr(service_factory,
                                   'service_class',
                                   service_factory)
    self.__service_factory = service_factory

  @property
  def service_class(self):
    return self.__service_class

  @property
  def service_factory(self):
    return self.__service_factory

  def _start_rpc(self, remote_info, request):
    """Start a remote procedure call.

    Args:
      remote_info: RemoteInfo instance describing remote method.
      request: Request message to send to service.

    Returns:
      An Rpc instance initialized with the request.
    """
    rpc = Rpc(request)
    def wait_impl():
      instance = self.__service_factory()
      try:
        initalize_request_state = instance.initialize_request_state
      except AttributeError:
        pass
      else:
        host = six.text_type(os.uname()[1])
        initalize_request_state(remote.RequestState(remote_host=host,
                                                    remote_address=u'127.0.0.1',
                                                    server_host=host,
                                                    server_port=-1))
      try:
        response = remote_info.method(instance, request)
        assert isinstance(response, remote_info.response_type)
      except remote.ApplicationError:
        raise
      except:
        exc_type, exc_value, traceback = sys.exc_info()
        message = 'Unexpected error %s: %s' % (exc_type.__name__, exc_value)
        six.reraise(remote.ServerError, message, traceback)
      rpc.set_response(response)
    rpc._wait_impl = wait_impl
    return rpc
