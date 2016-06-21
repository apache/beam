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

import errno
import six.moves.http_client
import os
import socket
import unittest

from protorpc import messages
from protorpc import protobuf
from protorpc import protojson
from protorpc import remote
from protorpc import test_util
from protorpc import transport
from protorpc import webapp_test_util
from protorpc.wsgi import util as wsgi_util

import mox

package = 'transport_test'


class ModuleInterfaceTest(test_util.ModuleInterfaceTest,
                          test_util.TestCase):

  MODULE = transport


class Message(messages.Message):

  value = messages.StringField(1)


class Service(remote.Service):

  @remote.method(Message, Message)
  def method(self, request):
    pass


# Remove when RPC is no longer subclasses.
class TestRpc(transport.Rpc):

  waited = False

  def _wait_impl(self):
    self.waited = True


class RpcTest(test_util.TestCase):

  def setUp(self):
    self.request = Message(value=u'request')
    self.response = Message(value=u'response')
    self.status = remote.RpcStatus(state=remote.RpcState.APPLICATION_ERROR,
                                   error_message='an error',
                                   error_name='blam')

    self.rpc = TestRpc(self.request)

  def testConstructor(self):
    self.assertEquals(self.request, self.rpc.request)
    self.assertEquals(remote.RpcState.RUNNING, self.rpc.state)
    self.assertEquals(None, self.rpc.error_message)
    self.assertEquals(None, self.rpc.error_name)

  def response(self):
    self.assertFalse(self.rpc.waited)
    self.assertEquals(None, self.rpc.response)
    self.assertTrue(self.rpc.waited)

  def testSetResponse(self):
    self.rpc.set_response(self.response)

    self.assertEquals(self.request, self.rpc.request)
    self.assertEquals(remote.RpcState.OK, self.rpc.state)
    self.assertEquals(self.response, self.rpc.response)
    self.assertEquals(None, self.rpc.error_message)
    self.assertEquals(None, self.rpc.error_name)

  def testSetResponseAlreadySet(self):
    self.rpc.set_response(self.response)

    self.assertRaisesWithRegexpMatch(
      transport.RpcStateError,
      'RPC must be in RUNNING state to change to OK',
      self.rpc.set_response,
      self.response)

  def testSetResponseAlreadyError(self):
    self.rpc.set_status(self.status)

    self.assertRaisesWithRegexpMatch(
      transport.RpcStateError,
      'RPC must be in RUNNING state to change to OK',
      self.rpc.set_response,
      self.response)

  def testSetStatus(self):
    self.rpc.set_status(self.status)

    self.assertEquals(self.request, self.rpc.request)
    self.assertEquals(remote.RpcState.APPLICATION_ERROR, self.rpc.state)
    self.assertEquals('an error', self.rpc.error_message)
    self.assertEquals('blam', self.rpc.error_name)
    self.assertRaisesWithRegexpMatch(remote.ApplicationError,
                                     'an error',
                                     getattr, self.rpc, 'response')

  def testSetStatusAlreadySet(self):
    self.rpc.set_response(self.response)

    self.assertRaisesWithRegexpMatch(
      transport.RpcStateError,
      'RPC must be in RUNNING state to change to OK',
      self.rpc.set_response,
      self.response)

  def testSetNonMessage(self):
    self.assertRaisesWithRegexpMatch(
      TypeError,
      'Expected Message type, received 10',
      self.rpc.set_response,
      10)

  def testSetStatusAlreadyError(self):
    self.rpc.set_status(self.status)

    self.assertRaisesWithRegexpMatch(
      transport.RpcStateError,
      'RPC must be in RUNNING state to change to OK',
      self.rpc.set_response,
      self.response)

  def testSetUninitializedStatus(self):
    self.assertRaises(messages.ValidationError,
                      self.rpc.set_status,
                      remote.RpcStatus())


class TransportTest(test_util.TestCase):

  def setUp(self):
    remote.Protocols.set_default(remote.Protocols.new_default())

  def do_test(self, protocol, trans):
    request = Message()
    request.value = u'request'

    response = Message()
    response.value = u'response'

    encoded_request = protocol.encode_message(request)
    encoded_response = protocol.encode_message(response)

    self.assertEquals(protocol, trans.protocol)

    received_rpc = [None]
    def transport_rpc(remote, rpc_request):
      self.assertEquals(remote, Service.method.remote)
      self.assertEquals(request, rpc_request)
      rpc = TestRpc(request)
      rpc.set_response(response)
      return rpc
    trans._start_rpc = transport_rpc

    rpc = trans.send_rpc(Service.method.remote, request)
    self.assertEquals(response, rpc.response)

  def testDefaultProtocol(self):
    trans = transport.Transport()
    self.do_test(protobuf, trans)
    self.assertEquals(protobuf, trans.protocol_config.protocol)
    self.assertEquals('default', trans.protocol_config.name)

  def testAlternateProtocol(self):
    trans = transport.Transport(protocol=protojson)
    self.do_test(protojson, trans)
    self.assertEquals(protojson, trans.protocol_config.protocol)
    self.assertEquals('default', trans.protocol_config.name)

  def testProtocolConfig(self):
    protocol_config = remote.ProtocolConfig(
      protojson, 'protoconfig', 'image/png')
    trans = transport.Transport(protocol=protocol_config)
    self.do_test(protojson, trans)
    self.assertTrue(trans.protocol_config is protocol_config)

  def testProtocolByName(self):
    remote.Protocols.get_default().add_protocol(
      protojson, 'png', 'image/png', ())
    trans = transport.Transport(protocol='png')
    self.do_test(protojson, trans)


@remote.method(Message, Message)
def my_method(self, request):
  self.fail('self.my_method should not be directly invoked.')


class FakeConnectionClass(object):

  def __init__(self, mox):
    self.request = mox.CreateMockAnything()
    self.response = mox.CreateMockAnything()


class HttpTransportTest(webapp_test_util.WebServerTestBase):

  def setUp(self):
    # Do not need much parent construction functionality.

    self.schema = 'http'
    self.server = None

    self.request = Message(value=u'The request value')
    self.encoded_request = protojson.encode_message(self.request)

    self.response = Message(value=u'The response value')
    self.encoded_response = protojson.encode_message(self.response)

  def testCallSucceeds(self):
    self.ResetServer(wsgi_util.static_page(self.encoded_response,
                                           content_type='application/json'))

    rpc = self.connection.send_rpc(my_method.remote, self.request)
    self.assertEquals(self.response, rpc.response)

  def testHttps(self):
    self.schema = 'https'
    self.ResetServer(wsgi_util.static_page(self.encoded_response,
                                           content_type='application/json'))

    # Create a fake https connection function that really just calls http.
    self.used_https = False
    def https_connection(*args, **kwargs):
      self.used_https = True
      return six.moves.http_client.HTTPConnection(*args, **kwargs)

    original_https_connection = six.moves.http_client.HTTPSConnection
    six.moves.http_client.HTTPSConnection = https_connection
    try:
      rpc = self.connection.send_rpc(my_method.remote, self.request)
    finally:
      six.moves.http_client.HTTPSConnection = original_https_connection
    self.assertEquals(self.response, rpc.response)
    self.assertTrue(self.used_https)

  def testHttpSocketError(self):
    self.ResetServer(wsgi_util.static_page(self.encoded_response,
                                           content_type='application/json'))

    bad_transport = transport.HttpTransport('http://localhost:-1/blar')
    try:
      bad_transport.send_rpc(my_method.remote, self.request)
    except remote.NetworkError as err:
      self.assertTrue(str(err).startswith('Socket error: error ('))
      self.assertEquals(errno.ECONNREFUSED, err.cause.errno)
    else:
      self.fail('Expected error')

  def testHttpRequestError(self):
    self.ResetServer(wsgi_util.static_page(self.encoded_response,
                                           content_type='application/json'))

    def request_error(*args, **kwargs):
      raise TypeError('Generic Error')
    original_request = six.moves.http_client.HTTPConnection.request
    six.moves.http_client.HTTPConnection.request = request_error
    try:
      try:
        self.connection.send_rpc(my_method.remote, self.request)
      except remote.NetworkError as err:
        self.assertEquals('Error communicating with HTTP server', str(err))
        self.assertEquals(TypeError, type(err.cause))
        self.assertEquals('Generic Error', str(err.cause))
      else:
        self.fail('Expected error')
    finally:
      six.moves.http_client.HTTPConnection.request = original_request

  def testHandleGenericServiceError(self):
    self.ResetServer(wsgi_util.error(six.moves.http_client.INTERNAL_SERVER_ERROR,
                                     'arbitrary error',
                                     content_type='text/plain'))

    rpc = self.connection.send_rpc(my_method.remote, self.request)
    try:
      rpc.response
    except remote.ServerError as err:
      self.assertEquals('HTTP Error 500: arbitrary error', str(err).strip())
    else:
      self.fail('Expected ServerError')

  def testHandleGenericServiceErrorNoMessage(self):
    self.ResetServer(wsgi_util.error(six.moves.http_client.NOT_IMPLEMENTED,
                                     ' ',
                                     content_type='text/plain'))

    rpc = self.connection.send_rpc(my_method.remote, self.request)
    try:
      rpc.response
    except remote.ServerError as err:
      self.assertEquals('HTTP Error 501: Not Implemented', str(err).strip())
    else:
      self.fail('Expected ServerError')

  def testHandleStatusContent(self):
    self.ResetServer(wsgi_util.static_page('{"state": "REQUEST_ERROR",'
                                           ' "error_message": "a request error"'
                                           '}',
                                           status=six.moves.http_client.BAD_REQUEST,
                                           content_type='application/json'))

    rpc = self.connection.send_rpc(my_method.remote, self.request)
    try:
      rpc.response
    except remote.RequestError as err:
      self.assertEquals('a request error', str(err))
    else:
      self.fail('Expected RequestError')

  def testHandleApplicationError(self):
    self.ResetServer(wsgi_util.static_page('{"state": "APPLICATION_ERROR",'
                                           ' "error_message": "an app error",'
                                           ' "error_name": "MY_ERROR_NAME"}',
                                           status=six.moves.http_client.BAD_REQUEST,
                                           content_type='application/json'))

    rpc = self.connection.send_rpc(my_method.remote, self.request)
    try:
      rpc.response
    except remote.ApplicationError as err:
      self.assertEquals('an app error', str(err))
      self.assertEquals('MY_ERROR_NAME', err.error_name)
    else:
      self.fail('Expected RequestError')

  def testHandleUnparsableErrorContent(self):
    self.ResetServer(wsgi_util.static_page('oops',
                                           status=six.moves.http_client.BAD_REQUEST,
                                           content_type='application/json'))

    rpc = self.connection.send_rpc(my_method.remote, self.request)
    try:
      rpc.response
    except remote.ServerError as err:
      self.assertEquals('HTTP Error 400: oops', str(err))
    else:
      self.fail('Expected ServerError')

  def testHandleEmptyBadRpcStatus(self):
    self.ResetServer(wsgi_util.static_page('{"error_message": "x"}',
                                           status=six.moves.http_client.BAD_REQUEST,
                                           content_type='application/json'))

    rpc = self.connection.send_rpc(my_method.remote, self.request)
    try:
      rpc.response
    except remote.ServerError as err:
      self.assertEquals('HTTP Error 400: {"error_message": "x"}', str(err))
    else:
      self.fail('Expected ServerError')

  def testUseProtocolConfigContentType(self):
    expected_content_type = 'image/png'
    def expect_content_type(environ, start_response):
      self.assertEquals(expected_content_type, environ['CONTENT_TYPE'])
      app = wsgi_util.static_page('', content_type=environ['CONTENT_TYPE'])
      return app(environ, start_response)

    self.ResetServer(expect_content_type)

    protocol_config = remote.ProtocolConfig(protojson, 'json', 'image/png')
    self.connection = self.CreateTransport(self.service_url, protocol_config)

    rpc = self.connection.send_rpc(my_method.remote, self.request)
    self.assertEquals(Message(), rpc.response)


class SimpleRequest(messages.Message):

  content = messages.StringField(1)


class SimpleResponse(messages.Message):

  content = messages.StringField(1)
  factory_value = messages.StringField(2)
  remote_host = messages.StringField(3)
  remote_address = messages.StringField(4)
  server_host = messages.StringField(5)
  server_port = messages.IntegerField(6)


class LocalService(remote.Service):

  def __init__(self, factory_value='default'):
    self.factory_value = factory_value

  @remote.method(SimpleRequest, SimpleResponse)
  def call_method(self, request):
    return SimpleResponse(content=request.content,
                          factory_value=self.factory_value,
                          remote_host=self.request_state.remote_host,
                          remote_address=self.request_state.remote_address,
                          server_host=self.request_state.server_host,
                          server_port=self.request_state.server_port)

  @remote.method()
  def raise_totally_unexpected(self, request):
    raise TypeError('Kablam')

  @remote.method()
  def raise_unexpected(self, request):
    raise remote.RequestError('Huh?')

  @remote.method()
  def raise_application_error(self, request):
    raise remote.ApplicationError('App error', 10)


class LocalTransportTest(test_util.TestCase):

  def CreateService(self, factory_value='default'):
    return

  def testBasicCallWithClass(self):
    stub = LocalService.Stub(transport.LocalTransport(LocalService))
    response = stub.call_method(content='Hello')
    self.assertEquals(SimpleResponse(content='Hello',
                                     factory_value='default',
                                     remote_host=os.uname()[1],
                                     remote_address='127.0.0.1',
                                     server_host=os.uname()[1],
                                     server_port=-1),
                      response)

  def testBasicCallWithFactory(self):
    stub = LocalService.Stub(
      transport.LocalTransport(LocalService.new_factory('assigned')))
    response = stub.call_method(content='Hello')
    self.assertEquals(SimpleResponse(content='Hello',
                                     factory_value='assigned',
                                     remote_host=os.uname()[1],
                                     remote_address='127.0.0.1',
                                     server_host=os.uname()[1],
                                     server_port=-1),
                      response)

  def testTotallyUnexpectedError(self):
    stub = LocalService.Stub(transport.LocalTransport(LocalService))
    self.assertRaisesWithRegexpMatch(
      remote.ServerError,
      'Unexpected error TypeError: Kablam',
      stub.raise_totally_unexpected)

  def testUnexpectedError(self):
    stub = LocalService.Stub(transport.LocalTransport(LocalService))
    self.assertRaisesWithRegexpMatch(
      remote.ServerError,
      'Unexpected error RequestError: Huh?',
      stub.raise_unexpected)

  def testApplicationError(self):
    stub = LocalService.Stub(transport.LocalTransport(LocalService))
    self.assertRaisesWithRegexpMatch(
      remote.ApplicationError,
      'App error',
      stub.raise_application_error)


def main():
  unittest.main()


if __name__ == '__main__':
  main()
