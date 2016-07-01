#!/usr/bin/env python
#
# Copyright 2011 Google Inc.
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

"""End to end tests for ProtoRPC."""

__author__ = 'rafek@google.com (Rafe Kaplan)'


import unittest

from protorpc import protojson
from protorpc import remote
from protorpc import test_util
from protorpc import util
from protorpc import webapp_test_util

package = 'test_package'


class EndToEndTest(webapp_test_util.EndToEndTestBase):

  def testSimpleRequest(self):
    self.assertEquals(test_util.OptionalMessage(string_value='+blar'),
                      self.stub.optional_message(string_value='blar'))

  def testSimpleRequestComplexContentType(self):
    response = self.DoRawRequest(
      'optional_message',
      content='{"string_value": "blar"}',
      content_type='application/json; charset=utf-8')
    headers = response.headers
    self.assertEquals(200, response.code)
    self.assertEquals('{"string_value": "+blar"}', response.read())
    self.assertEquals('application/json', headers['content-type'])

  def testInitParameter(self):
    self.assertEquals(test_util.OptionalMessage(string_value='uninitialized'),
                      self.stub.init_parameter())
    self.assertEquals(test_util.OptionalMessage(string_value='initialized'),
                      self.other_stub.init_parameter())

  def testMissingContentType(self):
    code, content, headers = self.RawRequestError(
      'optional_message',
      content='{"string_value": "blar"}',
      content_type='')
    self.assertEquals(400, code)
    self.assertEquals(util.pad_string('Bad Request'), content)
    self.assertEquals('text/plain; charset=utf-8', headers['content-type'])

  def testWrongPath(self):
    self.assertRaisesWithRegexpMatch(remote.ServerError,
                                     'HTTP Error 404: Not Found',
                                     self.bad_path_stub.optional_message)

  def testUnsupportedContentType(self):
    code, content, headers = self.RawRequestError(
      'optional_message',
      content='{"string_value": "blar"}',
      content_type='image/png')
    self.assertEquals(415, code)
    self.assertEquals(util.pad_string('Unsupported Media Type'), content)
    self.assertEquals(headers['content-type'], 'text/plain; charset=utf-8')

  def testUnsupportedHttpMethod(self):
    code, content, headers = self.RawRequestError('optional_message')
    self.assertEquals(405, code)
    self.assertEquals(
      util.pad_string('/my/service.optional_message is a ProtoRPC method.\n\n'
                      'Service protorpc.webapp_test_util.TestService\n\n'
                      'More about ProtoRPC: '
                      'http://code.google.com/p/google-protorpc\n'),
      content)
    self.assertEquals(headers['content-type'], 'text/plain; charset=utf-8')

  def testMethodNotFound(self):
    self.assertRaisesWithRegexpMatch(remote.MethodNotFoundError,
                                     'Unrecognized RPC method: does_not_exist',
                                     self.mismatched_stub.does_not_exist)

  def testBadMessageError(self):
    code, content, headers = self.RawRequestError('nested_message',
                                                  content='{}')
    self.assertEquals(400, code)

    expected_content = protojson.encode_message(remote.RpcStatus(
      state=remote.RpcState.REQUEST_ERROR,
      error_message=('Error parsing ProtoRPC request '
                     '(Unable to parse request content: '
                     'Message NestedMessage is missing '
                     'required field a_value)')))
    self.assertEquals(util.pad_string(expected_content), content)
    self.assertEquals(headers['content-type'], 'application/json')

  def testApplicationError(self):
    try:
      self.stub.raise_application_error()
    except remote.ApplicationError as err:
      self.assertEquals('This is an application error', err.message)
      self.assertEquals('ERROR_NAME', err.error_name)
    else:
      self.fail('Expected application error')

  def testRpcError(self):
    try:
      self.stub.raise_rpc_error()
    except remote.ServerError as err:
      self.assertEquals('Internal Server Error', err.message)
    else:
      self.fail('Expected server error')

  def testUnexpectedError(self):
    try:
      self.stub.raise_unexpected_error()
    except remote.ServerError as err:
      self.assertEquals('Internal Server Error', err.message)
    else:
      self.fail('Expected server error')

  def testBadResponse(self):
    try:
      self.stub.return_bad_message()
    except remote.ServerError as err:
      self.assertEquals('Internal Server Error', err.message)
    else:
      self.fail('Expected server error')


def main():
  unittest.main()


if __name__ == '__main__':
  main()
