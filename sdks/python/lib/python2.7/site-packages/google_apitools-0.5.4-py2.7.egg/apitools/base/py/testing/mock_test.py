#
# Copyright 2015 Google Inc.
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

"""Tests for apitools.base.py.testing.mock."""

import unittest2

from apitools.base.protorpclite import messages

import apitools.base.py as apitools_base
from apitools.base.py.testing import mock
from apitools.base.py.testing import testclient as fusiontables


class MockTest(unittest2.TestCase):

    def testMockFusionBasic(self):
        with mock.Client(fusiontables.FusiontablesV1) as client_class:
            client_class.column.List.Expect(request=1, response=2)
            client = fusiontables.FusiontablesV1(get_credentials=False)
            self.assertEqual(client.column.List(1), 2)
            with self.assertRaises(mock.UnexpectedRequestException):
                client.column.List(3)

    def testMockFusionException(self):
        with mock.Client(fusiontables.FusiontablesV1) as client_class:
            client_class.column.List.Expect(
                request=1,
                exception=apitools_base.HttpError({'status': 404}, '', ''))
            client = fusiontables.FusiontablesV1(get_credentials=False)
            with self.assertRaises(apitools_base.HttpError):
                client.column.List(1)

    def testMockFusionOrder(self):
        with mock.Client(fusiontables.FusiontablesV1) as client_class:
            client_class.column.List.Expect(request=1, response=2)
            client_class.column.List.Expect(request=2, response=1)
            client = fusiontables.FusiontablesV1(get_credentials=False)
            self.assertEqual(client.column.List(1), 2)
            self.assertEqual(client.column.List(2), 1)

    def testMockFusionWrongOrder(self):
        with mock.Client(fusiontables.FusiontablesV1) as client_class:
            client_class.column.List.Expect(request=1, response=2)
            client_class.column.List.Expect(request=2, response=1)
            client = fusiontables.FusiontablesV1(get_credentials=False)
            with self.assertRaises(mock.UnexpectedRequestException):
                self.assertEqual(client.column.List(2), 1)
            with self.assertRaises(mock.UnexpectedRequestException):
                self.assertEqual(client.column.List(1), 2)

    def testMockFusionTooMany(self):
        with mock.Client(fusiontables.FusiontablesV1) as client_class:
            client_class.column.List.Expect(request=1, response=2)
            client = fusiontables.FusiontablesV1(get_credentials=False)
            self.assertEqual(client.column.List(1), 2)
            with self.assertRaises(mock.UnexpectedRequestException):
                self.assertEqual(client.column.List(2), 1)

    def testMockFusionTooFew(self):
        with self.assertRaises(mock.ExpectedRequestsException):
            with mock.Client(fusiontables.FusiontablesV1) as client_class:
                client_class.column.List.Expect(request=1, response=2)
                client_class.column.List.Expect(request=2, response=1)
                client = fusiontables.FusiontablesV1(get_credentials=False)
                self.assertEqual(client.column.List(1), 2)

    def testFusionUnmock(self):
        with mock.Client(fusiontables.FusiontablesV1):
            client = fusiontables.FusiontablesV1(get_credentials=False)
            mocked_service_type = type(client.column)
        client = fusiontables.FusiontablesV1(get_credentials=False)
        self.assertNotEqual(type(client.column), mocked_service_type)

    def testClientUnmock(self):
        mock_client = mock.Client(fusiontables.FusiontablesV1)
        attributes = set(mock_client.__dict__.keys())
        mock_client = mock_client.Mock()
        self.assertTrue(set(mock_client.__dict__.keys()) - attributes)
        mock_client.Unmock()
        self.assertEqual(attributes, set(mock_client.__dict__.keys()))


class _NestedMessage(messages.Message):
    nested = messages.StringField(1)


class _NestedListMessage(messages.Message):
    nested_list = messages.MessageField(_NestedMessage, 1, repeated=True)


class _NestedNestedMessage(messages.Message):
    nested = messages.MessageField(_NestedMessage, 1)


class UtilTest(unittest2.TestCase):

    def testMessagesEqual(self):
        self.assertFalse(mock._MessagesEqual(
            _NestedNestedMessage(
                nested=_NestedMessage(
                    nested='foo')),
            _NestedNestedMessage(
                nested=_NestedMessage(
                    nested='bar'))))

        self.assertTrue(mock._MessagesEqual(
            _NestedNestedMessage(
                nested=_NestedMessage(
                    nested='foo')),
            _NestedNestedMessage(
                nested=_NestedMessage(
                    nested='foo'))))

    def testListedMessagesEqual(self):
        self.assertTrue(mock._MessagesEqual(
            _NestedListMessage(
                nested_list=[_NestedMessage(nested='foo')]),
            _NestedListMessage(
                nested_list=[_NestedMessage(nested='foo')])))

        self.assertTrue(mock._MessagesEqual(
            _NestedListMessage(
                nested_list=[_NestedMessage(nested='foo'),
                             _NestedMessage(nested='foo2')]),
            _NestedListMessage(
                nested_list=[_NestedMessage(nested='foo'),
                             _NestedMessage(nested='foo2')])))

        self.assertFalse(mock._MessagesEqual(
            _NestedListMessage(
                nested_list=[_NestedMessage(nested='foo')]),
            _NestedListMessage(
                nested_list=[_NestedMessage(nested='bar')])))

        self.assertFalse(mock._MessagesEqual(
            _NestedListMessage(
                nested_list=[_NestedMessage(nested='foo')]),
            _NestedListMessage(
                nested_list=[_NestedMessage(nested='foo'),
                             _NestedMessage(nested='foo')])))
