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

"""Tests for list_pager."""

import unittest2

from apitools.base.py import list_pager
from apitools.base.py.testing import mock
from apitools.base.py.testing import testclient as fusiontables


class ListPagerTest(unittest2.TestCase):

    def _AssertInstanceSequence(self, results, n):
        counter = 0
        for instance in results:
            self.assertEqual(instance.name, 'c' + str(counter))
            counter += 1

        self.assertEqual(counter, n)

    def setUp(self):
        self.mocked_client = mock.Client(fusiontables.FusiontablesV1)
        self.mocked_client.Mock()
        self.addCleanup(self.mocked_client.Unmock)

    def testYieldFromList(self):
        self.mocked_client.column.List.Expect(
            fusiontables.FusiontablesColumnListRequest(
                maxResults=100,
                pageToken=None,
                tableId='mytable',
            ),
            fusiontables.ColumnList(
                items=[
                    fusiontables.Column(name='c0'),
                    fusiontables.Column(name='c1'),
                    fusiontables.Column(name='c2'),
                    fusiontables.Column(name='c3'),
                ],
                nextPageToken='x',
            ))
        self.mocked_client.column.List.Expect(
            fusiontables.FusiontablesColumnListRequest(
                maxResults=100,
                pageToken='x',
                tableId='mytable',
            ),
            fusiontables.ColumnList(
                items=[
                    fusiontables.Column(name='c4'),
                    fusiontables.Column(name='c5'),
                    fusiontables.Column(name='c6'),
                    fusiontables.Column(name='c7'),
                ],
            ))

        client = fusiontables.FusiontablesV1(get_credentials=False)
        request = fusiontables.FusiontablesColumnListRequest(tableId='mytable')
        results = list_pager.YieldFromList(client.column, request)

        self._AssertInstanceSequence(results, 8)

    def testYieldNoRecords(self):
        client = fusiontables.FusiontablesV1(get_credentials=False)
        request = fusiontables.FusiontablesColumnListRequest(tableId='mytable')
        results = list_pager.YieldFromList(client.column, request, limit=False)
        self.assertEqual(0, len(list(results)))

    def testYieldFromListPartial(self):
        self.mocked_client.column.List.Expect(
            fusiontables.FusiontablesColumnListRequest(
                maxResults=100,
                pageToken=None,
                tableId='mytable',
            ),
            fusiontables.ColumnList(
                items=[
                    fusiontables.Column(name='c0'),
                    fusiontables.Column(name='c1'),
                    fusiontables.Column(name='c2'),
                    fusiontables.Column(name='c3'),
                ],
                nextPageToken='x',
            ))
        self.mocked_client.column.List.Expect(
            fusiontables.FusiontablesColumnListRequest(
                maxResults=100,
                pageToken='x',
                tableId='mytable',
            ),
            fusiontables.ColumnList(
                items=[
                    fusiontables.Column(name='c4'),
                    fusiontables.Column(name='c5'),
                    fusiontables.Column(name='c6'),
                    fusiontables.Column(name='c7'),
                ],
            ))

        client = fusiontables.FusiontablesV1(get_credentials=False)
        request = fusiontables.FusiontablesColumnListRequest(tableId='mytable')
        results = list_pager.YieldFromList(client.column, request, limit=6)

        self._AssertInstanceSequence(results, 6)

    def testYieldFromListEmpty(self):
        self.mocked_client.column.List.Expect(
            fusiontables.FusiontablesColumnListRequest(
                maxResults=100,
                pageToken=None,
                tableId='mytable',
            ),
            fusiontables.ColumnList())

        client = fusiontables.FusiontablesV1(get_credentials=False)
        request = fusiontables.FusiontablesColumnListRequest(tableId='mytable')
        results = list_pager.YieldFromList(client.column, request, limit=6)

        self._AssertInstanceSequence(results, 0)

    def testYieldFromListWithPredicate(self):
        self.mocked_client.column.List.Expect(
            fusiontables.FusiontablesColumnListRequest(
                maxResults=100,
                pageToken=None,
                tableId='mytable',
            ),
            fusiontables.ColumnList(
                items=[
                    fusiontables.Column(name='c0'),
                    fusiontables.Column(name='bad0'),
                    fusiontables.Column(name='c1'),
                    fusiontables.Column(name='bad1'),
                ],
                nextPageToken='x',
            ))
        self.mocked_client.column.List.Expect(
            fusiontables.FusiontablesColumnListRequest(
                maxResults=100,
                pageToken='x',
                tableId='mytable',
            ),
            fusiontables.ColumnList(
                items=[
                    fusiontables.Column(name='c2'),
                ],
            ))

        client = fusiontables.FusiontablesV1(get_credentials=False)
        request = fusiontables.FusiontablesColumnListRequest(tableId='mytable')
        results = list_pager.YieldFromList(
            client.column, request, predicate=lambda x: 'c' in x.name)

        self._AssertInstanceSequence(results, 3)

    def testYieldFromListWithAttributes(self):
        self.mocked_client.columnalternate.List.Expect(
            fusiontables.FusiontablesColumnListAlternateRequest(
                pageSize=100,
                pageToken=None,
                tableId='mytable',
            ),
            fusiontables.ColumnListAlternate(
                columns=[
                    fusiontables.Column(name='c0'),
                    fusiontables.Column(name='c1'),
                ],
                nextPageToken='x',
            ))
        self.mocked_client.columnalternate.List.Expect(
            fusiontables.FusiontablesColumnListAlternateRequest(
                pageSize=100,
                pageToken='x',
                tableId='mytable',
            ),
            fusiontables.ColumnListAlternate(
                columns=[
                    fusiontables.Column(name='c2'),
                ],
            ))

        client = fusiontables.FusiontablesV1(get_credentials=False)
        request = fusiontables.FusiontablesColumnListAlternateRequest(
            tableId='mytable')
        results = list_pager.YieldFromList(
            client.columnalternate, request,
            batch_size_attribute='pageSize', field='columns')

        self._AssertInstanceSequence(results, 3)
