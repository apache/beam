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

from __future__ import absolute_import

import datetime
import logging
import unittest
from unittest import TestCase

import mock
from bson import objectid
from pymongo import ASCENDING
from pymongo import ReplaceOne

import apache_beam as beam
from apache_beam.io import ReadFromMongoDB
from apache_beam.io import WriteToMongoDB
from apache_beam.io import source_test_utils
from apache_beam.io.mongodbio import _BoundedMongoSource
from apache_beam.io.mongodbio import _GenerateObjectIdFn
from apache_beam.io.mongodbio import _MongoSink
from apache_beam.io.mongodbio import _ObjectIdHelper
from apache_beam.io.mongodbio import _ObjectIdRangeTracker
from apache_beam.io.mongodbio import _WriteMongoFn
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class _MockMongoColl(object):
  def __init__(self, docs):
    self.docs = docs

  def _filter(self, filter):
    match = []
    if not filter:
      return self
    start = filter['_id'].get('$gte')
    end = filter['_id'].get('$lt')
    for doc in self.docs:
      if start and doc['_id'] < start:
        continue
      if end and doc['_id'] >= end:
        continue
      match.append(doc)
    return match

  def find(self, filter=None, **kwargs):
    return _MockMongoColl(self._filter(filter))

  def sort(self, sort_items):
    key, order = sort_items[0]
    self.docs = sorted(self.docs,
                       key=lambda x: x[key],
                       reverse=(order != ASCENDING))
    return self

  def limit(self, num):
    return _MockMongoColl(self.docs[0:num])

  def count_documents(self, filter):
    return len(self._filter(filter))

  def __getitem__(self, index):
    return self.docs[index]


class _MockMongoDb(object):
  def __init__(self, docs):
    self.docs = docs

  def __getitem__(self, coll_name):
    return _MockMongoColl(self.docs)

  def command(self, command, *args, **kwargs):
    if command == 'collstats':
      return {'size': 5, 'avgSize': 1}
    elif command == 'splitVector':
      return self.get_split_key(command, *args, **kwargs)

  def get_split_key(self, command, ns, min, max, maxChunkSize, **kwargs):
    # assuming every doc is of size 1mb
    start_id = min['_id']
    end_id = max['_id']
    if start_id >= end_id:
      return []
    start_index = 0
    end_index = 0
    for doc in self.docs:
      if doc['_id'] < start_id:
        start_index += 1
      if doc['_id'] <= end_id:
        end_index += 1
      else:
        break
    return {
      'splitKeys':
        [x['_id'] for x in self.docs[start_index:end_index:maxChunkSize]][1:]
    }


class _MockMongoClient(object):
  def __init__(self, docs):
    self.docs = docs

  def __getitem__(self, db_name):
    return _MockMongoDb(self.docs)

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    pass


class MongoSourceTest(unittest.TestCase):
  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def setUp(self, mock_client):
    self._ids = [
      objectid.ObjectId.from_datetime(
        datetime.datetime(year=2020, month=i + 1, day=i + 1))
      for i in range(5)
    ]
    self._docs = [{'_id': self._ids[i], 'x': i} for i in range(len(self._ids))]
    mock_client.return_value = _MockMongoClient(self._docs)

    self.mongo_source = _BoundedMongoSource('mongodb://test', 'testdb',
                                            'testcoll')

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_estimate_size(self, mock_client):
    mock_client.return_value = _MockMongoClient(self._docs)
    self.assertEqual(self.mongo_source.estimate_size(), 5)

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_split(self, mock_client):
    mock_client.return_value = _MockMongoClient(self._docs)
    for size in [i * 1024 * 1024 for i in (1, 2, 10)]:
      splits = list(
        self.mongo_source.split(start_position=None,
                                stop_position=None,
                                desired_bundle_size=size))

      reference_info = (self.mongo_source, None, None)
      sources_info = ([(split.source, split.start_position, split.stop_position)
                       for split in splits])
      source_test_utils.assert_sources_equal_reference_source(
        reference_info, sources_info)

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_dynamic_work_rebalancing(self, mock_client):
    mock_client.return_value = _MockMongoClient(self._docs)
    splits = list(
      self.mongo_source.split(desired_bundle_size=3000 * 1024 * 1024))
    assert len(splits) == 1
    source_test_utils.assert_split_at_fraction_exhaustive(
      splits[0].source, splits[0].start_position, splits[0].stop_position)

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_get_range_tracker(self, mock_client):
    mock_client.return_value.__enter__.return_value.__getitem__.return_value \
      .__getitem__.return_value = _MockMongoColl(self._docs)
    self.assertIsInstance(self.mongo_source.get_range_tracker(None, None),
                          _ObjectIdRangeTracker)

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_read(self, mock_client):
    mock_tracker = mock.MagicMock()
    mock_tracker.start_position.return_value = self._ids[0]
    mock_tracker.stop_position.return_value = self._ids[2]

    mock_client.return_value.__enter__.return_value.__getitem__.return_value \
      .__getitem__.return_value = _MockMongoColl(self._docs)

    result = list(self.mongo_source.read(mock_tracker))
    self.assertListEqual(self._docs[0:2], result)

  def test_display_data(self):
    data = self.mongo_source.display_data()
    self.assertTrue('uri' in data)
    self.assertTrue('database' in data)
    self.assertTrue('collection' in data)


class ReadFromMongoDBTest(unittest.TestCase):
  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_read_from_mongodb(self, mock_client):
    documents = [{'_id': objectid.ObjectId(), 'x': i} for i in range(3)]
    # use index value as id
    mock_client.return_value.__enter__.return_value.__getitem__.return_value \
      .__getitem__.return_value = _MockMongoColl(documents)

    with TestPipeline() as p:
      docs = p | 'ReadFromMongoDB' >> ReadFromMongoDB(
        uri='mongodb://test', db='db', coll='collection')
      assert_that(docs, equal_to(documents))


class GenerateObjectIdFnTest(unittest.TestCase):
  def test_process(self):
    with TestPipeline() as p:
      output = (p | "Create" >> beam.Create([{
        'x': 1
      }, {
        'x': 2,
        '_id': 123
      }])
                | "Generate ID" >> beam.ParDo(_GenerateObjectIdFn())
                | "Check" >> beam.Map(lambda x: '_id' in x))
      assert_that(output, equal_to([True] * 2))


class WriteMongoFnTest(unittest.TestCase):
  @mock.patch('apache_beam.io.mongodbio._MongoSink')
  def test_process(self, mock_sink):
    docs = [{'x': 1}, {'x': 2}, {'x': 3}]
    with TestPipeline() as p:
      _ = (p | "Create" >> beam.Create(docs)
           | "Write" >> beam.ParDo(_WriteMongoFn(batch_size=2)))
      p.run()

      self.assertEqual(
        2, mock_sink.return_value.__enter__.return_value.write.call_count)

  def test_display_data(self):
    data = _WriteMongoFn(batch_size=10).display_data()
    self.assertEqual(10, data['batch_size'])


class MongoSinkTest(unittest.TestCase):
  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_write(self, mock_client):
    docs = [{'x': 1}, {'x': 2}, {'x': 3}]
    _MongoSink(uri='test', db='test', coll='test').write(docs)
    self.assertTrue(mock_client.return_value.__getitem__.return_value.
                    __getitem__.return_value.bulk_write.called)


class WriteToMongoDBTest(unittest.TestCase):
  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_write_to_mongodb_with_existing_id(self, mock_client):
    id = objectid.ObjectId()
    docs = [{'x': 1, '_id': id}]
    expected_update = [ReplaceOne({'_id': id}, {'x': 1, '_id': id}, True, None)]
    with TestPipeline() as p:
      _ = (p | "Create" >> beam.Create(docs)
           | "Write" >> WriteToMongoDB(db='test', coll='test'))
      p.run()
      mock_client.return_value.__getitem__.return_value.__getitem__. \
        return_value.bulk_write.assert_called_with(expected_update)

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_write_to_mongodb_with_generated_id(self, mock_client):
    docs = [{'x': 1}]
    expected_update = [
      ReplaceOne({'_id': mock.ANY}, {
        'x': 1,
        '_id': mock.ANY
      }, True, None)
    ]
    with TestPipeline() as p:
      _ = (p | "Create" >> beam.Create(docs)
           | "Write" >> WriteToMongoDB(db='test', coll='test'))
      p.run()
      mock_client.return_value.__getitem__.return_value.__getitem__. \
        return_value.bulk_write.assert_called_with(expected_update)

class ObjectIdHelperTest(TestCase):

  def test_conversion(self):
    test_cases = [
      (objectid.ObjectId('000000000000000000000000'), 0),
      (objectid.ObjectId('000000000000000100000000'), 2 ** 32),
      (objectid.ObjectId('0000000000000000ffffffff'), 2 ** 32 - 1),
      (objectid.ObjectId('000000010000000000000000'), 2 ** 64),
      (objectid.ObjectId('00000000ffffffffffffffff'), 2 ** 64 - 1),
      (objectid.ObjectId('ffffffffffffffffffffffff'), 2 ** 96 - 1),
    ]
    for (id, number) in test_cases:
      self.assertEqual(id, _ObjectIdHelper.int_to_id(number))
      self.assertEqual(number, _ObjectIdHelper.id_to_int(id))

    # random tests
    for i in range(100):
      id = objectid.ObjectId()
      number = int(id.binary.encode('hex'), 16)
      self.assertEqual(id, _ObjectIdHelper.int_to_id(number))
      self.assertEqual(number, _ObjectIdHelper.id_to_int(id))


  def test_increment_id(self):
    self.fail()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
