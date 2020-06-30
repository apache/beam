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

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division

import datetime
import logging
import random
import sys
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
  """Fake mongodb collection cursor."""
  def __init__(self, docs):
    self.docs = docs

  def _filter(self, filter, projection):
    projection = [] if projection is None else projection
    match = []
    if not filter:
      return self
    if '$and' not in filter or not filter['$and']:
      return self
    start = filter['$and'][1]['_id'].get('$gte')
    end = filter['$and'][1]['_id'].get('$lt')
    assert start is not None
    assert end is not None
    for doc in self.docs:
      if start and doc['_id'] < start:
        continue
      if end and doc['_id'] >= end:
        continue
      if len(projection) > 0:
        doc = {k: v for k, v in doc.items() if k in projection or k == '_id'}
      match.append(doc)
    return match

  def find(self, filter=None, projection=None, **kwargs):
    return _MockMongoColl(self._filter(filter, projection))

  def sort(self, sort_items):
    key, order = sort_items[0]
    self.docs = sorted(
        self.docs, key=lambda x: x[key], reverse=(order != ASCENDING))
    return self

  def limit(self, num):
    return _MockMongoColl(self.docs[0:num])

  def count_documents(self, filter):
    return len(self._filter(filter))

  def __getitem__(self, index):
    return self.docs[index]


class _MockMongoDb(object):
  """Fake Mongo Db."""
  def __init__(self, docs):
    self.docs = docs

  def __getitem__(self, coll_name):
    return _MockMongoColl(self.docs)

  def command(self, command, *args, **kwargs):
    if command == 'collstats':
      return {'size': 5, 'avgSize': 1}
    elif command == 'splitVector':
      return self.get_split_keys(command, *args, **kwargs)

  def get_split_keys(self, command, ns, min, max, maxChunkSize, **kwargs):
    # simulate mongo db splitVector command, return split keys base on chunk
    # size, assuming every doc is of size 1mb
    start_id = min['_id']
    end_id = max['_id']
    if start_id >= end_id:
      return []
    start_index = 0
    end_index = 0
    # get split range of [min, max]
    for doc in self.docs:
      if doc['_id'] < start_id:
        start_index += 1
      if doc['_id'] <= end_id:
        end_index += 1
      else:
        break
    # Return ids of elements in the range with chunk size skip and exclude
    # head element. For simplicity of tests every document is considered 1Mb
    # by default.
    return {
        'splitKeys': [{
            '_id': x['_id']
        } for x in self.docs[start_index:end_index:maxChunkSize]][1:]
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

    self.mongo_source = _BoundedMongoSource(
        'mongodb://test', 'testdb', 'testcoll')

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_estimate_size(self, mock_client):
    mock_client.return_value = _MockMongoClient(self._docs)
    self.assertEqual(self.mongo_source.estimate_size(), 5)

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_split(self, mock_client):
    mock_client.return_value = _MockMongoClient(self._docs)
    for size in [i * 1024 * 1024 for i in (1, 2, 10)]:
      splits = list(
          self.mongo_source.split(
              start_position=None, stop_position=None,
              desired_bundle_size=size))

      reference_info = (self.mongo_source, None, None)
      sources_info = ([
          (split.source, split.start_position, split.stop_position)
          for split in splits
      ])
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
    mock_client.return_value = _MockMongoClient(self._docs)
    self.assertIsInstance(
        self.mongo_source.get_range_tracker(None, None), _ObjectIdRangeTracker)

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_read(self, mock_client):
    mock_tracker = mock.MagicMock()
    test_cases = [
        {
            # range covers the first(inclusive) to third(exclusive) documents
            'start': self._ids[0],
            'stop': self._ids[2],
            'expected': self._docs[0:2]
        },
        {
            # range covers from the first to the third documents
            'start': _ObjectIdHelper.int_to_id(0),  # smallest possible id
            'stop': self._ids[2],
            'expected': self._docs[0:2]
        },
        {
            # range covers from the third to last documents
            'start': self._ids[2],
            'stop': _ObjectIdHelper.int_to_id(2**96 - 1),  # largest possible id
            'expected': self._docs[2:]
        },
        {
            # range covers all documents
            'start': _ObjectIdHelper.int_to_id(0),
            'stop': _ObjectIdHelper.int_to_id(2**96 - 1),
            'expected': self._docs
        },
        {
            # range doesn't include any document
            'start': _ObjectIdHelper.increment_id(self._ids[2], 1),
            'stop': _ObjectIdHelper.increment_id(self._ids[3], -1),
            'expected': []
        },
    ]
    mock_client.return_value = _MockMongoClient(self._docs)
    for case in test_cases:
      mock_tracker.start_position.return_value = case['start']
      mock_tracker.stop_position.return_value = case['stop']
      result = list(self.mongo_source.read(mock_tracker))
      self.assertListEqual(case['expected'], result)

  def test_display_data(self):
    data = self.mongo_source.display_data()
    self.assertTrue('uri' in data)
    self.assertTrue('database' in data)
    self.assertTrue('collection' in data)


class ReadFromMongoDBTest(unittest.TestCase):
  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_read_from_mongodb(self, mock_client):
    documents = [{
        '_id': objectid.ObjectId(), 'x': i, 'selected': 1, 'unselected': 2
    } for i in range(3)]
    mock_client.return_value = _MockMongoClient(documents)

    projection = ['x', 'selected']
    projected_documents = [{
        k: v
        for k, v in e.items() if k in projection or k == '_id'
    } for e in documents]

    with TestPipeline() as p:
      docs = p | 'ReadFromMongoDB' >> ReadFromMongoDB(
          uri='mongodb://test',
          db='db',
          coll='collection',
          projection=projection)
      assert_that(docs, equal_to(projected_documents))


class GenerateObjectIdFnTest(unittest.TestCase):
  def test_process(self):
    with TestPipeline() as p:
      output = (
          p | "Create" >> beam.Create([{
              'x': 1
          }, {
              'x': 2, '_id': 123
          }])
          | "Generate ID" >> beam.ParDo(_GenerateObjectIdFn())
          | "Check" >> beam.Map(lambda x: '_id' in x))
      assert_that(output, equal_to([True] * 2))


class WriteMongoFnTest(unittest.TestCase):
  @mock.patch('apache_beam.io.mongodbio._MongoSink')
  def test_process(self, mock_sink):
    docs = [{'x': 1}, {'x': 2}, {'x': 3}]
    with TestPipeline() as p:
      _ = (
          p | "Create" >> beam.Create(docs)
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
    self.assertTrue(
        mock_client.return_value.__getitem__.return_value.__getitem__.
        return_value.bulk_write.called)


class WriteToMongoDBTest(unittest.TestCase):
  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_write_to_mongodb_with_existing_id(self, mock_client):
    id = objectid.ObjectId()
    docs = [{'x': 1, '_id': id}]
    expected_update = [ReplaceOne({'_id': id}, {'x': 1, '_id': id}, True, None)]
    with TestPipeline() as p:
      _ = (
          p | "Create" >> beam.Create(docs)
          | "Write" >> WriteToMongoDB(db='test', coll='test'))
      p.run()
      mock_client.return_value.__getitem__.return_value.__getitem__. \
        return_value.bulk_write.assert_called_with(expected_update)

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_write_to_mongodb_with_generated_id(self, mock_client):
    docs = [{'x': 1}]
    expected_update = [
        ReplaceOne({'_id': mock.ANY}, {
            'x': 1, '_id': mock.ANY
        }, True, None)
    ]
    with TestPipeline() as p:
      _ = (
          p | "Create" >> beam.Create(docs)
          | "Write" >> WriteToMongoDB(db='test', coll='test'))
      p.run()
      mock_client.return_value.__getitem__.return_value.__getitem__. \
        return_value.bulk_write.assert_called_with(expected_update)


class ObjectIdHelperTest(TestCase):
  def test_conversion(self):
    test_cases = [
        (objectid.ObjectId('000000000000000000000000'), 0),
        (objectid.ObjectId('000000000000000100000000'), 2**32),
        (objectid.ObjectId('0000000000000000ffffffff'), 2**32 - 1),
        (objectid.ObjectId('000000010000000000000000'), 2**64),
        (objectid.ObjectId('00000000ffffffffffffffff'), 2**64 - 1),
        (objectid.ObjectId('ffffffffffffffffffffffff'), 2**96 - 1),
    ]
    for (id, number) in test_cases:
      self.assertEqual(id, _ObjectIdHelper.int_to_id(number))
      self.assertEqual(number, _ObjectIdHelper.id_to_int(id))

    # random tests
    for _ in range(100):
      id = objectid.ObjectId()
      if sys.version_info[0] < 3:
        number = int(id.binary.encode('hex'), 16)
      else:  # PY3
        number = int(id.binary.hex(), 16)
      self.assertEqual(id, _ObjectIdHelper.int_to_id(number))
      self.assertEqual(number, _ObjectIdHelper.id_to_int(id))

  def test_increment_id(self):
    test_cases = [
        (
            objectid.ObjectId('000000000000000100000000'),
            objectid.ObjectId('0000000000000000ffffffff')),
        (
            objectid.ObjectId('000000010000000000000000'),
            objectid.ObjectId('00000000ffffffffffffffff')),
    ]
    for (first, second) in test_cases:
      self.assertEqual(second, _ObjectIdHelper.increment_id(first, -1))
      self.assertEqual(first, _ObjectIdHelper.increment_id(second, 1))

    for _ in range(100):
      id = objectid.ObjectId()
      self.assertLess(id, _ObjectIdHelper.increment_id(id, 1))
      self.assertGreater(id, _ObjectIdHelper.increment_id(id, -1))


class ObjectRangeTrackerTest(TestCase):
  def test_fraction_position_conversion(self):
    start_int = 0
    stop_int = 2**96 - 1
    start = _ObjectIdHelper.int_to_id(start_int)
    stop = _ObjectIdHelper.int_to_id(stop_int)
    test_cases = ([start_int, stop_int, 2**32, 2**32 - 1, 2**64, 2**64 - 1] +
                  [random.randint(start_int, stop_int) for _ in range(100)])
    tracker = _ObjectIdRangeTracker()
    for pos in test_cases:
      id = _ObjectIdHelper.int_to_id(pos - start_int)
      desired_fraction = (pos - start_int) / (stop_int - start_int)
      self.assertAlmostEqual(
          tracker.position_to_fraction(id, start, stop),
          desired_fraction,
          places=20)

      convert_id = tracker.fraction_to_position(
          (pos - start_int) / (stop_int - start_int), start, stop)
      # due to precision loss, the convert fraction is only gonna be close to
      # original fraction.
      convert_fraction = tracker.position_to_fraction(convert_id, start, stop)

      self.assertGreater(convert_id, start)
      self.assertLess(convert_id, stop)
      self.assertAlmostEqual(convert_fraction, desired_fraction, places=20)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
