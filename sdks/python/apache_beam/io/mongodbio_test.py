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

import datetime
import logging
import random
import unittest
from typing import Union
from unittest import TestCase

import mock
from bson import ObjectId
from bson import objectid
from parameterized import parameterized_class
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
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class _MockMongoColl(object):
  """Fake mongodb collection cursor."""
  def __init__(self, docs):
    self.docs = docs

  def __getitem__(self, index):
    return self.docs[index]

  def __len__(self):
    return len(self.docs)

  @staticmethod
  def _make_filter(conditions):
    assert isinstance(conditions, dict)
    checks = []
    for field, value in conditions.items():
      if isinstance(value, dict):
        for op, val in value.items():
          if op == '$gte':
            op = '__ge__'
          elif op == '$lt':
            op = '__lt__'
          else:
            raise Exception('Operator "{0}" not supported.'.format(op))
          checks.append((field, op, val))
      else:
        checks.append((field, '__eq__', value))

    def func(doc):
      for field, op, value in checks:
        if not getattr(doc[field], op)(value):
          return False
      return True

    return func

  def _filter(self, filter):
    match = []
    if not filter:
      return self
    all_filters = []
    if '$and' in filter:
      for item in filter['$and']:
        all_filters.append(self._make_filter(item))
    else:
      all_filters.append(self._make_filter(filter))

    for doc in self.docs:
      if not all(check(doc) for check in all_filters):
        continue
      match.append(doc)

    return match

  @staticmethod
  def _projection(docs, projection=None):
    if projection:
      return [{k: v
               for k, v in doc.items() if k in projection or k == '_id'}
              for doc in docs]
    return docs

  def find(self, filter=None, projection=None, **kwargs):
    return _MockMongoColl(self._projection(self._filter(filter), projection))

  def sort(self, sort_items):
    key, order = sort_items[0]
    self.docs = sorted(
        self.docs, key=lambda x: x[key], reverse=(order != ASCENDING))
    return self

  def limit(self, num):
    return _MockMongoColl(self.docs[0:num])

  def count_documents(self, filter):
    return len(self._filter(filter))

  def aggregate(self, pipeline, **kwargs):
    # Simulate $bucketAuto aggregate pipeline.
    # Example splits doc count for the total of 5 docs:
    #   - 1 bucket:  [5]
    #   - 2 buckets: [3, 2]
    #   - 3 buckets: [2, 2, 1]
    #   - 4 buckets: [2, 1, 1, 1]
    #   - 5 buckets: [1, 1, 1, 1, 1]
    match_step = next((step for step in pipeline if '$match' in step), None)
    bucket_auto_step = next(step for step in pipeline if '$bucketAuto' in step)
    if match_step is None:
      docs = self.docs
    else:
      docs = self.find(filter=match_step['$match'])
    doc_count = len(docs)
    bucket_count = min(bucket_auto_step['$bucketAuto']['buckets'], doc_count)
    # bucket_count ≠ 0
    bucket_len, remainder = divmod(doc_count, bucket_count)
    bucket_sizes = (
        remainder * [bucket_len + 1] +
        (bucket_count - remainder) * [bucket_len])
    buckets = []
    start = 0
    for bucket_size in bucket_sizes:
      stop = start + bucket_size
      if stop >= doc_count:
        # MongoDB: the last bucket's 'max' is inclusive
        stop = doc_count - 1
        count = stop - start + 1
      else:
        # non-last bucket's 'max' is exclusive and == next bucket's 'min'
        count = stop - start
      buckets.append({
          '_id': {
              'min': docs[start]['_id'],
              'max': docs[stop]['_id'],
          },
          'count': count
      })
      start = stop

    return buckets


class _MockMongoDb(object):
  """Fake Mongo Db."""
  def __init__(self, docs):
    self.docs = docs

  def __getitem__(self, coll_name):
    return _MockMongoColl(self.docs)

  def command(self, command, *args, **kwargs):
    if command == 'collstats':
      return {'size': 5 * 1024 * 1024, 'avgObjSize': 1 * 1024 * 1024}
    if command == 'splitVector':
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


class _MockMongoClient:
  def __init__(self, docs):
    self.docs = docs

  def __getitem__(self, db_name):
    return _MockMongoDb(self.docs)

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    pass


# Generate test data for MongoDB collections of different types
OBJECT_IDS = [
    objectid.ObjectId.from_datetime(
        datetime.datetime(year=2020, month=i + 1, day=i + 1)) for i in range(5)
]

INT_IDS = [n for n in range(5)]  # [0, 1, 2, 3, 4]

STR_IDS_1 = [str(n) for n in range(5)]  # ['0', '1', '2', '3', '4']

# ['aaaaa', 'bbbbb', 'ccccc', 'ddddd', 'eeeee']
STR_IDS_2 = [chr(97 + n) * 5 for n in range(5)]

# ['AAAAAAAAAAAAAAAAAAAA', 'BBBBBBBBBBBBBBBBBBBB', ..., 'EEEEEEEEEEEEEEEEEEEE']
STR_IDS_3 = [chr(65 + n) * 20 for n in range(5)]


@parameterized_class(('bucket_auto', '_ids', 'min_id', 'max_id'),
                     [
                         (
                             None,
                             OBJECT_IDS,
                             _ObjectIdHelper.int_to_id(0),
                             _ObjectIdHelper.int_to_id(2**96 - 1)),
                         (
                             True,
                             OBJECT_IDS,
                             _ObjectIdHelper.int_to_id(0),
                             _ObjectIdHelper.int_to_id(2**96 - 1)),
                         (
                             None,
                             INT_IDS,
                             0,
                             2**96 - 1,
                         ),
                         (
                             True,
                             INT_IDS,
                             0,
                             2**96 - 1,
                         ),
                         (
                             None,
                             STR_IDS_1,
                             chr(0),
                             chr(0x10ffff),
                         ),
                         (
                             True,
                             STR_IDS_1,
                             chr(0),
                             chr(0x10ffff),
                         ),
                         (
                             None,
                             STR_IDS_2,
                             chr(0),
                             chr(0x10ffff),
                         ),
                         (
                             True,
                             STR_IDS_2,
                             chr(0),
                             chr(0x10ffff),
                         ),
                         (
                             None,
                             STR_IDS_3,
                             chr(0),
                             chr(0x10ffff),
                         ),
                         (
                             True,
                             STR_IDS_3,
                             chr(0),
                             chr(0x10ffff),
                         ),
                     ])
class MongoSourceTest(unittest.TestCase):
  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def setUp(self, mock_client):
    self._docs = [{'_id': self._ids[i], 'x': i} for i in range(len(self._ids))]
    mock_client.return_value = _MockMongoClient(self._docs)

    self.mongo_source = self._create_source(bucket_auto=self.bucket_auto)

  @staticmethod
  def _create_source(filter=None, bucket_auto=None):
    kwargs = {}
    if filter is not None:
      kwargs['filter'] = filter
    if bucket_auto is not None:
      kwargs['bucket_auto'] = bucket_auto
    return _BoundedMongoSource('mongodb://test', 'testdb', 'testcoll', **kwargs)

  def _increment_id(
      self,
      _id: Union[ObjectId, int, str],
      inc: int,
  ) -> Union[ObjectId, int, str]:
    """Helper method to increment `_id` of different types."""

    if isinstance(_id, ObjectId):
      return _ObjectIdHelper.increment_id(_id, inc)

    if isinstance(_id, int):
      return _id + inc

    if isinstance(_id, str):
      index = self._ids.index(_id) + inc
      if index <= 0:
        return self._ids[0]
      if index >= len(self._ids):
        return self._ids[-1]
      return self._ids[index]

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_estimate_size(self, mock_client):
    mock_client.return_value = _MockMongoClient(self._docs)
    self.assertEqual(self.mongo_source.estimate_size(), 5 * 1024 * 1024)

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_estimate_average_document_size(self, mock_client):
    mock_client.return_value = _MockMongoClient(self._docs)
    self.assertEqual(
        self.mongo_source._estimate_average_document_size(), 1 * 1024 * 1024)

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_split(self, mock_client):
    mock_client.return_value = _MockMongoClient(self._docs)
    for size_mb, expected_split_count in [(0.5, 5), (1, 5), (2, 3), (10, 1)]:
      size = size_mb * 1024 * 1024
      splits = list(
          self.mongo_source.split(
              start_position=None, stop_position=None,
              desired_bundle_size=size))

      self.assertEqual(len(splits), expected_split_count)
      reference_info = (self.mongo_source, None, None)
      sources_info = ([
          (split.source, split.start_position, split.stop_position)
          for split in splits
      ])
      source_test_utils.assert_sources_equal_reference_source(
          reference_info, sources_info)

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_split_single_document(self, mock_client):
    mock_client.return_value = _MockMongoClient(self._docs[0:1])
    for size_mb in [1, 5]:
      size = size_mb * 1024 * 1024
      splits = list(
          self.mongo_source.split(
              start_position=None, stop_position=None,
              desired_bundle_size=size))
      self.assertEqual(len(splits), 1)
      _id = self._docs[0]['_id']
      assert _id == splits[0].start_position
      assert _id <= splits[0].stop_position
      if isinstance(_id, (ObjectId, int)):
        # We can unambiguously determine next `_id`
        assert self._increment_id(_id, 1) == splits[0].stop_position

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_split_no_documents(self, mock_client):
    mock_client.return_value = _MockMongoClient([])
    with self.assertRaises(ValueError) as cm:
      list(
          self.mongo_source.split(
              start_position=None,
              stop_position=None,
              desired_bundle_size=1024 * 1024))
    self.assertEqual(str(cm.exception), 'Empty Mongodb collection')

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_split_filtered(self, mock_client):
    # filtering 2 documents: 2 <= 'x' < 4
    filtered_mongo_source = self._create_source(
        filter={'x': {
            '$gte': 2, '$lt': 4
        }}, bucket_auto=self.bucket_auto)

    mock_client.return_value = _MockMongoClient(self._docs)
    for size_mb, (bucket_auto_count, split_vector_count) in [(1, (2, 5)),
                                                             (2, (1, 3)),
                                                             (10, (1, 1))]:
      size = size_mb * 1024 * 1024
      splits = list(
          filtered_mongo_source.split(
              start_position=None, stop_position=None,
              desired_bundle_size=size))

      if self.bucket_auto:
        self.assertEqual(len(splits), bucket_auto_count)
      else:
        # Note: splitVector mode does not respect filter
        self.assertEqual(len(splits), split_vector_count)
      reference_info = (
          filtered_mongo_source, self._docs[2]['_id'], self._docs[4]['_id'])
      sources_info = ([
          (split.source, split.start_position, split.stop_position)
          for split in splits
      ])
      source_test_utils.assert_sources_equal_reference_source(
          reference_info, sources_info)

  @mock.patch('apache_beam.io.mongodbio.MongoClient')
  def test_split_filtered_empty(self, mock_client):
    # filtering doesn't match any documents
    filtered_mongo_source = self._create_source(
        filter={'x': {
            '$lt': 0
        }}, bucket_auto=self.bucket_auto)

    mock_client.return_value = _MockMongoClient(self._docs)
    for size_mb, (bucket_auto_count, split_vector_count) in [(1, (1, 5)),
                                                             (2, (1, 3)),
                                                             (10, (1, 1))]:
      size = size_mb * 1024 * 1024
      splits = list(
          filtered_mongo_source.split(
              start_position=None, stop_position=None,
              desired_bundle_size=size))

      if self.bucket_auto:
        # Note: if filter matches no docs - one split covers entire range
        self.assertEqual(len(splits), bucket_auto_count)
      else:
        # Note: splitVector mode does not respect filter
        self.assertEqual(len(splits), split_vector_count)
      reference_info = (
          filtered_mongo_source,
          # range to match no documents:
          self._increment_id(self._docs[-1]['_id'], 1),
          self._increment_id(self._docs[-1]['_id'], 2),
      )
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
    if self._ids == OBJECT_IDS:
      self.assertIsInstance(
          self.mongo_source.get_range_tracker(None, None),
          _ObjectIdRangeTracker,
      )
    elif self._ids == INT_IDS:
      self.assertIsInstance(
          self.mongo_source.get_range_tracker(None, None),
          OffsetRangeTracker,
      )
    elif self._ids == STR_IDS_1:
      self.assertIsInstance(
          self.mongo_source.get_range_tracker(None, None),
          LexicographicKeyRangeTracker,
      )

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
            'start': self.min_id,  # smallest possible id
            'stop': self._ids[2],
            'expected': self._docs[0:2]
        },
        {
            # range covers from the third to last documents
            'start': self._ids[2],
            'stop': self.max_id,  # largest possible id
            'expected': self._docs[2:]
        },
        {
            # range covers all documents
            'start': self.min_id,
            'stop': self.max_id,
            'expected': self._docs
        },
        {
            # range doesn't include any document
            'start': self._increment_id(self._ids[2], 1),
            'stop': self._increment_id(self._ids[3], -1),
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
    self.assertTrue('database' in data)
    self.assertTrue('collection' in data)

  def test_range_is_not_splittable(self):
    self.assertTrue(
        self.mongo_source._range_is_not_splittable(
            _ObjectIdHelper.int_to_id(1),
            _ObjectIdHelper.int_to_id(1),
        ))
    self.assertTrue(
        self.mongo_source._range_is_not_splittable(
            _ObjectIdHelper.int_to_id(1),
            _ObjectIdHelper.int_to_id(2),
        ))
    self.assertFalse(
        self.mongo_source._range_is_not_splittable(
            _ObjectIdHelper.int_to_id(1),
            _ObjectIdHelper.int_to_id(3),
        ))

    self.assertTrue(self.mongo_source._range_is_not_splittable(0, 0))
    self.assertTrue(self.mongo_source._range_is_not_splittable(0, 1))
    self.assertFalse(self.mongo_source._range_is_not_splittable(0, 2))

    self.assertTrue(self.mongo_source._range_is_not_splittable("AAA", "AAA"))
    self.assertFalse(
        self.mongo_source._range_is_not_splittable("AAA", "AAA\x00"))
    self.assertFalse(self.mongo_source._range_is_not_splittable("AAA", "AAB"))


@parameterized_class(('bucket_auto', ), [(False, ), (True, )])
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
          projection=projection,
          bucket_auto=self.bucket_auto)
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
    _id = objectid.ObjectId()
    docs = [{'x': 1, '_id': _id}]
    expected_update = [
        ReplaceOne({'_id': _id}, {
            'x': 1, '_id': _id
        }, True, None)
    ]
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
    for (_id, number) in test_cases:
      self.assertEqual(_id, _ObjectIdHelper.int_to_id(number))
      self.assertEqual(number, _ObjectIdHelper.id_to_int(_id))

    # random tests
    for _ in range(100):
      _id = objectid.ObjectId()
      number = int(_id.binary.hex(), 16)
      self.assertEqual(_id, _ObjectIdHelper.int_to_id(number))
      self.assertEqual(number, _ObjectIdHelper.id_to_int(_id))

  def test_increment_id(self):
    test_cases = [
        (
            objectid.ObjectId("000000000000000100000000"),
            objectid.ObjectId("0000000000000000ffffffff"),
        ),
        (
            objectid.ObjectId("000000010000000000000000"),
            objectid.ObjectId("00000000ffffffffffffffff"),
        ),
    ]
    for first, second in test_cases:
      self.assertEqual(second, _ObjectIdHelper.increment_id(first, -1))
      self.assertEqual(first, _ObjectIdHelper.increment_id(second, 1))

    for _ in range(100):
      _id = objectid.ObjectId()
      self.assertLess(_id, _ObjectIdHelper.increment_id(_id, 1))
      self.assertGreater(_id, _ObjectIdHelper.increment_id(_id, -1))


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
      _id = _ObjectIdHelper.int_to_id(pos - start_int)
      desired_fraction = (pos - start_int) / (stop_int - start_int)
      self.assertAlmostEqual(
          tracker.position_to_fraction(_id, start, stop),
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
