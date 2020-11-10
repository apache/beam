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

"""This module implements IO classes to read and write data on MongoDB.


Read from MongoDB
-----------------
:class:`ReadFromMongoDB` is a ``PTransform`` that reads from a configured
MongoDB source and returns a ``PCollection`` of dict representing MongoDB
documents.
To configure MongoDB source, the URI to connect to MongoDB server, database
name, collection name needs to be provided.

Example usage::

  pipeline | ReadFromMongoDB(uri='mongodb://localhost:27017',
                             db='testdb',
                             coll='input')


Write to MongoDB:
-----------------
:class:`WriteToMongoDB` is a ``PTransform`` that writes MongoDB documents to
configured sink, and the write is conducted through a mongodb bulk_write of
``ReplaceOne`` operations. If the document's _id field already existed in the
MongoDB collection, it results in an overwrite, otherwise, a new document
will be inserted.

Example usage::

  pipeline | WriteToMongoDB(uri='mongodb://localhost:27017',
                            db='testdb',
                            coll='output',
                            batch_size=10)


No backward compatibility guarantees. Everything in this module is experimental.
"""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division

import json
import logging
import struct

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.io.range_trackers import OrderedPositionRangeTracker
from apache_beam.transforms import DoFn
from apache_beam.transforms import PTransform
from apache_beam.transforms import Reshuffle
from apache_beam.utils.annotations import experimental

_LOGGER = logging.getLogger(__name__)

try:
  # Mongodb has its own bundled bson, which is not compatible with bson pakcage.
  # (https://github.com/py-bson/bson/issues/82). Try to import objectid and if
  # it fails because bson package is installed, MongoDB IO will not work but at
  # least rest of the SDK will work.
  from bson import objectid

  # pymongo also internally depends on bson.
  from pymongo import ASCENDING
  from pymongo import DESCENDING
  from pymongo import MongoClient
  from pymongo import ReplaceOne
except ImportError:
  objectid = None
  _LOGGER.warning("Could not find a compatible bson package.")

__all__ = ['ReadFromMongoDB', 'WriteToMongoDB']


@experimental()
class ReadFromMongoDB(PTransform):
  """A ``PTransform`` to read MongoDB documents into a ``PCollection``.
  """
  def __init__(
      self,
      uri='mongodb://localhost:27017',
      db=None,
      coll=None,
      filter=None,
      projection=None,
      extra_client_params=None):
    """Initialize a :class:`ReadFromMongoDB`

    Args:
      uri (str): The MongoDB connection string following the URI format
      db (str): The MongoDB database name
      coll (str): The MongoDB collection name
      filter: A `bson.SON
        <https://api.mongodb.com/python/current/api/bson/son.html>`_ object
        specifying elements which must be present for a document to be included
        in the result set
      projection: A list of field names that should be returned in the result
        set or a dict specifying the fields to include or exclude
      extra_client_params(dict): Optional `MongoClient
        <https://api.mongodb.com/python/current/api/pymongo/mongo_client.html>`_
        parameters

    Returns:
      :class:`~apache_beam.transforms.ptransform.PTransform`

    """
    if extra_client_params is None:
      extra_client_params = {}
    if not isinstance(db, str):
      raise ValueError('ReadFromMongDB db param must be specified as a string')
    if not isinstance(coll, str):
      raise ValueError(
          'ReadFromMongDB coll param must be specified as a '
          'string')
    self._mongo_source = _BoundedMongoSource(
        uri=uri,
        db=db,
        coll=coll,
        filter=filter,
        projection=projection,
        extra_client_params=extra_client_params)

  def expand(self, pcoll):
    return pcoll | iobase.Read(self._mongo_source)


class _BoundedMongoSource(iobase.BoundedSource):
  def __init__(
      self,
      uri=None,
      db=None,
      coll=None,
      filter=None,
      projection=None,
      extra_client_params=None):
    if extra_client_params is None:
      extra_client_params = {}
    if filter is None:
      filter = {}
    self.uri = uri
    self.db = db
    self.coll = coll
    self.filter = filter
    self.projection = projection
    self.spec = extra_client_params

  def estimate_size(self):
    with MongoClient(self.uri, **self.spec) as client:
      return client[self.db].command('collstats', self.coll).get('size')

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    start_position, stop_position = self._replace_none_positions(
        start_position, stop_position)

    desired_bundle_size_in_mb = desired_bundle_size // 1024 // 1024

    # for desired bundle size, if desired chunk size smaller than 1mb, use
    # mongodb default split size of 1mb.
    if desired_bundle_size_in_mb < 1:
      desired_bundle_size_in_mb = 1

    split_keys = self._get_split_keys(
        desired_bundle_size_in_mb, start_position, stop_position)

    bundle_start = start_position
    for split_key_id in split_keys:
      if bundle_start >= stop_position:
        break
      bundle_end = min(stop_position, split_key_id['_id'])
      yield iobase.SourceBundle(
          weight=desired_bundle_size_in_mb,
          source=self,
          start_position=bundle_start,
          stop_position=bundle_end)
      bundle_start = bundle_end
    # add range of last split_key to stop_position
    if bundle_start < stop_position:
      yield iobase.SourceBundle(
          weight=desired_bundle_size_in_mb,
          source=self,
          start_position=bundle_start,
          stop_position=stop_position)

  def get_range_tracker(self, start_position, stop_position):
    start_position, stop_position = self._replace_none_positions(
        start_position, stop_position)
    return _ObjectIdRangeTracker(start_position, stop_position)

  def read(self, range_tracker):
    with MongoClient(self.uri, **self.spec) as client:
      all_filters = self._merge_id_filter(range_tracker)
      docs_cursor = client[self.db][self.coll].find(
          filter=all_filters,
          projection=self.projection).sort([('_id', ASCENDING)])
      for doc in docs_cursor:
        if not range_tracker.try_claim(doc['_id']):
          return
        yield doc

  def display_data(self):
    res = super(_BoundedMongoSource, self).display_data()
    res['uri'] = self.uri
    res['database'] = self.db
    res['collection'] = self.coll
    res['filter'] = json.dumps(self.filter)
    res['projection'] = str(self.projection)
    res['mongo_client_spec'] = json.dumps(self.spec)
    return res

  def _get_split_keys(self, desired_chunk_size_in_mb, start_pos, end_pos):
    # calls mongodb splitVector command to get document ids at split position
    if start_pos >= end_pos:
      # single document not splittable
      return []
    with MongoClient(self.uri, **self.spec) as client:
      name_space = '%s.%s' % (self.db, self.coll)
      return (
          client[self.db].command(
              'splitVector',
              name_space,
              keyPattern={'_id': 1},  # Ascending index
              min={'_id': start_pos},
              max={'_id': end_pos},
              maxChunkSize=desired_chunk_size_in_mb)['splitKeys'])

  def _merge_id_filter(self, range_tracker):
    # Merge the default filter with refined _id field range of range_tracker.
    # see more at https://docs.mongodb.com/manual/reference/operator/query/and/
    all_filters = {
        '$and': [
            self.filter.copy(),
            # add additional range filter to query. $gte specifies start
            # position(inclusive) and $lt specifies the end position(exclusive),
            # see more at
            # https://docs.mongodb.com/manual/reference/operator/query/gte/ and
            # https://docs.mongodb.com/manual/reference/operator/query/lt/
            {
                '_id': {
                    '$gte': range_tracker.start_position(),
                    '$lt': range_tracker.stop_position()
                }
            },
        ]
    }

    return all_filters

  def _get_head_document_id(self, sort_order):
    with MongoClient(self.uri, **self.spec) as client:
      cursor = client[self.db][self.coll].find(
          filter={}, projection=[]).sort([('_id', sort_order)]).limit(1)
      try:
        return cursor[0]['_id']
      except IndexError:
        raise ValueError('Empty Mongodb collection')

  def _replace_none_positions(self, start_position, stop_position):
    if start_position is None:
      start_position = self._get_head_document_id(ASCENDING)
    if stop_position is None:
      last_doc_id = self._get_head_document_id(DESCENDING)
      # increment last doc id binary value by 1 to make sure the last document
      # is not excluded
      stop_position = _ObjectIdHelper.increment_id(last_doc_id, 1)
    return start_position, stop_position


class _ObjectIdHelper(object):
  """A Utility class to manipulate bson object ids."""
  @classmethod
  def id_to_int(cls, id):
    """
    Args:
      id: ObjectId required for each MongoDB document _id field.

    Returns: Converted integer value of ObjectId's 12 bytes binary value.

    """
    # converts object id binary to integer
    # id object is bytes type with size of 12
    ints = struct.unpack('>III', id.binary)
    return (ints[0] << 64) + (ints[1] << 32) + ints[2]

  @classmethod
  def int_to_id(cls, number):
    """
    Args:
      number(int): The integer value to be used to convert to ObjectId.

    Returns: The ObjectId that has the 12 bytes binary converted from the
      integer value.

    """
    # converts integer value to object id. Int value should be less than
    # (2 ^ 96) so it can be convert to 12 bytes required by object id.
    if number < 0 or number >= (1 << 96):
      raise ValueError('number value must be within [0, %s)' % (1 << 96))
    ints = [(number & 0xffffffff0000000000000000) >> 64,
            (number & 0x00000000ffffffff00000000) >> 32,
            number & 0x0000000000000000ffffffff]

    bytes = struct.pack('>III', *ints)
    return objectid.ObjectId(bytes)

  @classmethod
  def increment_id(cls, object_id, inc):
    """
    Args:
      object_id: The ObjectId to change.
      inc(int): The incremental int value to be added to ObjectId.

    Returns:

    """
    # increment object_id binary value by inc value and return new object id.
    id_number = _ObjectIdHelper.id_to_int(object_id)
    new_number = id_number + inc
    if new_number < 0 or new_number >= (1 << 96):
      raise ValueError(
          'invalid incremental, inc value must be within ['
          '%s, %s)' % (0 - id_number, 1 << 96 - id_number))
    return _ObjectIdHelper.int_to_id(new_number)


class _ObjectIdRangeTracker(OrderedPositionRangeTracker):
  """RangeTracker for tracking mongodb _id of bson ObjectId type."""
  def position_to_fraction(self, pos, start, end):
    pos_number = _ObjectIdHelper.id_to_int(pos)
    start_number = _ObjectIdHelper.id_to_int(start)
    end_number = _ObjectIdHelper.id_to_int(end)
    return (pos_number - start_number) / (end_number - start_number)

  def fraction_to_position(self, fraction, start, end):
    start_number = _ObjectIdHelper.id_to_int(start)
    end_number = _ObjectIdHelper.id_to_int(end)
    total = end_number - start_number
    pos = int(total * fraction + start_number)
    # make sure split position is larger than start position and smaller than
    # end position.
    if pos <= start_number:
      return _ObjectIdHelper.increment_id(start, 1)
    if pos >= end_number:
      return _ObjectIdHelper.increment_id(end, -1)
    return _ObjectIdHelper.int_to_id(pos)


@experimental()
class WriteToMongoDB(PTransform):
  """WriteToMongoDB is a ``PTransform`` that writes a ``PCollection`` of
  mongodb document to the configured MongoDB server.

  In order to make the document writes idempotent so that the bundles are
  retry-able without creating duplicates, the PTransform added 2 transformations
  before final write stage:
  a ``GenerateId`` transform and a ``Reshuffle`` transform.::

                  -----------------------------------------------
    Pipeline -->  |GenerateId --> Reshuffle --> WriteToMongoSink|
                  -----------------------------------------------
                                  (WriteToMongoDB)

  The ``GenerateId`` transform adds a random and unique*_id* field to the
  documents if they don't already have one, it uses the same format as MongoDB
  default. The ``Reshuffle`` transform makes sure that no fusion happens between
  ``GenerateId`` and the final write stage transform,so that the set of
  documents and their unique IDs are not regenerated if final write step is
  retried due to a failure. This prevents duplicate writes of the same document
  with different unique IDs.

  """
  def __init__(
      self,
      uri='mongodb://localhost:27017',
      db=None,
      coll=None,
      batch_size=100,
      extra_client_params=None):
    """

    Args:
      uri (str): The MongoDB connection string following the URI format
      db (str): The MongoDB database name
      coll (str): The MongoDB collection name
      batch_size(int): Number of documents per bulk_write to  MongoDB,
        default to 100
      extra_client_params(dict): Optional `MongoClient
       <https://api.mongodb.com/python/current/api/pymongo/mongo_client.html>`_
       parameters as keyword arguments

    Returns:
      :class:`~apache_beam.transforms.ptransform.PTransform`

    """
    if extra_client_params is None:
      extra_client_params = {}
    if not isinstance(db, str):
      raise ValueError('WriteToMongoDB db param must be specified as a string')
    if not isinstance(coll, str):
      raise ValueError(
          'WriteToMongoDB coll param must be specified as a '
          'string')
    self._uri = uri
    self._db = db
    self._coll = coll
    self._batch_size = batch_size
    self._spec = extra_client_params

  def expand(self, pcoll):
    return pcoll \
           | beam.ParDo(_GenerateObjectIdFn()) \
           | Reshuffle() \
           | beam.ParDo(_WriteMongoFn(self._uri, self._db, self._coll,
                                      self._batch_size, self._spec))


class _GenerateObjectIdFn(DoFn):
  def process(self, element, *args, **kwargs):
    # if _id field already exist we keep it as it is, otherwise the ptransform
    # generates a new _id field to achieve idempotent write to mongodb.
    if '_id' not in element:
      # object.ObjectId() generates a unique identifier that follows mongodb
      # default format, if _id is not present in document, mongodb server
      # generates it with this same function upon write. However the
      # uniqueness of generated id may not be guaranteed if the work load are
      # distributed across too many processes. See more on the ObjectId format
      # https://docs.mongodb.com/manual/reference/bson-types/#objectid.
      element['_id'] = objectid.ObjectId()

    yield element


class _WriteMongoFn(DoFn):
  def __init__(
      self, uri=None, db=None, coll=None, batch_size=100, extra_params=None):
    if extra_params is None:
      extra_params = {}
    self.uri = uri
    self.db = db
    self.coll = coll
    self.spec = extra_params
    self.batch_size = batch_size
    self.batch = []

  def finish_bundle(self):
    self._flush()

  def process(self, element, *args, **kwargs):
    self.batch.append(element)
    if len(self.batch) >= self.batch_size:
      self._flush()

  def _flush(self):
    if len(self.batch) == 0:
      return
    with _MongoSink(self.uri, self.db, self.coll, self.spec) as sink:
      sink.write(self.batch)
      self.batch = []

  def display_data(self):
    res = super(_WriteMongoFn, self).display_data()
    res['uri'] = self.uri
    res['database'] = self.db
    res['collection'] = self.coll
    res['mongo_client_params'] = json.dumps(self.spec)
    res['batch_size'] = self.batch_size
    return res


class _MongoSink(object):
  def __init__(self, uri=None, db=None, coll=None, extra_params=None):
    if extra_params is None:
      extra_params = {}
    self.uri = uri
    self.db = db
    self.coll = coll
    self.spec = extra_params
    self.client = None

  def write(self, documents):
    if self.client is None:
      self.client = MongoClient(host=self.uri, **self.spec)
    requests = []
    for doc in documents:
      # match document based on _id field, if not found in current collection,
      # insert new one, otherwise overwrite it.
      requests.append(
          ReplaceOne(
              filter={'_id': doc.get('_id', None)},
              replacement=doc,
              upsert=True))
    resp = self.client[self.db][self.coll].bulk_write(requests)
    _LOGGER.debug(
        'BulkWrite to MongoDB result in nModified:%d, nUpserted:%d, '
        'nMatched:%d, Errors:%s' % (
            resp.modified_count,
            resp.upserted_count,
            resp.matched_count,
            resp.bulk_api_result.get('writeErrors')))

  def __enter__(self):
    if self.client is None:
      self.client = MongoClient(host=self.uri, **self.spec)
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self.client is not None:
      self.client.close()
