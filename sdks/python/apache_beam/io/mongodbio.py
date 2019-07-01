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

from __future__ import absolute_import

import logging

from bson import objectid
from pymongo import MongoClient
from pymongo import ReplaceOne

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.transforms import DoFn
from apache_beam.transforms import PTransform
from apache_beam.transforms import Reshuffle
from apache_beam.utils.annotations import experimental

__all__ = ['ReadFromMongoDB', 'WriteToMongoDB']


@experimental()
class ReadFromMongoDB(PTransform):
  """A ``PTransfrom`` to read MongoDB documents into a ``PCollection``.
  """

  def __init__(self,
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
      raise ValueError('ReadFromMongDB coll param must be specified as a '
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
  def __init__(self,
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
    self.doc_count = self._get_document_count()
    self.avg_doc_size = self._get_avg_document_size()
    self.client = None

  def estimate_size(self):
    return self.avg_doc_size * self.doc_count

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    # use document cursor index as the start and stop positions
    if start_position is None:
      start_position = 0
    if stop_position is None:
      stop_position = self.doc_count

    # get an estimate on how many documents should be included in a split batch
    desired_bundle_count = desired_bundle_size // self.avg_doc_size

    bundle_start = start_position
    while bundle_start < stop_position:
      bundle_end = min(stop_position, bundle_start + desired_bundle_count)
      yield iobase.SourceBundle(weight=bundle_end - bundle_start,
                                source=self,
                                start_position=bundle_start,
                                stop_position=bundle_end)
      bundle_start = bundle_end

  def get_range_tracker(self, start_position, stop_position):
    if start_position is None:
      start_position = 0
    if stop_position is None:
      stop_position = self.doc_count
    return OffsetRangeTracker(start_position, stop_position)

  def read(self, range_tracker):
    with MongoClient(self.uri, **self.spec) as client:
      # docs is a MongoDB Cursor
      docs = client[self.db][self.coll].find(
          filter=self.filter, projection=self.projection
      )[range_tracker.start_position():range_tracker.stop_position()]
      for index in range(range_tracker.start_position(),
                         range_tracker.stop_position()):
        if not range_tracker.try_claim(index):
          return
        yield docs[index - range_tracker.start_position()]

  def display_data(self):
    res = super(_BoundedMongoSource, self).display_data()
    res['uri'] = self.uri
    res['database'] = self.db
    res['collection'] = self.coll
    res['filter'] = self.filter
    res['project'] = self.projection
    res['mongo_client_spec'] = self.spec
    return res

  def _get_avg_document_size(self):
    with MongoClient(self.uri, **self.spec) as client:
      size = client[self.db].command('collstats', self.coll).get('avgObjSize')
      if size is None or size <= 0:
        raise ValueError(
            'Collection %s not found or average doc size is '
            'incorrect', self.coll)
      return size

  def _get_document_count(self):
    with MongoClient(self.uri, **self.spec) as client:
      return max(client[self.db][self.coll].count_documents(self.filter), 0)


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

  def __init__(self,
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
      raise ValueError('WriteToMongoDB coll param must be specified as a '
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
  def __init__(self,
               uri=None,
               db=None,
               coll=None,
               batch_size=100,
               extra_params=None):
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
    res['mongo_client_params'] = self.spec
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
          ReplaceOne(filter={'_id': doc.get('_id', None)},
                     replacement=doc,
                     upsert=True))
    resp = self.client[self.db][self.coll].bulk_write(requests)
    logging.debug('BulkWrite to MongoDB result in nModified:%d, nUpserted:%d, '
                  'nMatched:%d, Errors:%s' %
                  (resp.modified_count, resp.upserted_count, resp.matched_count,
                   resp.bulk_api_result.get('writeErrors')))

  def __enter__(self):
    if self.client is None:
      self.client = MongoClient(host=self.uri, **self.spec)
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self.client is not None:
      self.client.close()
