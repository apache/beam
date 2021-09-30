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

To read from MongoDB Atlas, use ``bucket_auto`` option to enable
``@bucketAuto`` MongoDB aggregation instead of ``splitVector``
command which is a high-privilege function that cannot be assigned
to any user in Atlas.

Example usage::

  pipeline | ReadFromMongoDB(uri='mongodb+srv://user:pwd@cluster0.mongodb.net',
                             db='testdb',
                             coll='input',
                             bucket_auto=True)


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

import itertools
import json
import logging
import math
import struct
from typing import Union

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.io.range_trackers import OrderedPositionRangeTracker
from apache_beam.transforms import DoFn
from apache_beam.transforms import PTransform
from apache_beam.transforms import Reshuffle
from apache_beam.utils.annotations import experimental

_LOGGER = logging.getLogger(__name__)

try:
  # Mongodb has its own bundled bson, which is not compatible with bson package.
  # (https://github.com/py-bson/bson/issues/82). Try to import objectid and if
  # it fails because bson package is installed, MongoDB IO will not work but at
  # least rest of the SDK will work.
  from bson import json_util
  from bson import objectid
  from bson.objectid import ObjectId

  # pymongo also internally depends on bson.
  from pymongo import ASCENDING
  from pymongo import DESCENDING
  from pymongo import MongoClient
  from pymongo import ReplaceOne
except ImportError:
  objectid = None
  json_util = None
  ObjectId = None
  ASCENDING = 1
  DESCENDING = -1
  MongoClient = None
  ReplaceOne = None
  _LOGGER.warning("Could not find a compatible bson package.")

__all__ = ["ReadFromMongoDB", "WriteToMongoDB"]


@experimental()
class ReadFromMongoDB(PTransform):
  """A ``PTransform`` to read MongoDB documents into a ``PCollection``."""
  def __init__(
      self,
      uri="mongodb://localhost:27017",
      db=None,
      coll=None,
      filter=None,
      projection=None,
      extra_client_params=None,
      bucket_auto=False,
  ):
    """Initialize a :class:`ReadFromMongoDB`

    Args:
      uri (str): The MongoDB connection string following the URI format.
      db (str): The MongoDB database name.
      coll (str): The MongoDB collection name.
      filter: A `bson.SON
        <https://api.mongodb.com/python/current/api/bson/son.html>`_ object
        specifying elements which must be present for a document to be included
        in the result set.
      projection: A list of field names that should be returned in the result
        set or a dict specifying the fields to include or exclude.
      extra_client_params(dict): Optional `MongoClient
        <https://api.mongodb.com/python/current/api/pymongo/mongo_client.html>`_
        parameters.
      bucket_auto (bool): If :data:`True`, use MongoDB `$bucketAuto` aggregation
        to split collection into bundles instead of `splitVector` command,
        which does not work with MongoDB Atlas.
        If :data:`False` (the default), use `splitVector` command for bundling.

    Returns:
      :class:`~apache_beam.transforms.ptransform.PTransform`
    """
    if extra_client_params is None:
      extra_client_params = {}
    if not isinstance(db, str):
      raise ValueError("ReadFromMongDB db param must be specified as a string")
    if not isinstance(coll, str):
      raise ValueError(
          "ReadFromMongDB coll param must be specified as a string")
    self._mongo_source = _BoundedMongoSource(
        uri=uri,
        db=db,
        coll=coll,
        filter=filter,
        projection=projection,
        extra_client_params=extra_client_params,
        bucket_auto=bucket_auto,
    )

  def expand(self, pcoll):
    return pcoll | iobase.Read(self._mongo_source)


class _ObjectIdRangeTracker(OrderedPositionRangeTracker):
  """RangeTracker for tracking mongodb _id of bson ObjectId type."""
  def position_to_fraction(
      self,
      pos: ObjectId,
      start: ObjectId,
      end: ObjectId,
  ):
    """Returns the fraction of keys in the range [start, end) that
    are less than the given key.
    """
    pos_number = _ObjectIdHelper.id_to_int(pos)
    start_number = _ObjectIdHelper.id_to_int(start)
    end_number = _ObjectIdHelper.id_to_int(end)
    return (pos_number - start_number) / (end_number - start_number)

  def fraction_to_position(
      self,
      fraction: float,
      start: ObjectId,
      end: ObjectId,
  ):
    """Converts a fraction between 0 and 1
    to a position between start and end.
    """
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


class _BoundedMongoSource(iobase.BoundedSource):
  """A MongoDB source that reads a finite amount of input records.

  This class defines following operations which can be used to read
  MongoDB source efficiently.

  * Size estimation - method ``estimate_size()`` may return an accurate
    estimation in bytes for the size of the source.
  * Splitting into bundles of a given size - method ``split()`` can be used to
    split the source into a set of sub-sources (bundles) based on a desired
    bundle size.
  * Getting a RangeTracker - method ``get_range_tracker()`` should return a
    ``RangeTracker`` object for a given position range for the position type
    of the records returned by the source.
  * Reading the data - method ``read()`` can be used to read data from the
    source while respecting the boundaries defined by a given
    ``RangeTracker``.

  A runner will perform reading the source in two steps.

  (1) Method ``get_range_tracker()`` will be invoked with start and end
      positions to obtain a ``RangeTracker`` for the range of positions the
      runner intends to read. Source must define a default initial start and end
      position range. These positions must be used if the start and/or end
      positions passed to the method ``get_range_tracker()`` are ``None``
  (2) Method read() will be invoked with the ``RangeTracker`` obtained in the
      previous step.

  **Mutability**

  A ``_BoundedMongoSource`` object should not be mutated while
  its methods (for example, ``read()``) are being invoked by a runner. Runner
  implementations may invoke methods of ``_BoundedMongoSource`` objects through
  multi-threaded and/or reentrant execution modes.
  """
  def __init__(
      self,
      uri=None,
      db=None,
      coll=None,
      filter=None,
      projection=None,
      extra_client_params=None,
      bucket_auto=False,
  ):
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
    self.bucket_auto = bucket_auto

  def estimate_size(self):
    with MongoClient(self.uri, **self.spec) as client:
      return client[self.db].command("collstats", self.coll).get("size")

  def _estimate_average_document_size(self):
    with MongoClient(self.uri, **self.spec) as client:
      return client[self.db].command("collstats", self.coll).get("avgObjSize")

  def split(
      self,
      desired_bundle_size: int,
      start_position: Union[int, str, bytes, ObjectId] = None,
      stop_position: Union[int, str, bytes, ObjectId] = None,
  ):
    """Splits the source into a set of bundles.

    Bundles should be approximately of size ``desired_bundle_size`` bytes.

    Args:
      desired_bundle_size: the desired size (in bytes) of the bundles returned.
      start_position: if specified the given position must be used as the
                      starting position of the first bundle.
      stop_position: if specified the given position must be used as the ending
                     position of the last bundle.
    Returns:
      an iterator of objects of type 'SourceBundle' that gives information about
      the generated bundles.
    """

    desired_bundle_size_in_mb = desired_bundle_size // 1024 // 1024

    # for desired bundle size, if desired chunk size smaller than 1mb, use
    # MongoDB default split size of 1mb.
    desired_bundle_size_in_mb = max(desired_bundle_size_in_mb, 1)

    is_initial_split = start_position is None and stop_position is None
    start_position, stop_position = self._replace_none_positions(
      start_position, stop_position
    )

    if self.bucket_auto:
      # Use $bucketAuto for bundling
      split_keys = []
      weights = []
      for bucket in self._get_auto_buckets(
          desired_bundle_size_in_mb,
          start_position,
          stop_position,
          is_initial_split,
      ):
        split_keys.append({"_id": bucket["_id"]["max"]})
        weights.append(bucket["count"])
    else:
      # Use splitVector for bundling
      split_keys = self._get_split_keys(
          desired_bundle_size_in_mb, start_position, stop_position)
      weights = itertools.cycle((desired_bundle_size_in_mb, ))

    bundle_start = start_position
    for split_key_id, weight in zip(split_keys, weights):
      if bundle_start >= stop_position:
        break
      bundle_end = min(stop_position, split_key_id["_id"])
      yield iobase.SourceBundle(
          weight=weight,
          source=self,
          start_position=bundle_start,
          stop_position=bundle_end,
      )
      bundle_start = bundle_end
    # add range of last split_key to stop_position
    if bundle_start < stop_position:
      # bucket_auto mode can come here if not split due to single document
      weight = 1 if self.bucket_auto else desired_bundle_size_in_mb
      yield iobase.SourceBundle(
          weight=weight,
          source=self,
          start_position=bundle_start,
          stop_position=stop_position,
      )

  def get_range_tracker(
      self,
      start_position: Union[int, str, ObjectId] = None,
      stop_position: Union[int, str, ObjectId] = None,
  ) -> Union[
      _ObjectIdRangeTracker, OffsetRangeTracker, LexicographicKeyRangeTracker]:
    """Returns a RangeTracker for a given position range depending on type.

    Args:
      start_position: starting position of the range. If 'None' default start
                      position of the source must be used.
      stop_position:  ending position of the range. If 'None' default stop
                      position of the source must be used.
    Returns:
      a ``_ObjectIdRangeTracker``, ``OffsetRangeTracker``
      or ``LexicographicKeyRangeTracker`` depending on the given position range.
    """
    start_position, stop_position = self._replace_none_positions(
      start_position, stop_position
    )

    if isinstance(start_position, ObjectId):
      return _ObjectIdRangeTracker(start_position, stop_position)

    if isinstance(start_position, int):
      return OffsetRangeTracker(start_position, stop_position)

    if isinstance(start_position, str):
      return LexicographicKeyRangeTracker(start_position, stop_position)

    raise NotImplementedError(
        f"RangeTracker for {type(start_position)} not implemented!")

  def read(self, range_tracker):
    """Returns an iterator that reads data from the source.

    The returned set of data must respect the boundaries defined by the given
    ``RangeTracker`` object. For example:

      * Returned set of data must be for the range
        ``[range_tracker.start_position, range_tracker.stop_position)``. Note
        that a source may decide to return records that start after
        ``range_tracker.stop_position``. See documentation in class
        ``RangeTracker`` for more details. Also, note that framework might
        invoke ``range_tracker.try_split()`` to perform dynamic split
        operations. range_tracker.stop_position may be updated
        dynamically due to successful dynamic split operations.
      * Method ``range_tracker.try_split()`` must be invoked for every record
        that starts at a split point.
      * Method ``range_tracker.record_current_position()`` may be invoked for
        records that do not start at split points.

    Args:
      range_tracker: a ``RangeTracker`` whose boundaries must be respected
                     when reading data from the source. A runner that reads this
                     source muss pass a ``RangeTracker`` object that is not
                     ``None``.
    Returns:
      an iterator of data read by the source.
    """
    with MongoClient(self.uri, **self.spec) as client:
      all_filters = self._merge_id_filter(
          range_tracker.start_position(), range_tracker.stop_position())
      docs_cursor = (
          client[self.db][self.coll].find(
              filter=all_filters,
              projection=self.projection).sort([("_id", ASCENDING)]))
      for doc in docs_cursor:
        if not range_tracker.try_claim(doc["_id"]):
          return
        yield doc

  def display_data(self):
    """Returns the display data associated to a pipeline component."""
    res = super().display_data()
    res["database"] = self.db
    res["collection"] = self.coll
    res["filter"] = json.dumps(self.filter, default=json_util.default)
    res["projection"] = str(self.projection)
    res["bucket_auto"] = self.bucket_auto
    return res

  @staticmethod
  def _range_is_not_splittable(
      start_pos: Union[int, str, ObjectId],
      end_pos: Union[int, str, ObjectId],
  ):
    """Return `True` if splitting range doesn't make sense
    (single document is not splittable),
    Return `False` otherwise.
    """
    return ((
        isinstance(start_pos, ObjectId) and
        start_pos >= _ObjectIdHelper.increment_id(end_pos, -1)) or
            (isinstance(start_pos, int) and start_pos >= end_pos - 1) or
            (isinstance(start_pos, str) and start_pos >= end_pos))

  def _get_split_keys(
      self,
      desired_chunk_size_in_mb: int,
      start_pos: Union[int, str, ObjectId],
      end_pos: Union[int, str, ObjectId],
  ):
    """Calls MongoDB `splitVector` command
    to get document ids at split position.
    """
    # single document not splittable
    if self._range_is_not_splittable(start_pos, end_pos):
      return []

    with MongoClient(self.uri, **self.spec) as client:
      name_space = "%s.%s" % (self.db, self.coll)
      return client[self.db].command(
        "splitVector",
        name_space,
        keyPattern={"_id": 1},  # Ascending index
        min={"_id": start_pos},
        max={"_id": end_pos},
        maxChunkSize=desired_chunk_size_in_mb,
      )["splitKeys"]

  def _get_auto_buckets(
      self,
      desired_chunk_size_in_mb: int,
      start_pos: Union[int, str, ObjectId],
      end_pos: Union[int, str, ObjectId],
      is_initial_split: bool,
  ) -> list:
    """Use MongoDB `$bucketAuto` aggregation to split collection into bundles
    instead of `splitVector` command, which does not work with MongoDB Atlas.
    """
    # single document not splittable
    if self._range_is_not_splittable(start_pos, end_pos):
      return []

    if is_initial_split and not self.filter:
      # total collection size in MB
      size_in_mb = self.estimate_size() / float(1 << 20)
    else:
      # size of documents within start/end id range and possibly filtered
      documents_count = self._count_id_range(start_pos, end_pos)
      avg_document_size = self._estimate_average_document_size()
      size_in_mb = documents_count * avg_document_size / float(1 << 20)

    if size_in_mb == 0:
      # no documents not splittable (maybe a result of filtering)
      return []

    bucket_count = math.ceil(size_in_mb / desired_chunk_size_in_mb)
    with beam.io.mongodbio.MongoClient(self.uri, **self.spec) as client:
      pipeline = [
          {
              # filter by positions and by the custom filter if any
              "$match": self._merge_id_filter(start_pos, end_pos)
          },
          {
              "$bucketAuto": {
                  "groupBy": "$_id", "buckets": bucket_count
              }
          },
      ]
      buckets = list(
          # Use `allowDiskUse` option to avoid aggregation limit of 100 Mb RAM
          client[self.db][self.coll].aggregate(pipeline, allowDiskUse=True))
      if buckets:
        buckets[-1]["_id"]["max"] = end_pos

      return buckets

  def _merge_id_filter(
      self,
      start_position: Union[int, str, bytes, ObjectId],
      stop_position: Union[int, str, bytes, ObjectId] = None,
  ) -> dict:
    """Merge the default filter (if any) with refined _id field range
    of range_tracker.
    $gte specifies start position (inclusive)
    and $lt specifies the end position (exclusive),
    see more at
    https://docs.mongodb.com/manual/reference/operator/query/gte/ and
    https://docs.mongodb.com/manual/reference/operator/query/lt/
    """

    if stop_position is None:
      id_filter = {"_id": {"$gte": start_position}}
    else:
      id_filter = {"_id": {"$gte": start_position, "$lt": stop_position}}

    if self.filter:
      all_filters = {
          # see more at
          # https://docs.mongodb.com/manual/reference/operator/query/and/
          "$and": [self.filter.copy(), id_filter]
      }
    else:
      all_filters = id_filter

    return all_filters

  def _get_head_document_id(self, sort_order):
    with MongoClient(self.uri, **self.spec) as client:
      cursor = (
          client[self.db][self.coll].find(filter={}, projection=[]).sort([
              ("_id", sort_order)
          ]).limit(1))
      try:
        return cursor[0]["_id"]

      except IndexError:
        raise ValueError("Empty Mongodb collection")

  def _replace_none_positions(self, start_position, stop_position):

    if start_position is None:
      start_position = self._get_head_document_id(ASCENDING)
    if stop_position is None:
      last_doc_id = self._get_head_document_id(DESCENDING)
      # increment last doc id binary value by 1 to make sure the last document
      # is not excluded
      if isinstance(last_doc_id, ObjectId):
        stop_position = _ObjectIdHelper.increment_id(last_doc_id, 1)
      elif isinstance(last_doc_id, int):
        stop_position = last_doc_id + 1
      elif isinstance(last_doc_id, str):
        stop_position = last_doc_id + '\x00'

    return start_position, stop_position

  def _count_id_range(self, start_position, stop_position):
    """Number of documents between start_position (inclusive)
    and stop_position (exclusive), respecting the custom filter if any.
    """
    with MongoClient(self.uri, **self.spec) as client:
      return client[self.db][self.coll].count_documents(
          filter=self._merge_id_filter(start_position, stop_position))


class _ObjectIdHelper:
  """A Utility class to manipulate bson object ids."""
  @classmethod
  def id_to_int(cls, _id: Union[int, ObjectId]) -> int:
    """
    Args:
      _id: ObjectId required for each MongoDB document _id field.

    Returns: Converted integer value of ObjectId's 12 bytes binary value.
    """
    if isinstance(_id, int):
      return _id

    # converts object id binary to integer
    # id object is bytes type with size of 12
    ints = struct.unpack(">III", _id.binary)
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
      raise ValueError("number value must be within [0, %s)" % (1 << 96))
    ints = [
        (number & 0xFFFFFFFF0000000000000000) >> 64,
        (number & 0x00000000FFFFFFFF00000000) >> 32,
        number & 0x0000000000000000FFFFFFFF,
    ]

    number_bytes = struct.pack(">III", *ints)
    return ObjectId(number_bytes)

  @classmethod
  def increment_id(
      cls,
      _id: ObjectId,
      inc: int,
  ) -> ObjectId:
    """
    Increment object_id binary value by inc value and return new object id.

    Args:
      _id: The `_id` to change.
      inc(int): The incremental int value to be added to `_id`.

    Returns:
        `_id` incremented by `inc` value
    """
    id_number = _ObjectIdHelper.id_to_int(_id)
    new_number = id_number + inc
    if new_number < 0 or new_number >= (1 << 96):
      raise ValueError(
          "invalid incremental, inc value must be within ["
          "%s, %s)" % (0 - id_number, 1 << 96 - id_number))
    return _ObjectIdHelper.int_to_id(new_number)


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
      uri="mongodb://localhost:27017",
      db=None,
      coll=None,
      batch_size=100,
      extra_client_params=None,
  ):
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
      raise ValueError("WriteToMongoDB db param must be specified as a string")
    if not isinstance(coll, str):
      raise ValueError(
          "WriteToMongoDB coll param must be specified as a string")
    self._uri = uri
    self._db = db
    self._coll = coll
    self._batch_size = batch_size
    self._spec = extra_client_params

  def expand(self, pcoll):
    return (
        pcoll
        | beam.ParDo(_GenerateObjectIdFn())
        | Reshuffle()
        | beam.ParDo(
            _WriteMongoFn(
                self._uri, self._db, self._coll, self._batch_size, self._spec)))


class _GenerateObjectIdFn(DoFn):
  def process(self, element, *args, **kwargs):
    # if _id field already exist we keep it as it is, otherwise the ptransform
    # generates a new _id field to achieve idempotent write to mongodb.
    if "_id" not in element:
      # object.ObjectId() generates a unique identifier that follows mongodb
      # default format, if _id is not present in document, mongodb server
      # generates it with this same function upon write. However the
      # uniqueness of generated id may not be guaranteed if the work load are
      # distributed across too many processes. See more on the ObjectId format
      # https://docs.mongodb.com/manual/reference/bson-types/#objectid.
      element["_id"] = objectid.ObjectId()

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
    res = super().display_data()
    res["database"] = self.db
    res["collection"] = self.coll
    res["batch_size"] = self.batch_size
    return res


class _MongoSink:
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
              filter={"_id": doc.get("_id", None)},
              replacement=doc,
              upsert=True))
    resp = self.client[self.db][self.coll].bulk_write(requests)
    _LOGGER.debug(
        "BulkWrite to MongoDB result in nModified:%d, nUpserted:%d, "
        "nMatched:%d, Errors:%s" % (
            resp.modified_count,
            resp.upserted_count,
            resp.matched_count,
            resp.bulk_api_result.get("writeErrors"),
        ))

  def __enter__(self):
    if self.client is None:
      self.client = MongoClient(host=self.uri, **self.spec)
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self.client is not None:
      self.client.close()
