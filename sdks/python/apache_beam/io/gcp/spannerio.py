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


from __future__ import absolute_import
import collections
import time

import apache_beam as beam
from apache_beam import pvalue
from apache_beam import typehints
from apache_beam.transforms import PTransform

from google.cloud.spanner import Client
from google.cloud.spanner import KeySet
from google.cloud.spanner_v1.database import BatchSnapshot
from google.cloud.spanner_v1 import batch
from google.cloud.spanner_v1.database import SnapshotCheckout


__all__ = ['WriteToSpanner', 'ReadFromSpanner',
           'ReadOperation', "WriteMutation", "BatchFn"]

T = typehints.TypeVariable('T')


# from google.cloud.spanner_v1.proto import mutation_pb2
# from google.cloud.spanner_v1.proto.mutation_pb2 import Mutation
from google.cloud.spanner_v1.types import Mutation




class MutationSizeEstimator:

  def __init__(self, m):
    self._m = m

  @staticmethod
  def get_operation(m):
    pass


class WriteMutation:

  @staticmethod
  def insert(table, columns, values):
    """Insert one or more new table rows.

    :type table: str
    :param table: Name of the table to be modified.

    :type columns: list of str
    :param columns: Name of the table columns to be modified.

    :type values: list of lists
    :param values: Values to be modified.
    """
    return Mutation(insert=batch._make_write_pb(table, columns, values))


  @staticmethod
  def update(table, columns, values):
    """Update one or more existing table rows.

    :type table: str
    :param table: Name of the table to be modified.

    :type columns: list of str
    :param columns: Name of the table columns to be modified.

    :type values: list of lists
    :param values: Values to be modified.
    """
    return Mutation(update=batch._make_write_pb(table, columns, values))

  @staticmethod
  def insert_or_update(table, columns, values):
    """Insert/update one or more table rows.

    :type table: str
    :param table: Name of the table to be modified.

    :type columns: list of str
    :param columns: Name of the table columns to be modified.

    :type values: list of lists
    :param values: Values to be modified.
    """
    return Mutation(insert_or_update=batch._make_write_pb(table, columns, values))

  @staticmethod
  def replace(table, columns, values):
    """Replace one or more table rows.

    :type table: str
    :param table: Name of the table to be modified.

    :type columns: list of str
    :param columns: Name of the table columns to be modified.

    :type values: list of lists
    :param values: Values to be modified.
    """
    return Mutation(replace=batch._make_write_pb(table, columns, values))

  @staticmethod
  def delete(self, table, keyset):
    """Delete one or more table rows.

    :type table: str
    :param table: Name of the table to be modified.

    :type keyset: :class:`~google.cloud.spanner_v1.keyset.Keyset`
    :param keyset: Keys/ranges identifying rows to delete.
    """
    delete = Mutation.Delete(table=table, key_set=keyset._to_pb())
    return Mutation(delete=delete)


class WriteToSpanner(object):

  def __init__(self, project_id, instance_id, database_id):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id

  def insert(self):
    return _Insert(self._project_id, self._instance_id, self._database_id)

  def batch(self):
    pass



class _BatchWrite(PTransform):

  def __init__(self, project_id, instance_id, database_id):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id

  def expand(self, pcoll):
    return pcoll | beam.ParDo(_WriteToSpanner(
        self._project_id, self._instance_id, self._database_id
    ))



# @typehints.with_input_types(
#   typehints.Tuple[str, typehints.List[str],
#                   typehints.List[typehints.Tuple[T, ...]]])
class _Insert(PTransform):

  def __init__(self, project_id, instance_id, database_id):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id

  def expand(self, pcoll):

    """
    SpannerIO.java:914
    // First, read the Cloud Spanner schema.

    // Build a set of Mutation groups from the current bundle,
      // sort them by table/key then split into batches.

    // Merge the batchable and unbatchable mutations and write to Spanner.


    """
    return pcoll | beam.ParDo(_WriteToSpanner(
        self._project_id, self._instance_id, self._database_id
    ))



class MutationSizeEstimator:

  def __init__(self):
    pass

  @staticmethod
  def get_operation(m):
    ops = ('insert', 'insert_or_update', 'replace', 'update', 'delete')
    for op in ops:
      if getattr(m, op).table is not None:
        return op
    return ValueError("Operation is not defined!")



class BatchFn(beam.DoFn):

  def __init__(self, max_batch_size_bytes, max_num_mutations, schema_view):
    self._max_batch_size_bytes = max_batch_size_bytes
    self._max_num_mutations = max_num_mutations
    self._schema_view = schema_view

  def process(self, element):
    batch_size_bytes = 0
    batch_cells = 0

    for mg in element:
      group_size = 0
      print(mg.operaion)
      group_cell = 0

      x = 1
    return element

class _WriteToSpanner(beam.DoFn):

  def __init__(self, project_id, instance_id, database_id):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._db_instance = None

  def to_runner_api_parameter(self, unused_context):
    pass

  def setup(self):
    spanner_client = Client(self._project_id)
    instance = spanner_client.instance(self._instance_id)
    self._db_instance = instance.database(self._database_id)


  def process(self, element):
    with self._db_instance.batch() as b:
      b._mutations.extend(element)

    return element

  # def process(self, element):
  #   _id = int(time.time())
  #   def _process(transaction):
  #     sql = "INSERT roles (key, rolename) VALUES ({}, 'insert-role-{}')".format(_id, _id)
  #     transaction.execute_update(sql)
  #   self._db_instance.run_in_transaction(_process)
  #   return element




















# WriteToSpanner -> insert -> InsertTransform -> Group Mutations -> ParDO


class ReadOperation(collections.namedtuple("ReadOperation",
                                           "read_operation batch_action "
                                           "transaction_action kwargs")):

  __slots__ = ()

  @classmethod
  def with_query(cls, sql, params=None, param_types=None):
    return cls(
        read_operation="process_query_batch",
        batch_action="generate_query_batches", transaction_action="execute_sql",
        kwargs={'sql': sql, 'params': params, 'param_types': param_types}
    )

  @classmethod
  def with_table(cls, table, columns, index="", keyset=None):
    keyset = keyset or KeySet(all_=True)
    return cls(
        read_operation="process_read_batch",
        batch_action="generate_read_batches", transaction_action="read",
        kwargs={'table': table, 'columns': columns, 'index': index,
                'keyset': keyset}
    )


class ReadFromSpanner(object):

  def __init__(self, project_id, instance_id, database_id, snapshot_options=None):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._transaction = None
    self._timestamp = None
    self._snapshot_options = snapshot_options

  def with_query(self, sql, params=None, param_types=None):
    read_operation = [ReadOperation.with_query(sql, params, param_types)]
    return self.read_all(read_operation)

  def with_table(self, table, columns, index="", keyset=None):
    read_operation = [ReadOperation.with_table(
        table=table, columns=columns, index=index, keyset=keyset
    )]
    return self.read_all(read_operation)

  def read_all(self, read_operations):
    if self._transaction is None:
      return _BatchRead(self._project_id, self._instance_id,
                        self._database_id, read_operations, self._snapshot_options)
    else:
      return _NaiveSpannerRead(self._project_id, self._instance_id,
                              self._database_id, self._transaction, read_operations)

  @staticmethod
  def create_transaction(project_id, instance_id, database_id, snapshot_options={}):
    spanner_client = Client(project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    snapshot = database.batch_snapshot(**snapshot_options)
    return snapshot.to_dict()


  def with_transaction(self, transaction):
      # if not isinstance(transaction, SnapshotCheckout):
      #   raise Exception('Transaction must be of type SnapshotCheckout')
      self._transaction = transaction
      return self



class _NaiveSpannerReadDoFn(beam.DoFn):

  def __init__(self, project_id, instance_id, database_id, snapshot_dict):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._snapshot = None
    self._snapshot_dict = snapshot_dict

  def to_runner_api_parameter(self, unused_context):
    pass

  def setup(self):
    spanner_client = Client(self._project_id)
    instance = spanner_client.instance(self._instance_id)
    database = instance.database(self._database_id)
    self._snapshot = BatchSnapshot.from_dict(database, self._snapshot_dict)

  def process(self, element):
    # for row in getattr(self._snapshot, element.transaction_action)(**element.kwargs):
    #   yield row

    # transaction = self._snapshot._get_session().transaction()
    # transaction.begin()
    # for row in getattr(transaction, element.transaction_action)(
    #     **element.kwargs):
    #   yield row
    # transaction.commit()

    with self._snapshot._get_session().transaction() as transaction:
      for row in getattr(transaction, element.transaction_action)(
          **element.kwargs):
        yield row

  def teardown(self):
    if self._snapshot:
      self._snapshot.close()


class _NaiveSpannerRead(PTransform):

  def __init__(self, project_id, instance_id, database_id, transaction, read_operations):
    self._transaction = transaction
    self._read_operations = read_operations

    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._sql = ""  #todo: remove this


  def expand(self, pbegin):
    return (pbegin
            | 'Add Read Operations' >> beam.Create(self._read_operations)
            | 'Reshuffle' >> beam.Reshuffle()
            | 'Perform Read' >> beam.ParDo(
            _NaiveSpannerReadDoFn(self._project_id, self._instance_id,
                                 self._database_id, self._transaction))
            )


class _BatchRead(PTransform):

  def __init__(self, project_id, instance_id, database_id, read_operations, snapshot_options=None):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._read_operations = read_operations
    self._snapshot_options = snapshot_options or {}


  def expand(self, pbegin):
    if not isinstance(pbegin, pvalue.PBegin):
      raise Exception("ReadFromSpanner must be a root transform")

    spanner_client = Client(self._project_id)
    instance = spanner_client.instance(self._instance_id)
    database = instance.database(self._database_id)
    snapshot = database.batch_snapshot(**self._snapshot_options)

    reads = [
        {"read_operation": ro.read_operation, "partitions": p}
        for ro in self._read_operations
        for p in getattr(snapshot, ro.batch_action)(**ro.kwargs)
    ]

    return (pbegin
            | 'Generate Partitions' >> beam.Create(reads)
            | 'Reshuffle' >> beam.Reshuffle()
            | 'Read From Partitions' >> beam.ParDo(
            _ReadFromPartitionFn(self._project_id, self._instance_id,
                                 self._database_id, snapshot.to_dict()))
            )


class _ReadFromPartitionFn(beam.DoFn):

  def __init__(self, project_id, instance_id, database_id, snapshot_dict):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._snapshot = None
    self._snapshot_dict = snapshot_dict
    self._database = None

  def to_runner_api_parameter(self, unused_context):
    pass

  def setup(self):
    spanner_client = Client(self._project_id)
    instance = spanner_client.instance(self._instance_id)
    self._database = instance.database(self._database_id)

  def process(self, element):
    self._snapshot = BatchSnapshot.from_dict(self._database, self._snapshot_dict)
    read_operation = element['read_operation']
    elem = element['partitions']

    for row in getattr(self._snapshot, read_operation)(elem):
      yield row

  def teardown(self):
    if self._snapshot:
      self._snapshot.close()