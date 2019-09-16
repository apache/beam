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

import apache_beam as beam
from apache_beam import pvalue
from apache_beam import typehints
from apache_beam.transforms import PTransform

from google.cloud.spanner import Client
from google.cloud.spanner import KeySet
from google.cloud.spanner_v1.database import BatchSnapshot
from google.cloud.spanner_v1.database import SnapshotCheckout

__all__ = ['ReadFromSpanner', 'WriteToSpanner', 'NewReadFromSpanner',
           'ReadOperation']

T = typehints.TypeVariable('T')


class WriteToSpanner(object):

  def __init__(self, project_id, instance_id, database_id):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id

  def insert(self):
    return _Insert(self._project_id, self._instance_id, self._database_id)


@typehints.with_input_types(
    typehints.Tuple[str, typehints.List[str],
                    typehints.List[typehints.Tuple[T, ...]]])
class _Insert(PTransform):
  def __init__(self, project_id, instance_id, databse_id):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = databse_id

  def expand(self, pcoll):
    spanner_client = Client(self._project_id)
    instance = spanner_client.instance(self._instance_id)
    database = instance.database(self._database_id)
    return pcoll | beam.ParDo(BatchFn())


class BatchFn(beam.DoFn):

  def __init__(self):
    pass

  def process(self, element):
    pass



















# WriteToSpanner -> insert -> InsertTransform -> Group Mutations -> ParDO



import collections


class ReadOperation(collections.namedtuple("ReadOperation",
                                           "read_operation batch_action "
                                           "transaction_action kwargs")):

  __slots__ = ()

  @classmethod
  def with_query(cls, sql):
    return cls(
        read_operation="process_query_batch",
        batch_action="generate_query_batches", transaction_action="execute_sql",
        kwargs={'sql': sql}
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


class NewReadFromSpanner(object):

  def __init__(self, project_id, instance_id, database_id):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._transaction = None
    self._timestamp = None

  def with_query(self, sql):
    read_operation = [ReadOperation.with_query(sql)]
    return self.read_all(read_operation)

  def with_table(self, table, columns, index="", keyset=None):
    read_operation = [ReadOperation.with_table(
        table=table, columns=columns, index=index, keyset=keyset
    )]
    return self.read_all(read_operation)

  def read_all(self, read_operations):
    if self._transaction is None:
      return _BatchRead(self._project_id, self._instance_id,
                        self._database_id, read_operations)
    else:
      return _TransactionRead(self._transaction, read_operations)

  @staticmethod
  def create_transaction(project_id, instance_id, database_id, timestamp=None):
    spanner_client = Client(project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    snapshot_options = {'multi_use': True}
    if timestamp:
      # `multi_use=True` is incompatible with either max_staleness.
      snapshot_options['multi_use'] = False
      snapshot_options['max_staleness'] = timestamp

    return database.snapshot(**snapshot_options)

  def with_transaction(self, transaction):
    if not isinstance(transaction, SnapshotCheckout):
      raise Exception('Transaction must be of type SnapshotCheckout')
    self._transaction = transaction
    return self


class ReadFromSpanner(object):

  def __init__(self, project_id, instance_id, database_id):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._transaction = None

  def with_query(self, sql):
    if self._transaction is None:
      return _WithQueryBatch(self._project_id, self._instance_id,
                             self._database_id, sql)
    else:
      return _WithQueryTransaction(self._transaction, sql)

  def with_table(self, table, columns, index=""):
    if self._transaction is None:
      return _WithTableBatch(self._project_id, self._instance_id,
                             self._database_id, table, columns, index)
    else:
      return _WithTableTransaction(self._transaction, table, columns, index)

  @classmethod
  def create_transaction(cls, project_id, instance_id, database_id):
    spanner_client = Client(project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    return database.snapshot(multi_use=True)

  def with_transaction(self, transaction):
    if not isinstance(transaction, SnapshotCheckout):
      raise Exception('Transaction must of type SnapshotCheckout')
    self._transaction = transaction
    return self




class org___ReadFromSpanner(object):

  def __init__(self, project_id, instance_id, database_id):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._transaction = None

  def with_query(self, sql):
    if self._transaction is None:
      return _WithQueryBatch(self._project_id, self._instance_id,
                             self._database_id, sql)
    else:
      return _WithQueryTransaction(self._transaction, sql)

  def with_table(self, table, columns, index=""):
    if self._transaction is None:
      return _WithTableBatch(self._project_id, self._instance_id,
                             self._database_id, table, columns, index)
    else:
      return _WithTableTransaction(self._transaction, table, columns, index)

  @classmethod
  def create_transaction(cls, project_id, instance_id, database_id):
    spanner_client = Client(project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    return database.snapshot(multi_use=True)

  def with_transaction(self, transaction):
    if not isinstance(transaction, SnapshotCheckout):
      raise Exception('Transaction must of type SnapshotCheckout')
    self._transaction = transaction
    return self




class _BatchRead(PTransform):

  def __init__(self, project_id, instance_id, database_id, read_operations):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._sql = ""  #todo: remove this
    self._read_operations = read_operations

  # @property
  # def read_operation(self):
  #   return 'process_query_batch'

  def expand(self, pbegin):
    if not isinstance(pbegin, pvalue.PBegin):
      raise Exception("ReadFromSpanner must be a root transform")

    spanner_client = Client(self._project_id)
    instance = spanner_client.instance(self._instance_id)
    database = instance.database(self._database_id)
    snapshot = database.batch_snapshot()

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
                                 self._database_id), snapshot.to_dict())
            )


class _TransactionRead(PTransform):

  def __init__(self, transaction, read_operations):
    self._transaction = transaction
    self._read_operations = read_operations

  def expand(self, pbegin):
    if not isinstance(pbegin, pvalue.PBegin):
      raise Exception("ReadFromSpanner must be a root transform")

    with self._transaction as transaction:
      return pbegin | 'Read With Transaction' >> beam.Create(
          [
              row
              for ro in self._read_operations
              for row in
              getattr(transaction, ro.transaction_action)(**ro.kwargs)
          ]
      )





class _WithQueryBatch(PTransform):

  def __init__(self, project_id, instance_id, database_id, sql):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._sql = sql

  @property
  def read_operation(self):
    return 'process_query_batch'

  def expand(self, pbegin):
    if not isinstance(pbegin, pvalue.PBegin):
      raise Exception("ReadFromSpanner must be a root transform")

    spanner_client = Client(self._project_id)
    instance = spanner_client.instance(self._instance_id)
    database = instance.database(self._database_id)
    snapshot = database.batch_snapshot()
    return (pbegin
            | 'Generate Partitions' >> beam.Create(
            [p for p in snapshot.generate_query_batches(self._sql)])
            | 'Reshuffle' >> beam.Reshuffle()
            | 'Read From Partitions' >> beam.ParDo(
            _ReadFromPartitionFn(self._project_id, self._instance_id,
                                 self._database_id, self.read_operation),
            snapshot.to_dict()
        )
            )


class _WithTableBatch(PTransform):

  def __init__(self, project_id, instance_id, database_id, table, columns,
               index):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._table = table
    self._columns = columns
    self._index = index

  @property
  def read_operation(self):
    return 'process_read_batch'

  def expand(self, pbegin):
    if not isinstance(pbegin, pvalue.PBegin):
      raise Exception("ReadFromSpanner must be a root transform")

    spanner_client = Client(self._project_id)
    instance = spanner_client.instance(self._instance_id)
    database = instance.database(self._database_id)
    snapshot = database.batch_snapshot()

    return (pbegin
            | 'Generate Partitions' >> beam.Create(
            [p for p in snapshot.generate_read_batches(self._table,
                                                       self._columns,
                                                       KeySet(all_=True),
                                                       self._index)]) \
            | 'Reshuffle' >> beam.Reshuffle() \
            | 'Read From Partitions' >> beam.ParDo(
            _ReadFromPartitionFn(self._project_id, self._instance_id,
                                 self._database_id, self.read_operation),
            snapshot.to_dict()))


class _ReadFromPartitionFn(beam.DoFn):

  def __init__(self, project_id, instance_id, database_id, read_operation=None):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._read_operation = read_operation

  def to_runner_api_parameter(self, unused_context):
    pass

  def process(self, element, mapping):
    spanner_client = Client(self._project_id)
    instance = spanner_client.instance(self._instance_id)
    database = instance.database(self._database_id)
    snapshot = BatchSnapshot.from_dict(database, mapping)

    read_operation = element['read_operation']
    elem = element['partitions']

    for row in getattr(snapshot, read_operation)(elem):
      yield row


class _WithQueryTransaction(PTransform):

  def __init__(self, transaction, sql):
    self._transaction = transaction
    self._sql = sql

  def expand(self, pbegin):
    if not isinstance(pbegin, pvalue.PBegin):
      raise Exception("ReadFromSpanner must be a root transform")

    with self._transaction as transaction:
      return pbegin | 'Read With Query Transaction' >> beam.Create(
          [row for row in transaction.execute_sql(self._sql)]
      )


class _WithTableTransaction(PTransform):

  def __init__(self, transaction, table, columns, index):
    self._transaction = transaction
    self._table = table
    self._columns = columns
    self._index = index

  def expand(self, pbegin):
    if not isinstance(pbegin, pvalue.PBegin):
      raise Exception("ReadFromSpanner must be a root transform")

    with self._transaction as transaction:
      keyset = KeySet(all_=True)
      return pbegin | 'Read With Table Transaction' >> beam.Create(
          [row for row in transaction.read(table=self._table,
                                           columns=self._columns,
                                           keyset=keyset, )]
      )
