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

"""Google Cloud Spanner IO

Experimental; no backwards-compatibility guarantees.

This is an experimental module for reading and writing data from Google Cloud
Spanner. Visit: https://cloud.google.com/spanner for more details.

Reading Data from Cloud Spanner.

To read from Cloud Spanner apply ReadFromSpanner transformation. It will
return a PCollection, where each element represents an individual row returned
from the read operation. Both Query and Read APIs are supported.

ReadFromSpanner relies on the ReadOperation objects which is exposed by the
SpannerIO API. ReadOperation holds the immutable data which is responsible to
execute batch and naive reads on Cloud Spanner. This is done for more
convenient programming.

ReadFromSpanner reads from Cloud Spanner by providing either an 'sql' param
in the constructor or 'table' name with 'columns' as list. For example:::

  records = (pipeline
            | ReadFromSpanner(PROJECT_ID, INSTANCE_ID, DB_NAME,
            sql='Select * from users'))

  records = (pipeline
            | ReadFromSpanner(PROJECT_ID, INSTANCE_ID, DB_NAME,
            table='users', columns=['id', 'name', 'email']))

You can also perform multiple reads by providing a list of ReadOperations
to the ReadFromSpanner transform constructor. ReadOperation exposes two static
methods. Use 'query' to perform sql based reads, 'table' to perform read from
table name. For example:::

  read_operations = [
                      ReadOperation.table(table='customers', columns=['name',
                      'email']),
                      ReadOperation.table(table='vendors', columns=['name',
                      'email']),
                    ]
  all_users = pipeline | ReadFromSpanner(PROJECT_ID, INSTANCE_ID, DB_NAME,
        read_operations=read_operations)

  ...OR...

  read_operations = [
                      ReadOperation.query(sql='Select name, email from
                      customers'),
                      ReadOperation.query(
                        sql='Select * from users where id <= @user_id',
                        params={'user_id': 100},
                        params_type={'user_id': param_types.INT64}
                      ),
                    ]
  # `params_types` are instance of `google.cloud.spanner.param_types`
  all_users = pipeline | ReadFromSpanner(PROJECT_ID, INSTANCE_ID, DB_NAME,
        read_operations=read_operations)

For more information, please review the docs on class ReadOperation.

User can also able to provide the ReadOperation in form of PCollection via
pipeline. For example:::

  users = (pipeline
           | beam.Create([ReadOperation...])
           | ReadFromSpanner(PROJECT_ID, INSTANCE_ID, DB_NAME))

User may also create cloud spanner transaction from the transform called
`create_transaction` which is available in the SpannerIO API.

The transform is guaranteed to be executed on a consistent snapshot of data,
utilizing the power of read only transactions. Staleness of data can be
controlled by providing the `read_timestamp` or `exact_staleness` param values
in the constructor.

This transform requires root of the pipeline (PBegin) and returns PTransform
which is passed later to the `ReadFromSpanner` constructor. `ReadFromSpanner`
pass this transaction PTransform as a singleton side input to the
`_NaiveSpannerReadDoFn` containing 'session_id' and 'transaction_id'.
For example:::

  transaction = (pipeline | create_transaction(TEST_PROJECT_ID,
                                              TEST_INSTANCE_ID,
                                              DB_NAME))

  users = pipeline | ReadFromSpanner(PROJECT_ID, INSTANCE_ID, DB_NAME,
        sql='Select * from users', transaction=transaction)

  tweets = pipeline | ReadFromSpanner(PROJECT_ID, INSTANCE_ID, DB_NAME,
        sql='Select * from tweets', transaction=transaction)

For further details of this transform, please review the docs on the
:meth:`create_transaction` method available in the SpannerIO API.

ReadFromSpanner takes this transform in the constructor and pass this to the
read pipeline as the singleton side input.

Writing Data to Cloud Spanner.

The WriteToSpanner transform writes to Cloud Spanner by executing a
collection a input rows (WriteMutation). The mutations are grouped into
batches for efficiency.

WriteToSpanner transform relies on the WriteMutation objects which is exposed
by the SpannerIO API. WriteMutation have five static methods (insert, update,
insert_or_update, replace, delete). These methods returns the instance of the
_Mutator object which contains the mutation type and the Spanner Mutation
object. For more details, review the docs of the class SpannerIO.WriteMutation.
For example:::

  mutations = [
                WriteMutation.insert(table='user', columns=('name', 'email'),
                values=[('sara'. 'sara@dev.com')])
              ]
  _ = (p
       | beam.Create(mutations)
       | WriteToSpanner(
          project_id=SPANNER_PROJECT_ID,
          instance_id=SPANNER_INSTANCE_ID,
          database_id=SPANNER_DATABASE_NAME)
        )

You can also create WriteMutation via calling its constructor. For example:::

  mutations = [
      WriteMutation(insert='users', columns=('name', 'email'),
                    values=[('sara", 'sara@example.com')])
  ]

For more information, review the docs available on WriteMutation class.

WriteToSpanner transform also takes three batching parameters (max_number_rows,
max_number_cells and max_batch_size_bytes). By default, max_number_rows is set
to 50 rows, max_number_cells is set to 500 cells and max_batch_size_bytes is
set to 1MB (1048576 bytes). These parameter used to reduce the number of
transactions sent to spanner by grouping the mutation into batches. Setting
these param values either to smaller value or zero to disable batching.
Unlike the Java connector, this connector does not create batches of
transactions sorted by table and primary key.

WriteToSpanner transforms starts with the grouping into batches. The first step
in this process is to make the make the mutation groups of the WriteMutation
objects and then filtering them into batchable and unbatchable mutation
groups. There are three batching parameters (max_number_cells, max_number_rows
& max_batch_size_bytes). We calculated th mutation byte size from the method
available in the `google.cloud.spanner_v1.proto.mutation_pb2.Mutation.ByteSize`.
if the mutation rows, cells or byte size are larger than value of the any
batching parameters param, it will be tagged as "unbatchable" mutation. After
this all the batchable mutation are merged into a single mutation group whos
size is not larger than the "max_batch_size_bytes", after this process, all the
mutation groups together to process. If the Mutation references a table or
column does not exits, it will cause a exception and fails the entire pipeline.
"""
from __future__ import absolute_import

import typing
from collections import deque
from collections import namedtuple

from apache_beam import Create
from apache_beam import DoFn
from apache_beam import Flatten
from apache_beam import ParDo
from apache_beam import Reshuffle
from apache_beam.metrics import Metrics
from apache_beam.pvalue import AsSingleton
from apache_beam.pvalue import PBegin
from apache_beam.pvalue import TaggedOutput
from apache_beam.transforms import PTransform
from apache_beam.transforms import ptransform_fn
from apache_beam.transforms import window
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.typehints import with_input_types
from apache_beam.typehints import with_output_types
from apache_beam.utils.annotations import experimental

try:
  from google.cloud.spanner import Client
  from google.cloud.spanner import KeySet
  from google.cloud.spanner_v1 import batch
  from google.cloud.spanner_v1.database import BatchSnapshot
  from google.cloud.spanner_v1.proto.mutation_pb2 import Mutation
except ImportError:
  Client = None
  KeySet = None
  BatchSnapshot = None

__all__ = [
    'create_transaction',
    'ReadFromSpanner',
    'ReadOperation',
    'WriteToSpanner',
    'WriteMutation',
    'MutationGroup'
]


class _SPANNER_TRANSACTION(namedtuple("SPANNER_TRANSACTION", ["transaction"])):
  """
  Holds the spanner transaction details.
  """

  __slots__ = ()


class ReadOperation(namedtuple(
    "ReadOperation", ["is_sql", "is_table", "read_operation", "kwargs"])):
  """
  Encapsulates a spanner read operation.
  """

  __slots__ = ()

  @classmethod
  def query(cls, sql, params=None, param_types=None):
    """
    A convenient method to construct ReadOperation from sql query.

    Args:
      sql: SQL query statement
      params: (optional) values for parameter replacement. Keys must match the
        names used in sql
      param_types: (optional) maps explicit types for one or more param values;
        required if parameters are passed.
    """

    if params:
      assert param_types is not None

    return cls(
        is_sql=True,
        is_table=False,
        read_operation="process_query_batch",
        kwargs={
            'sql': sql, 'params': params, 'param_types': param_types
        })

  @classmethod
  def table(cls, table, columns, index="", keyset=None):
    """
    A convenient method to construct ReadOperation from table.

    Args:
      table: name of the table from which to fetch data.
      columns: names of columns to be retrieved.
      index: (optional) name of index to use, rather than the table's primary
        key.
      keyset: (optional) `KeySet` keys / ranges identifying rows to be
        retrieved.
    """
    keyset = keyset or KeySet(all_=True)
    if not isinstance(keyset, KeySet):
      raise ValueError(
          "keyset must be an instance of class "
          "google.cloud.spanner.KeySet")
    return cls(
        is_sql=False,
        is_table=True,
        read_operation="process_read_batch",
        kwargs={
            'table': table,
            'columns': columns,
            'index': index,
            'keyset': keyset
        })


class _BeamSpannerConfiguration(namedtuple("_BeamSpannerConfiguration",
                                           ["project",
                                            "instance",
                                            "database",
                                            "credentials",
                                            "pool",
                                            "snapshot_read_timestamp",
                                            "snapshot_exact_staleness"])):
  """
  A namedtuple holds the immutable data of the connection string to the cloud
  spanner.
  """
  @property
  def snapshot_options(self):
    snapshot_options = {}
    if self.snapshot_exact_staleness:
      snapshot_options['exact_staleness'] = self.snapshot_exact_staleness
    if self.snapshot_read_timestamp:
      snapshot_options['read_timestamp'] = self.snapshot_read_timestamp
    return snapshot_options


@with_input_types(ReadOperation, typing.Dict[typing.Any, typing.Any])
@with_output_types(typing.List[typing.Any])
class _NaiveSpannerReadDoFn(DoFn):
  def __init__(self, spanner_configuration):
    """
    A naive version of Spanner read which uses the transaction API of the
    cloud spanner.
    https://googleapis.dev/python/spanner/latest/transaction-api.html
    In Naive reads, this transform performs single reads, where as the
    Batch reads use the spanner partitioning query to create batches.

    Args:
      spanner_configuration: (_BeamSpannerConfiguration) Connection details to
        connect with cloud spanner.
    """
    self._spanner_configuration = spanner_configuration
    self._snapshot = None
    self._session = None

  def _get_session(self):
    if self._session is None:
      session = self._session = self._database.session()
      session.create()
    return self._session

  def _close_session(self):
    if self._session is not None:
      self._session.delete()

  def setup(self):
    # setting up client to connect with cloud spanner
    spanner_client = Client(self._spanner_configuration.project)
    instance = spanner_client.instance(self._spanner_configuration.instance)
    self._database = instance.database(
        self._spanner_configuration.database,
        pool=self._spanner_configuration.pool)

  def process(self, element, spanner_transaction):
    # `spanner_transaction` should be the instance of the _SPANNER_TRANSACTION
    # object.
    if not isinstance(spanner_transaction, _SPANNER_TRANSACTION):
      raise ValueError(
          "Invalid transaction object: %s. It should be instance "
          "of SPANNER_TRANSACTION object created by "
          "spannerio.create_transaction transform." % type(spanner_transaction))

    transaction_info = spanner_transaction.transaction

    # We used batch snapshot to reuse the same transaction passed through the
    # side input
    self._snapshot = BatchSnapshot.from_dict(self._database, transaction_info)

    # getting the transaction from the snapshot's session to run read operation.
    # with self._snapshot.session().transaction() as transaction:
    with self._get_session().transaction() as transaction:
      if element.is_sql is True:
        transaction_read = transaction.execute_sql
      elif element.is_table is True:
        transaction_read = transaction.read
      else:
        raise ValueError(
            "ReadOperation is improperly configure: %s" % str(element))

      for row in transaction_read(**element.kwargs):
        yield row


@with_input_types(ReadOperation)
@with_output_types(typing.Dict[typing.Any, typing.Any])
class _CreateReadPartitions(DoFn):
  """
  A DoFn to create partitions. Uses the Partitioning API (PartitionRead /
  PartitionQuery) request to start a partitioned query operation. Returns a
  list of batch information needed to perform the actual queries.

  If the element is the instance of :class:`ReadOperation` is to perform sql
  query, `PartitionQuery` API is used the create partitions and returns mappings
  of information used perform actual partitioned reads via
  :meth:`process_query_batch`.

  If the element is the instance of :class:`ReadOperation` is to perform read
  from table, `PartitionRead` API is used the create partitions and returns
  mappings of information used perform actual partitioned reads via
  :meth:`process_read_batch`.
  """
  def __init__(self, spanner_configuration):
    self._spanner_configuration = spanner_configuration

  def setup(self):
    spanner_client = Client(
        project=self._spanner_configuration.project,
        credentials=self._spanner_configuration.credentials)
    instance = spanner_client.instance(self._spanner_configuration.instance)
    self._database = instance.database(
        self._spanner_configuration.database,
        pool=self._spanner_configuration.pool)
    self._snapshot = self._database.batch_snapshot(
        **self._spanner_configuration.snapshot_options)
    self._snapshot_dict = self._snapshot.to_dict()

  def process(self, element):
    if element.is_sql is True:
      partitioning_action = self._snapshot.generate_query_batches
    elif element.is_table is True:
      partitioning_action = self._snapshot.generate_read_batches
    else:
      raise ValueError(
          "ReadOperation is improperly configure: %s" % str(element))

    for p in partitioning_action(**element.kwargs):
      yield {
          "is_sql": element.is_sql,
          "is_table": element.is_table,
          "read_operation": element.read_operation,
          "partitions": p,
          "transaction_info": self._snapshot_dict
      }


@with_input_types(int)
@with_output_types(typing.Dict[typing.Any, typing.Any])
class _CreateTransactionFn(DoFn):
  """
  A DoFn to create the transaction of cloud spanner.
  It connects to the database and and returns the transaction_id and session_id
  by using the batch_snapshot.to_dict() method available in the google cloud
  spanner sdk.

  https://googleapis.dev/python/spanner/latest/database-api.html?highlight=
  batch_snapshot#google.cloud.spanner_v1.database.BatchSnapshot.to_dict
  """
  def __init__(
      self,
      project_id,
      instance_id,
      database_id,
      credentials,
      pool,
      read_timestamp,
      exact_staleness):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._credentials = credentials
    self._pool = pool

    self._snapshot_options = {}
    if read_timestamp:
      self._snapshot_options['read_timestamp'] = read_timestamp
    if exact_staleness:
      self._snapshot_options['exact_staleness'] = exact_staleness
    self._snapshot = None

  def setup(self):
    self._spanner_client = Client(
        project=self._project_id, credentials=self._credentials)
    self._instance = self._spanner_client.instance(self._instance_id)
    self._database = self._instance.database(self._database_id, pool=self._pool)

  def process(self, element, *args, **kwargs):
    self._snapshot = self._database.batch_snapshot(**self._snapshot_options)
    return [_SPANNER_TRANSACTION(self._snapshot.to_dict())]


@ptransform_fn
def create_transaction(
    pbegin,
    project_id,
    instance_id,
    database_id,
    credentials=None,
    pool=None,
    read_timestamp=None,
    exact_staleness=None):
  """
  A PTransform method to create a batch transaction.

  Args:
    pbegin: Root of the pipeline
    project_id: Cloud spanner project id. Be sure to use the Project ID,
      not the Project Number.
    instance_id: Cloud spanner instance id.
    database_id: Cloud spanner database id.
    credentials: (optional) The authorization credentials to attach to requests.
      These credentials identify this application to the service.
      If none are specified, the client will attempt to ascertain
      the credentials from the environment.
    pool: (optional) session pool to be used by database. If not passed,
      Spanner Cloud SDK uses the BurstyPool by default.
      `google.cloud.spanner.BurstyPool`. Ref:
      https://googleapis.dev/python/spanner/latest/database-api.html?#google.
      cloud.spanner_v1.database.Database
    read_timestamp: (optional) An instance of the `datetime.datetime` object to
      execute all reads at the given timestamp.
    exact_staleness: (optional) An instance of the `datetime.timedelta`
      object. These timestamp bounds execute reads at a user-specified
      timestamp.
  """

  assert isinstance(pbegin, PBegin)

  return (
      pbegin | Create([1]) | ParDo(
          _CreateTransactionFn(
              project_id,
              instance_id,
              database_id,
              credentials,
              pool,
              read_timestamp,
              exact_staleness)))


@with_input_types(typing.Dict[typing.Any, typing.Any])
@with_output_types(typing.List[typing.Any])
class _ReadFromPartitionFn(DoFn):
  """
  A DoFn to perform reads from the partition.
  """
  def __init__(self, spanner_configuration):
    self._spanner_configuration = spanner_configuration

  def setup(self):
    spanner_client = Client(self._spanner_configuration.project)
    instance = spanner_client.instance(self._spanner_configuration.instance)
    self._database = instance.database(
        self._spanner_configuration.database,
        pool=self._spanner_configuration.pool)
    self._snapshot = self._database.batch_snapshot(
        **self._spanner_configuration.snapshot_options)

  def process(self, element):
    self._snapshot = BatchSnapshot.from_dict(
        self._database, element['transaction_info'])

    if element['is_sql'] is True:
      read_action = self._snapshot.process_query_batch
    elif element['is_table'] is True:
      read_action = self._snapshot.process_read_batch
    else:
      raise ValueError(
          "ReadOperation is improperly configure: %s" % str(element))

    for row in read_action(element['partitions']):
      yield row

  def teardown(self):
    if self._snapshot:
      self._snapshot.close()


@experimental(extra_message="No backwards-compatibility guarantees.")
class ReadFromSpanner(PTransform):
  """
  A PTransform to perform reads from cloud spanner.
  ReadFromSpanner uses BatchAPI to perform all read operations.
  """

  def __init__(self, project_id, instance_id, database_id, pool=None,
               read_timestamp=None, exact_staleness=None, credentials=None,
               sql=None, params=None, param_types=None,  # with_query
               table=None, columns=None, index="", keyset=None,  # with_table
               read_operations=None,  # for read all
               transaction=None
              ):
    """
    A PTransform that uses Spanner Batch API to perform reads.

    Args:
      project_id: Cloud spanner project id. Be sure to use the Project ID,
        not the Project Number.
      instance_id: Cloud spanner instance id.
      database_id: Cloud spanner database id.
      pool: (optional) session pool to be used by database. If not passed,
        Spanner Cloud SDK uses the BurstyPool by default.
        `google.cloud.spanner.BurstyPool`. Ref:
        https://googleapis.dev/python/spanner/latest/database-api.html?#google.
        cloud.spanner_v1.database.Database
      read_timestamp: (optional) An instance of the `datetime.datetime` object
        to execute all reads at the given timestamp. By default, set to `None`.
      exact_staleness: (optional) An instance of the `datetime.timedelta`
        object. These timestamp bounds execute reads at a user-specified
        timestamp. By default, set to `None`.
      credentials: (optional) The authorization credentials to attach to
        requests. These credentials identify this application to the service.
        If none are specified, the client will attempt to ascertain
        the credentials from the environment. By default, set to `None`.
      sql: (optional) SQL query statement.
      params: (optional) Values for parameter replacement. Keys must match the
        names used in sql. By default, set to `None`.
      param_types: (optional) maps explicit types for one or more param values;
        required if params are passed. By default, set to `None`.
      table: (optional) Name of the table from which to fetch data. By
        default, set to `None`.
      columns: (optional) List of names of columns to be retrieved; required if
        the table is passed. By default, set to `None`.
      index: (optional) name of index to use, rather than the table's primary
        key. By default, set to `None`.
      keyset: (optional) keys / ranges identifying rows to be retrieved. By
        default, set to `None`.
      read_operations: (optional) List of the objects of :class:`ReadOperation`
        to perform read all. By default, set to `None`.
      transaction: (optional) PTransform of the :meth:`create_transaction` to
        perform naive read on cloud spanner. By default, set to `None`.
    """
    self._configuration = _BeamSpannerConfiguration(
        project=project_id,
        instance=instance_id,
        database=database_id,
        credentials=credentials,
        pool=pool,
        snapshot_read_timestamp=read_timestamp,
        snapshot_exact_staleness=exact_staleness)

    self._read_operations = read_operations
    self._transaction = transaction

    if self._read_operations is None:
      if table is not None:
        if columns is None:
          raise ValueError("Columns are required with the table name.")
        self._read_operations = [
            ReadOperation.table(
                table=table, columns=columns, index=index, keyset=keyset)
        ]
      elif sql is not None:
        self._read_operations = [
            ReadOperation.query(
                sql=sql, params=params, param_types=param_types)
        ]

  def expand(self, pbegin):
    if self._read_operations is not None and isinstance(pbegin, PBegin):
      pcoll = pbegin.pipeline | Create(self._read_operations)
    elif not isinstance(pbegin, PBegin):
      if self._read_operations is not None:
        raise ValueError(
            "Read operation in the constructor only works with "
            "the root of the pipeline.")
      pcoll = pbegin
    else:
      raise ValueError(
          "Spanner required read operation, sql or table "
          "with columns.")

    if self._transaction is None:
      # reading as batch read using the spanner partitioning query to create
      # batches.
      p = (
          pcoll
          | 'Generate Partitions' >> ParDo(
              _CreateReadPartitions(spanner_configuration=self._configuration))
          | 'Reshuffle' >> Reshuffle()
          | 'Read From Partitions' >> ParDo(
              _ReadFromPartitionFn(spanner_configuration=self._configuration)))
    else:
      # reading as naive read, in which we don't make batches and execute the
      # queries as a single read.
      p = (
          pcoll
          | 'Reshuffle' >> Reshuffle().with_input_types(ReadOperation)
          | 'Perform Read' >> ParDo(
              _NaiveSpannerReadDoFn(spanner_configuration=self._configuration),
              AsSingleton(self._transaction)))
    return p

  def display_data(self):
    res = dict()
    sql = []
    table = []
    if self._read_operations is not None:
      for ro in self._read_operations:
        if ro.is_sql is True:
          sql.append(ro.kwargs)
        elif ro.is_table is True:
          table.append(ro.kwargs)

      if sql:
        res['sql'] = DisplayDataItem(str(sql), label='Sql')
      if table:
        res['table'] = DisplayDataItem(str(table), label='Table')

    if self._transaction:
      res['transaction'] = DisplayDataItem(
          str(self._transaction), label='transaction')

    return res


@experimental(extra_message="No backwards-compatibility guarantees.")
class WriteToSpanner(PTransform):
  def __init__(
      self,
      project_id,
      instance_id,
      database_id,
      pool=None,
      credentials=None,
      max_batch_size_bytes=1048576,
      max_number_rows=50,
      max_number_cells=500):
    """
    A PTransform to write onto Google Cloud Spanner.

    Args:
      project_id: Cloud spanner project id. Be sure to use the Project ID,
        not the Project Number.
      instance_id: Cloud spanner instance id.
      database_id: Cloud spanner database id.
      max_batch_size_bytes: (optional) Split the mutations into batches to
        reduce the number of transaction sent to Spanner. By default it is
        set to 1 MB (1048576 Bytes).
      max_number_rows: (optional) Split the mutations into batches to
        reduce the number of transaction sent to Spanner. By default it is
        set to 50 rows per batch.
      max_number_cells: (optional) Split the mutations into batches to
        reduce the number of transaction sent to Spanner. By default it is
        set to 500 cells per batch.
    """
    self._configuration = _BeamSpannerConfiguration(
        project=project_id,
        instance=instance_id,
        database=database_id,
        credentials=credentials,
        pool=pool,
        snapshot_read_timestamp=None,
        snapshot_exact_staleness=None)
    self._max_batch_size_bytes = max_batch_size_bytes
    self._max_number_rows = max_number_rows
    self._max_number_cells = max_number_cells
    self._database_id = database_id
    self._project_id = project_id
    self._instance_id = instance_id
    self._pool = pool

  def display_data(self):
    res = {
        'project_id': DisplayDataItem(self._project_id, label='Project Id'),
        'instance_id': DisplayDataItem(self._instance_id, label='Instance Id'),
        'pool': DisplayDataItem(str(self._pool), label='Pool'),
        'database': DisplayDataItem(self._database_id, label='Database'),
        'batch_size': DisplayDataItem(
            self._max_batch_size_bytes, label="Batch Size"),
        'max_number_rows': DisplayDataItem(
            self._max_number_rows, label="Max Rows"),
        'max_number_cells': DisplayDataItem(
            self._max_number_cells, label="Max Cells"),
    }
    return res

  def expand(self, pcoll):
    return (
        pcoll
        | "make batches" >> _WriteGroup(
            max_batch_size_bytes=self._max_batch_size_bytes,
            max_number_rows=self._max_number_rows,
            max_number_cells=self._max_number_cells)
        |
        'Writing to spanner' >> ParDo(_WriteToSpannerDoFn(self._configuration)))


class _Mutator(namedtuple('_Mutator',
                          ["mutation", "operation", "kwargs", "rows", "cells"])
               ):
  __slots__ = ()

  @property
  def byte_size(self):
    return self.mutation.ByteSize()


class MutationGroup(deque):
  """
  A Bundle of Spanner Mutations (_Mutator).
  """
  @property
  def info(self):
    cells = 0
    rows = 0
    bytes = 0
    for m in self.__iter__():
      bytes += m.byte_size
      rows += m.rows
      cells += m.cells
    return {"rows": rows, "cells": cells, "byte_size": bytes}

  def primary(self):
    return next(self.__iter__())


class WriteMutation(object):

  _OPERATION_DELETE = "delete"
  _OPERATION_INSERT = "insert"
  _OPERATION_INSERT_OR_UPDATE = "insert_or_update"
  _OPERATION_REPLACE = "replace"
  _OPERATION_UPDATE = "update"

  def __init__(
      self,
      insert=None,
      update=None,
      insert_or_update=None,
      replace=None,
      delete=None,
      columns=None,
      values=None,
      keyset=None):
    """
    A convenient class to create Spanner Mutations for Write. User can provide
    the operation via constructor or via static methods.

    Note: If a user passing the operation via construction, make sure that it
    will only accept one operation at a time. For example, if a user passing
    a table name in the `insert` parameter, and he also passes the `update`
    parameter value, this will cause an error.

    Args:
      insert: (Optional) Name of the table in which rows will be inserted.
      update: (Optional) Name of the table in which existing rows will be
        updated.
      insert_or_update: (Optional) Table name in which rows will be written.
        Like insert, except that if the row already exists, then its column
        values are overwritten with the ones provided. Any column values not
        explicitly written are preserved.
      replace: (Optional) Table name in which rows will be replaced. Like
        insert, except that if the row already exists, it is deleted, and the
        column values provided are inserted instead. Unlike `insert_or_update`,
        this means any values not explicitly written become `NULL`.
      delete: (Optional) Table name from which rows will be deleted. Succeeds
        whether or not the named rows were present.
      columns: The names of the columns in table to be written. The list of
        columns must contain enough columns to allow Cloud Spanner to derive
        values for all primary key columns in the row(s) to be modified.
      values: The values to be written. `values` can contain more than one
        list of values. If it does, then multiple rows are written, one for
        each entry in `values`. Each list in `values` must have exactly as
        many entries as there are entries in columns above. Sending multiple
        lists is equivalent to sending multiple Mutations, each containing one
        `values` entry and repeating table and columns.
      keyset: (Optional) The primary keys of the rows within table to delete.
        Delete is idempotent. The transaction will succeed even if some or
        all rows do not exist.
    """
    self._columns = columns
    self._values = values
    self._keyset = keyset

    self._insert = insert
    self._update = update
    self._insert_or_update = insert_or_update
    self._replace = replace
    self._delete = delete

    if sum([1 for x in [self._insert,
                        self._update,
                        self._insert_or_update,
                        self._replace,
                        self._delete] if x is not None]) != 1:
      raise ValueError(
          "No or more than one write mutation operation "
          "provided: <%s: %s>" % (self.__class__.__name__, str(self.__dict__)))

  def __call__(self, *args, **kwargs):
    if self._insert is not None:
      return WriteMutation.insert(
          table=self._insert, columns=self._columns, values=self._values)
    elif self._update is not None:
      return WriteMutation.update(
          table=self._update, columns=self._columns, values=self._values)
    elif self._insert_or_update is not None:
      return WriteMutation.insert_or_update(
          table=self._insert_or_update,
          columns=self._columns,
          values=self._values)
    elif self._replace is not None:
      return WriteMutation.replace(
          table=self._replace, columns=self._columns, values=self._values)
    elif self._delete is not None:
      return WriteMutation.delete(table=self._delete, keyset=self._keyset)

  @staticmethod
  def insert(table, columns, values):
    """Insert one or more new table rows.

    Args:
      table: Name of the table to be modified.
      columns: Name of the table columns to be modified.
      values: Values to be modified.
    """
    rows = len(values)
    cells = len(columns) * len(values)
    return _Mutator(
        mutation=Mutation(insert=batch._make_write_pb(table, columns, values)),
        operation=WriteMutation._OPERATION_INSERT,
        rows=rows,
        cells=cells,
        kwargs={
            "table": table, "columns": columns, "values": values
        })

  @staticmethod
  def update(table, columns, values):
    """Update one or more existing table rows.

    Args:
      table: Name of the table to be modified.
      columns: Name of the table columns to be modified.
      values: Values to be modified.
    """
    rows = len(values)
    cells = len(columns) * len(values)
    return _Mutator(
        mutation=Mutation(update=batch._make_write_pb(table, columns, values)),
        operation=WriteMutation._OPERATION_UPDATE,
        rows=rows,
        cells=cells,
        kwargs={
            "table": table, "columns": columns, "values": values
        })

  @staticmethod
  def insert_or_update(table, columns, values):
    """Insert/update one or more table rows.
    Args:
      table: Name of the table to be modified.
      columns: Name of the table columns to be modified.
      values: Values to be modified.
    """
    rows = len(values)
    cells = len(columns) * len(values)
    return _Mutator(
        mutation=Mutation(
            insert_or_update=batch._make_write_pb(table, columns, values)),
        operation=WriteMutation._OPERATION_INSERT_OR_UPDATE,
        rows=rows,
        cells=cells,
        kwargs={
            "table": table, "columns": columns, "values": values
        })

  @staticmethod
  def replace(table, columns, values):
    """Replace one or more table rows.

    Args:
      table: Name of the table to be modified.
      columns: Name of the table columns to be modified.
      values: Values to be modified.
    """
    rows = len(values)
    cells = len(columns) * len(values)
    return _Mutator(
        mutation=Mutation(replace=batch._make_write_pb(table, columns, values)),
        operation=WriteMutation._OPERATION_REPLACE,
        rows=rows,
        cells=cells,
        kwargs={
            "table": table, "columns": columns, "values": values
        })

  @staticmethod
  def delete(table, keyset):
    """Delete one or more table rows.

    Args:
      table: Name of the table to be modified.
      keyset: Keys/ranges identifying rows to delete.
    """
    delete = Mutation.Delete(table=table, key_set=keyset._to_pb())
    return _Mutator(
        mutation=Mutation(delete=delete),
        rows=0,
        cells=0,
        operation=WriteMutation._OPERATION_DELETE,
        kwargs={
            "table": table, "keyset": keyset
        })


@with_input_types(typing.Union[MutationGroup, TaggedOutput])
@with_output_types(MutationGroup)
class _BatchFn(DoFn):
  """
  Batches mutations together.
  """
  def __init__(self, max_batch_size_bytes, max_number_rows, max_number_cells):
    self._max_batch_size_bytes = max_batch_size_bytes
    self._max_number_rows = max_number_rows
    self._max_number_cells = max_number_cells

  def start_bundle(self):
    self._batch = MutationGroup()
    self._size_in_bytes = 0
    self._rows = 0
    self._cells = 0

  def _reset_count(self):
    self._batch = MutationGroup()
    self._size_in_bytes = 0
    self._rows = 0
    self._cells = 0

  def process(self, element):
    mg_info = element.info

    if mg_info['byte_size'] + self._size_in_bytes > self._max_batch_size_bytes \
        or mg_info['cells'] + self._cells > self._max_number_cells \
        or mg_info['rows'] + self._rows > self._max_number_rows:
      # Batch is full, output the batch and resetting the count.
      if self._batch:
        yield self._batch
      self._reset_count()

    self._batch.extend(element)

    # total byte size of the mutation group.
    self._size_in_bytes += mg_info['byte_size']

    # total rows in the mutation group.
    self._rows += mg_info['rows']

    # total cells in the mutation group.
    self._cells += mg_info['cells']

  def finish_bundle(self):
    if self._batch is not None:
      yield window.GlobalWindows.windowed_value(self._batch)
      self._batch = None


@with_input_types(MutationGroup)
@with_output_types(MutationGroup)
class _BatchableFilterFn(DoFn):
  """
  Filters MutationGroups larger than the batch size to the output tagged with
  OUTPUT_TAG_UNBATCHABLE.
  """
  OUTPUT_TAG_UNBATCHABLE = 'unbatchable'

  def __init__(self, max_batch_size_bytes, max_number_rows, max_number_cells):
    self._max_batch_size_bytes = max_batch_size_bytes
    self._max_number_rows = max_number_rows
    self._max_number_cells = max_number_cells
    self._batchable = None
    self._unbatchable = None

  def process(self, element):
    if element.primary().operation == WriteMutation._OPERATION_DELETE:
      # As delete mutations are not batchable.
      yield TaggedOutput(_BatchableFilterFn.OUTPUT_TAG_UNBATCHABLE, element)
    else:
      mg_info = element.info
      if mg_info['byte_size'] > self._max_batch_size_bytes \
          or mg_info['cells'] > self._max_number_cells \
          or mg_info['rows'] > self._max_number_rows:
        yield TaggedOutput(_BatchableFilterFn.OUTPUT_TAG_UNBATCHABLE, element)
      else:
        yield element


class _WriteToSpannerDoFn(DoFn):
  def __init__(self, spanner_configuration):
    self._spanner_configuration = spanner_configuration
    self._db_instance = None
    self.batches = Metrics.counter(self.__class__, 'SpannerBatches')

  def setup(self):
    spanner_client = Client(self._spanner_configuration.project)
    instance = spanner_client.instance(self._spanner_configuration.instance)
    self._db_instance = instance.database(
        self._spanner_configuration.database,
        pool=self._spanner_configuration.pool)

  def process(self, element):
    self.batches.inc()
    with self._db_instance.batch() as b:
      for m in element:
        if m.operation == WriteMutation._OPERATION_DELETE:
          batch_func = b.delete
        elif m.operation == WriteMutation._OPERATION_REPLACE:
          batch_func = b.replace
        elif m.operation == WriteMutation._OPERATION_INSERT_OR_UPDATE:
          batch_func = b.insert_or_update
        elif m.operation == WriteMutation._OPERATION_INSERT:
          batch_func = b.insert
        elif m.operation == WriteMutation._OPERATION_UPDATE:
          batch_func = b.update
        else:
          raise ValueError("Unknown operation action: %s" % m.operation)

        batch_func(**m.kwargs)


@with_input_types(typing.Union[MutationGroup, _Mutator])
@with_output_types(MutationGroup)
class _MakeMutationGroupsFn(DoFn):
  """
  Make Mutation group object if the element is the instance of _Mutator.
  """
  def process(self, element):
    if isinstance(element, MutationGroup):
      yield element
    elif isinstance(element, _Mutator):
      yield MutationGroup([element])
    else:
      raise ValueError(
          "Invalid object type: %s. Object must be an instance of "
          "MutationGroup or WriteMutations" % str(element))


class _WriteGroup(PTransform):
  def __init__(self, max_batch_size_bytes, max_number_rows, max_number_cells):
    self._max_batch_size_bytes = max_batch_size_bytes
    self._max_number_rows = max_number_rows
    self._max_number_cells = max_number_cells

  def expand(self, pcoll):
    filter_batchable_mutations = (
        pcoll
        | 'Making mutation groups' >> ParDo(_MakeMutationGroupsFn())
        | 'Filtering Batchable Mutations' >> ParDo(
            _BatchableFilterFn(
                max_batch_size_bytes=self._max_batch_size_bytes,
                max_number_rows=self._max_number_rows,
                max_number_cells=self._max_number_cells)).with_outputs(
                    _BatchableFilterFn.OUTPUT_TAG_UNBATCHABLE, main='batchable')
    )

    batching_batchables = (
        filter_batchable_mutations['batchable']
        | ParDo(
            _BatchFn(
                max_batch_size_bytes=self._max_batch_size_bytes,
                max_number_rows=self._max_number_rows,
                max_number_cells=self._max_number_cells)))

    return ((
        batching_batchables,
        filter_batchable_mutations[_BatchableFilterFn.OUTPUT_TAG_UNBATCHABLE])
            | 'Merging batchable and unbatchable' >> Flatten())
