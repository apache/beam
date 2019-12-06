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

This is an experimental module for reading and writing data from Google Cloud
Spanner. Visit: https://cloud.google.com/spanner for more details.

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
                      ReadOperation.table('customers', ['name', 'email']),
                      ReadOperation.table('vendors', ['name', 'email']),
                    ]
  all_users = pipeline | ReadFromSpanner(PROJECT_ID, INSTANCE_ID, DB_NAME,
        read_operations=read_operations)

  ...OR...

  read_operations = [
                      ReadOperation.query('Select name, email from customers'),
                      ReadOperation.query(
                        sql='Select * from users where id <= @user_id',
                        params={'user_id': 100},
                        params_type={'user_id': param_types.INT64}
                      ),
                    ]
  # `params_types` are instance of `google.cloud.spanner_v1.param_types`
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

This transform requires root of the pipeline (PBegin) and returns the dict
containing 'session_id' and 'transaction_id'. This `create_transaction`
PTransform later passed to the constructor of ReadFromSpanner. For example:::

  transaction = (pipeline | create_transaction(TEST_PROJECT_ID,
                                              TEST_INSTANCE_ID,
                                              DB_NAME))

  users = pipeline | ReadFromSpanner(PROJECT_ID, INSTANCE_ID, DB_NAME,
        sql='Select * from users', transaction=transaction)

  tweets = pipeline | ReadFromSpanner(PROJECT_ID, INSTANCE_ID, DB_NAME,
        sql='Select * from tweets', transaction=transaction)

For further details of this transform, please review the docs on the
`create_transaction` method available in the SpannerIO API.

ReadFromSpanner takes this transform in the constructor and pass this to the
read pipeline as the singleton side input.


Writing Data to Cloud Spanner
=============================

The `WriteToSpanner` transform writes to Cloud Spanner by executing a collection
a input rows (WriteMutation). The mutations are grouped into batches for
efficiency.

WriteToSpanner relies on the WriteMutation objects which is exposed by the
SpannerIO API. WriteMutation have five static methods (insert, update,
insert_or_update, replace, delete). These methods returns the instance of the
`_Mutator` object which contains the mutation type and the Spanner Mutation
object. For more details, review the docs of the class SpannerIO.WriteMutation.

To perform WriteToSpanner, user can provide the list of WriteMutation
(WriteMutation.insert, WriteMutation.update, WriteMutation.insert_or_update,
WriteMutation.replace or WriteMutation.delete). For example:::

  with Pipeline() as p:
    mutations = [
        WriteMutation.insert('users', ('name', 'email'),
                             [('john smith', 'johnsmith@example.com')]),
        WriteMutation.insert('users', ('name', 'email', 'dob'),
                             [('sara', 'sara.dev@example.com', '1990-10-15')])
    ]
    _ = ( p
          | beam.Create(mutations)
          | WriteToSpanner(
              project_id=SPANNER_PROJECT_ID,
              instance_id=SPANNER_INSTANCE_ID,
              database_id=SPANNER_DATABASE_NAME
        )
    )

You can also create WriteMutation via calling its constructor. For example:::

  mutations = [
      WriteMutation(insert="users", columns=("name", "email"),
                    values=[("sara", "sara@example.com")]
  ]

For more information, review the docs available on `WriteMutation` class.

WriteToSpanner transform also takes "max_batch_size_bytes" param which is set to
1MB (1048576) by default. This parameter used to reduce the number of
transactions sent to spanner by grouping the mutation into batches. Setting this
either to smaller value or zero to disable batching.

WriteToSpanner transforms starts with the grouping into batches. The first step
in this process is to make the make the mutation groups of the WriteMutation
objects and then filtering them into batchable and non-batchable mutation
groups by getting the mutation size from the method available in the
`google.cloud.spanner_v1.proto.mutation_pb2.Mutation.ByteSize`, if the size is
smaller than value of "max_batch_size_bytes" param, it will be tagged as
"unbatchable" mutation. After this all the batchable mutation are merged into a
single mutation group whos size is not larger than the "max_batch_size_bytes",
after this, all the mutation groups flatten together to process.

For processing these batches, we used Spanner "BatchCheckout"
(google.cloud.spanner_v1.database.BatchCheckout). For further information,
visit: https://googleapis.dev/python/spanner/1.12.0/database-api.html
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
    'create_transaction', 'ReadFromSpanner', 'ReadOperation', 'WriteToSpanner',
    'WriteMutation', 'MutationGroup'
]


class ReadOperation(namedtuple("ReadOperation",
                               ["read_operation", "batch_action",
                                "transaction_action", "kwargs"])):
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
        read_operation="process_query_batch",
        batch_action="generate_query_batches", transaction_action="execute_sql",
        kwargs={'sql': sql, 'params': params, 'param_types': param_types}
    )

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
      raise ValueError("keyset must be an instance of class "
                       "google.cloud.spanner_v1.keyset.KeySet")
    return cls(
        read_operation="process_read_batch",
        batch_action="generate_read_batches", transaction_action="read",
        kwargs={'table': table, 'columns': columns, 'index': index,
                'keyset': keyset}
    )


class _BeamSpannerConfiguration(namedtuple(
    "_BeamSpannerConfiguration", ["project", "instance", "database",
                                  "credentials", "pool",
                                  "snapshot_read_timestamp",
                                  "snapshot_exact_staleness"])):
  """
  It holds the immutable data of the connection string to the cloud spanner.
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

  def to_runner_api_parameter(self, context):
    return self.to_runner_api_pickled(context)

  def setup(self):
    # setting up client to connect with cloud spanner
    spanner_client = Client(self._spanner_configuration.project)
    instance = spanner_client.instance(self._spanner_configuration.instance)
    self._database = instance.database(self._spanner_configuration.database,
                                       pool=self._spanner_configuration.pool)

  def process(self, element, transaction_info):
    # We used batch snapshot to reuse the same transaction passed through the
    # side input
    self._snapshot = BatchSnapshot.from_dict(self._database, transaction_info)

    # getting the transaction from the snapshot's session to run read operation.
    # with self._snapshot.session().transaction() as transaction:
    with self._get_session().transaction() as transaction:
      if element.transaction_action == 'execute_sql':
        transaction_read = transaction.execute_sql
      elif element.transaction_action == 'read':
        transaction_read = transaction.read
      else:
        raise ValueError("Unknown transaction action: %s" %
                         element.transaction_action)

      for row in transaction_read(**element.kwargs):
        yield row

  def teardown(self):
    self._close_session()

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

  def to_runner_api_parameter(self, context):
    return self.to_runner_api_pickled(context)

  def setup(self):
    spanner_client = Client(project=self._spanner_configuration.project,
                            credentials=self._spanner_configuration.credentials)
    instance = spanner_client.instance(self._spanner_configuration.instance)
    self._database = instance.database(self._spanner_configuration.database,
                                       pool=self._spanner_configuration.pool)
    self._snapshot = self._database.batch_snapshot(**self._spanner_configuration
                                                   .snapshot_options)
    self._snapshot_dict = self._snapshot.to_dict()

  def process(self, element):
    if element.batch_action == 'generate_query_batches':
      partitioning_action = self._snapshot.generate_query_batches
    elif element.batch_action == 'generate_read_batches':
      partitioning_action = self._snapshot.generate_read_batches

    for p in partitioning_action(**element.kwargs):
      yield {"read_operation": element.read_operation, "partitions": p,
             "transaction_info": self._snapshot_dict}

  def teardown(self):
    if self._snapshot:
      self._snapshot.close()

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

  def __init__(self, project_id, instance_id, database_id, credentials,
               pool, read_timestamp,
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

  def to_runner_api_parameter(self, context):
    return self.to_runner_api_pickled(context)

  def setup(self):
    self._spanner_client = Client(project=self._project_id,
                                  credentials=self._credentials)
    self._instance = self._spanner_client.instance(self._instance_id)
    self._database = self._instance.database(self._database_id, pool=self._pool)

  def process(self, element, *args, **kwargs):
    self._snapshot = self._database.batch_snapshot(**self._snapshot_options)
    return [self._snapshot.to_dict()]

  def teardown(self):
    if self._snapshot:
      self._snapshot.close()


@ptransform_fn
def create_transaction(pbegin, project_id, instance_id, database_id,
                       credentials=None, pool=None, read_timestamp=None,
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
    pool: (optional) session pool to be used by database. If not passed, the
      database will construct an instance of BurstyPool.
    read_timestamp: (optional) An instance of the `datetime.datetime` object to
      execute all reads at the given timestamp.
    exact_staleness: (optional) And instance of `datetime.timedelta` object to
      execute all reads at a timestamp that is exact_staleness old.
  """

  assert isinstance(pbegin, PBegin)

  return (pbegin | Create([1]) | ParDo(_CreateTransactionFn(
      project_id, instance_id, database_id, credentials,
      pool, read_timestamp,
      exact_staleness)))

@with_input_types(typing.Dict[typing.Any, typing.Any])
@with_output_types(typing.List[typing.Any])
class _ReadFromPartitionFn(DoFn):
  """
  A DoFn to perform reads from the partition.
  """

  def __init__(self, spanner_configuration):
    self._spanner_configuration = spanner_configuration

  def to_runner_api_parameter(self, context):
    return self.to_runner_api_pickled(context)

  def setup(self):
    spanner_client = Client(self._spanner_configuration.project)
    instance = spanner_client.instance(self._spanner_configuration.instance)
    self._database = instance.database(self._spanner_configuration.database,
                                       pool=self._spanner_configuration.pool)
    self._snapshot = self._database.batch_snapshot(**self._spanner_configuration
                                                   .snapshot_options)

  def process(self, element):
    self._snapshot = BatchSnapshot.from_dict(
        self._database,
        element['transaction_info']
    )

    if element['read_operation'] == 'process_query_batch':
      read_action = self._snapshot.process_query_batch
    elif element['read_operation'] == 'process_read_batch':
      read_action = self._snapshot.process_read_batch
    else:
      raise ValueError("Unknown read action.")

    for row in read_action(element['partitions']):
      yield row

  def teardown(self):
    if self._snapshot:
      self._snapshot.close()


@experimental()
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
      pool: (optional) Session pool to be used by database. If not passed, the
        database will construct an instance of BurstyPool.
      read_timestamp: (optional) An instance of the `datetime.datetime` object
        to execute all reads at the given timestamp.
      exact_staleness: (optional) An instance of `datetime.timedelta` object
        to execute all reads at a timestamp that is exact_staleness old.
      credentials: (optional) The authorization credentials to attach to
        requests. These credentials identify this application to the service.
        If none are specified, the client will attempt to ascertain
        the credentials from the environment.
      sql: (optional) SQL query statement
      params: (optional) Values for parameter replacement. Keys must match the
        names used in sql.
      param_types: (optional) maps explicit types for one or more param values;
        required if params are passed.
      table: (optional) Name of the table from which to fetch data.
      columns: (optional) List of names of columns to be retrieved; required if
        the table is passed.
      index: (optional) name of index to use, rather than the table's primary
        key.
      keyset: (optional) keys / ranges identifying rows to be retrieved.
      read_operations: (optional) List of the objects of :class:`ReadOperation`
        to perform read all.
      transaction: (optional) PTransform of the :meth:`create_transaction` to
        perform naive read on cloud spanner.
    """
    self._configuration = _BeamSpannerConfiguration(
        project=project_id, instance=instance_id, database=database_id,
        credentials=credentials, pool=pool,
        snapshot_read_timestamp=read_timestamp,
        snapshot_exact_staleness=exact_staleness
    )

    self._read_operations = read_operations
    self._transaction = transaction

    if self._read_operations is None:
      if table is not None:
        if columns is None:
          raise ValueError("Columns are required with the table name.")
        self._read_operations = [ReadOperation.table(
            table=table, columns=columns, index=index, keyset=keyset)]
      elif sql is not None:
        self._read_operations = [ReadOperation.query(
            sql=sql, params=params, param_types=param_types)]

  def expand(self, pbegin):
    if self._read_operations is not None and isinstance(pbegin,
                                                        PBegin):
      pcoll = pbegin.pipeline | Create(self._read_operations)
    elif not isinstance(pbegin, PBegin):
      if self._read_operations is not None:
        raise ValueError("Read operation in the constructor only works with "
                         "the root of the pipeline.")
      pcoll = pbegin
    else:
      raise ValueError("Spanner required read operation, sql or table "
                       "with columns.")

    if self._transaction is None:
      # reading as batch read
      p = (pcoll
           | 'Generate Partitions' >> ParDo(_CreateReadPartitions(
               spanner_configuration=self._configuration))
           #.with_input_types(ReadOperation)
           | 'Reshuffle' >> Reshuffle()
           | 'Read From Partitions' >> ParDo(_ReadFromPartitionFn(
               spanner_configuration=self._configuration)))
    else:
      # reading as naive read
      p = (pcoll
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
        if ro.read_operation == 'process_query_batch':
          sql.append(ro.kwargs)
        elif ro.read_operation == 'process_read_batch':
          table.append(ro.kwargs)

      if sql:
        res['sql'] = DisplayDataItem(str(sql), label='Sql')
      if table:
        res['table'] = DisplayDataItem(str(table), label='Table')

    if self._transaction:
      res['transaction'] = DisplayDataItem(str(self._transaction),
                                           label='transaction')

    return res


class WriteToSpanner(PTransform):

  def __init__(self,
               project_id,
               instance_id,
               database_id,
               max_batch_size_bytes=1048576):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._max_batch_size_bytes = max_batch_size_bytes

  def expand(self, pcoll):

    return (pcoll
            | "making batches" >>
            _WriteGroup(max_batch_size_bytes=self._max_batch_size_bytes)
            | 'Writing to spanner' >> ParDo(
                _WriteToSpannerDoFn(self._project_id, self._instance_id,
                                    self._database_id)))


class _Mutator(namedtuple('_Mutator', ["mutation", "operation"])):
  __slots__ = ()

  @property
  def byte_size(self):
    return self.mutation.ByteSize()


class MutationGroup(deque):
  """
  A Bundle of Spanner Mutations (_Mutator).
  """

  @property
  def byte_size(self):
    s = 0
    for m in self.__iter__():
      s += m.byte_size
    return s

  def primary(self):
    return next(self.__iter__())


class WriteMutation(object):

  def __init__(self,
               insert=None,
               update=None,
               insert_or_update=None,
               replace=None,
               delete=None,
               columns=None,
               values=None,
               keyset=None):
    """
    A convenient class to create Spanner Mutations for Write.

    Args:
      insert: (Optional) Name of the table in which rows will be inserted. If
        any of the rows already exist, the write or  transaction fails with
        error `ALREADY_EXISTS`.
      update: (Optional) Name of the table in which existing rows will be
        updated. If any of the rows does not already exist, the transaction
        fails with error `NOT_FOUND`.
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
      keyset: The primary keys of the rows within table to delete. Delete is
        idempotent. The transaction will succeed even if some or all rows do
        not exist.
    """
    self._columns = columns
    self._values = values
    self._keyset = keyset

    self._insert = insert
    self._update = update
    self._insert_or_update = update
    self._replace = replace
    self._delete = delete

    if sum([
        1 for x in [insert, update, insert_or_update, replace, delete]
        if x is not None
    ]) != 1:
      raise ValueError("No or more than one write mutation operation provided.")

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
    return _Mutator(
        mutation=Mutation(insert=batch._make_write_pb(table, columns, values)),
        operation='insert')

  @staticmethod
  def update(table, columns, values):
    """Update one or more existing table rows.
    Args:
      table: Name of the table to be modified.
      columns: Name of the table columns to be modified.
      values: Values to be modified.
    """
    return _Mutator(
        mutation=Mutation(update=batch._make_write_pb(table, columns, values)),
        operation='update')

  @staticmethod
  def insert_or_update(table, columns, values):
    """Insert/update one or more table rows.
    Args:
      table: Name of the table to be modified.
      columns: Name of the table columns to be modified.
      values: Values to be modified.
    """
    return _Mutator(
        mutation=Mutation(
            insert_or_update=batch._make_write_pb(table, columns, values)),
        operation='insert_or_update')

  @staticmethod
  def replace(table, columns, values):
    """Replace one or more table rows.
    Args:
      table: Name of the table to be modified.
      columns: Name of the table columns to be modified.
      values: Values to be modified.
    """
    return _Mutator(
        mutation=Mutation(replace=batch._make_write_pb(table, columns, values)),
        operation="replace")

  @staticmethod
  def delete(table, keyset):
    """Delete one or more table rows.
    Args:
      table: Name of the table to be modified.
      keyset: Keys/ranges identifying rows to delete.
    """
    delete = Mutation.Delete(table=table, key_set=keyset._to_pb())
    return _Mutator(mutation=Mutation(delete=delete), operation='delete')


class _BatchFn(DoFn):
  """
  Batches mutations together.
  """

  def __init__(
      self,
      max_batch_size_bytes,
  ):
    self._max_batch_size_bytes = max_batch_size_bytes

  def start_bundle(self):
    self._batch = MutationGroup()
    self._size_in_bytes = 0

  def process(self, element):
    _max_bytes = self._max_batch_size_bytes
    mg = element
    mg_size = mg.byte_size  # total size of the mutation group.

    if mg_size + self._size_in_bytes > _max_bytes:
      # Batch is full, output the batch and resetting the count.
      yield self._batch
      self._size_in_bytes = 0
      self._batch = MutationGroup()

    self._batch.extend(mg)
    self._size_in_bytes += mg_size

  def finish_bundle(self):
    if self._batch is not None:
      yield window.GlobalWindows.windowed_value(self._batch)
      self._batch = None


class _BatchableFilterFn(DoFn):
  """
  Filters MutationGroups larger than the batch size to the output tagged with
  OUTPUT_TAG_UNBATCHABLE.
  """
  OUTPUT_TAG_UNBATCHABLE = 'unbatchable'

  def __init__(self, max_batch_size_bytes):
    self._max_batch_size_bytes = max_batch_size_bytes
    self._batchable = None
    self._unbatchable = None

  def process(self, element):
    if element.primary().operation == 'delete':
      # As delete mutations are not batchable.
      yield TaggedOutput(_BatchableFilterFn.OUTPUT_TAG_UNBATCHABLE, element)
    else:
      _max_bytes = self._max_batch_size_bytes
      mg = element
      mg_size = mg.byte_size
      if mg_size > _max_bytes:
        yield TaggedOutput(_BatchableFilterFn.OUTPUT_TAG_UNBATCHABLE, element)
      else:
        yield element


class _WriteToSpannerDoFn(DoFn):

  def __init__(self, project_id, instance_id, database_id):
    self._project_id = project_id
    self._instance_id = instance_id
    self._database_id = database_id
    self._db_instance = None
    self.batches = Metrics.counter(self.__class__, 'SpannerBatches')

  def to_runner_api_parameter(self, context):
    return self.to_runner_api_pickled(context)

  def setup(self):
    spanner_client = Client(self._project_id)
    instance = spanner_client.instance(self._instance_id)
    self._db_instance = instance.database(self._database_id)

  def process(self, element):
    self.batches.inc()
    with self._db_instance.batch() as b:
      b._mutations.extend([x.mutation for x in element])


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
          "Invalid object type. Object must be an instance of MutationGroup or "
          "WriteMutations")


class _WriteGroup(PTransform):

  def __init__(self, max_batch_size_bytes=1024):
    self._max_batch_size_bytes = max_batch_size_bytes

  def expand(self, pcoll):
    filter_batchable_mutations = (
        pcoll
        | 'Making mutation groups' >> ParDo(
            _MakeMutationGroupsFn()).with_input_types(
                typing.Union[MutationGroup, _Mutator])
        | 'Filtering Batchable Mutations' >> ParDo(
            _BatchableFilterFn(self._max_batch_size_bytes)).with_outputs(
                _BatchableFilterFn.OUTPUT_TAG_UNBATCHABLE, main='batchable'))

    batching_batchables = (
        filter_batchable_mutations['batchable']
        | ParDo(_BatchFn(self._max_batch_size_bytes)))

    return (
        (batching_batchables,
         filter_batchable_mutations[_BatchableFilterFn.OUTPUT_TAG_UNBATCHABLE])
        | 'Merging batchable and unbatchable' >> Flatten())
