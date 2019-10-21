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
:meth:`create_transaction` method available in the SpannerIO API.

ReadFromSpanner takes this transform in the constructor and pass this to the
read pipeline as the singleton side input.
"""
from __future__ import absolute_import

import typing
from collections import namedtuple

from apache_beam import Create
from apache_beam import DoFn
from apache_beam import ParDo
from apache_beam import Reshuffle
from apache_beam.pvalue import AsSingleton
from apache_beam.pvalue import PBegin
from apache_beam.transforms import PTransform
from apache_beam.transforms import ptransform_fn
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.typehints import with_input_types
from apache_beam.typehints import with_output_types
from apache_beam.utils.annotations import experimental

try:
  from google.cloud.spanner import Client
  from google.cloud.spanner import KeySet
  from google.cloud.spanner_v1.database import BatchSnapshot
except ImportError:
  Client = None
  KeySet = None
  BatchSnapshot = None

__all__ = ['create_transaction', 'ReadFromSpanner', 'ReadOperation']


class ReadOperation(namedtuple("ReadOperation", ["is_sql", "is_table",
                                                 "read_operation", "kwargs"])):
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
                       "google.cloud.spanner.KeySet")
    return cls(
        is_sql=False,
        is_table=True,
        read_operation="process_read_batch",
        kwargs={'table': table, 'columns': columns, 'index': index,
                'keyset': keyset}
    )


class _BeamSpannerConfiguration(namedtuple(
    "_BeamSpannerConfiguration", ["project", "instance", "database",
                                  "credentials", "pool",
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
    In Naive reads, this transform performs single reads, where as the the
    Batch read use the spanner partitioning query to create batches.

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
    self._database = instance.database(self._spanner_configuration.database,
                                       pool=self._spanner_configuration.pool)

  def process(self, element, transaction_info):
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
        raise ValueError("ReadOperation is improperly configure: %s" % str(
            element))

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
    spanner_client = Client(project=self._spanner_configuration.project,
                            credentials=self._spanner_configuration.credentials)
    instance = spanner_client.instance(self._spanner_configuration.instance)
    self._database = instance.database(self._spanner_configuration.database,
                                       pool=self._spanner_configuration.pool)
    self._snapshot = self._database.batch_snapshot(**self._spanner_configuration
                                                   .snapshot_options)
    self._snapshot_dict = self._snapshot.to_dict()

  def process(self, element):
    if element.is_sql is True:
      partitioning_action = self._snapshot.generate_query_batches
    elif element.is_table is True:
      partitioning_action = self._snapshot.generate_read_batches
    else:
      raise ValueError("ReadOperation is improperly configure: %s" % str(
          element))

    for p in partitioning_action(**element.kwargs):
      yield {"is_sql": element.is_sql, "is_table": element.is_table,
             "read_operation": element.read_operation, "partitions": p,
             "transaction_info": self._snapshot_dict}


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

  def setup(self):
    self._spanner_client = Client(project=self._project_id,
                                  credentials=self._credentials)
    self._instance = self._spanner_client.instance(self._instance_id)
    self._database = self._instance.database(self._database_id, pool=self._pool)

  def process(self, element, *args, **kwargs):
    self._snapshot = self._database.batch_snapshot(**self._snapshot_options)
    return [self._snapshot.to_dict()]


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

  # def to_runner_api_parameter(self, context):
  #   return self.to_runner_api_pickled(context)

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

    if element['is_sql'] is True:
      read_action = self._snapshot.process_query_batch
    elif element['is_table'] is True:
      read_action = self._snapshot.process_read_batch
    else:
      raise ValueError("ReadOperation is improperly configure: %s" % str(
          element))

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
      # reading as batch read using the spanner partitioning query to create
      # batches.
      p = (pcoll
           | 'Generate Partitions' >> ParDo(_CreateReadPartitions(
               spanner_configuration=self._configuration))
           | 'Reshuffle' >> Reshuffle()
           | 'Read From Partitions' >> ParDo(_ReadFromPartitionFn(
               spanner_configuration=self._configuration)))
    else:
      # reading as naive read, in which we dont make batches and execute the
      # queries and a single read.
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
        if ro.is_sql is True:
          sql.append(ro.kwargs)
        elif ro.is_table is True:
          table.append(ro.kwargs)

      if sql:
        res['sql'] = DisplayDataItem(str(sql), label='Sql')
      if table:
        res['table'] = DisplayDataItem(str(table), label='Table')

    if self._transaction:
      res['transaction'] = DisplayDataItem(str(self._transaction),
                                           label='transaction')

    return res
