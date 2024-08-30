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

"""PTransforms for supporting Spanner in Python pipelines.

  These transforms are currently supported by Beam portable
  Flink and Spark runners.

  **Setup**

  Transforms provided in this module are cross-language transforms
  implemented in the Beam Java SDK. During the pipeline construction, Python SDK
  will connect to a Java expansion service to expand these transforms.
  To facilitate this, a small amount of setup is needed before using these
  transforms in a Beam Python pipeline.

  There are several ways to setup cross-language Spanner transforms.

  * Option 1: use the default expansion service
  * Option 2: specify a custom expansion service

  See below for details regarding each of these options.

  *Option 1: Use the default expansion service*

  This is the recommended and easiest setup option for using Python Spanner
  transforms. This option is only available for Beam 2.26.0 and later.

  This option requires following pre-requisites before running the Beam
  pipeline.

  * Install Java runtime in the computer from where the pipeline is constructed
    and make sure that 'java' command is available.

  In this option, Python SDK will either download (for released Beam version) or
  build (when running from a Beam Git clone) a expansion service jar and use
  that to expand transforms. Currently Spanner transforms use the
  'beam-sdks-java-io-google-cloud-platform-expansion-service' jar for this
  purpose.

  *Option 2: specify a custom expansion service*

  In this option, you startup your own expansion service and provide that as
  a parameter when using the transforms provided in this module.

  This option requires following pre-requisites before running the Beam
  pipeline.

  * Startup your own expansion service.
  * Update your pipeline to provide the expansion service address when
    initiating Spanner transforms provided in this module.

  Flink Users can use the built-in Expansion Service of the Flink Runner's
  Job Server. If you start Flink's Job Server, the expansion service will be
  started on port 8097. For a different address, please set the
  expansion_service parameter.

  **More information**

  For more information regarding cross-language transforms see:
  - https://beam.apache.org/roadmap/portability/

  For more information specific to Flink runner see:
  - https://beam.apache.org/documentation/runners/flink/
"""

# pytype: skip-file

from enum import Enum
from enum import auto
from typing import NamedTuple
from typing import Optional

from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder
from apache_beam.typehints.schemas import named_tuple_to_schema

__all__ = [
    'ReadFromSpanner',
    'SpannerDelete',
    'SpannerInsert',
    'SpannerInsertOrUpdate',
    'SpannerReplace',
    'SpannerUpdate',
    'TimestampBoundMode',
    'TimeUnit',
]


def default_io_expansion_service():
  return BeamJarExpansionService(
      'sdks:java:io:google-cloud-platform:expansion-service:shadowJar')


class TimeUnit(Enum):
  NANOSECONDS = auto()
  MICROSECONDS = auto()
  MILLISECONDS = auto()
  SECONDS = auto()
  HOURS = auto()
  DAYS = auto()


class TimestampBoundMode(Enum):
  MAX_STALENESS = auto()
  EXACT_STALENESS = auto()
  READ_TIMESTAMP = auto()
  MIN_READ_TIMESTAMP = auto()
  STRONG = auto()


class FailureMode(Enum):
  FAIL_FAST = auto()
  REPORT_FAILURES = auto()


class ReadFromSpannerSchema(NamedTuple):
  instance_id: str
  database_id: str
  schema: bytes
  sql: Optional[str]
  table: Optional[str]
  project_id: Optional[str]
  host: Optional[str]
  emulator_host: Optional[str]
  batching: Optional[bool]
  timestamp_bound_mode: Optional[str]
  read_timestamp: Optional[str]
  staleness: Optional[int]
  time_unit: Optional[str]


class ReadFromSpanner(ExternalTransform):
  """
  A PTransform which reads from the specified Spanner instance's database.

  This transform required type of the row it has to return to provide the
  schema. Example::

    from typing import NamedTuple
    from apache_beam import coders

    class ExampleRow(NamedTuple):
      id: int
      name: unicode

    coders.registry.register_coder(ExampleRow, coders.RowCoder)

    with Pipeline() as p:
      result = (
          p
          | ReadFromSpanner(
              instance_id='your_instance_id',
              database_id='your_database_id',
              project_id='your_project_id',
              row_type=ExampleRow,
              sql='SELECT * FROM some_table',
              timestamp_bound_mode=TimestampBoundMode.MAX_STALENESS,
              staleness=3,
              time_unit=TimeUnit.HOURS,
          ).with_output_types(ExampleRow))

  Experimental; no backwards compatibility guarantees.
  """

  URN = 'beam:transform:org.apache.beam:spanner_read:v1'

  def __init__(
      self,
      project_id,
      instance_id,
      database_id,
      row_type=None,
      sql=None,
      table=None,
      host=None,
      emulator_host=None,
      batching=None,
      timestamp_bound_mode=None,
      read_timestamp=None,
      staleness=None,
      time_unit=None,
      expansion_service=None,
  ):
    """
    Initializes a read operation from Spanner.

    :param project_id: Specifies the Cloud Spanner project.
    :param instance_id: Specifies the Cloud Spanner instance.
    :param database_id: Specifies the Cloud Spanner database.
    :param row_type: Row type that fits the given query or table. Passed as
        NamedTuple, e.g. NamedTuple('name', [('row_name', unicode)])
    :param sql: An sql query to execute. It's results must fit the
        provided row_type. Don't use when table is set.
    :param table: A spanner table. When provided all columns from row_type
        will be selected to query. Don't use when query is set.
    :param batching: By default Batch API is used to read data from Cloud
        Spanner. It is useful to disable batching when the underlying query
        is not root-partitionable.
    :param host: Specifies the Cloud Spanner host.
    :param emulator_host: Specifies Spanner emulator host.
    :param timestamp_bound_mode: Defines how Cloud Spanner will choose a
        timestamp for a read-only transaction or a single read/query.
        Passed as TimestampBoundMode enum. Possible values:
        STRONG: A timestamp bound that will perform reads and queries at a
        timestamp where all previously committed transactions are visible.
        READ_TIMESTAMP: Returns a timestamp bound that will perform reads
        and queries at the given timestamp.
        MIN_READ_TIMESTAMP: Returns a timestamp bound that will perform reads
        and queries at a timestamp chosen to be at least given timestamp value.
        EXACT_STALENESS: Returns a timestamp bound that will perform reads and
        queries at an exact staleness. The timestamp is chosen soon after the
        read is started.
        MAX_STALENESS: Returns a timestamp bound that will perform reads and
        queries at a timestamp chosen to be at most time_unit stale.
    :param read_timestamp: Timestamp in string. Use only when
        timestamp_bound_mode is set to READ_TIMESTAMP or MIN_READ_TIMESTAMP.
    :param staleness: Staleness value as int. Use only when
        timestamp_bound_mode is set to EXACT_STALENESS or MAX_STALENESS.
        time_unit has to be set along with this param.
    :param time_unit: Time unit for staleness_value passed as TimeUnit enum.
        Possible values: NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS,
        HOURS, DAYS.
    :param expansion_service: The address (host:port) of the ExpansionService.
    """
    assert row_type
    assert sql or table and not (sql and table)
    staleness_value = int(staleness) if staleness else None

    if staleness_value or time_unit:
      assert staleness_value and time_unit and \
             timestamp_bound_mode is TimestampBoundMode.MAX_STALENESS or \
             timestamp_bound_mode is TimestampBoundMode.EXACT_STALENESS

    if read_timestamp:
      assert timestamp_bound_mode is TimestampBoundMode.MIN_READ_TIMESTAMP\
             or timestamp_bound_mode is TimestampBoundMode.READ_TIMESTAMP

    super().__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            ReadFromSpannerSchema(
                instance_id=instance_id,
                database_id=database_id,
                sql=sql,
                table=table,
                schema=named_tuple_to_schema(row_type).SerializeToString(),
                project_id=project_id,
                host=host,
                emulator_host=emulator_host,
                batching=batching,
                timestamp_bound_mode=_get_enum_name(timestamp_bound_mode),
                read_timestamp=read_timestamp,
                staleness=staleness,
                time_unit=_get_enum_name(time_unit),
            ),
        ),
        expansion_service or default_io_expansion_service(),
    )


class WriteToSpannerSchema(NamedTuple):
  project_id: str
  instance_id: str
  database_id: str
  table: str
  max_batch_size_bytes: Optional[int]
  max_number_mutations: Optional[int]
  max_number_rows: Optional[int]
  grouping_factor: Optional[int]
  host: Optional[str]
  emulator_host: Optional[str]
  commit_deadline: Optional[int]
  max_cumulative_backoff: Optional[int]
  failure_mode: Optional[str]
  high_priority: bool


_CLASS_DOC = \
  """
  A PTransform which writes {operation} mutations to the specified Spanner
  table.

  This transform receives rows defined as NamedTuple. Example::

    from typing import NamedTuple
    from apache_beam import coders

    class {row_type}(NamedTuple):
       id: int
       name: unicode

    coders.registry.register_coder({row_type}, coders.RowCoder)

    with Pipeline() as p:
      _ = (
          p
          | 'Impulse' >> beam.Impulse()
          | 'Generate' >> beam.FlatMap(lambda x: range(num_rows))
          | 'To row' >> beam.Map(lambda n: {row_type}(n, str(n))
              .with_output_types({row_type})
          | 'Write to Spanner' >> Spanner{operation_suffix}(
              instance_id='your_instance',
              database_id='existing_database',
              project_id='your_project_id',
              table='your_table'))

  Experimental; no backwards compatibility guarantees.
  """

_INIT_DOC = \
  """
  Initializes {operation} operation to a Spanner table.

  :param project_id: Specifies the Cloud Spanner project.
  :param instance_id: Specifies the Cloud Spanner instance.
  :param database_id: Specifies the Cloud Spanner database.
  :param table: Specifies the Cloud Spanner table.
  :param max_batch_size_bytes: Specifies the batch size limit (max number of
      bytes mutated per batch). Default value is 1048576 bytes = 1MB.
  :param max_number_mutations: Specifies the cell mutation limit (maximum
      number of mutated cells per batch). Default value is 5000.
  :param max_number_rows: Specifies the row mutation limit (maximum number of
      mutated rows per batch). Default value is 500.
  :param grouping_factor: Specifies the multiple of max mutation (in terms
      of both bytes per batch and cells per batch) that is used to select a
      set of mutations to sort by key for batching. This sort uses local
      memory on the workers, so using large values can cause out of memory
      errors. Default value is 1000.
  :param host: Specifies the Cloud Spanner host.
  :param emulator_host: Specifies Spanner emulator host.
  :param commit_deadline: Specifies the deadline for the Commit API call.
      Default is 15 secs. DEADLINE_EXCEEDED errors will prompt a backoff/retry
      until the value of commit_deadline is reached. DEADLINE_EXCEEDED errors
      are ar reported with logging and counters. Pass seconds as value.
  :param max_cumulative_backoff: Specifies the maximum cumulative backoff
      time when retrying after DEADLINE_EXCEEDED errors. Default is 900s
      (15min). If the mutations still have not been written after this time,
      they are treated as a failure, and handled according to the setting of
      failure_mode. Pass seconds as value.
  :param failure_mode: Specifies the behavior for mutations that fail to be
      written to Spanner. Default is FAIL_FAST. When FAIL_FAST is set,
      an exception will be thrown for any failed mutation. When REPORT_FAILURES
      is set, processing will continue instead of throwing an exception. Note
      that REPORT_FAILURES can cause data loss if used incorrectly.
  :param expansion_service: The address (host:port) of the ExpansionService.
  """


def _add_doc(
    value,
    operation=None,
    row_type=None,
    operation_suffix=None,
):
  def _doc(obj):
    obj.__doc__ = value.format(
        operation=operation,
        row_type=row_type,
        operation_suffix=operation_suffix,
    )
    return obj

  return _doc


@_add_doc(
    _CLASS_DOC,
    operation='delete',
    row_type='ExampleKey',
    operation_suffix='Delete',
)
class SpannerDelete(ExternalTransform):

  URN = 'beam:transform:org.apache.beam:spanner_delete:v1'

  @_add_doc(_INIT_DOC, operation='a delete')
  def __init__(
      self,
      project_id,
      instance_id,
      database_id,
      table,
      max_batch_size_bytes=None,
      max_number_mutations=None,
      max_number_rows=None,
      grouping_factor=None,
      host=None,
      emulator_host=None,
      commit_deadline=None,
      max_cumulative_backoff=None,
      failure_mode=None,
      expansion_service=None,
      high_priority=False,
  ):
    max_cumulative_backoff = int(
        max_cumulative_backoff) if max_cumulative_backoff else None
    commit_deadline = int(commit_deadline) if commit_deadline else None
    super().__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            WriteToSpannerSchema(
                project_id=project_id,
                instance_id=instance_id,
                database_id=database_id,
                table=table,
                max_batch_size_bytes=max_batch_size_bytes,
                max_number_mutations=max_number_mutations,
                max_number_rows=max_number_rows,
                grouping_factor=grouping_factor,
                host=host,
                emulator_host=emulator_host,
                commit_deadline=commit_deadline,
                max_cumulative_backoff=max_cumulative_backoff,
                failure_mode=_get_enum_name(failure_mode),
                high_priority=high_priority,
            ),
        ),
        expansion_service=expansion_service or default_io_expansion_service(),
    )


@_add_doc(
    _CLASS_DOC,
    operation='insert',
    row_type='ExampleRow',
    operation_suffix='Insert',
)
class SpannerInsert(ExternalTransform):

  URN = 'beam:transform:org.apache.beam:spanner_insert:v1'

  @_add_doc(_INIT_DOC, operation='an insert')
  def __init__(
      self,
      project_id,
      instance_id,
      database_id,
      table,
      max_batch_size_bytes=None,
      max_number_mutations=None,
      max_number_rows=None,
      grouping_factor=None,
      host=None,
      emulator_host=None,
      commit_deadline=None,
      max_cumulative_backoff=None,
      expansion_service=None,
      failure_mode=None,
      high_priority=False,
  ):
    max_cumulative_backoff = int(
        max_cumulative_backoff) if max_cumulative_backoff else None
    commit_deadline = int(commit_deadline) if commit_deadline else None
    super().__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            WriteToSpannerSchema(
                project_id=project_id,
                instance_id=instance_id,
                database_id=database_id,
                table=table,
                max_batch_size_bytes=max_batch_size_bytes,
                max_number_mutations=max_number_mutations,
                max_number_rows=max_number_rows,
                grouping_factor=grouping_factor,
                host=host,
                emulator_host=emulator_host,
                commit_deadline=commit_deadline,
                max_cumulative_backoff=max_cumulative_backoff,
                failure_mode=_get_enum_name(failure_mode),
                high_priority=high_priority,
            ),
        ),
        expansion_service=expansion_service or default_io_expansion_service(),
    )


@_add_doc(
    _CLASS_DOC,
    operation='replace',
    row_type='ExampleRow',
    operation_suffix='Replace',
)
class SpannerReplace(ExternalTransform):

  URN = 'beam:transform:org.apache.beam:spanner_replace:v1'

  @_add_doc(_INIT_DOC, operation='a replace')
  def __init__(
      self,
      project_id,
      instance_id,
      database_id,
      table,
      max_batch_size_bytes=None,
      max_number_mutations=None,
      max_number_rows=None,
      grouping_factor=None,
      host=None,
      emulator_host=None,
      commit_deadline=None,
      max_cumulative_backoff=None,
      expansion_service=None,
      failure_mode=None,
      high_priority=False,
  ):
    max_cumulative_backoff = int(
        max_cumulative_backoff) if max_cumulative_backoff else None
    commit_deadline = int(commit_deadline) if commit_deadline else None
    super().__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            WriteToSpannerSchema(
                project_id=project_id,
                instance_id=instance_id,
                database_id=database_id,
                table=table,
                max_batch_size_bytes=max_batch_size_bytes,
                max_number_mutations=max_number_mutations,
                max_number_rows=max_number_rows,
                grouping_factor=grouping_factor,
                host=host,
                emulator_host=emulator_host,
                commit_deadline=commit_deadline,
                max_cumulative_backoff=max_cumulative_backoff,
                failure_mode=_get_enum_name(failure_mode),
                high_priority=high_priority,
            ),
        ),
        expansion_service=expansion_service or default_io_expansion_service(),
    )


@_add_doc(
    _CLASS_DOC,
    operation='insert-or-update',
    row_type='ExampleRow',
    operation_suffix='InsertOrUpdate',
)
class SpannerInsertOrUpdate(ExternalTransform):

  URN = 'beam:transform:org.apache.beam:spanner_insert_or_update:v1'

  @_add_doc(_INIT_DOC, operation='an insert-or-update')
  def __init__(
      self,
      project_id,
      instance_id,
      database_id,
      table,
      max_batch_size_bytes=None,
      max_number_mutations=None,
      max_number_rows=None,
      grouping_factor=None,
      host=None,
      emulator_host=None,
      commit_deadline=None,
      max_cumulative_backoff=None,
      failure_mode=None,
      expansion_service=None,
      high_priority=False,
  ):
    max_cumulative_backoff = int(
        max_cumulative_backoff) if max_cumulative_backoff else None
    commit_deadline = int(commit_deadline) if commit_deadline else None
    super().__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            WriteToSpannerSchema(
                project_id=project_id,
                instance_id=instance_id,
                database_id=database_id,
                table=table,
                max_batch_size_bytes=max_batch_size_bytes,
                max_number_mutations=max_number_mutations,
                max_number_rows=max_number_rows,
                grouping_factor=grouping_factor,
                host=host,
                emulator_host=emulator_host,
                commit_deadline=commit_deadline,
                max_cumulative_backoff=max_cumulative_backoff,
                failure_mode=_get_enum_name(failure_mode),
                high_priority=high_priority,
            ),
        ),
        expansion_service=expansion_service or default_io_expansion_service(),
    )


@_add_doc(
    _CLASS_DOC,
    operation='update',
    row_type='ExampleRow',
    operation_suffix='Update',
)
class SpannerUpdate(ExternalTransform):

  URN = 'beam:transform:org.apache.beam:spanner_update:v1'

  @_add_doc(_INIT_DOC, operation='an update')
  def __init__(
      self,
      project_id,
      instance_id,
      database_id,
      table,
      max_batch_size_bytes=None,
      max_number_mutations=None,
      max_number_rows=None,
      grouping_factor=None,
      host=None,
      emulator_host=None,
      commit_deadline=None,
      max_cumulative_backoff=None,
      failure_mode=None,
      expansion_service=None,
      high_priority=False,
  ):
    max_cumulative_backoff = int(
        max_cumulative_backoff) if max_cumulative_backoff else None
    commit_deadline = int(commit_deadline) if commit_deadline else None
    super().__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            WriteToSpannerSchema(
                project_id=project_id,
                instance_id=instance_id,
                database_id=database_id,
                table=table,
                max_batch_size_bytes=max_batch_size_bytes,
                max_number_mutations=max_number_mutations,
                max_number_rows=max_number_rows,
                grouping_factor=grouping_factor,
                host=host,
                emulator_host=emulator_host,
                commit_deadline=commit_deadline,
                max_cumulative_backoff=max_cumulative_backoff,
                failure_mode=_get_enum_name(failure_mode),
                high_priority=high_priority,
            ),
        ),
        expansion_service=expansion_service or default_io_expansion_service(),
    )


def _get_enum_name(enum):
  return None if enum is None else enum.name
