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
  transforms. This option is only available for Beam 2.24.0 and later.

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

from __future__ import absolute_import

import typing

from past.builtins import unicode

from apache_beam import Row
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

__all__ = [
    'WriteToSpanner',
]


def default_io_expansion_service():
  return BeamJarExpansionService('sdks:java:io:google-cloud-platform:expansion-service:shadowJar')


WriteToSpannerSchema = typing.NamedTuple(
    'WriteToSpannerSchema',
    [
        ('instance_id', unicode),
        ('database_id', unicode),
        ('project_id', typing.Optional[unicode]),
        ('batch_size_bytes', typing.Optional[int]),
        ('max_num_mutations', typing.Optional[int]),
        ('max_num_rows', typing.Optional[int]),
        ('failure_mode', typing.Optional[unicode]),
        ('grouping_factor', typing.Optional[int]),
        ('host', typing.Optional[unicode]),
        ('commit_deadline', typing.Optional[int]),
        ('max_cumulative_backoff', typing.Optional[int]),
    ],
)


Mutation = typing.NamedTuple(
    'Mutation',
    [
        ('operation', unicode),
        ('table', unicode),
        ('key_set', typing.List[typing.NamedTuple]),
        ('row', typing.List[typing.NamedTuple]),
    ],
)

class FailureMode:
  """
  FAIL_FAST: Invalid write to Spanner will cause the pipeline to fail. Default.
  REPORT_FAILURES: Invalid mutations will be returned as part of the result
      of the write transform.
  """
  FAIL_FAST = 'FAIL_FAST'
  REPORT_FAILURES = 'REPORT_FAILURES'


class Operation:
  INSERT = 'INSERT'
  DELETE = 'DELETE'
  UPDATE = 'UPDATE'
  REPLACE = 'REPLACE'
  INSERT_OR_UPDATE = 'INSERT_OR_UPDATE'

class WriteToSpanner(ExternalTransform):
  """
  A PTransform which writes Rows to the specified instance's database
  via Spanner.

  This transform receives Rows defined as NamedTuple type and registered in
  the coders registry, e.g.::

    ExampleRow = typing.NamedTuple('ExampleRow',
                                   [('id', int), ('name', unicode)])
    coders.registry.register_coder(ExampleRow, coders.RowCoder)

    with TestPipeline() as p:
      _ = (
          p
          | beam.Create([ExampleRow(1, 'abc')])
              .with_output_types(ExampleRow)
          | 'Write to Spanner' >> WriteToSpanner(
              driver_class_name='org.postgresql.Driver',
              jdbc_url='jdbc:postgresql://localhost:5432/example',
              username='postgres',
              password='postgres',
              statement='INSERT INTO example_table VALUES(?, ?)',
          ))

  Experimental; no backwards compatibility guarantees.
  """

  URN = 'beam:external:java:spanner:write_rows:v1'

  def __init__(
      self,
      instance_id,
      database_id,
      project_id=None,
      batch_size_bytes=None,
      max_num_mutations=None,
      max_num_rows=None,
      failure_mode=None,
      grouping_factor=None,
      host=None,
      commit_deadline=None,
      max_cumulative_backoff=None,
      expansion_service=None,
  ):
    """
    Initializes a write operation to Spanner.

    :param project_id: Specifies the Cloud Spanner project.
    :param instance_id: Specifies the Cloud Spanner instance.
    :param database_id: Specifies the Cloud Spanner database.
    :param batch_size_bytes: Specifies the batch size limit (max number of
        bytes mutated per batch). Default value is 1MB = 1048576 bytes.
    :param max_num_mutations: Specifies the cell mutation limit (maximum number
        of mutated cells per batch). Default value is 5000.
    :param max_num_rows: Specifies the row mutation limit (maximum number of
        mutated rows per batch). Default value is 1000.
    :param failure_mode: Specifies failure mode. 'FAIL_FAST' mode is selected
        by default. Another possible value is 'REPORT_FAILURES'.
    :param grouping_factor: Specifies the multiple of max mutation (in terms
        of both bytes per batch and cells per batch) that is used to select a
        set of mutations to sort by key for batching. This sort uses local
        memory on the workers, so using large values can cause out of memory
        errors. Default value is 1000.
    :param host: Specifies the Cloud Spanner host.
    :param commit_deadline: Specifies the deadline for the Commit API call.
        Default is 15 secs. DEADLINE_EXCEEDED errors will prompt a backoff/retry
        until the value of commit_deadline is reached. DEADLINE_EXCEEDED errors
        are are reported with logging and counters.
    :param max_cumulative_backoff: Specifies the maximum cumulative backoff
        time when retrying after DEADLINE_EXCEEDED errors. Default is 15 mins
        (900s). If the mutations still have not been written after this time,
        they are treated as a failure, and handled according to the setting of
        failure_mode. Pass seconds as value.
    :param expansion_service: The address (host:port) of the ExpansionService.
    """

    super(WriteToSpanner, self).__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            WriteToSpannerSchema(
                instance_id=instance_id,
                database_id=database_id,
                project_id=project_id,
                batch_size_bytes=batch_size_bytes,
                max_num_mutations=max_num_mutations,
                max_num_rows=max_num_rows,
                failure_mode=failure_mode,
                grouping_factor=grouping_factor,
                host=host,
                commit_deadline=commit_deadline,
                max_cumulative_backoff=max_cumulative_backoff,
            ),
        ),
        expansion_service or default_io_expansion_service(),
    )
