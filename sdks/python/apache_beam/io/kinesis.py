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

"""PTransforms for supporting Kinesis streaming in Python pipelines.

  These transforms are currently supported by Beam Flink and Spark portable
  runners.

  **Setup**

  Transforms provided in this module are cross-language transforms
  implemented in the Beam Java SDK. During the pipeline construction, Python SDK
  will connect to a Java expansion service to expand these transforms.
  To facilitate this, a small amount of setup is needed before using these
  transforms in a Beam Python pipeline.

  There are several ways to setup cross-language Kinesis transforms.

  * Option 1: use the default expansion service
  * Option 2: specify a custom expansion service

  See below for details regarding each of these options.

  *Option 1: Use the default expansion service*

  This is the recommended and easiest setup option for using Python Kinesis
  transforms. This option is only available for Beam 2.25.0 and later.

  This option requires following pre-requisites before running the Beam
  pipeline.

  * Install Java runtime in the computer from where the pipeline is constructed
    and make sure that 'java' command is available.

  In this option, Python SDK will either download (for released Beam version) or
  build (when running from a Beam Git clone) a expansion service jar and use
  that to expand transforms. Currently Kinesis transforms use the
  'beam-sdks-java-io-amazon-web-services2-expansion-service' jar for this
  purpose.

  *Option 2: specify a custom expansion service*

  In this option, you startup your own expansion service and provide that as
  a parameter when using the transforms provided in this module.

  This option requires following pre-requisites before running the Beam
  pipeline.

  * Startup your own expansion service.
  * Update your pipeline to provide the expansion service address when
    initiating Kinesis transforms provided in this module.

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

import logging
import time
from typing import Mapping
from typing import NamedTuple
from typing import Optional

from apache_beam import BeamJarExpansionService
from apache_beam import ExternalTransform
from apache_beam import NamedTupleBasedPayloadBuilder

__all__ = [
    'WriteToKinesis',
    'ReadDataFromKinesis',
    'InitialPositionInStream',
    'WatermarkPolicy',
]


def default_io_expansion_service():
  return BeamJarExpansionService(
      'sdks:java:io:amazon-web-services2:expansion-service:shadowJar')


WriteToKinesisSchema = NamedTuple(
    'WriteToKinesisSchema',
    [
        ('stream_name', str),
        ('aws_access_key', str),
        ('aws_secret_key', str),
        ('region', str),
        ('partition_key', str),
        ('service_endpoint', Optional[str]),
        ('producer_properties', Optional[Mapping[str, str]]),
    ],
)


class WriteToKinesis(ExternalTransform):
  """
    An external PTransform which writes byte array stream to Amazon Kinesis.

    Experimental; no backwards compatibility guarantees.
  """
  URN = 'beam:transform:org.apache.beam:kinesis_write:v2'

  def __init__(
      self,
      stream_name,
      aws_access_key,
      aws_secret_key,
      region,
      partition_key,
      service_endpoint=None,
      verify_certificate=None,
      producer_properties=None,
      expansion_service=None,
  ):
    """
    Initializes a write operation to Kinesis.

    :param stream_name: Kinesis stream name.
    :param aws_access_key: Kinesis access key.
    :param aws_secret_key: Kinesis access key secret.
    :param region: AWS region. Example: 'us-east-1'.
    :param service_endpoint: Kinesis service endpoint
    :param verify_certificate: Deprecated - certificates will always be
        verified.
    :param partition_key: Specify default partition key.
    :param producer_properties: Specify the configuration properties for Kinesis
        Producer Library (KPL) as dictionary.
        Example: {'CollectionMaxCount': '1000', 'ConnectTimeout': '10000'}
    :param expansion_service: The address (host:port) of the ExpansionService.
    """
    if verify_certificate is False:
      # Previously, we supported this via
      # https://javadoc.io/doc/com.amazonaws/amazon-kinesis-producer/0.14.0/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.html#isVerifyCertificate--
      # With the new AWS client, we no longer support it and it is always True
      raise ValueError(
        'verify_certificate set to False. This option is no longer ' +
        'supported and certificate verification will still happen.')
    if verify_certificate is True:
      logging.warning(
        'verify_certificate set to True. This option is no longer ' +
        'supported and certificate verification will automatically happen. ' +
        'This option may be removed in a future release')
    super().__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            WriteToKinesisSchema(
                stream_name=stream_name,
                aws_access_key=aws_access_key,
                aws_secret_key=aws_secret_key,
                region=region,
                partition_key=partition_key,
                service_endpoint=service_endpoint,
                producer_properties=producer_properties,
            )),
        expansion_service or default_io_expansion_service(),
    )


ReadFromKinesisSchema = NamedTuple(
    'ReadFromKinesisSchema',
    [
        ('stream_name', str),
        ('aws_access_key', str),
        ('aws_secret_key', str),
        ('region', str),
        ('service_endpoint', Optional[str]),
        ('max_num_records', Optional[int]),
        ('max_read_time', Optional[int]),
        ('initial_position_in_stream', Optional[str]),
        ('initial_timestamp_in_stream', Optional[int]),
        ('request_records_limit', Optional[int]),
        ('up_to_date_threshold', Optional[int]),
        ('max_capacity_per_shard', Optional[int]),
        ('watermark_policy', Optional[str]),
        ('watermark_idle_duration_threshold', Optional[int]),
        ('rate_limit', Optional[int]),
    ],
)


class ReadDataFromKinesis(ExternalTransform):
  """
    An external PTransform which reads byte array stream from Amazon Kinesis.

    Experimental; no backwards compatibility guarantees.
  """
  URN = 'beam:transform:org.apache.beam:kinesis_read_data:v2'

  def __init__(
      self,
      stream_name,
      aws_access_key,
      aws_secret_key,
      region,
      service_endpoint=None,
      verify_certificate=None,
      max_num_records=None,
      max_read_time=None,
      initial_position_in_stream=None,
      initial_timestamp_in_stream=None,
      request_records_limit=None,
      up_to_date_threshold=None,
      max_capacity_per_shard=None,
      watermark_policy=None,
      watermark_idle_duration_threshold=None,
      rate_limit=None,
      expansion_service=None,
  ):
    """
    Initializes a read operation from Kinesis.

    :param stream_name: Kinesis stream name.
    :param aws_access_key: Kinesis access key.
    :param aws_secret_key: Kinesis access key secret.
    :param region: AWS region. Example: 'us-east-1'.
    :param service_endpoint: Kinesis service endpoint
    :param verify_certificate:  Deprecated - certificates will always be
        verified.
    :param max_num_records: Specifies to read at most a given number of records.
        Must be greater than 0.
    :param max_read_time: Specifies to read records during x milliseconds.
    :param initial_timestamp_in_stream: Specify reading beginning at the given
        timestamp in milliseconds. Must be in the past.
    :param initial_position_in_stream: Specify reading from some initial
        position in stream. Possible values:
        LATEST - Start after the most recent data record (fetch new data).
        TRIM_HORIZON - Start from the oldest available data record.
        AT_TIMESTAMP - Start from the record at or after the specified
        server-side timestamp.
    :param request_records_limit: Specifies the maximum number of records in
        GetRecordsResult returned by GetRecords call which is limited by 10K
        records. If should be adjusted according to average size of data record
        to prevent shard overloading. More at:
        docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html
    :param up_to_date_threshold: Specifies how late in milliseconds records
        consumed by this source can be to still be considered on time. Defaults
        to zero.
    :param max_capacity_per_shard: Specifies the maximum number of messages per
        one shard. Defaults to 10'000.
    :param watermark_policy: Specifies the watermark policy. Possible values:
        PROCESSING_TYPE, ARRIVAL_TIME. Defaults to ARRIVAL_TIME.
    :param watermark_idle_duration_threshold: Use only when watermark policy is
        ARRIVAL_TIME. Denotes the duration for which the watermark can be idle.
        Passed in milliseconds.
    :param rate_limit: Sets fixed rate policy for given milliseconds value. By
        default there is no rate limit.
    :param expansion_service: The address (host:port) of the ExpansionService.
    """
    WatermarkPolicy.validate_param(watermark_policy)
    InitialPositionInStream.validate_param(initial_position_in_stream)

    if watermark_idle_duration_threshold:
      assert WatermarkPolicy.ARRIVAL_TIME == watermark_policy

    if request_records_limit:
      assert 0 < request_records_limit <= 10000

    initial_timestamp_in_stream = int(
        initial_timestamp_in_stream) if initial_timestamp_in_stream else None

    if initial_timestamp_in_stream and initial_timestamp_in_stream < time.time(
    ):
      logging.warning('Provided timestamp emplaced not in the past.')

    if verify_certificate is False:
      # Previously, we supported this via
      # https://javadoc.io/doc/com.amazonaws/amazon-kinesis-producer/0.14.0/com/amazonaws/services/kinesis/producer/KinesisProducerConfiguration.html#isVerifyCertificate--
      # With the new AWS client, we no longer support it and it is always True
      raise ValueError(
        'verify_certificate set to False. This option is no longer ' +
        'supported and certificate verification will still happen.')
    if verify_certificate is True:
      logging.warning(
        'verify_certificate set to True. This option is no longer ' +
        'supported and certificate verification will automatically happen. ' +
        'This option may be removed in a future release')

    super().__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            ReadFromKinesisSchema(
                stream_name=stream_name,
                aws_access_key=aws_access_key,
                aws_secret_key=aws_secret_key,
                region=region,
                service_endpoint=service_endpoint,
                max_num_records=max_num_records,
                max_read_time=max_read_time,
                initial_position_in_stream=initial_position_in_stream,
                initial_timestamp_in_stream=initial_timestamp_in_stream,
                request_records_limit=request_records_limit,
                up_to_date_threshold=up_to_date_threshold,
                max_capacity_per_shard=max_capacity_per_shard,
                watermark_policy=watermark_policy,
                watermark_idle_duration_threshold=
                watermark_idle_duration_threshold,
                rate_limit=rate_limit,
            )),
        expansion_service or default_io_expansion_service(),
    )


class InitialPositionInStream:
  LATEST = 'LATEST'
  TRIM_HORIZON = 'TRIM_HORIZON'
  AT_TIMESTAMP = 'AT_TIMESTAMP'

  @staticmethod
  def validate_param(param):
    if param and not hasattr(InitialPositionInStream, param):
      raise RuntimeError('Invalid initial position in stream: {}'.format(param))


class WatermarkPolicy:
  PROCESSING_TYPE = 'PROCESSING_TYPE'
  ARRIVAL_TIME = 'ARRIVAL_TIME'

  @staticmethod
  def validate_param(param):
    if param and not hasattr(WatermarkPolicy, param):
      raise RuntimeError('Invalid watermark policy: {}'.format(param))
