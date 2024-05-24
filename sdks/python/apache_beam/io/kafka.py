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

"""Unbounded source and sink transforms for
   `Kafka <href="http://kafka.apache.org/>`_.

  These transforms are currently supported by Beam portable runners (for
  example, portable Flink and Spark) as well as Dataflow runner.

  **Setup**

  Transforms provided in this module are cross-language transforms
  implemented in the Beam Java SDK. During the pipeline construction, Python SDK
  will connect to a Java expansion service to expand these transforms.
  To facilitate this, a small amount of setup is needed before using these
  transforms in a Beam Python pipeline.

  There are several ways to setup cross-language Kafka transforms.

  * Option 1: use the default expansion service
  * Option 2: specify a custom expansion service

  See below for details regarding each of these options.

  *Option 1: Use the default expansion service*

  This is the recommended and easiest setup option for using Python Kafka
  transforms. This option is only available for Beam 2.22.0 and later.

  This option requires following pre-requisites before running the Beam
  pipeline.

  * Install Java runtime in the computer from where the pipeline is constructed
    and make sure that 'java' command is available.

  In this option, Python SDK will either download (for released Beam version) or
  build (when running from a Beam Git clone) an expansion service jar and use
  that to expand transforms. Currently Kafka transforms use the
  'beam-sdks-java-io-expansion-service' jar for this purpose.

  *Option 2: specify a custom expansion service*

  In this option, you startup your own expansion service and provide that as
  a parameter when using the transforms provided in this module.

  This option requires following pre-requisites before running the Beam
  pipeline.

  * Startup your own expansion service.
  * Update your pipeline to provide the expansion service address when
    initiating Kafka transforms provided in this module.

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

import typing

import numpy as np

from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

ReadFromKafkaSchema = typing.NamedTuple(
    'ReadFromKafkaSchema',
    [
        ('consumer_config', typing.Mapping[str, str]),
        ('topics', typing.List[str]),
        ('key_deserializer', str),
        ('value_deserializer', str),
        ('start_read_time', typing.Optional[int]),
        ('max_num_records', typing.Optional[int]),
        ('max_read_time', typing.Optional[int]),
        ('commit_offset_in_finalize', bool),
        ('timestamp_policy', str),
        ('consumer_polling_timeout', typing.Optional[int]),
        ('redistribute', typing.Optional[bool]),
        ('redistribute_num_keys', typing.Optional[np.int32]),
        ('allow_duplicates', typing.Optional[bool]),
    ])


def default_io_expansion_service(append_args=None):
  return BeamJarExpansionService(
      'sdks:java:io:expansion-service:shadowJar', append_args=append_args)


class ReadFromKafka(ExternalTransform):
  """
    An external PTransform which reads from Kafka and returns a KV pair for
    each item in the specified Kafka topics. If no Kafka Deserializer for
    key/value is provided, then the data will be returned as a raw byte array.

    Experimental; no backwards compatibility guarantees.
  """

  # Returns the key/value data as raw byte arrays
  byte_array_deserializer = (
      'org.apache.kafka.common.serialization.ByteArrayDeserializer')

  processing_time_policy = 'ProcessingTime'
  create_time_policy = 'CreateTime'
  log_append_time = 'LogAppendTime'

  URN_WITH_METADATA = (
      'beam:transform:org.apache.beam:kafka_read_with_metadata:v1')
  URN_WITHOUT_METADATA = (
      'beam:transform:org.apache.beam:kafka_read_without_metadata:v1')

  def __init__(
      self,
      consumer_config,
      topics,
      key_deserializer=byte_array_deserializer,
      value_deserializer=byte_array_deserializer,
      start_read_time=None,
      max_num_records=None,
      max_read_time=None,
      commit_offset_in_finalize=False,
      timestamp_policy=processing_time_policy,
      consumer_polling_timeout=2,
      with_metadata=False,
      expansion_service=None,
      redistribute=False,
      redistribute_num_keys=np.int32(0),
      allow_duplicates=False,
  ):
    """
    Initializes a read operation from Kafka.

    :param consumer_config: A dictionary containing the consumer configuration.
    :param topics: A list of topic strings.
    :param key_deserializer: A fully-qualified Java class name of a Kafka
        Deserializer for the topic's key, e.g.
        'org.apache.kafka.common.serialization.LongDeserializer'.
        Default: 'org.apache.kafka.common.serialization.ByteArrayDeserializer'.
    :param value_deserializer: A fully-qualified Java class name of a Kafka
        Deserializer for the topic's value, e.g.
        'org.apache.kafka.common.serialization.LongDeserializer'.
        Default: 'org.apache.kafka.common.serialization.ByteArrayDeserializer'.
    :param start_read_time: Use timestamp to set up start offset in milliseconds
        epoch.
    :param max_num_records: Maximum amount of records to be read. Mainly used
        for tests and demo applications.
    :param max_read_time: Maximum amount of time in seconds the transform
        executes. Mainly used for tests and demo applications.
    :param commit_offset_in_finalize: Whether to commit offsets when finalizing.
    :param timestamp_policy: The built-in timestamp policy which is used for
        extracting timestamp from KafkaRecord.
    :param consumer_polling_timeout: Kafka client polling request
        timeout time in seconds. A lower timeout optimizes for latency. Increase                                   
        the timeout if the consumer is not fetching any records. Default is 2
        seconds.
    :param with_metadata: whether the returned PCollection should contain
        Kafka related metadata or not. If False (default), elements of the
        returned PCollection will be of type 'bytes' if True, elements of the
        returned PCollection will be of the type 'Row'. Note that, currently
        this only works when using default key and value deserializers where
        Java Kafka Reader reads keys and values as 'byte[]'.
    :param expansion_service: The address (host:port) of the ExpansionService.
    :param redistribute: whether a Redistribute transform should be applied 
        immediately after the read.
    :param redistribute_num_keys: Configures how many keys the Redistribute 
        spreads the data across.
    :param allow_duplicates: whether the Redistribute transform allows for 
        duplicates (this serves solely as a hint to the underlying runner).
    """
    if timestamp_policy not in [ReadFromKafka.processing_time_policy,
                                ReadFromKafka.create_time_policy,
                                ReadFromKafka.log_append_time]:
      raise ValueError(
          'timestamp_policy should be one of '
          '[ProcessingTime, CreateTime, LogAppendTime]')

    super().__init__(
        self.URN_WITH_METADATA if with_metadata else self.URN_WITHOUT_METADATA,
        NamedTupleBasedPayloadBuilder(
            ReadFromKafkaSchema(
                consumer_config=consumer_config,
                topics=topics,
                key_deserializer=key_deserializer,
                value_deserializer=value_deserializer,
                max_num_records=max_num_records,
                max_read_time=max_read_time,
                start_read_time=start_read_time,
                commit_offset_in_finalize=commit_offset_in_finalize,
                timestamp_policy=timestamp_policy,
                consumer_polling_timeout=consumer_polling_timeout,
                redistribute=redistribute,
                redistribute_num_keys=redistribute_num_keys,
                allow_duplicates=allow_duplicates)),
        expansion_service or default_io_expansion_service())


WriteToKafkaSchema = typing.NamedTuple(
    'WriteToKafkaSchema',
    [
        ('producer_config', typing.Mapping[str, str]),
        ('topic', str),
        ('key_serializer', str),
        ('value_serializer', str),
    ])


class WriteToKafka(ExternalTransform):
  """
    An external PTransform which writes KV data to a specified Kafka topic.
    If no Kafka Serializer for key/value is provided, then key/value are
    assumed to be byte arrays.

    Experimental; no backwards compatibility guarantees.
  """

  # Default serializer which passes raw bytes to Kafka
  byte_array_serializer = (
      'org.apache.kafka.common.serialization.ByteArraySerializer')

  URN = 'beam:transform:org.apache.beam:kafka_write:v1'

  def __init__(
      self,
      producer_config,
      topic,
      key_serializer=byte_array_serializer,
      value_serializer=byte_array_serializer,
      expansion_service=None):
    """
    Initializes a write operation to Kafka.

    :param producer_config: A dictionary containing the producer configuration.
    :param topic: A Kafka topic name.
    :param key_deserializer: A fully-qualified Java class name of a Kafka
        Serializer for the topic's key, e.g.
        'org.apache.kafka.common.serialization.LongSerializer'.
        Default: 'org.apache.kafka.common.serialization.ByteArraySerializer'.
    :param value_deserializer: A fully-qualified Java class name of a Kafka
        Serializer for the topic's value, e.g.
        'org.apache.kafka.common.serialization.LongSerializer'.
        Default: 'org.apache.kafka.common.serialization.ByteArraySerializer'.
    :param expansion_service: The address (host:port) of the ExpansionService.
    """
    super().__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            WriteToKafkaSchema(
                producer_config=producer_config,
                topic=topic,
                key_serializer=key_serializer,
                value_serializer=value_serializer,
            )),
        expansion_service or default_io_expansion_service())
