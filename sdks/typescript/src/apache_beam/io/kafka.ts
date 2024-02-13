/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as beam from "../../apache_beam";
import * as external from "../transforms/external";
import { Schema } from "../proto/schema";
import { RowCoder } from "../coders/row_coder";
import { serviceProviderFromJavaGradleTarget } from "../utils/service";
import * as protobufjs from "protobufjs";
import { camelToSnakeOptions } from "../utils/utils";

const KAFKA_EXPANSION_GRADLE_TARGET =
  "sdks:java:io:expansion-service:shadowJar";

export type ReadFromKafkaOptions = {
  keyDeserializer?: string;
  valueDeserializer?: string;
  startReadTime?: number;
  maxNumRecords?: number;
  maxReadTime?: number;
  commitOffsetInFinalize?: boolean;
  timestampPolicy?: "ProcessingTime" | "CreateTime" | "LogAppendTime";
};

const defaultReadFromKafkaOptions = {
  keyDeserializer:
    "org.apache.kafka.common.serialization.ByteArrayDeserializer",
  valueDeserializer:
    "org.apache.kafka.common.serialization.ByteArrayDeserializer",
  timestampPolicy: "ProcessingTime",
};

export function readFromKafka<T>(
  consumerConfig: { [key: string]: string }, // TODO: Or a map?
  topics: string[],
  options: ReadFromKafkaOptions = {},
): beam.AsyncPTransform<beam.Root, beam.PCollection<T>> {
  return readFromKafkaMaybeWithMetadata(
    "readFromKafkaWithMetadata",
    "beam:transform:org.apache.beam:kafka_read_without_metadata:v1",
    consumerConfig,
    topics,
    options,
  );
}

export function readFromKafkaWithMetadata<T>(
  consumerConfig: { [key: string]: string }, // TODO: Or a map?
  topics: string[],
  options: ReadFromKafkaOptions = {},
): beam.AsyncPTransform<beam.Root, beam.PCollection<T>> {
  return readFromKafkaMaybeWithMetadata<T>(
    "readFromKafkaWithMetadata",
    "beam:transform:org.apache.beam:kafka_read_with_metadata:v1",
    consumerConfig,
    topics,
    options,
  );
}

function readFromKafkaMaybeWithMetadata<T>(
  name: string,
  urn: string,
  consumerConfig: { [key: string]: string }, // TODO: Or a map?
  topics: string[],
  options: ReadFromKafkaOptions = {},
): beam.AsyncPTransform<beam.Root, beam.PCollection<T>> {
  return beam.withName(
    name,
    external.rawExternalTransform<beam.Root, beam.PCollection<T>>(
      urn,
      {
        topics,
        consumerConfig,
        ...camelToSnakeOptions({ ...defaultReadFromKafkaOptions, ...options }),
      },
      serviceProviderFromJavaGradleTarget(KAFKA_EXPANSION_GRADLE_TARGET),
    ),
  );
}

export type WriteToKafkaOptions = {
  keySerializer?: string;
  valueSerializer?: string;
};

const defaultWriteToKafkaOptions = {
  keySerializer: "org.apache.kafka.common.serialization.ByteArraySerializer",
  valueSerializer: "org.apache.kafka.common.serialization.ByteArraySerializer",
};

export function writeToKafka<K = Uint8Array, V = Uint8Array>(
  producerConfig: { [key: string]: string }, // TODO: Or a map?
  topics: string[],
  options: WriteToKafkaOptions = {},
): beam.AsyncPTransform<beam.PCollection<{ key: K; value: V }>, {}> {
  return beam.withName(
    "writeToKafka",
    external.rawExternalTransform<beam.PCollection<{ key: K; value: V }>, {}>(
      "beam:transform:org.apache.beam:kafka_write:v1",
      {
        topics,
        producerConfig,
        ...camelToSnakeOptions({ ...defaultWriteToKafkaOptions, ...options }),
      },
      serviceProviderFromJavaGradleTarget(KAFKA_EXPANSION_GRADLE_TARGET),
    ),
  );
}
