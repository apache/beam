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

import * as PubSub from "@google-cloud/pubsub";

import * as beam from "../pvalue";
import * as external from "../transforms/external";
import * as internal from "../transforms/internal";
import { AsyncPTransform, withName } from "../transforms/transform";
import { RowCoder } from "../coders/row_coder";
import { BytesCoder } from "../coders/required_coders";
import { requireForSerialization } from "../serialization";
import { packageName } from "../utils/packageJson";
import { serviceProviderFromJavaGradleTarget } from "../utils/service";
import { camelToSnakeOptions } from "../utils/utils";

const PUBSUB_EXPANSION_GRADLE_TARGET =
  "sdks:java:io:google-cloud-platform:expansion-service:shadowJar";

const readSchema = RowCoder.inferSchemaOfJSON({
  topic: "string",
  subscription: "string",
  idAttribute: "string",
  timestampAttribute: "string",
});

type ReadOptions =
  | {
      topic: string;
      subscription?: never;
      idAttribute?: string;
      timestampAttribute?: string;
    }
  | {
      topic?: never;
      subscription: string;
      idAttribute?: string;
      timestampAttribute?: string;
    };

// TODO: Schema-producing variants.
export function readFromPubSub(
  options: ReadOptions,
): AsyncPTransform<beam.Root, beam.PCollection<Uint8Array>> {
  if (options.topic && options.subscription) {
    throw new TypeError(
      "Exactly one of topic or subscription must be provided.",
    );
  }
  return withName(
    "readFromPubSubRaw",
    external.rawExternalTransform<beam.Root, beam.PCollection<Uint8Array>>(
      "beam:transform:org.apache.beam:pubsub_read:v1",
      camelToSnakeOptions(options),
      serviceProviderFromJavaGradleTarget(PUBSUB_EXPANSION_GRADLE_TARGET),
    ),
  );
}

function readFromPubSubWithAttributesRaw(
  options: ReadOptions,
): AsyncPTransform<beam.Root, beam.PCollection<Uint8Array>> {
  if (options.topic && options.subscription) {
    throw new TypeError(
      "Exactly one of topic or subscription must be provided.",
    );
  }
  return withName(
    "readFromPubSubWithAttributesRaw",
    external.rawExternalTransform<beam.Root, beam.PCollection<Uint8Array>>(
      "beam:transform:org.apache.beam:pubsub_read:v1",
      { needsAttributes: true, ...camelToSnakeOptions(options) },
      serviceProviderFromJavaGradleTarget(PUBSUB_EXPANSION_GRADLE_TARGET),
    ),
  );
}

export function readFromPubSubWithAttributes(
  options: ReadOptions,
): AsyncPTransform<
  beam.Root,
  beam.PCollection<PubSub.protos.google.pubsub.v1.PubsubMessage>
> {
  return async function readFromPubSubWithAttributes(root: beam.Root) {
    return (
      await root.applyAsync(readFromPubSubWithAttributesRaw(options))
    ).map((encoded) =>
      PubSub.protos.google.pubsub.v1.PubsubMessage.decode(encoded),
    );
  };
}

type WriteOptions = { idAttribute?: string; timestampAttribute?: string };

function writeToPubSubRaw(
  topic: string,
  options: WriteOptions = {},
): AsyncPTransform<beam.PCollection<Uint8Array>, {}> {
  return withName(
    "writeToPubSubRaw",
    external.rawExternalTransform<beam.PCollection<Uint8Array>, {}>(
      "beam:transform:org.apache.beam:pubsub_write:v1",
      { topic, ...camelToSnakeOptions(options) },
      serviceProviderFromJavaGradleTarget(PUBSUB_EXPANSION_GRADLE_TARGET),
    ),
  );
}

export function writeToPubSub(topic: string, options: WriteOptions = {}) {
  return async function writeToPubSub(dataPColl: beam.PCollection<Uint8Array>) {
    return dataPColl //
      .map((data) =>
        PubSub.protos.google.pubsub.v1.PubsubMessage.encode({ data }).finish(),
      )
      .apply(internal.withCoderInternal(new BytesCoder()))
      .applyAsync(writeToPubSubRaw(topic, options));
  };
}

requireForSerialization(`${packageName}/io/pubsub`, PubSub);
