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
import { RowCoder } from "../coders/row_coder";
import { serviceProviderFromJavaGradleTarget } from "../utils/service";
import { camelToSnakeOptions } from "../utils/utils";

const PUBSUBLITE_EXPANSION_GRADLE_TARGET =
  "sdks:java:io:google-cloud-platform:expansion-service:shadowJar";

// TODO: Schema-producing variants.
export function readFromPubSubLiteRaw(
  subscriptionPath: string,
  options: { minBundleTimeout?: number; deduplicate?: boolean } = {},
): beam.AsyncPTransform<beam.Root, beam.PCollection<Uint8Array>> {
  return beam.withName(
    "readFromPubSubLiteRaw",
    external.rawExternalTransform<beam.Root, beam.PCollection<Uint8Array>>(
      "beam:transform:org.apache.beam:pubsublite_read:v1",
      { subscription_path: subscriptionPath, ...camelToSnakeOptions(options) },
      serviceProviderFromJavaGradleTarget(PUBSUBLITE_EXPANSION_GRADLE_TARGET),
    ),
  );
}

export function writeToPubSubLiteRaw(
  topicPath: string,
  options: { addUuids?: boolean } = {},
): beam.AsyncPTransform<beam.PCollection<Uint8Array>, {}> {
  return beam.withName(
    "writeToPubSubLiteRaw",
    external.rawExternalTransform<beam.PCollection<Uint8Array>, {}>(
      "beam:transform:org.apache.beam:pubsublite_write:v1",
      { topic_path: topicPath, ...camelToSnakeOptions(options) },
      serviceProviderFromJavaGradleTarget(PUBSUBLITE_EXPANSION_GRADLE_TARGET),
    ),
  );
}
