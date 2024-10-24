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

import * as serialize_closures from "serialize-closures";
import Long from "long";

import { requireForSerialization, registeredObjects } from "../serialization";
import { packageName } from "../utils/packageJson";

const BIGINT_PREFIX = ":bigint:";

const generator = function* () {};

requireForSerialization(packageName, {
  generator: generator,
  generator_prototype: generator.prototype,
  TextEncoder: TextEncoder,
  TextDecoder: TextDecoder,
  Long,
});

export function serializeFn(obj: unknown): Uint8Array {
  return new TextEncoder().encode(
    JSON.stringify(
      serialize_closures.serialize(
        obj,
        serialize_closures.defaultBuiltins.concat(registeredObjects),
      ),
      (key, value) =>
        typeof value === "bigint" ? `${BIGINT_PREFIX}${value}` : value,
    ),
  );
}

export function deserializeFn(s: Uint8Array): any {
  return serialize_closures.deserialize(
    JSON.parse(new TextDecoder().decode(s), (key, value) =>
      typeof value === "string" && value.startsWith(BIGINT_PREFIX)
        ? BigInt(value.substr(BIGINT_PREFIX.length))
        : value,
    ),
    serialize_closures.defaultBuiltins.concat(registeredObjects),
  );
}
