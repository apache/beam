import * as serialize_closures from "serialize-closures";
import Long from "long";

import { requireForSerialization, registeredObjects } from "../serialization";

const BIGINT_PREFIX = ":bigint:";

const generator = function* () {};

requireForSerialization("apache_beam", {
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
        serialize_closures.defaultBuiltins.concat(registeredObjects)
      ),
      (key, value) =>
        typeof value === "bigint" ? `${BIGINT_PREFIX}${value}` : value
    )
  );
}

export function deserializeFn(s: Uint8Array): any {
  return serialize_closures.deserialize(
    JSON.parse(new TextDecoder().decode(s), (key, value) =>
      typeof value === "string" && value.startsWith(BIGINT_PREFIX)
        ? BigInt(value.substr(BIGINT_PREFIX.length))
        : value
    ),
    serialize_closures.defaultBuiltins.concat(registeredObjects)
  );
}

let fakeSerializeCounter = 0;
const fakeSerializeMap = new Map<string, any>();

function fakeSeralize(obj) {
  fakeSerializeCounter += 1;
  const id = "s_" + fakeSerializeCounter;
  fakeSerializeMap.set(id, obj);
  return new TextEncoder().encode(id);
}

function fakeDeserialize(s) {
  return fakeSerializeMap.get(new TextDecoder().decode(s));
}
