export function serializeFn(obj) {
  return fakeSeralize(obj);
}

export function deserializeFn(s) {
  return fakeDeserialize(s);
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
