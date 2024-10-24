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

import * as grpc from "@grpc/grpc-js";

import * as fnApi from "../proto/beam_fn_api";
import { BeamFnStateClient } from "../proto/beam_fn_api.grpc-client";

// TODO: (Extension) Lazy iteration via continuation tokens.
// This will likely require promises all the way up to the consumer.

interface PromiseWrapper<T> {
  type: "promise";
  promise: Promise<T>;
}

interface ValueWrapper<T> {
  type: "value";
  value: T;
}

// We want to avoid promises when not needed (e.g. for a cache hit) as they
// have to bubble all the way up the stack.
export type MaybePromise<T> = PromiseWrapper<T> | ValueWrapper<T>;

export interface StateProvider {
  getState: <T>(
    stateKey: fnApi.StateKey,
    decode: (data: Uint8Array) => T,
  ) => MaybePromise<T>;
}

// TODO: (Advanced) Cross-bundle caching.
export class CachingStateProvider implements StateProvider {
  underlying: StateProvider;
  cache: Map<string, MaybePromise<any>> = new Map();

  constructor(underlying: StateProvider) {
    this.underlying = underlying;
  }

  getState<T>(stateKey: fnApi.StateKey, decode: (data: Uint8Array) => T) {
    // TODO: (Perf) Consider caching on something ligher-weight than the full
    // serialized key, only constructing this proto when interacting with
    // the runner.
    const cacheKey = Buffer.from(fnApi.StateKey.toBinary(stateKey)).toString(
      "base64",
    );
    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey)!;
    }
    let result = this.underlying.getState(stateKey, decode);
    const this_ = this;
    if (result.type === "promise") {
      result = {
        type: "promise",
        promise: result.promise.then((value) => {
          this_.cache.set(cacheKey, { type: "value", value });
          return value;
        }),
      };
    }
    // TODO: (Perf) Cache eviction.
    this.cache.set(cacheKey, result);
    return result;
  }
}

export class GrpcStateProvider implements StateProvider {
  constructor(
    private multiplexingChannel: MultiplexingStateChannel,
    private instructionId,
  ) {}

  getState<T>(stateKey: fnApi.StateKey, decode: (data: Uint8Array) => T) {
    return this.multiplexingChannel.getState(
      this.instructionId,
      stateKey,
      decode,
    );
  }
}

/**
 * This class manages a single state channel that can be shared across multiple
 * bundles.
 *
 * Per the Beam state protocol [link], every state request is tagged with a
 * unique id which is then part of the (possibly out of order) response.
 * We model that by registering callbacks associated with each id that
 * resolve the corresponding promise that was created when the request was sent.
 */
export class MultiplexingStateChannel {
  stateClient: BeamFnStateClient;
  stateChannel: grpc.ClientDuplexStream<
    fnApi.StateRequest,
    fnApi.StateResponse
  >;
  callbacks: Map<string, (response: fnApi.StateResponse) => void> = new Map();
  closed: boolean;
  error?: Error;
  idCounter = 0;

  constructor(endpoint: string, workerId: string) {
    this.stateClient = new BeamFnStateClient(
      endpoint,
      grpc.ChannelCredentials.createInsecure(),
      {},
      {},
    );
    const metadata = new grpc.Metadata();
    metadata.add("worker_id", workerId);
    this.stateChannel = this.stateClient.state(metadata);
    const this_ = this;
    this.stateChannel.on("data", (response) => {
      const cb = this_.callbacks.get(response.id);
      this_.callbacks.delete(response.id);
      cb!(response);
    });
    this.stateChannel.on("error", (error) => {
      this.error = error;
    });
  }

  close() {
    this.closed = true;
    this.stateChannel.end();
  }

  getState<T>(
    instructionId: string,
    stateKey: fnApi.StateKey,
    decode: (data: Uint8Array) => T,
  ): MaybePromise<T> {
    if (this.closed) {
      throw new Error("State stream is closed.");
    } else if (this.error) {
      throw this.error;
    }

    const this_ = this;

    // Not inlined as it may need to be called recursively to handle
    // continuation tokens.
    function responseCallback(
      resolve,
      reject,
      prevChunks: Uint8Array[] = [],
    ): (response: fnApi.StateResponse) => void {
      return (response) => {
        if (this_.error) {
          reject(this_.error);
          return;
        }
        if (response.error) {
          reject(response.error);
          return;
        }
        let getResponse: fnApi.StateGetResponse;
        if (response.response.oneofKind === "get") {
          getResponse = response.response.get;
        } else {
          reject("Expected get response " + response.response.oneofKind);
          return;
        }

        // We have a properly formed response.
        const allChunks = prevChunks.concat([getResponse.data]);
        if (
          getResponse.continuationToken &&
          getResponse.continuationToken.length > 0
        ) {
          // Register another callback to fetch the rest of the data.
          const continueId = "continueStateRequest" + this_.idCounter++;
          this_.callbacks.set(
            continueId,
            responseCallback(resolve, reject, allChunks),
          );
          this_.stateChannel.write({
            id: continueId,
            instructionId,
            stateKey,
            request: {
              oneofKind: "get",
              get: {
                continuationToken: getResponse.continuationToken,
              },
            },
          });
        } else {
          // End of the data stream; resolve with everything we got.
          resolve(decode(Uint8ArrayConcat(allChunks)));
        }
      };
    }

    const id = "stateRequest" + this.idCounter++;
    const promise = new Promise<T>((resolve, reject) => {
      this_.callbacks.set(id, responseCallback(resolve, reject));
    });

    this.stateChannel.write({
      id,
      instructionId,
      stateKey,
      request: {
        oneofKind: "get",
        get: fnApi.StateGetRequest.create({}),
      },
    });
    return { type: "promise", promise };
  }
}

export function Uint8ArrayConcat(chunks: Uint8Array[]) {
  if (chunks.length === 1) {
    // (Very) common case.
    return chunks[0];
  } else if (chunks.length === 0) {
    return new Uint8Array();
  } else {
    const fullData = new Uint8Array(
      chunks.map((chunk) => chunk.length).reduce((a, b) => a + b),
    );
    let start = 0;
    for (const chunk of chunks) {
      fullData.set(chunk, start);
      start += chunk.length;
    }
    return fullData;
  }
}
