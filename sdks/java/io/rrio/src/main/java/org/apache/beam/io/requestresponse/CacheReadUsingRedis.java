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
package org.apache.beam.io.requestresponse;

import java.util.Base64;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Reads an associated {@link ResponseT} from a {@link RequestT} using a {@link RedisClient}.
 * Implements {@link Caller} and {@link SetupTeardown} so that {@link CacheReadUsingRedis} can be
 * used by {@link Call}.
 */
class CacheReadUsingRedis<@NonNull RequestT, @Nullable ResponseT>
    implements Caller<@NonNull RequestT, KV<@NonNull RequestT, ResponseT>>, SetupTeardown {

  private final RedisClient client;
  private final CacheSerializer<@NonNull RequestT> requestSerializer;
  private final CacheSerializer<@NonNull ResponseT> responseSerializer;

  CacheReadUsingRedis(
      RedisClient client,
      CacheSerializer<@NonNull RequestT> requestSerializer,
      CacheSerializer<@NonNull ResponseT> responseSerializer) {
    this.client = client;
    this.requestSerializer = requestSerializer;
    this.responseSerializer = responseSerializer;
  }

  /** Implements {@link SetupTeardown#setup} to be called during a {@link DoFn.Setup}. */
  @Override
  public void setup() throws UserCodeExecutionException {
    client.setup();
  }

  /** Implements {@link SetupTeardown#teardown} to be called during a {@link DoFn.Teardown}. */
  @Override
  public void teardown() throws UserCodeExecutionException {
    client.teardown();
  }

  /**
   * Implements {@link Caller#call} reading an associated {@link RequestT} to a {@link ResponseT}.
   * If no association exists, returns a {@link KV} with a null value.
   */
  @Override
  public KV<@NonNull RequestT, @Nullable ResponseT> call(@NonNull RequestT request)
      throws UserCodeExecutionException {
    byte[] requestBytes = requestSerializer.serialize(request);
    byte[] encodedRequestBytes = Base64.getEncoder().encode(requestBytes);
    byte @Nullable [] encodedResponseBytes = client.getBytes(encodedRequestBytes);
    if (encodedResponseBytes == null) {
      return KV.of(request, null);
    }
    byte[] responseBytes = Base64.getDecoder().decode(encodedResponseBytes);
    ResponseT response = responseSerializer.deserialize(responseBytes);
    return KV.of(request, response);
  }
}
