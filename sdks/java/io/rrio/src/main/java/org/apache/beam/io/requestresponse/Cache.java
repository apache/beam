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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/** Transforms for reading and writing request/response associations to a cache. */
class Cache {

  /**
   * Read {@link RequestT} {@link ResponseT} associations from a cache. The {@link KV} value is null
   * when no association exists. This method does not enforce {@link Coder#verifyDeterministic} and
   * defers to the user to determine whether to enforce this given the cache implementation.
   */
  static <
          @NonNull RequestT,
          @Nullable ResponseT,
          CallerSetupTeardownT extends
              @NonNull Caller<@NonNull RequestT, KV<@NonNull RequestT, @Nullable ResponseT>>
                  & SetupTeardown>
      PTransform<
              @NonNull PCollection<@NonNull RequestT>,
              Call.@NonNull Result<KV<@NonNull RequestT, @Nullable ResponseT>>>
          read(
              @NonNull CallerSetupTeardownT implementsCallerSetupTeardown,
              @NonNull Coder<@NonNull RequestT> requestTCoder,
              @NonNull Coder<@NonNull ResponseT> responseTCoder) {
    return Call.ofCallerAndSetupTeardown(
        implementsCallerSetupTeardown, KvCoder.of(requestTCoder, responseTCoder));
  }

  /**
   * Write a {@link RequestT} {@link ResponseT} association to a cache. This method does not enforce
   * {@link Coder#verifyDeterministic} and defers to the user to determine whether to enforce this
   * given the cache implementation.
   */
  static <
          @NonNull RequestT,
          @NonNull ResponseT,
          CallerSetupTeardownT extends
              Caller<
                      KV<@NonNull RequestT, @NonNull ResponseT>,
                      KV<@NonNull RequestT, @NonNull ResponseT>>
                  & SetupTeardown>
      PTransform<
              PCollection<KV<@NonNull RequestT, @NonNull ResponseT>>,
              Call.Result<KV<@NonNull RequestT, @NonNull ResponseT>>>
          write(
              @NonNull CallerSetupTeardownT implementsCallerSetupTeardown,
              @NonNull KvCoder<@NonNull RequestT, @NonNull ResponseT> kvCoder) {
    return Call.ofCallerAndSetupTeardown(implementsCallerSetupTeardown, kvCoder);
  }

  static <@NonNull RequestT, ResponseT> UsingRedis<RequestT, ResponseT> usingRedis(
      @NonNull Coder<@NonNull RequestT> requestTCoder,
      @NonNull Coder<@NonNull ResponseT> responseTCoder,
      @NonNull RedisClient client)
      throws Coder.NonDeterministicException {
    return new UsingRedis<>(requestTCoder, responseTCoder, client);
  }

  /** Provides cache read and write support using a {@link RedisClient}. */
  static class UsingRedis<@NonNull RequestT, ResponseT> {
    private final @NonNull Coder<@NonNull RequestT> requestTCoder;
    private final @NonNull Coder<@NonNull ResponseT> responseTCoder;
    private final @NonNull RedisClient client;

    /**
     * Instantiates a {@link UsingRedis}. Given redis reads and writes using bytes, throws a {@link
     * Coder.NonDeterministicException} if either {@link RequestT} or {@link ResponseT} {@link
     * Coder} fails to {@link Coder#verifyDeterministic}.
     */
    private UsingRedis(
        @NonNull Coder<@NonNull RequestT> requestTCoder,
        @NonNull Coder<@NonNull ResponseT> responseTCoder,
        @NonNull RedisClient client)
        throws Coder.NonDeterministicException {
      this.client = client;
      requestTCoder.verifyDeterministic();
      responseTCoder.verifyDeterministic();
      this.requestTCoder = requestTCoder;
      this.responseTCoder = responseTCoder;
    }

    /** Instantiates {@link Read}. */
    Read<@NonNull RequestT, @Nullable ResponseT> read() {
      return new Read<>(requestTCoder, responseTCoder, client);
    }

    /**
     * Instantiates {@link Write}. The {@link Duration} determines how long the associated {@link
     * RequestT} and {@link ResponseT} lasts in the cache.
     */
    Write<@NonNull RequestT, @NonNull ResponseT> write(Duration expiry) {
      return new Write<>(expiry, requestTCoder, responseTCoder, client);
    }

    /** Reads associated {@link RequestT} {@link ResponseT} using a {@link RedisClient}. */
    static class Read<@NonNull RequestT, @Nullable ResponseT>
        implements Caller<@NonNull RequestT, KV<@NonNull RequestT, @Nullable ResponseT>>,
            SetupTeardown {

      private final @NonNull Coder<@NonNull RequestT> requestTCoder;
      private final @NonNull Coder<@NonNull ResponseT> responseTCoder;
      private final @NonNull RedisClient client;

      private Read(
          @NonNull Coder<@NonNull RequestT> requestTCoder,
          @NonNull Coder<@NonNull ResponseT> responseTCoder,
          @NonNull RedisClient client) {
        this.requestTCoder = requestTCoder;
        this.responseTCoder = responseTCoder;
        this.client = client;
      }

      @Override
      public KV<@NonNull RequestT, @Nullable ResponseT> call(@NonNull RequestT request)
          throws UserCodeExecutionException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          requestTCoder.encode(request, baos);
          byte[] encodedRequest = baos.toByteArray();
          byte[] encodedResponse = client.getBytes(encodedRequest);
          if (encodedResponse == null) {
            return KV.of(request, null);
          }
          ResponseT response =
              checkStateNotNull(
                  responseTCoder.decode(ByteSource.wrap(encodedResponse).openStream()));
          return KV.of(request, response);
        } catch (IllegalStateException | IOException e) {
          throw new UserCodeExecutionException(e);
        }
      }

      @Override
      public void setup() throws UserCodeExecutionException {
        client.setup();
      }

      @Override
      public void teardown() throws UserCodeExecutionException {
        client.teardown();
      }
    }
  }

  static class Write<@NonNull RequestT, @NonNull ResponseT>
      implements Caller<
              @NonNull KV<@NonNull RequestT, @NonNull ResponseT>,
              @NonNull KV<@NonNull RequestT, @NonNull ResponseT>>,
          SetupTeardown {
    private final @NonNull Duration expiry;
    private final @NonNull Coder<@NonNull RequestT> requestTCoder;
    private final @NonNull Coder<@NonNull ResponseT> responseTCoder;
    private final @NonNull RedisClient client;

    private Write(
        @NonNull Duration expiry,
        @NonNull Coder<@NonNull RequestT> requestTCoder,
        @NonNull Coder<@NonNull ResponseT> responseTCoder,
        @NonNull RedisClient client) {
      this.expiry = expiry;
      this.requestTCoder = requestTCoder;
      this.responseTCoder = responseTCoder;
      this.client = client;
    }

    @Override
    public @NonNull KV<@NonNull RequestT, @NonNull ResponseT> call(
        @NonNull KV<@NonNull RequestT, @NonNull ResponseT> request)
        throws UserCodeExecutionException {
      ByteArrayOutputStream keyStream = new ByteArrayOutputStream();
      ByteArrayOutputStream valueStream = new ByteArrayOutputStream();
      try {
        requestTCoder.encode(request.getKey(), keyStream);
        responseTCoder.encode(request.getValue(), valueStream);
      } catch (IOException e) {
        throw new UserCodeExecutionException(e);
      }
      client.setex(keyStream.toByteArray(), valueStream.toByteArray(), expiry);
      return request;
    }

    @Override
    public void setup() throws UserCodeExecutionException {
      client.setup();
    }

    @Override
    public void teardown() throws UserCodeExecutionException {
      client.teardown();
    }
  }
}
