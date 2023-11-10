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
final class Cache {

  /**
   * Read {@link RequestT} {@link ResponseT} associations from a cache. The {@link KV} value is null
   * when no association exists.
   */
  static <
          @NonNull RequestT,
          @Nullable ResponseT,
          CallerSetupTeardownT extends
              Caller<@NonNull RequestT, KV<@NonNull RequestT, @Nullable ResponseT>> & SetupTeardown>
      PTransform<
              PCollection<@NonNull RequestT>,
              Call.Result<KV<@NonNull RequestT, @Nullable ResponseT>>>
          read(
              CallerSetupTeardownT implementsCallerSetupTeardown,
              Coder<@NonNull RequestT> requestTCoder,
              Coder<@NonNull ResponseT> responseTCoder) {
    return Call.ofCallerAndSetupTeardown(
        implementsCallerSetupTeardown, KvCoder.of(requestTCoder, responseTCoder));
  }

  /** Write a {@link RequestT} {@link ResponseT} association to a cache. */
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
              CallerSetupTeardownT implementsCallerSetupTeardown,
              KvCoder<@NonNull RequestT, @NonNull ResponseT> kvCoder) {
    return Call.ofCallerAndSetupTeardown(implementsCallerSetupTeardown, kvCoder);
  }

  /** Provides cache read and write support using a {@link RedisClient}. */
  static class UsingRedis<RequestT, ResponseT> {
    private final Coder<@NonNull RequestT> requestTCoder;
    private final Coder<@NonNull ResponseT> responseTCoder;
    private final RedisClient client;

    /**
     * Instantiates a {@link UsingRedis}. Throws a {@link Coder.NonDeterministicException} if either
     * {@link RequestT} or {@link ResponseT} {@link Coder} fails to {@link
     * Coder#verifyDeterministic}.
     */
    UsingRedis(
        Coder<@NonNull RequestT> requestTCoder,
        Coder<@NonNull ResponseT> responseTCoder,
        RedisClient client)
        throws Coder.NonDeterministicException {
      this.client = client;
      requestTCoder.verifyDeterministic();
      responseTCoder.verifyDeterministic();
      this.requestTCoder = requestTCoder;
      this.responseTCoder = responseTCoder;
    }

    /** Instantiates {@link Read}. */
    Read<@NonNull RequestT, @Nullable ResponseT> read() {
      return new Read<>(this);
    }

    /**
     * Instantiates {@link Write}. The {@link Duration} determines how long the associated {@link
     * RequestT} and {@link ResponseT} lasts in the cache.
     */
    Write<@NonNull RequestT, @NonNull ResponseT> write(Duration expiry) {
      return new Write<>(expiry, this);
    }

    /** Reads associated {@link RequestT} {@link ResponseT} using a {@link RedisClient}. */
    static class Read<@NonNull RequestT, @Nullable ResponseT>
        implements Caller<@NonNull RequestT, KV<@NonNull RequestT, @Nullable ResponseT>>,
            SetupTeardown {

      private final UsingRedis<RequestT, ResponseT> spec;

      private Read(UsingRedis<RequestT, ResponseT> spec) {
        this.spec = spec;
      }

      @Override
      public KV<@NonNull RequestT, @Nullable ResponseT> call(@NonNull RequestT request)
          throws UserCodeExecutionException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          spec.requestTCoder.encode(request, baos);
          byte[] encodedRequest = baos.toByteArray();
          byte[] encodedResponse = spec.client.getBytes(encodedRequest);
          if (encodedResponse == null) {
            return KV.of(request, null);
          }
          ResponseT response =
              checkStateNotNull(
                  spec.responseTCoder.decode(ByteSource.wrap(encodedResponse).openStream()));
          return KV.of(request, response);
        } catch (IllegalStateException | IOException e) {
          throw new UserCodeExecutionException(e);
        }
      }

      @Override
      public void setup() throws UserCodeExecutionException {
        spec.client.setup();
      }

      @Override
      public void teardown() throws UserCodeExecutionException {
        spec.client.teardown();
      }
    }
  }

  static class Write<@NonNull RequestT, @NonNull ResponseT>
      implements Caller<
              KV<@NonNull RequestT, @NonNull ResponseT>, KV<@NonNull RequestT, @NonNull ResponseT>>,
          SetupTeardown {
    private final Duration expiry;

    private final UsingRedis<RequestT, ResponseT> spec;

    private Write(Duration expiry, UsingRedis<RequestT, ResponseT> spec) {
      this.expiry = expiry;
      this.spec = spec;
    }

    @Override
    public KV<@NonNull RequestT, @NonNull ResponseT> call(
        KV<@NonNull RequestT, @NonNull ResponseT> request) throws UserCodeExecutionException {
      ByteArrayOutputStream keyStream = new ByteArrayOutputStream();
      ByteArrayOutputStream valueStream = new ByteArrayOutputStream();
      try {
        spec.requestTCoder.encode(request.getKey(), keyStream);
        spec.responseTCoder.encode(request.getValue(), valueStream);
      } catch (IOException e) {
        throw new UserCodeExecutionException(e);
      }
      spec.client.setex(keyStream.toByteArray(), valueStream.toByteArray(), expiry);
      return request;
    }

    @Override
    public void setup() throws UserCodeExecutionException {
      spec.client.setup();
    }

    @Override
    public void teardown() throws UserCodeExecutionException {
      spec.client.teardown();
    }
  }
}
