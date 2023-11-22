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
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteSource;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/** Transforms for reading and writing request/response associations to a cache. */
final class Cache {

  /**
   * Instantiates a {@link Call} {@link PTransform} that reads {@link RequestT} {@link ResponseT}
   * associations from a cache. The {@link KV} value is null when no association exists. This method
   * does not enforce {@link Coder#verifyDeterministic} and defers to the user to determine whether
   * to enforce this given the cache implementation.
   */
  static <
          RequestT,
          @Nullable ResponseT,
          CallerSetupTeardownT extends
              Caller<RequestT, KV<RequestT, @Nullable ResponseT>> & SetupTeardown>
      PTransform<PCollection<RequestT>, Call.Result<KV<RequestT, @Nullable ResponseT>>> read(
          CallerSetupTeardownT implementsCallerSetupTeardown,
          Coder<RequestT> requestTCoder,
          Coder<@Nullable ResponseT> responseTCoder) {
    return Call.ofCallerAndSetupTeardown(
        implementsCallerSetupTeardown, KvCoder.of(requestTCoder, responseTCoder));
  }

  /**
   * Instantiates a {@link Call} {@link PTransform}, calling {@link #read} with a {@link Caller}
   * that employs a redis client.
   *
   * <p>This method requires both the {@link RequestT} and {@link ResponseT}s' {@link
   * Coder#verifyDeterministic}. Otherwise, it throws a {@link NonDeterministicException}.
   *
   * <p><a href="https://redis.io">Redis</a> is designed for multiple workloads, simultaneously
   * reading and writing to a shared instance. See <a
   * href="https://redis.io/docs/get-started/faq/">Redis FAQ</a> for more information on important
   * considerations when using this method to achieve cache reads.
   */
  static <RequestT, @Nullable ResponseT>
      PTransform<PCollection<RequestT>, Call.Result<KV<RequestT, @Nullable ResponseT>>>
          readUsingRedis(
              RedisClient client,
              Coder<RequestT> requestTCoder,
              Coder<@Nullable ResponseT> responseTCoder)
              throws NonDeterministicException {
    return read(
        new UsingRedis<>(requestTCoder, responseTCoder, client).read(),
        requestTCoder,
        responseTCoder);
  }

  /**
   * Write a {@link RequestT} {@link ResponseT} association to a cache. This method does not enforce
   * {@link Coder#verifyDeterministic} and defers to the user to determine whether to enforce this
   * given the cache implementation.
   */
  static <
          RequestT,
          ResponseT,
          CallerSetupTeardownT extends
              Caller<KV<RequestT, ResponseT>, KV<RequestT, ResponseT>> & SetupTeardown>
      PTransform<PCollection<KV<RequestT, ResponseT>>, Call.Result<KV<RequestT, ResponseT>>> write(
          CallerSetupTeardownT implementsCallerSetupTeardown,
          KvCoder<RequestT, ResponseT> kvCoder) {
    return Call.ofCallerAndSetupTeardown(implementsCallerSetupTeardown, kvCoder);
  }

  /**
   * Instantiates a {@link Call} {@link PTransform}, calling {@link #write} with a {@link Caller}
   * that employs a redis client.
   *
   * <p>This method requires both the {@link RequestT} and {@link ResponseT}s' {@link
   * Coder#verifyDeterministic}. Otherwise, it throws a {@link NonDeterministicException}.
   *
   * <p><a href="https://redis.io">Redis</a> is designed for multiple workloads, simultaneously
   * reading and writing to a shared instance. See <a
   * href="https://redis.io/docs/get-started/faq/">Redis FAQ</a> for more information on important
   * considerations when using this method to achieve cache writes.
   */
  static <RequestT, ResponseT>
      PTransform<PCollection<KV<RequestT, ResponseT>>, Call.Result<KV<RequestT, ResponseT>>>
          writeUsingRedis(
              Duration expiry,
              RedisClient client,
              Coder<RequestT> requestTCoder,
              Coder<@Nullable ResponseT> responseTCoder)
              throws NonDeterministicException {
    return write(
        new UsingRedis<>(requestTCoder, responseTCoder, client).write(expiry),
        KvCoder.of(requestTCoder, responseTCoder));
  }

  private static class UsingRedis<RequestT, ResponseT> {
    private final Coder<RequestT> requestTCoder;
    private final Coder<@Nullable ResponseT> responseTCoder;
    private final RedisClient client;

    private UsingRedis(
        Coder<RequestT> requestTCoder,
        Coder<@Nullable ResponseT> responseTCoder,
        RedisClient client)
        throws Coder.NonDeterministicException {
      this.client = client;
      requestTCoder.verifyDeterministic();
      responseTCoder.verifyDeterministic();
      this.requestTCoder = requestTCoder;
      this.responseTCoder = responseTCoder;
    }

    private Read<RequestT, @Nullable ResponseT> read() {
      return new Read<>(requestTCoder, responseTCoder, client);
    }

    private Write<RequestT, ResponseT> write(Duration expiry) {
      return new Write<>(expiry, requestTCoder, responseTCoder, client);
    }

    /** Reads associated {@link RequestT} {@link ResponseT} using a {@link RedisClient}. */
    private static class Read<RequestT, @Nullable ResponseT>
        implements Caller<RequestT, KV<RequestT, @Nullable ResponseT>>, SetupTeardown {

      private final Coder<RequestT> requestTCoder;
      private final Coder<@Nullable ResponseT> responseTCoder;
      private final RedisClient client;

      private Read(
          Coder<RequestT> requestTCoder,
          Coder<@Nullable ResponseT> responseTCoder,
          RedisClient client) {
        this.requestTCoder = requestTCoder;
        this.responseTCoder = responseTCoder;
        this.client = client;
      }

      @Override
      public KV<RequestT, @Nullable ResponseT> call(RequestT request)
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

  private static class Write<RequestT, ResponseT>
      implements Caller<KV<RequestT, ResponseT>, KV<RequestT, ResponseT>>, SetupTeardown {
    private final Duration expiry;
    private final Coder<RequestT> requestTCoder;
    private final Coder<@Nullable ResponseT> responseTCoder;
    private final RedisClient client;

    private Write(
        Duration expiry,
        Coder<RequestT> requestTCoder,
        Coder<@Nullable ResponseT> responseTCoder,
        RedisClient client) {
      this.expiry = expiry;
      this.requestTCoder = requestTCoder;
      this.responseTCoder = responseTCoder;
      this.client = client;
    }

    @Override
    public KV<RequestT, ResponseT> call(KV<RequestT, ResponseT> request)
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
