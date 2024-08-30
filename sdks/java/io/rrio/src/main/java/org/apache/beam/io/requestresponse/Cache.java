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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteSource;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/** Transforms for reading and writing request/response associations to a cache. */
// TODO(damondouglas): Add metrics per https://github.com/apache/beam/issues/29888.
public final class Cache {

  /**
   * Builds a {@link Pair} using a <a href="https://redis.io">Redis</a> cache to read and write
   * {@link RequestT} and {@link ResponseT} pairs. The purpose of the cache is to offload {@link
   * RequestT}s from the API and instead return the {@link ResponseT} if the association is known.
   * Since the {@link RequestT}s and {@link ResponseT}s need encoding and decoding, checks are made
   * whether the requestTCoder and responseTCoders are {@link Coder#verifyDeterministic}.
   * <strong>This feature is only appropriate for API reads such as HTTP list, get, etc.</strong>
   *
   * <pre>Below describes the parameters in more detail and their usage.</pre>
   *
   * <ul>
   *   <li>{@code URI uri} - the {@link URI} of the Redis instance.
   *   <li>{@code Coder<RequestT> requestTCoder} - the {@link RequestT} {@link Coder} to encode and
   *       decode {@link RequestT}s during cache read and writes.
   *   <li>{@code Duration expiry} - the duration to hold {@link RequestT} and {@link ResponseT}
   *       pairs in the cache.
   * </ul>
   */
  public static <RequestT, ResponseT> Pair<RequestT, ResponseT> usingRedis(
      URI uri, Coder<RequestT> requestTCoder, Coder<ResponseT> responseTCoder, Duration expiry)
      throws NonDeterministicException {
    PTransform<PCollection<RequestT>, Result<KV<RequestT, @Nullable ResponseT>>> read =
        Cache.<RequestT, @Nullable ResponseT>readUsingRedis(
            new RedisClient(uri), requestTCoder, new CacheResponseCoder<>(responseTCoder));

    PTransform<PCollection<KV<RequestT, ResponseT>>, Result<KV<RequestT, ResponseT>>> write =
        // Type arguments needed to resolve "error: [assignment] incompatible types in assignment."
        Cache.<RequestT, ResponseT>writeUsingRedis(
            expiry, new RedisClient(uri), requestTCoder, new CacheResponseCoder<>(responseTCoder));

    return Pair.<RequestT, ResponseT>of(read, write);
  }

  /**
   * A simple POJO that holds both cache read and write {@link PTransform}s. Functionally, these go
   * together and must at times be instantiated using the same inputs.
   */
  public static class Pair<RequestT, ResponseT> {
    private final PTransform<PCollection<RequestT>, Result<KV<RequestT, @Nullable ResponseT>>> read;
    private final PTransform<PCollection<KV<RequestT, ResponseT>>, Result<KV<RequestT, ResponseT>>>
        write;

    public static <RequestT, ResponseT> Pair<RequestT, ResponseT> of(
        PTransform<PCollection<RequestT>, Result<KV<RequestT, @Nullable ResponseT>>> read,
        PTransform<PCollection<KV<RequestT, ResponseT>>, Result<KV<RequestT, ResponseT>>> write) {
      return new Pair<>(read, write);
    }

    private Pair(
        PTransform<PCollection<RequestT>, Result<KV<RequestT, @Nullable ResponseT>>> read,
        PTransform<PCollection<KV<RequestT, ResponseT>>, Result<KV<RequestT, ResponseT>>> write) {
      this.read = read;
      this.write = write;
    }

    public PTransform<PCollection<RequestT>, Result<KV<RequestT, @Nullable ResponseT>>> getRead() {
      return read;
    }

    public PTransform<PCollection<KV<RequestT, ResponseT>>, Result<KV<RequestT, ResponseT>>>
        getWrite() {
      return write;
    }
  }

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
      PTransform<PCollection<RequestT>, Result<KV<RequestT, @Nullable ResponseT>>> read(
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
      PTransform<PCollection<RequestT>, Result<KV<RequestT, @Nullable ResponseT>>> readUsingRedis(
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
      PTransform<PCollection<KV<RequestT, ResponseT>>, Result<KV<RequestT, ResponseT>>> write(
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
      PTransform<PCollection<KV<RequestT, ResponseT>>, Result<KV<RequestT, ResponseT>>>
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

    Read<RequestT, @Nullable ResponseT> read() {
      return new Read<>(requestTCoder, responseTCoder, client);
    }

    Write<RequestT, ResponseT> write(Duration expiry) {
      return new Write<>(expiry, requestTCoder, responseTCoder, client);
    }

    /** Reads associated {@link RequestT} {@link ResponseT} using a {@link RedisClient}. */
    static class Read<RequestT, @Nullable ResponseT>
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

    static class Write<RequestT, ResponseT>
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

  /** Resolves checker error: incompatible argument for parameter ResponseT Coder. */
  private static class CacheResponseCoder<ResponseT> extends CustomCoder<@Nullable ResponseT> {
    private final NullableCoder<ResponseT> basis;

    private CacheResponseCoder(Coder<ResponseT> basis) {
      this.basis = NullableCoder.of(basis);
    }

    @Override
    public void encode(@Nullable ResponseT value, OutputStream outStream)
        throws CoderException, IOException {
      basis.encode(value, outStream);
    }

    @Override
    public @Nullable ResponseT decode(InputStream inStream) throws CoderException, IOException {
      return basis.decode(inStream);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return basis.getCoderArguments();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      basis.verifyDeterministic();
    }
  }
}
