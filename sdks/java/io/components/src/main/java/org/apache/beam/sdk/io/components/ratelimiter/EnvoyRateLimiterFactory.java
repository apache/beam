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
package org.apache.beam.sdk.io.components.ratelimiter;

import io.envoyproxy.envoy.extensions.common.ratelimit.v3.RateLimitDescriptor;
import io.envoyproxy.envoy.service.ratelimit.v3.RateLimitRequest;
import io.envoyproxy.envoy.service.ratelimit.v3.RateLimitResponse;
import io.envoyproxy.envoy.service.ratelimit.v3.RateLimitServiceGrpc;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.components.throttling.ThrottlingSignaler;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.util.Sleeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link RateLimiterFactory} for Envoy Rate Limit Service. */
public class EnvoyRateLimiterFactory implements RateLimiterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(EnvoyRateLimiterFactory.class);
  private static final int RPC_RETRY_COUNT = 3;
  private static final long RPC_RETRY_DELAY_MILLIS = 5000;

  private final RateLimiterOptions options;

  private transient volatile @Nullable RateLimitServiceGrpc.RateLimitServiceBlockingStub stub;
  private transient @Nullable RateLimiterClientCache clientCache;
  private final ThrottlingSignaler throttlingSignaler;

  private final Counter requestsTotal;
  private final Counter requestsAllowed;
  private final Counter requestsThrottled;
  private final Counter rpcErrors;
  private final Counter rpcRetries;
  private final Distribution rpcLatency;

  public EnvoyRateLimiterFactory(RateLimiterOptions options) {
    this.options = options;
    String namespace = EnvoyRateLimiterFactory.class.getName();
    this.throttlingSignaler = new ThrottlingSignaler(namespace);
    this.requestsTotal = Metrics.counter(namespace, "ratelimit-requests-total");
    this.requestsAllowed = Metrics.counter(namespace, "ratelimit-requests-allowed");
    this.requestsThrottled = Metrics.counter(namespace, "ratelimit-requests-throttled");
    this.rpcErrors = Metrics.counter(namespace, "ratelimit-rpc-errors");
    this.rpcRetries = Metrics.counter(namespace, "ratelimit-rpc-retries");
    this.rpcLatency = Metrics.distribution(namespace, "ratelimit-rpc-latency-ms");
  }

  @Override
  public synchronized void close() {
    if (clientCache != null) {
      clientCache.release();
      clientCache = null;
      stub = null;
    }
  }

  private void init() {
    if (stub != null) {
      return;
    }
    synchronized (this) {
      if (stub == null) {
        RateLimiterClientCache cache = RateLimiterClientCache.getOrCreate(options.getAddress());
        this.clientCache = cache;
        stub = RateLimitServiceGrpc.newBlockingStub(cache.getChannel());
      }
    }
  }

  @Override
  public RateLimiter getLimiter(RateLimiterContext context) {
    if (!(context instanceof EnvoyRateLimiterContext)) {
      throw new IllegalArgumentException(
          "EnvoyRateLimiterFactory requires EnvoyRateLimiterContext");
    }
    return new EnvoyRateLimiter(this, (EnvoyRateLimiterContext) context);
  }

  @Override
  public boolean allow(RateLimiterContext context, int permits)
      throws IOException, InterruptedException {
    if (!(context instanceof EnvoyRateLimiterContext)) {
      throw new IllegalArgumentException(
          "EnvoyRateLimiterFactory requires EnvoyRateLimiterContext, got: "
              + context.getClass().getName());
    }
    EnvoyRateLimiterContext envoyContext = (EnvoyRateLimiterContext) context;
    return callEnvoy(envoyContext, permits);
  }

  private boolean callEnvoy(EnvoyRateLimiterContext context, int tokens)
      throws IOException, InterruptedException {

    init();
    Sleeper sleeper = Sleeper.DEFAULT;
    RateLimitServiceGrpc.RateLimitServiceBlockingStub currentStub = stub;
    if (currentStub == null) {
      throw new IllegalStateException("RateLimitService stub is null");
    }

    Map<String, String> descriptors = context.getDescriptors();
    RateLimitDescriptor.Builder descriptorBuilder = RateLimitDescriptor.newBuilder();

    for (Map.Entry<String, String> entry : descriptors.entrySet()) {
      descriptorBuilder.addEntries(
          RateLimitDescriptor.Entry.newBuilder()
              .setKey(entry.getKey())
              .setValue(entry.getValue())
              .build());
    }

    RateLimitRequest request =
        RateLimitRequest.newBuilder()
            .setDomain(context.getDomain())
            .setHitsAddend(tokens)
            .addDescriptors(descriptorBuilder.build())
            .build();

    boolean blockUntilAllowed = options.isBlockUntilAllowed();
    int maxRetries = options.getMaxRetries();
    long timeoutMillis = options.getTimeout().toMillis();

    requestsTotal.inc();
    int attempt = 0;
    while (true) {
      if (!blockUntilAllowed && attempt > maxRetries) {
        return false;
      }

      // RPC Retry Loop
      RateLimitResponse response = null;
      long startTime = System.currentTimeMillis();
      for (int i = 0; i < RPC_RETRY_COUNT; i++) {
        try {
          response =
              currentStub
                  .withDeadlineAfter(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS)
                  .shouldRateLimit(request);
          long endTime = System.currentTimeMillis();
          rpcLatency.update(endTime - startTime);
          break;
        } catch (StatusRuntimeException e) {
          rpcErrors.inc();
          if (i == RPC_RETRY_COUNT - 1) {
            LOG.error("RateLimitService call failed after {} attempts", RPC_RETRY_COUNT, e);
            throw new IOException("Failed to call Rate Limit Service", e);
          }
          rpcRetries.inc();
          LOG.warn("RateLimitService call failed, retrying", e);
          if (sleeper != null) {
            sleeper.sleep(RPC_RETRY_DELAY_MILLIS);
          }
        }
      }

      if (response == null) {
        throw new IOException("Failed to get response from Rate Limit Service");
      }

      if (response.getOverallCode() == RateLimitResponse.Code.OK) {
        requestsAllowed.inc();
        return true;
      } else if (response.getOverallCode() == RateLimitResponse.Code.OVER_LIMIT) {
        long sleepMillis = 0;
        for (RateLimitResponse.DescriptorStatus status : response.getStatusesList()) {
          if (status.getCode() == RateLimitResponse.Code.OVER_LIMIT
              && status.hasDurationUntilReset()) {
            long durationMillis =
                status.getDurationUntilReset().getSeconds() * 1000
                    + status.getDurationUntilReset().getNanos() / 1_000_000;
            if (durationMillis > sleepMillis) {
              sleepMillis = durationMillis;
            }
          }
        }

        if (sleepMillis == 0) {
          sleepMillis = 1000;
        }

        long jitter =
            (long)
                (java.util.concurrent.ThreadLocalRandom.current().nextDouble()
                    * (0.01 * sleepMillis));
        sleepMillis += jitter;

        LOG.warn("Throttled by RLS, sleeping for {} ms", sleepMillis);
        if (sleeper != null) {
          requestsThrottled.inc();
          if (throttlingSignaler != null) {
            throttlingSignaler.signalThrottling(sleepMillis);
          }
          sleeper.sleep(sleepMillis);
        }
        attempt++;
      } else {
        throw new IOException(
            "Rate Limit Service returned unknown code: " + response.getOverallCode());
      }
    }
  }
}
