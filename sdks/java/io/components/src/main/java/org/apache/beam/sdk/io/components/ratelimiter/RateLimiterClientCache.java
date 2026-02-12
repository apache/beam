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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A static cache for {@link ManagedChannel}s to Rate Limit Service.
 *
 * <p>This class ensures that multiple DoFn instances (threads) in the same Worker sharing the same
 * RLS address will share a single {@link ManagedChannel}.
 *
 * <p>It uses reference counting to close the channel when it is no longer in use by any RateLimiter
 * instance.
 */
public class RateLimiterClientCache {
  private static final Logger LOG = LoggerFactory.getLogger(RateLimiterClientCache.class);
  private static final Map<String, RateLimiterClientCache> CACHE = new ConcurrentHashMap<>();

  private final ManagedChannel channel;
  private final String address;
  private int refCount = 0;

  private RateLimiterClientCache(String address) {
    this.address = address;
    LOG.info("Creating new ManagedChannel for RLS at {}", address);
    this.channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
  }

  /**
   * Gets or creates a cached client for the given address. Increments the reference count.
   * Synchronized on the class to prevent race conditions when multiple instances call getOrCreate()
   * simultaneously
   */
  public static synchronized RateLimiterClientCache getOrCreate(String address) {
    RateLimiterClientCache client = CACHE.get(address);
    if (client == null) {
      client = new RateLimiterClientCache(address);
      CACHE.put(address, client);
    }
    client.refCount++;
    LOG.debug("Referenced RLS Channel for {}. New RefCount: {}", address, client.refCount);
    return client;
  }

  public ManagedChannel getChannel() {
    return channel;
  }

  /**
   * Releases the client. Decrements the reference count. If reference count reaches 0, the channel
   * is shut down and removed from the cache. Synchronized on the class to prevent race conditions
   * when multiple threads call release() simultaneously and to prevent race conditions between
   * getOrCreate() and release() calls.
   */
  public void release() {
    synchronized (RateLimiterClientCache.class) {
      refCount--;
      LOG.debug("Released RLS Channel for {}. New RefCount: {}", address, refCount);
      if (refCount <= 0) {
        LOG.info("Closing ManagedChannel for RLS at {}", address);
        CACHE.remove(address);
        channel.shutdown();
        try {
          channel.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          LOG.error("Couldn't gracefully close gRPC channel={}", channel, e);
          Thread.currentThread().interrupt();
        }
        channel.shutdownNow();
      }
    }
  }
}
