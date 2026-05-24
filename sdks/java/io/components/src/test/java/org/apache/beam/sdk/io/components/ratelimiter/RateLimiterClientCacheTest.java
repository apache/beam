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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RateLimiterClientCache}. */
@RunWith(JUnit4.class)
public class RateLimiterClientCacheTest {

  @Test
  public void testGetOrCreate_SameAddress() {
    String address = "addr1";
    RateLimiterClientCache client1 = RateLimiterClientCache.getOrCreate(address);
    RateLimiterClientCache client2 = RateLimiterClientCache.getOrCreate(address);

    assertSame(client1, client2);
    assertFalse(client1.getChannel().isShutdown());

    // cleanup
    client1.release();
    // client2 is still using the same channel
    assertFalse(client1.getChannel().isShutdown());
    client2.release();
    assertTrue(client1.getChannel().isShutdown());
  }

  @Test
  public void testGetOrCreate_DifferentAddress_ReturnsDifferentInstances() {
    RateLimiterClientCache client1 = RateLimiterClientCache.getOrCreate("addr1");
    RateLimiterClientCache client2 = RateLimiterClientCache.getOrCreate("addr2");

    assertNotSame(client1, client2);

    assertFalse(client1.getChannel().isShutdown());
    assertFalse(client2.getChannel().isShutdown());
    client1.release();
    assertTrue(client1.getChannel().isShutdown());
    client2.release();
    assertTrue(client2.getChannel().isShutdown());
  }

  @Test
  public void testConcurrency() throws InterruptedException, ExecutionException {
    int threads = 10;
    int iterations = 100;
    String address = "concurrent-addr";
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    List<Future<Boolean>> futures = new ArrayList<>();

    for (int i = 0; i < threads; i++) {
      futures.add(
          pool.submit(
              new Callable<Boolean>() {
                @Override
                public Boolean call() {
                  for (int j = 0; j < iterations; j++) {
                    RateLimiterClientCache client = RateLimiterClientCache.getOrCreate(address);
                    // do some tiny work
                    try {
                      Thread.sleep(1);
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                    }
                    client.release();
                  }
                  return true;
                }
              }));
    }

    for (Future<Boolean> f : futures) {
      assertTrue(f.get());
    }

    pool.shutdown();
    pool.awaitTermination(5, TimeUnit.SECONDS);

    // After all threads are done, cache should be empty or create new one cleanly
    RateLimiterClientCache client = RateLimiterClientCache.getOrCreate(address);
    assertFalse(client.getChannel().isShutdown());
    client.release();
    assertTrue(client.getChannel().isShutdown());
  }
}
