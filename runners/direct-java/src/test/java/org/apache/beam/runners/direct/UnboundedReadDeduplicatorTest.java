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
package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.direct.UnboundedReadDeduplicator.CachedIdDeduplicator;
import org.apache.beam.runners.direct.UnboundedReadDeduplicator.NeverDeduplicator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link UnboundedReadDeduplicator}.
 */
@RunWith(JUnit4.class)
public class UnboundedReadDeduplicatorTest {
  @Test
  public void neverDeduplicatorAlwaysTrue() {
    byte[] id = new byte[] {-1, 2, 4, 22};
    UnboundedReadDeduplicator dedupper = NeverDeduplicator.create();

    assertThat(dedupper.shouldOutput(id), is(true));
    assertThat(dedupper.shouldOutput(id), is(true));
  }

  @Test
  public void cachedIdDeduplicatorTrueForFirstIdThenFalse() {
    byte[] id = new byte[] {-1, 2, 4, 22};
    UnboundedReadDeduplicator dedupper = CachedIdDeduplicator.create();

    assertThat(dedupper.shouldOutput(id), is(true));
    assertThat(dedupper.shouldOutput(id), is(false));
  }

  @Test
  public void cachedIdDeduplicatorMultithreaded() throws InterruptedException {
    byte[] id = new byte[] {-1, 2, 4, 22};
    UnboundedReadDeduplicator dedupper = CachedIdDeduplicator.create();
    final CountDownLatch startSignal = new CountDownLatch(1);
    int numThreads = 1000;
    final CountDownLatch readyLatch = new CountDownLatch(numThreads);
    final CountDownLatch finishLine = new CountDownLatch(numThreads);

    ExecutorService executor = Executors.newCachedThreadPool();
    AtomicInteger successCount = new AtomicInteger();
    AtomicInteger failureCount = new AtomicInteger();
    for (int i = 0; i < numThreads; i++) {
      executor.submit(new TryOutputIdRunnable(dedupper,
          id,
          successCount,
          failureCount,
          readyLatch,
          startSignal,
          finishLine));
    }

    readyLatch.await();
    startSignal.countDown();
    finishLine.await(10L, TimeUnit.SECONDS);
    executor.shutdownNow();

    assertThat(successCount.get(), equalTo(1));
    assertThat(failureCount.get(), equalTo(numThreads - 1));
  }

  private static class TryOutputIdRunnable implements Runnable {
    private final UnboundedReadDeduplicator deduplicator;
    private final byte[] id;
    private final AtomicInteger successCount;
    private final AtomicInteger failureCount;
    private final CountDownLatch readyLatch;
    private final CountDownLatch startSignal;
    private final CountDownLatch finishLine;

    public TryOutputIdRunnable(
        UnboundedReadDeduplicator dedupper,
        byte[] id,
        AtomicInteger successCount,
        AtomicInteger failureCount,
        CountDownLatch readyLatch,
        CountDownLatch startSignal,
        CountDownLatch finishLine) {
      this.deduplicator = dedupper;
      this.id = id;
      this.successCount = successCount;
      this.failureCount = failureCount;
      this.readyLatch = readyLatch;
      this.startSignal = startSignal;
      this.finishLine = finishLine;
    }

    @Override
    public void run() {
      readyLatch.countDown();
      try {
        startSignal.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      if (deduplicator.shouldOutput(id)) {
        successCount.incrementAndGet();
      } else {
        failureCount.incrementAndGet();
      }
      finishLine.countDown();
    }
  }
}
