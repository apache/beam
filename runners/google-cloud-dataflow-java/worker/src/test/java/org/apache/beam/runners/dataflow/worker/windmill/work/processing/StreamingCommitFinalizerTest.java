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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingCommitFinalizerTest {

  private StreamingCommitFinalizer finalizer;
  private BoundedQueueExecutor executor;

  @Before
  public void setUp() {
    executor =
        new BoundedQueueExecutor(
            10,
            60,
            TimeUnit.SECONDS,
            10,
            10000000,
            new ThreadFactoryBuilder()
                .setNameFormat("FinalizationCallback-%d")
                .setDaemon(true)
                .build(),
            /*useFairMonitor=*/ false);
    finalizer = StreamingCommitFinalizer.create(executor);
  }

  @Test
  public void testCreateAndInit() {
    assertEquals(0, finalizer.cleanupQueueSize());
  }

  @Test
  public void testCacheCommitFinalizer() {
    Runnable callback = mock(Runnable.class);
    finalizer.cacheCommitFinalizers(
        ImmutableMap.of(1L, Pair.of(Instant.now().plus(Duration.standardHours(1)), callback)));
    assertEquals(1, finalizer.cleanupQueueSize());
    verify(callback, never()).run();
  }

  @Test
  public void testThrowErrorOnDuplicateIds() {
    Runnable callback1 = mock(Runnable.class);
    Instant expiry = Instant.now().plus(Duration.standardHours(1));
    finalizer.cacheCommitFinalizers(ImmutableMap.of(1L, Pair.of(expiry, callback1)));

    Runnable callback2 = mock(Runnable.class);
    Map<Long, Pair<Instant, Runnable>> duplicateCallback =
        ImmutableMap.of(1L, Pair.of(expiry, callback2));
    assertThrows(
        IllegalStateException.class, () -> finalizer.cacheCommitFinalizers(duplicateCallback));
  }

  @Test
  public void testFinalizeCommits() throws Exception {
    CountDownLatch callbackExecuted = new CountDownLatch(1);
    finalizer.cacheCommitFinalizers(
        ImmutableMap.of(
            1L,
            Pair.of(
                Instant.now().plus(Duration.standardHours(1)),
                () -> callbackExecuted.countDown())));
    finalizer.finalizeCommits(Collections.singletonList(1L));
    assertTrue(callbackExecuted.await(30, TimeUnit.SECONDS));
    assertEquals(0, finalizer.cleanupQueueSize());
  }

  @Test
  public void testMultipleCommits() throws Exception {
    CountDownLatch callback1Executed = new CountDownLatch(1);
    CountDownLatch callback2Executed = new CountDownLatch(1);
    CountDownLatch callback3Executed = new CountDownLatch(1);

    Instant expiryTime = Instant.now().plus(Duration.standardHours(1));
    finalizer.cacheCommitFinalizers(
        ImmutableMap.<Long, Pair<Instant, Runnable>>builder()
            .put(1L, Pair.of(expiryTime, () -> callback1Executed.countDown()))
            .put(2L, Pair.of(expiryTime, () -> callback2Executed.countDown()))
            .put(3L, Pair.of(expiryTime, () -> callback3Executed.countDown()))
            .build());
    // Finalize commits one at a time (in different order from added).
    finalizer.finalizeCommits(Collections.singletonList(2L));
    assertTrue(callback2Executed.await(30, TimeUnit.SECONDS));

    finalizer.finalizeCommits(Collections.singletonList(3L));
    assertTrue(callback3Executed.await(30, TimeUnit.SECONDS));

    finalizer.finalizeCommits(Collections.singletonList(1L));
    assertTrue(callback1Executed.await(30, TimeUnit.SECONDS));

    assertEquals(0, finalizer.cleanupQueueSize());
  }

  @Test
  public void testIgnoresUnknownIds() throws Exception {
    Runnable callback = mock(Runnable.class);
    finalizer.cacheCommitFinalizers(
        ImmutableMap.of(1L, Pair.of(Instant.now().plus(Duration.standardHours(1)), callback)));
    finalizer.finalizeCommits(Collections.singletonList(2L));
    assertEquals(1, executor.elementsOutstanding());
    verify(callback, never()).run();
    assertEquals(1, finalizer.cleanupQueueSize());
  }

  @Test
  public void testCleanupOnExpiration() throws Exception {
    CountDownLatch callback1Executed = new CountDownLatch(1);
    finalizer.cacheCommitFinalizers(
        ImmutableMap.of(
            1L,
            Pair.of(
                Instant.now().plus(Duration.standardHours(1)),
                () -> callback1Executed.countDown())));
    assertEquals(1, finalizer.cleanupQueueSize());

    Runnable callback2 = mock(Runnable.class);
    Runnable callback3 = mock(Runnable.class);
    Instant shortTimeout = Instant.now().plus(Duration.millis(100));
    finalizer.cacheCommitFinalizers(
        ImmutableMap.<Long, Pair<Instant, Runnable>>builder()
            .put(2L, Pair.of(shortTimeout, callback2))
            .put(3L, Pair.of(shortTimeout, callback3))
            .build());

    while (finalizer.cleanupQueueSize() > 1) {
      // Wait until the two 100ms timeouts expire.
      Thread.sleep(200);
    }
    assertEquals(1, executor.elementsOutstanding());
    finalizer.finalizeCommits(ImmutableList.of(2L, 3L));
    verify(callback2, never()).run();
    verify(callback3, never()).run();

    finalizer.finalizeCommits(Collections.singletonList(1L));
    assertTrue(callback1Executed.await(30, TimeUnit.SECONDS));
    assertEquals(0, finalizer.cleanupQueueSize());
  }
}
