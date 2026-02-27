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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.tuple.Pair;
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
    Runnable callback = mock(Runnable.class);
    finalizer.cacheCommitFinalizers(
        ImmutableMap.of(1L, Pair.of(Instant.now().plus(Duration.standardHours(1)), callback)));
    finalizer.finalizeCommits(Collections.singletonList(1L));
    // The executor always has the cleanup thread running. So elementsOutstanding == 2 while we're
    // waiting for the finalization callback to run.
    while (executor.elementsOutstanding() > 1) {
      Thread.sleep(500);
    }
    verify(callback).run();
    assertEquals(0, finalizer.cleanupQueueSize());
  }

  @Test
  public void testIgnoresUnknownIds() throws Exception {
    Runnable callback = mock(Runnable.class);
    finalizer.cacheCommitFinalizers(
        ImmutableMap.of(1L, Pair.of(Instant.now().plus(Duration.standardHours(1)), callback)));
    finalizer.finalizeCommits(Collections.singletonList(2L));
    while (executor.elementsOutstanding() > 1) {
      Thread.sleep(500);
    }
    verify(callback, never()).run();
    assertEquals(1, finalizer.cleanupQueueSize());
  }

  @Test
  public void testCleanupOnExpiration() throws Exception {
    Runnable callback = mock(Runnable.class);
    finalizer.cacheCommitFinalizers(
        ImmutableMap.of(1L, Pair.of(Instant.now().plus(Duration.standardHours(1)), callback)));
    assertEquals(1, finalizer.cleanupQueueSize());

    Runnable callback2 = mock(Runnable.class);
    finalizer.cacheCommitFinalizers(
        ImmutableMap.of(2L, Pair.of(Instant.now().plus(Duration.millis(100)), callback2)));

    Runnable callback3 = mock(Runnable.class);
    finalizer.cacheCommitFinalizers(
        ImmutableMap.of(3L, Pair.of(Instant.now().plus(Duration.millis(100)), callback3)));

    while (finalizer.cleanupQueueSize() > 1) {
      // Wait until it expires
      Thread.sleep(500);
    }
    // We can call finalize even though these were already cleaned up.
    finalizer.finalizeCommits(ImmutableList.of(2L, 3L));
    while (executor.elementsOutstanding() > 1) {
      Thread.sleep(500);
    }
    verify(callback2, never()).run();
    verify(callback3, never()).run();

    finalizer.finalizeCommits(Collections.singletonList(1L));
    while (executor.elementsOutstanding() > 1) {
      Thread.sleep(500);
    }
    verify(callback).run();
    assertEquals(0, finalizer.cleanupQueueSize());
  }
}
