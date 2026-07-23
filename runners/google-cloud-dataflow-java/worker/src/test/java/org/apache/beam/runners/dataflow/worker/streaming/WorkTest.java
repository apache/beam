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
package org.apache.beam.runners.dataflow.worker.streaming;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WorkTest {

  private static Work createTestWork() {
    Windmill.WorkItem workItem =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("key"))
            .setWorkToken(1L)
            .setShardingKey(2L)
            .build();
    return Work.create(
        workItem,
        workItem.getSerializedSize(),
        Watermarks.builder().setInputDataWatermark(Instant.now()).build(),
        Work.createProcessingContext(
            "comp",
            mock(
                org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient
                    .class),
            commit -> {},
            mock(HeartbeatSender.class)),
        false,
        Instant::now,
        ImmutableList.of());
  }

  @Test
  public void testSetFailedBeforeListener() {
    Work work = createTestWork();
    assertFalse(work.isFailed());

    work.setFailed();
    assertTrue(work.isFailed());

    AtomicBoolean listener = new AtomicBoolean(false);
    work.setOnFailureListener(listener);
    assertTrue(listener.get());
  }

  @Test
  public void testSetFailedAfterListener() {
    Work work = createTestWork();
    AtomicBoolean listener = new AtomicBoolean(false);
    work.setOnFailureListener(listener);
    assertFalse(listener.get());
    assertFalse(work.isFailed());

    work.setFailed();
    assertTrue(work.isFailed());
    assertTrue(listener.get());
  }

  @Test
  public void testConcurrentSetFailedAndSetOnFailureListener() throws Exception {
    int numTrials = 5000;
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int i = 0; i < numTrials; i++) {
        Work work = createTestWork();
        AtomicBoolean listener = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        Future<?> f1 =
            executor.submit(
                () -> {
                  try {
                    latch.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  work.setFailed();
                });

        Future<?> f2 =
            executor.submit(
                () -> {
                  try {
                    latch.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  work.setOnFailureListener(listener);
                });

        latch.countDown();
        f1.get(5, TimeUnit.SECONDS);
        f2.get(5, TimeUnit.SECONDS);

        assertTrue("Trial " + i + " failed: work should be failed", work.isFailed());
        assertTrue("Trial " + i + " failed: listener should be set to true", listener.get());
      }
    } finally {
      executor.shutdownNow();
    }
  }
}
