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
package org.apache.beam.runners.dataflow.worker.windmill.client;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.function.Supplier;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamDebugMetricsTest {

  @Test
  public void testSummaryMetrics_noRestarts() throws InterruptedException {
    StreamDebugMetrics streamDebugMetrics = StreamDebugMetrics.forTesting(Instant::now);
    streamDebugMetrics.recordStart();
    streamDebugMetrics.recordSend();
    streamDebugMetrics.recordResponse();
    Thread.sleep(1000);
    StreamDebugMetrics.Snapshot metricsSnapshot = streamDebugMetrics.getSummaryMetrics();
    assertFalse(metricsSnapshot.shutdownTime().isPresent());
    assertTrue(metricsSnapshot.sleepLeft() <= 0);
    assertThat(metricsSnapshot.streamAge()).isGreaterThan(0);
    assertThat(metricsSnapshot.timeSinceLastSend()).isGreaterThan(0);
    assertThat(metricsSnapshot.timeSinceLastResponse()).isGreaterThan(0);
    assertFalse(metricsSnapshot.restartMetrics().isPresent());

    streamDebugMetrics.recordShutdown();
    StreamDebugMetrics.Snapshot metricsSnapshotAfterShutdown =
        streamDebugMetrics.getSummaryMetrics();
    assertTrue(metricsSnapshotAfterShutdown.shutdownTime().isPresent());
  }

  @Test
  public void testSummaryMetrics_sleep() {
    long sleepMs = 100;
    Instant aLongTimeAgo = Instant.parse("1998-09-04T00:00:00Z");
    Supplier<Instant> fakeClock = () -> aLongTimeAgo;
    StreamDebugMetrics streamDebugMetrics = StreamDebugMetrics.forTesting(fakeClock);
    streamDebugMetrics.recordSleep(sleepMs);
    StreamDebugMetrics.Snapshot metricsSnapshot = streamDebugMetrics.getSummaryMetrics();
    assertEquals(sleepMs, metricsSnapshot.sleepLeft());
  }

  @Test
  public void testSummaryMetrics_withRestarts() {
    String restartReason = "something bad happened";
    StreamDebugMetrics streamDebugMetrics = StreamDebugMetrics.forTesting(Instant::now);
    streamDebugMetrics.incrementAndGetErrors();
    streamDebugMetrics.incrementAndGetRestarts();
    streamDebugMetrics.recordRestartReason(restartReason);

    StreamDebugMetrics.Snapshot metricsSnapshot = streamDebugMetrics.getSummaryMetrics();
    assertTrue(metricsSnapshot.restartMetrics().isPresent());
    StreamDebugMetrics.RestartMetrics restartMetrics = metricsSnapshot.restartMetrics().get();
    assertThat(restartMetrics.lastRestartReason()).isEqualTo(restartReason);
    assertThat(restartMetrics.restartCount()).isEqualTo(1);
    assertThat(restartMetrics.errorCount()).isEqualTo(1);
    assertThat(restartMetrics.lastRestartTime()).isLessThan(DateTime.now());
    assertThat(restartMetrics.lastRestartTime().toInstant()).isGreaterThan(Instant.EPOCH);
  }

  @Test
  public void testResponseDebugString_neverReceivedResponse() {
    StreamDebugMetrics streamDebugMetrics = StreamDebugMetrics.forTesting(Instant::now);
    assertFalse(streamDebugMetrics.responseDebugString(Instant.now().getMillis()).isPresent());
  }

  @Test
  public void testResponseDebugString_withResponse() {
    StreamDebugMetrics streamDebugMetrics = StreamDebugMetrics.forTesting(Instant::now);
    streamDebugMetrics.recordResponse();
    assertTrue(streamDebugMetrics.responseDebugString(Instant.now().getMillis()).isPresent());
  }

  @Test
  public void testGetStartTime() {
    Instant aLongTimeAgo = Instant.parse("1998-09-04T00:00:00Z");
    Supplier<Instant> fakeClock = () -> aLongTimeAgo;
    StreamDebugMetrics streamDebugMetrics = StreamDebugMetrics.forTesting(fakeClock);
    assertEquals(0, streamDebugMetrics.getStartTimeMs());
    streamDebugMetrics.recordStart();
    assertThat(streamDebugMetrics.getStartTimeMs()).isEqualTo(aLongTimeAgo.getMillis());
  }

  @Test
  public void testGetLastSendTime() {
    Instant aLongTimeAgo = Instant.parse("1998-09-04T00:00:00Z");
    Supplier<Instant> fakeClock = () -> aLongTimeAgo;
    StreamDebugMetrics streamDebugMetrics = StreamDebugMetrics.forTesting(fakeClock);
    assertEquals(0, streamDebugMetrics.getLastSendTimeMs());
    streamDebugMetrics.recordSend();
    assertThat(streamDebugMetrics.getLastSendTimeMs()).isEqualTo(aLongTimeAgo.getMillis());
  }
}
