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
package org.apache.beam.runners.dataflow.worker.windmill;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WindmillStreamPoolTest {
  private static final int DEFAULT_NUM_STREAMS = 10;
  private static final int NEW_STREAM_HOLDS = 2;
  private final List<WindmillStreamPool.StreamData<TestWindmillStream>> streams =
      new ArrayList<>(DEFAULT_NUM_STREAMS);
  private final ConcurrentHashMap<
          TestWindmillStream, WindmillStreamPool.StreamData<TestWindmillStream>>
      holds = new ConcurrentHashMap<>();

  @Before
  public void setUp() {
    resetStreamList();
    holds.clear();
  }

  private void resetStreamList() {
    streams.clear();
    for (int i = 0; i < DEFAULT_NUM_STREAMS; i++) {
      streams.add(null);
    }
  }

  @Test
  public void testGetStream_returnsAndCachesNewStream() {
    Duration streamTimeout = Duration.standardSeconds(1);
    WindmillStreamPool<TestWindmillStream> streamPool =
        WindmillStreamPool.forTesting(
            streamTimeout, () -> new TestWindmillStream(Instant.now()), streams, holds);
    TestWindmillStream stream = streamPool.getStream();
    assertEquals(DEFAULT_NUM_STREAMS, streams.size());
    assertTrue(holds.containsKey(stream));
    assertEquals(2, holds.get(stream).holds);
    assertTrue(streams.contains(holds.get(stream)));
  }

  @Test
  public void testGetStream_returnsCachedStreamAndIncrementsHolds() {
    Duration streamTimeout = Duration.standardDays(1);
    int cachedStreamHolds = 2;
    // Populate the stream data.
    for (int i = 0; i < DEFAULT_NUM_STREAMS; i++) {
      WindmillStreamPool.StreamData<TestWindmillStream> streamData =
          new WindmillStreamPool.StreamData<>(new TestWindmillStream(Instant.now()));
      streamData.holds = cachedStreamHolds;
      streams.set(i, streamData);
      holds.put(streamData.stream, streamData);
    }

    WindmillStreamPool<TestWindmillStream> streamPool =
        WindmillStreamPool.forTesting(
            streamTimeout, () -> new TestWindmillStream(Instant.now()), streams, holds);
    TestWindmillStream stream = streamPool.getStream();
    assertEquals(cachedStreamHolds + 1, holds.get(stream).holds);
  }

  @Test
  public void testGetStream_returnsAndCachesNewStream_whenOldStreamInvalid() {
    Duration streamTimeout = Duration.ZERO;
    Instant expired = Instant.EPOCH;
    // Populate the stream data.
    for (int i = 0; i < DEFAULT_NUM_STREAMS; i++) {
      WindmillStreamPool.StreamData<TestWindmillStream> streamData =
          new WindmillStreamPool.StreamData<>(new TestWindmillStream(expired));
      streams.set(i, streamData);
      holds.put(streamData.stream, streamData);
    }

    WindmillStreamPool<TestWindmillStream> streamPool =
        WindmillStreamPool.forTesting(
            streamTimeout, () -> new TestWindmillStream(Instant.now()), streams, holds);
    TestWindmillStream stream = streamPool.getStream();
    assertEquals(NEW_STREAM_HOLDS, holds.get(stream).holds);
  }

  @Test
  public void testGetStream_closesInvalidStream() {
    Duration streamTimeout = Duration.ZERO;
    Instant expired = Instant.EPOCH;
    WindmillStreamPool.StreamData<TestWindmillStream> streamData =
        new WindmillStreamPool.StreamData<>(new TestWindmillStream(expired));
    List<WindmillStreamPool.StreamData<TestWindmillStream>> streams = new ArrayList<>();
    streams.add(streamData);
    holds.put(streamData.stream, streamData);

    WindmillStreamPool<TestWindmillStream> streamPool =
        WindmillStreamPool.forTesting(
            streamTimeout, () -> new TestWindmillStream(Instant.now()), streams, holds);

    TestWindmillStream stream = streamPool.getStream();
    assertEquals(NEW_STREAM_HOLDS, holds.get(stream).holds);
    assertTrue(streamData.stream.closed);
    assertEquals(1, streams.size());
    assertEquals(1, holds.size());
  }

  @Test
  public void testReleaseStream_closesStream() {
    Duration streamTimeout = Duration.standardDays(1);
    WindmillStreamPool.StreamData<TestWindmillStream> streamData =
        new WindmillStreamPool.StreamData<>(new TestWindmillStream(Instant.now()));
    List<WindmillStreamPool.StreamData<TestWindmillStream>> streams = new ArrayList<>();
    streams.add(streamData);
    holds.put(streamData.stream, streamData);

    WindmillStreamPool<TestWindmillStream> streamPool =
        WindmillStreamPool.forTesting(
            streamTimeout, () -> new TestWindmillStream(Instant.now()), streams, holds);
    TestWindmillStream stream = streamPool.getStream();
    holds.get(stream).holds = 1;
    streamPool.releaseStream(stream);
    assertFalse(holds.containsKey(stream));
    assertTrue(stream.closed);
  }

  @Test
  public void testReleaseStream_doesNotCloseStream_ifStreamHasHolds() {
    Duration streamTimeout = Duration.standardDays(1);
    WindmillStreamPool.StreamData<TestWindmillStream> streamData =
        new WindmillStreamPool.StreamData<>(new TestWindmillStream(Instant.now()));
    List<WindmillStreamPool.StreamData<TestWindmillStream>> streams = new ArrayList<>();
    streams.add(streamData);
    holds.put(streamData.stream, streamData);

    WindmillStreamPool<TestWindmillStream> streamPool =
        WindmillStreamPool.forTesting(
            streamTimeout, () -> new TestWindmillStream(Instant.now()), streams, holds);
    TestWindmillStream stream = streamPool.getStream();
    streamPool.releaseStream(stream);
    assertTrue(holds.containsKey(stream));
    assertFalse(stream.closed);
  }

  @Test
  public void testReleaseStream_throwsExceptionWhenAttemptingToReleaseUnheldStream() {
    WindmillStreamPool<TestWindmillStream> streamPool =
        WindmillStreamPool.forTesting(
            Duration.ZERO, () -> new TestWindmillStream(Instant.now()), streams, holds);
    TestWindmillStream unheldStream = new TestWindmillStream(Instant.now());
    assertThrows(IllegalStateException.class, () -> streamPool.releaseStream(unheldStream));
  }

  private static class TestWindmillStream implements WindmillServerStub.WindmillStream {
    private final Instant startTime;
    private boolean closed;

    private TestWindmillStream(Instant startTime) {
      this.startTime = startTime;
      this.closed = false;
    }

    @Override
    public void close() {
      closed = true;
    }

    @Override
    public boolean awaitTermination(int time, TimeUnit unit) {
      return false;
    }

    @Override
    public Instant startTime() {
      return startTime;
    }
  }
}
