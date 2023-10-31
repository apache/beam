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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Pool of homogeneous streams to Windmill.
 *
 * <p>The pool holds a fixed total number of streams, and keeps each stream open for a specified
 * time to allow for better load-balancing.
 */
@ThreadSafe
public class WindmillStreamPool<StreamT extends WindmillStream> {

  private final Duration streamTimeout;
  private final Supplier<StreamT> streamSupplier;

  /** @implNote Size of streams never changes once initialized. */
  private final List<@Nullable StreamData<StreamT>> streams;

  @GuardedBy("this")
  private final Map<StreamT, StreamData<StreamT>> holds;

  private WindmillStreamPool(
      Duration streamTimeout,
      Supplier<StreamT> streamSupplier,
      List<@Nullable StreamData<StreamT>> streams,
      Map<StreamT, StreamData<StreamT>> holds) {
    this.streams = streams;
    this.streamTimeout = streamTimeout;
    this.streamSupplier = streamSupplier;
    this.holds = holds;
  }

  public static <StreamT extends WindmillStream> WindmillStreamPool<StreamT> create(
      int numStreams, Duration streamTimeout, Supplier<StreamT> streamSupplier) {
    return new WindmillStreamPool<>(
        streamTimeout, streamSupplier, newStreamList(numStreams), new HashMap<>());
  }

  @VisibleForTesting
  static <StreamT extends WindmillStream> WindmillStreamPool<StreamT> forTesting(
      Duration streamTimeout,
      Supplier<StreamT> streamSupplier,
      List<@Nullable StreamData<StreamT>> streamPool,
      Map<StreamT, StreamData<StreamT>> holds) {
    return new WindmillStreamPool<>(streamTimeout, streamSupplier, streamPool, holds);
  }

  /**
   * Creates a new list of streams of the given capacity with all values initialized to null. This
   * is because we randomly load balance across all the streams in the pool.
   */
  @VisibleForTesting
  static <StreamT extends WindmillStream> List<@Nullable StreamData<StreamT>> newStreamList(
      int numStreams) {
    List<@Nullable StreamData<StreamT>> streamPool = new ArrayList<>(numStreams);
    for (int i = 0; i < numStreams; i++) {
      streamPool.add(null);
    }
    return streamPool;
  }

  /**
   * Returns a stream for use that may be cached from a previous call. Each call of getStream must
   * be matched with a call of {@link WindmillStreamPool#releaseStream(WindmillStream)}. If the
   * stream has been cached but has timed out and drained (no longer has any holds), the stream will
   * be closed.
   */
  public StreamT getStream() {
    int index = streams.size() == 1 ? 0 : ThreadLocalRandom.current().nextInt(streams.size());
    // We will return this stream
    StreamT resultStream;
    StreamT closeThisStream = null;
    try {
      synchronized (this) {
        WindmillStreamPool.StreamData<StreamT> existingStreamData = streams.get(index);
        // There are 3 possible states that can result from fetching the stream from the cache.
        if (existingStreamData == null) {
          // 1. Stream doesn't exist create and cache a new one.
          resultStream = createAndCacheStream(index).stream;
        } else if (existingStreamData.hasTimedOut(streamTimeout)) {
          // 2. The stream exists, but has timed out. The timed out stream is not returned (a new
          // one is created and returned here) and evicted from the cache if the stream has
          // completely drained. Every call to getStream(), is matched with a call to
          // releaseStream(), so the stream will eventually drain and be closed.
          if (--existingStreamData.holds == 0) {
            holds.remove(existingStreamData.stream);
            closeThisStream = existingStreamData.stream;
          }
          // Create and cache a new stream at the timed out stream's index.
          resultStream = createAndCacheStream(index).stream;
        } else {
          // 3. The stream exists and is in a valid state.
          existingStreamData.holds++;
          resultStream = existingStreamData.stream;
        }
      }
      return resultStream;
    } finally {
      if (closeThisStream != null) {
        closeThisStream.close();
      }
    }
  }

  private synchronized WindmillStreamPool.StreamData<StreamT> createAndCacheStream(int cacheKey) {
    WindmillStreamPool.StreamData<StreamT> newStreamData =
        new WindmillStreamPool.StreamData<>(streamSupplier.get());
    newStreamData.holds++;
    streams.set(cacheKey, newStreamData);
    holds.put(newStreamData.stream, newStreamData);
    return newStreamData;
  }

  /** Releases a stream that was obtained with {@link WindmillStreamPool#getStream()}. */
  public void releaseStream(StreamT stream) {
    boolean closeStream = false;
    synchronized (this) {
      StreamData<StreamT> streamData = holds.get(stream);
      // All streams that are created by an instance of a pool will be present.
      if (streamData == null) {
        throw new IllegalStateException(
            "Attempted to release stream that does not exist in this pool. This stream "
                + "may not have been created by this pool or may have been released more "
                + "times than acquired.");
      }
      if (--streamData.holds == 0) {
        closeStream = true;
        holds.remove(stream);
      }
    }

    if (closeStream) {
      stream.close();
    }
  }

  @VisibleForTesting
  static final class StreamData<StreamT extends WindmillStream> {
    final StreamT stream;
    int holds;

    @VisibleForTesting
    StreamData(StreamT stream) {
      this.stream = stream;
      holds = 1;
    }

    private boolean hasTimedOut(Duration timeout) {
      return stream.startTime().isBefore(Instant.now().minus(timeout));
    }
  }
}
