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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Pool of homogeneous streams to Windmill.
 *
 * <p>The pool holds a fixed total number of streams, and keeps each stream open for a specified
 * time to allow for better load-balancing.
 */
@ThreadSafe
public class WindmillStreamPool<StreamT extends WindmillServerStub.WindmillStream> {

  private final Duration streamTimeout;
  private final Supplier<StreamT> streamSupplier;
  private final List<WindmillStreamPool.StreamData<StreamT>> streams;
  private final Map<StreamT, StreamData<StreamT>> holds;

  private WindmillStreamPool(
      Duration streamTimeout,
      Supplier<StreamT> streamSupplier,
      List<WindmillStreamPool.StreamData<StreamT>> streams,
      ConcurrentHashMap<StreamT, StreamData<StreamT>> holds) {
    this.streams = streams;
    this.streamTimeout = streamTimeout;
    this.streamSupplier = streamSupplier;
    this.holds = holds;
  }

  public static <StreamT extends WindmillServerStub.WindmillStream>
      WindmillStreamPool<StreamT> create(
          int numStreams, Duration streamTimeout, Supplier<StreamT> streamSupplier) {
    return new WindmillStreamPool<>(
        streamTimeout, streamSupplier, emptyStreamList(numStreams), new ConcurrentHashMap<>());
  }

  /**
   * Return a list of the given capacity populated with null. This list is used to load balance
   * {@link WindmillStreamPool#getStream()} requests. It never grows in size and must be filled to
   * capacity since the random used to grab a random stream is bound by the size of the list.
   */
  @SuppressWarnings("nullness")
  private static <StreamT extends WindmillServerStub.WindmillStream>
      List<WindmillStreamPool.StreamData<StreamT>> emptyStreamList(int capacity) {
    List<WindmillStreamPool.StreamData<StreamT>> streams = new ArrayList<>(capacity);
    for (int i = 0; i < capacity; i++) {
      streams.add(null);
    }

    return streams;
  }

  @VisibleForTesting
  static <StreamT extends WindmillServerStub.WindmillStream> WindmillStreamPool<StreamT> forTesting(
      Duration streamTimeout,
      Supplier<StreamT> streamSupplier,
      List<WindmillStreamPool.StreamData<StreamT>> streams,
      ConcurrentHashMap<StreamT, StreamData<StreamT>> holds) {
    return new WindmillStreamPool<>(streamTimeout, streamSupplier, streams, holds);
  }

  /**
   * Returns a stream for use that may be cached from a previous call. Each call of getStream must
   * be matched with a call of releaseStream. If the stream has been cached but has timed out and no
   * longer has any holds, the stream will be closed.
   *
   * @implNote Streams are stored in an indexed list where the index is the key to fetch the stream.
   *     This is so that we can "randomly" fetch a stream for better load balancing.
   */
  public StreamT getStream() {
    int index = ThreadLocalRandom.current().nextInt(streams.size());
    StreamT validStream;
    StreamT closeThisStream = null;
    synchronized (this) {
      WindmillStreamPool.StreamData<StreamT> existingStreamData = streams.get(index);
      // There are 3 possible states that can result from fetching the stream from the cache.
      // 1. Stream doesn't exist create and cache a new one.
      if (existingStreamData == null) {
        validStream = createAndCacheStream(index).stream;
      }
      // 2. The stream exists, but has timed out and can be marked as closed. The timed out stream
      // is not returned (a new one is created and returned here) and evicted from the cache. Every
      // call to getStream(), is matched with a call to releaseStream(), so the stream will
      // eventually be closed there if not here.
      else if (existingStreamData.hasTimedOut(streamTimeout) && --existingStreamData.holds == 0) {
        holds.remove(existingStreamData.stream);
        // Mark the stream as to be closed outside the lock.
        closeThisStream = existingStreamData.stream;
        // Cached stream is marked as to be closed, create and cache a new stream at the closed
        // stream's index.
        validStream = createAndCacheStream(index).stream;
      } else {
        // 3. The stream exists and is in a valid state.
        existingStreamData.holds++;
        validStream = existingStreamData.stream;
      }
    }

    if (closeThisStream != null) {
      closeThisStream.close();
    }

    return validStream;
  }

  private synchronized WindmillStreamPool.StreamData<StreamT> createAndCacheStream(int cacheKey) {
    WindmillStreamPool.StreamData<StreamT> newStreamData =
        new WindmillStreamPool.StreamData<>(streamSupplier.get());
    newStreamData.holds++;
    streams.set(cacheKey, newStreamData);
    holds.put(newStreamData.stream, newStreamData);
    return newStreamData;
  }

  /** Releases a stream that was obtained with getStream. */
  public void releaseStream(StreamT stream) {
    boolean closeStream = false;
    synchronized (this) {
      StreamData<StreamT> streamData = holds.get(stream);
      // All streams that are created by an instance of a pool will be present.
      if (streamData == null) {
        throw new IllegalStateException(
            "Attempted to release stream that does not exist in this pool. Streams not created from this pool "
                + "cannot be released by it.");
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
  static final class StreamData<StreamT extends WindmillServerStub.WindmillStream> {
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
