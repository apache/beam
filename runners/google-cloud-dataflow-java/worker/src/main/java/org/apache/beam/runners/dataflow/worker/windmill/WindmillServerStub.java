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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitStatus;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataResponse;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.net.HostAndPort;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Stub for communicating with a Windmill server. */
public abstract class WindmillServerStub implements StatusDataProvider {

  /**
   * Sets the new endpoints used to talk to windmill. Upon first call, the stubs are initialized. On
   * subsequent calls, if endpoints are different from previous values new stubs are created,
   * replacing the previous ones.
   */
  public abstract void setWindmillServiceEndpoints(Set<HostAndPort> endpoints) throws IOException;

  /** Returns true iff this WindmillServerStub is ready for making API calls. */
  public abstract boolean isReady();

  /** Get a batch of work to process. */
  public abstract Windmill.GetWorkResponse getWork(Windmill.GetWorkRequest request);

  /** Get additional data such as state needed to process work. */
  public abstract Windmill.GetDataResponse getData(Windmill.GetDataRequest request);

  /** Commit the work, issuing any output productions, state modifications etc. */
  public abstract Windmill.CommitWorkResponse commitWork(Windmill.CommitWorkRequest request);

  /** Get configuration data from the server. */
  public abstract Windmill.GetConfigResponse getConfig(Windmill.GetConfigRequest request);

  /** Report execution information to the server. */
  public abstract Windmill.ReportStatsResponse reportStats(Windmill.ReportStatsRequest request);

  /** Functional interface for receiving WorkItems. */
  @FunctionalInterface
  public interface WorkItemReceiver {
    void receiveWork(
        String computation,
        @Nullable Instant inputDataWatermark,
        Instant synchronizedProcessingTime,
        Windmill.WorkItem workItem);
  }

  /**
   * Gets work to process, returned as a stream.
   *
   * <p>Each time a WorkItem is received, it will be passed to the given receiver. The returned
   * GetWorkStream object can be used to control the lifetime of the stream.
   */
  public abstract GetWorkStream getWorkStream(
      Windmill.GetWorkRequest request, WorkItemReceiver receiver);

  /** Get additional data such as state needed to process work, returned as a stream. */
  public abstract GetDataStream getDataStream();

  /** Returns a stream allowing individual WorkItemCommitRequests to be streamed to Windmill. */
  public abstract CommitWorkStream commitWorkStream();

  /** Returns the amount of time the server has been throttled and resets the time to 0. */
  public abstract long getAndResetThrottleTime();

  @Override
  public void appendSummaryHtml(PrintWriter writer) {}

  /** Superclass for streams returned by streaming Windmill methods. */
  @ThreadSafe
  public interface WindmillStream {
    /** Indicates that no more requests will be sent. */
    void close();

    /** Waits for the server to close its end of the connection, with timeout. */
    boolean awaitTermination(int time, TimeUnit unit) throws InterruptedException;

    /** Returns when the stream was opened. */
    Instant startTime();
  }

  /** Handle representing a stream of GetWork responses. */
  @ThreadSafe
  public interface GetWorkStream extends WindmillStream {}

  /** Interface for streaming GetDataRequests to Windmill. */
  @ThreadSafe
  public interface GetDataStream extends WindmillStream {
    /** Issues a keyed GetData fetch, blocking until the result is ready. */
    KeyedGetDataResponse requestKeyedData(String computation, Windmill.KeyedGetDataRequest request);

    /** Issues a global GetData fetch, blocking until the result is ready. */
    Windmill.GlobalData requestGlobalData(Windmill.GlobalDataRequest request);

    /** Tells windmill processing is ongoing for the given keys. */
    void refreshActiveWork(Map<String, List<KeyedGetDataRequest>> active);
  }

  /** Interface for streaming CommitWorkRequests to Windmill. */
  @ThreadSafe
  public interface CommitWorkStream extends WindmillStream {
    /**
     * Commits a work item and running onDone when the commit has been processed by the server.
     * Returns true if the request was accepted. If false is returned the stream should be flushed
     * and the request recommitted.
     *
     * <p>onDone will be called with the status of the commit.
     */
    boolean commitWorkItem(
        String computation, Windmill.WorkItemCommitRequest request, Consumer<CommitStatus> onDone);

    /** Flushes any pending work items to the wire. */
    void flush();
  }

  /**
   * Pool of homogeneous streams to Windmill.
   *
   * <p>The pool holds a fixed total number of streams, and keeps each stream open for a specified
   * time to allow for better load-balancing.
   */
  @ThreadSafe
  public static class StreamPool<S extends WindmillStream> {

    private final Duration streamTimeout;

    private final class StreamData {
      final S stream = supplier.get();
      int holds = 1;
    };

    private final List<StreamData> streams;
    private final Supplier<S> supplier;
    private final HashMap<S, StreamData> holds;

    public StreamPool(int numStreams, Duration streamTimeout, Supplier<S> supplier) {
      this.streams = new ArrayList<>(numStreams);
      for (int i = 0; i < numStreams; i++) {
        streams.add(null);
      }
      this.streamTimeout = streamTimeout;
      this.supplier = supplier;
      this.holds = new HashMap<>();
    }

    // Returns a stream for use that may be cached from a previous call.  Each call of getStream
    // must be matched with a call of releaseStream.
    public S getStream() {
      int index = ThreadLocalRandom.current().nextInt(streams.size());
      S result;
      S closeStream = null;
      synchronized (this) {
        StreamData streamData = streams.get(index);
        if (streamData == null
            || streamData.stream.startTime().isBefore(Instant.now().minus(streamTimeout))) {
          if (streamData != null && --streamData.holds == 0) {
            holds.remove(streamData.stream);
            closeStream = streamData.stream;
          }
          streamData = new StreamData();
          streams.set(index, streamData);
          holds.put(streamData.stream, streamData);
        }
        streamData.holds++;
        result = streamData.stream;
      }
      if (closeStream != null) {
        closeStream.close();
      }
      return result;
    }

    // Releases a stream that was obtained with getStream.
    public void releaseStream(S stream) {
      boolean closeStream = false;
      synchronized (this) {
        if (--holds.get(stream).holds == 0) {
          closeStream = true;
          holds.remove(stream);
        }
      }
      if (closeStream) {
        stream.close();
      }
    }
  }

  /** Generic Exception type for implementors to use to represent errors while making RPCs. */
  public static class RpcException extends RuntimeException {
    public RpcException() {
      super();
    }

    public RpcException(Throwable cause) {
      super(cause);
    }

    public RpcException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
