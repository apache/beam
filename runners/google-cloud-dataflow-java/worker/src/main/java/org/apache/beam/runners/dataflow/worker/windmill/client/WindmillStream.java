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

import com.google.auto.value.AutoValue;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.HeartbeatRequest;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.joda.time.Instant;

/** Superclass for streams returned by streaming Windmill methods. */
@ThreadSafe
public interface WindmillStream {
  Id id();

  /** Indicates that no more requests will be sent. */
  void close();

  /** Waits for the server to close its end of the connection, with timeout. */
  boolean awaitTermination(int time, TimeUnit unit) throws InterruptedException;

  /** Returns when the stream was opened. */
  Instant startTime();

  /** Reflects that {@link #close()} was explicitly called. */
  boolean isClosed();

  /**
   * Shutdown the stream. Logically closes the stream and terminates the stream. The stream instance
   * should not have any interactions after this point.
   */
  void shutdown();

  /** Indicates that the stream is shutdown and should not be used. */
  boolean isShutdown();

  /** Handle representing a stream of GetWork responses. */
  @ThreadSafe
  interface GetWorkStream extends WindmillStream {
    /** Adjusts the {@link GetWorkBudget} for the stream. */
    void adjustBudget(long itemsDelta, long bytesDelta);

    /** Returns the remaining in-flight {@link GetWorkBudget}. */
    GetWorkBudget remainingBudget();
  }

  /** Interface for streaming GetDataRequests to Windmill. */
  @ThreadSafe
  interface GetDataStream extends WindmillStream {
    /** Issues a keyed GetData fetch, blocking until the result is ready. */
    Windmill.KeyedGetDataResponse requestKeyedData(
        String computation, Windmill.KeyedGetDataRequest request);

    /** Issues a global GetData fetch, blocking until the result is ready. */
    Windmill.GlobalData requestGlobalData(Windmill.GlobalDataRequest request);

    /** Tells windmill processing is ongoing for the given keys. */
    void refreshActiveWork(Map<String, List<HeartbeatRequest>> heartbeats);

    void onHeartbeatResponse(List<Windmill.ComputationHeartbeatResponse> responses);
  }

  /** Interface for streaming CommitWorkRequests to Windmill. */
  @ThreadSafe
  interface CommitWorkStream extends WindmillStream {
    /**
     * Returns a builder that can be used for sending requests. Each builder is not thread-safe but
     * different builders for the same stream may be used simultaneously.
     */
    Optional<RequestBatcher> newBatcher();

    @NotThreadSafe
    interface RequestBatcher extends Closeable {
      /**
       * Commits a work item and running onDone when the commit has been processed by the server.
       * Returns true if the request was accepted. If false is returned the stream should be flushed
       * and the request recommitted.
       *
       * <p>onDone will be called with the status of the commit.
       */
      boolean commitWorkItem(
          String computation,
          Windmill.WorkItemCommitRequest request,
          Consumer<Windmill.CommitStatus> onDone);

      /** Flushes any pending work items to the wire. */
      void flush();

      @Override
      default void close() {
        flush();
      }
    }
  }

  /** Interface for streaming GetWorkerMetadata requests to Windmill. */
  @ThreadSafe
  interface GetWorkerMetadataStream extends WindmillStream {}

  @AutoValue
  abstract class Id {
    private static final String GET_WORK_STREAM_TYPE = "GetWorkStream";
    private static final String GET_DATA_STREAM_TYPE = "GetDataStream";
    private static final String GET_WORKER_METADATA_STREAM_TYPE = "GetWorkerMetadataStream";
    private static final String COMMIT_WORK_STREAM_TYPE = "CommitWorkStream";

    public static Id create(WindmillStream stream, String backendWorkerToken, boolean isDirect) {
      return new AutoValue_WindmillStream_Id(
          Id.getStreamType(stream), backendWorkerToken, isDirect);
    }

    private static String getStreamType(WindmillStream windmillStream) {
      if (windmillStream instanceof GetWorkStream) {
        return GET_WORK_STREAM_TYPE;
      } else if (windmillStream instanceof GetWorkerMetadataStream) {
        return GET_WORKER_METADATA_STREAM_TYPE;
      } else if (windmillStream instanceof GetDataStream) {
        return GET_DATA_STREAM_TYPE;
      } else if (windmillStream instanceof CommitWorkStream) {
        return COMMIT_WORK_STREAM_TYPE;
      }

      // Should not happen conditions above are exhaustive.
      throw new IllegalArgumentException("Unknown stream type.");
    }

    abstract String streamType();

    public abstract String backendWorkerToken();

    abstract boolean isDirect();

    @Override
    public final String toString() {
      String id = String.format("%s-%s", streamType(), isDirect() ? "Direct" : "Dispatched");
      return !backendWorkerToken().isEmpty()
          ? id + String.format("-[%s]", backendWorkerToken())
          : id;
    }
  }
}
