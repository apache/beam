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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** Superclass for streams returned by streaming Windmill methods. */
@ThreadSafe
public interface WindmillStream {
  /** Indicates that no more requests will be sent. */
  void close();

  /** Waits for the server to close its end of the connection, with timeout. */
  boolean awaitTermination(int time, TimeUnit unit) throws InterruptedException;

  /** Returns when the stream was opened. */
  Instant startTime();

  /** Handle representing a stream of GetWork responses. */
  @ThreadSafe
  interface GetWorkStream extends WindmillStream {
    /** Functional interface for receiving WorkItems. */
    @FunctionalInterface
    interface WorkItemReceiver {
      void receiveWork(
          String computation,
          @Nullable Instant inputDataWatermark,
          @Nullable Instant synchronizedProcessingTime,
          Windmill.WorkItem workItem,
          Collection<Windmill.LatencyAttribution> getWorkStreamLatencies);
    }
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
    void refreshActiveWork(Map<String, List<Windmill.KeyedGetDataRequest>> active);
  }

  /** Interface for streaming CommitWorkRequests to Windmill. */
  @ThreadSafe
  interface CommitWorkStream extends WindmillStream {

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
  }

  /** Interface for streaming GetWorkerMetadata requests to Windmill. */
  @ThreadSafe
  interface GetWorkerMetadataStream extends WindmillStream {}
}
