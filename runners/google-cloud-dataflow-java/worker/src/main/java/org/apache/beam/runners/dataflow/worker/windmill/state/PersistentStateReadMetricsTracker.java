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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalData;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.HeartbeatRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.sdk.annotations.Internal;

/** Decorates calls to Streaming Engine persistence by tracking metrics on RPCs. */
@Internal
@ThreadSafe
public final class PersistentStateReadMetricsTracker {

  private final AtomicInteger activeSideInputReads;
  private final AtomicInteger activeStateReads;
  private final AtomicInteger activeHeartbeats;
  private final MemoryMonitor gcThrashingMonitor;

  public PersistentStateReadMetricsTracker(MemoryMonitor gcThrashingMonitor) {
    this.gcThrashingMonitor = gcThrashingMonitor;
    this.activeSideInputReads = new AtomicInteger();
    this.activeStateReads = new AtomicInteger();
    this.activeHeartbeats = new AtomicInteger();
  }

  public KeyedGetDataResponse getState(
      GetDataStream getDataStream, String computation, KeyedGetDataRequest request) {
    gcThrashingMonitor.waitForResources("GetStateData");
    activeStateReads.getAndIncrement();
    try {
      return getDataStream.requestKeyedData(computation, request);
    } catch (Exception e) {
      throw new PersistentStateReadException(
          "Failed to fetch state for computation: " + computation + "; request: " + request, e);
    } finally {
      activeStateReads.getAndDecrement();
    }
  }

  public GlobalData getSideInputState(GetDataStream getDataStream, GlobalDataRequest request) {
    gcThrashingMonitor.waitForResources("GetSideInputData");
    activeSideInputReads.getAndIncrement();
    try {
      return getDataStream.requestGlobalData(request);
    } catch (Exception e) {
      throw new PersistentStateReadException("Failed to get side input for request: " + request, e);
    } finally {
      activeSideInputReads.getAndDecrement();
    }
  }

  public void refreshActiveWork(
      GetDataStream getDataStream, Map<String, List<HeartbeatRequest>> heartbeats) {
    activeHeartbeats.set(heartbeats.size());
    try {
      // With streaming requests, always send the request even when it is empty, to ensure that
      // we trigger health checks for the stream even when it is idle.
      getDataStream.refreshActiveWork(heartbeats);
    } finally {
      activeHeartbeats.set(0);
    }
  }

  public void printHtml(PrintWriter writer) {
    writer.println("Active Fetches:");
    writer.println("  Side Inputs: " + activeSideInputReads.get());
    writer.println("  State Reads: " + activeStateReads.get());
    writer.println("Heartbeat Keys Active: " + activeHeartbeats.get());
  }

  private static class PersistentStateReadException extends RuntimeException {
    private PersistentStateReadException(String msg, Throwable sourceException) {
      super(msg, sourceException);
    }
  }
}
